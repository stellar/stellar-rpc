#!/usr/bin/env bash
#
# Install RocksDB 10.9.1 for the backfill CGo bindings (grocksdb v1.10.7).
#
# Works on Linux and macOS. Used by both CI (setup-go action) and developers.
#
# Linux: installs build deps (snappy, lz4, zstd, zlib, bz2), downloads the
#   source tarball with SHA256 verification, builds a static library, and
#   installs headers + .a to PREFIX. ~10 min from scratch, cached in CI.
#
#   Static linking (librocksdb.a) is used instead of shared (librocksdb.so)
#   because the .so has a runtime dependency on the host's glibc version.
#   CI jobs run on both ubuntu-22.04 and ubuntu-24.04 — a .so built on
#   24.04 fails on 22.04 with "undefined reference to GLIBC_2.38". A .a
#   is resolved entirely at compile time, so it works on any host.
#
# macOS: delegates to brew install rocksdb (brew handles version + deps).
#
# Usage:
#   ./scripts/install-rocksdb.sh                        # → /usr/local (needs sudo/write access)
#   PREFIX=$HOME/.rocksdb ./scripts/install-rocksdb.sh  # → ~/.rocksdb (no root, used by CI)
#
# Version mapping (grocksdb → RocksDB):
#   grocksdb v1.10.7 → RocksDB 10.9.1
#   Bump both together when upgrading.
#
set -euo pipefail

ROCKSDB_VERSION=10.9.1
# SHA256 of the GitHub-generated source tarball (not a release asset).
ROCKSDB_SHA256=e2e2e0254ddcb5338a58ba0723c90e792dbdca10aec520f7186e7b3a3e1c5223
PREFIX="${PREFIX:-/usr/local}"

case "$(uname -s)" in
  Darwin)
    if command -v brew &>/dev/null; then
      brew install rocksdb
    else
      echo "error: homebrew not found, install rocksdb manually" >&2
      exit 1
    fi
    ;;
  Linux)
    # Build deps — RocksDB links against these compression libs.
    if command -v apt-get &>/dev/null; then
      sudo apt-get update -qq
      sudo apt-get install -y -qq cmake ninja-build \
        libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev
    fi

    WORKDIR=$(mktemp -d)
    trap 'rm -rf "$WORKDIR"' EXIT

    # Download + verify source tarball.
    curl -sSfL -o "$WORKDIR/rocksdb.tar.gz" \
      "https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB_VERSION}.tar.gz"
    echo "${ROCKSDB_SHA256}  $WORKDIR/rocksdb.tar.gz" | sha256sum -c
    tar xzf "$WORKDIR/rocksdb.tar.gz" -C "$WORKDIR"

    # Build static library (.a) using cmake + ninja.
    #
    # cmake instead of make because:
    #   - cmake has WITH_BZ2=OFF (default OFF). The Makefile auto-detects
    #     bz2 via #include <bzlib.h> with no way to disable it. If libbz2-dev
    #     is pre-installed on the runner (it is), the Makefile compiles bz2
    #     support in, requiring -lbz2 at link time — which breaks ARM64
    #     cross-compilation (no ARM64 libbz2 available without multi-arch apt).
    #   - ninja is faster than make for parallel C++ compilation.
    #
    # Static (not shared) because CI runs on mixed Ubuntu versions:
    #   - unit tests: ubuntu-22.04 (glibc 2.35)
    #   - lint/build: ubuntu-24.04 (glibc 2.38)
    # A shared .so built on 24.04 fails on 22.04 with:
    #   /usr/bin/ld: librocksdb.so: undefined reference to `__isoc23_strtol@GLIBC_2.38'
    # A static .a has no runtime glibc dependency.
    #
    # Always build with zstd compression support.
    # ZSTD_HOME: if set, use that zstd install (e.g. ~/.zstd from
    # install-zstd.sh in CI). Otherwise cmake finds system libzstd
    # (from apt libzstd-dev above).
    ZSTD_PREFIX_FLAG=""
    if [ -n "${ZSTD_HOME:-}" ] && [ -d "$ZSTD_HOME" ]; then
      ZSTD_PREFIX_FLAG="-DCMAKE_PREFIX_PATH=$ZSTD_HOME"
    fi

    cmake -S "$WORKDIR/rocksdb-${ROCKSDB_VERSION}" -B "$WORKDIR/build" \
      -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="$PREFIX" \
      -DROCKSDB_BUILD_SHARED=OFF \
      -DWITH_TESTS=OFF \
      -DWITH_TOOLS=OFF \
      -DWITH_BENCHMARK_TOOLS=OFF \
      -DWITH_CORE_TOOLS=OFF \
      -DWITH_BZ2=OFF \
      -DWITH_GFLAGS=OFF \
      -DWITH_ZSTD=ON \
      -DPORTABLE=1 \
      $ZSTD_PREFIX_FLAG
    ninja -C "$WORKDIR/build" -j"$(nproc)"
    ninja -C "$WORKDIR/build" install
    ;;
  *)
    echo "error: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac
