#!/usr/bin/env bash
#
# Install RocksDB 10.9.1 for the backfill CGo bindings (grocksdb v1.10.7).
#
# Works on Linux and macOS. Used by both CI (setup-go action) and developers.
#
# Linux: installs build deps (snappy, lz4, zstd, zlib), downloads the source
#   tarball with SHA256 verification, builds a shared library (.so), and
#   installs headers + lib to PREFIX. ~5 min from scratch, cached in CI.
#
#   cmake is used (not make) because:
#     - cmake has WITH_BZ2=OFF (default OFF). The Makefile auto-detects bz2
#       via #include <bzlib.h> with no way to disable it.
#     - ninja is faster than make for parallel C++ compilation.
#
# macOS: delegates to brew install rocksdb (brew handles version + deps).
#
# Usage:
#   ./scripts/install-rocksdb.sh                        # → /usr/local
#   PREFIX=$HOME/.rocksdb ./scripts/install-rocksdb.sh  # → ~/.rocksdb (CI)
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

    # ZSTD_HOME: if set, use that zstd install (e.g. ~/.zstd from
    # install-zstd.sh in CI). Otherwise cmake finds system libzstd
    # (from apt libzstd-dev above).
    ZSTD_PREFIX_FLAG=""
    if [ -n "${ZSTD_HOME:-}" ] && [ -d "$ZSTD_HOME" ]; then
      ZSTD_PREFIX_FLAG="-DCMAKE_PREFIX_PATH=$ZSTD_HOME"
    fi

    # Build shared library (.so).
    #
    # Shared (not static) because all CI runners and Docker stages use
    # ubuntu:24.04 — same glibc version everywhere. LD_LIBRARY_PATH is
    # set in the CI action to find the .so at runtime.
    #
    # shellcheck disable=SC2086
    cmake -S "$WORKDIR/rocksdb-${ROCKSDB_VERSION}" -B "$WORKDIR/build" \
      -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="$PREFIX" \
      -DROCKSDB_BUILD_SHARED=ON \
      -DWITH_TESTS=OFF \
      -DWITH_TOOLS=OFF \
      -DWITH_BENCHMARK_TOOLS=OFF \
      -DWITH_CORE_TOOLS=OFF \
      -DWITH_BZ2=OFF \
      -DWITH_GFLAGS=OFF \
      -DWITH_ZSTD=ON \
      -DPORTABLE=1 \
      $ZSTD_PREFIX_FLAG

    # Build only the shared target. RocksDB's cmake unconditionally adds
    # a static target (no option to disable it). Building all targets
    # compiles every source file twice (~355 × 2). Targeting rocksdb-shared
    # explicitly halves the build.
    ninja -C "$WORKDIR/build" -j"$(nproc)" rocksdb-shared

    # Manual install — 'ninja install' requires the static lib which we
    # didn't build. Copy shared lib + headers directly.
    mkdir -p "$PREFIX/lib" "$PREFIX/include"
    cp -a "$WORKDIR/build"/librocksdb.so* "$PREFIX/lib/"
    cp -r "$WORKDIR/rocksdb-${ROCKSDB_VERSION}/include/rocksdb" "$PREFIX/include/"
    ;;
  *)
    echo "error: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac
