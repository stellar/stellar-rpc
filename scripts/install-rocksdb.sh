#!/usr/bin/env bash
#
# Build RocksDB 10.9.1 from source (shared lib) for the grocksdb v1.10.7 CGo
# bindings and install to PREFIX (default /usr/local). Pinned from source
# because brew/apt track a newer major than grocksdb v1.10.7 supports, and
# because from source it can target a cross-compile arch via MACOS_ARCH.
# Bump ROCKSDB_VERSION and grocksdb together.
#
#   PREFIX=$HOME/.rocksdb ./scripts/install-rocksdb.sh   # lib only (CI, dev)
#   WITH_TOOLS=1 ./scripts/install-rocksdb.sh            # + ldb/sst_dump/db_bench (Docker)
#
set -euo pipefail

ROCKSDB_VERSION=10.9.1
PREFIX="${PREFIX:-/usr/local}"
WITH_TOOLS="${WITH_TOOLS:-0}"

# Per-OS: parallel jobs, build deps, installed lib glob, cross arch flag.
ARCH_FLAG=""
case "$(uname -s)" in
  Darwin)
    JOBS="$(sysctl -n hw.ncpu)"
    LIB_GLOB='librocksdb*.dylib'
    command -v brew &>/dev/null || { echo "error: homebrew not found, install cmake/ninja manually" >&2; exit 1; }
    command -v cmake &>/dev/null || brew install cmake
    command -v ninja &>/dev/null || brew install ninja
    if [ -n "${MACOS_ARCH:-}" ]; then
      ARCH_FLAG="-DCMAKE_OSX_ARCHITECTURES=$MACOS_ARCH"
    fi
    ;;
  Linux)
    JOBS="$(nproc)"
    LIB_GLOB='librocksdb.so*'
    # Bare-machine fallback: install only what's missing. CI runners and the
    # Docker image already ship cmake/ninja, and ZSTD_HOME supplies zstd, so
    # this is skipped there.
    pkgs=()
    if ! command -v cmake &>/dev/null || ! command -v ninja &>/dev/null; then
      pkgs+=(cmake ninja-build)
    fi
    if [ -z "${ZSTD_HOME:-}" ]; then
      pkgs+=(libzstd-dev)
    fi
    if command -v apt-get &>/dev/null && [ "${#pkgs[@]}" -gt 0 ]; then
      sudo apt-get update -qq
      sudo apt-get install -y -qq "${pkgs[@]}"
    fi
    ;;
  *)
    echo "error: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

curl -sSfL -o "$WORKDIR/rocksdb.tar.gz" \
  "https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB_VERSION}.tar.gz"
tar xzf "$WORKDIR/rocksdb.tar.gz" -C "$WORKDIR"

# Build against ZSTD_HOME if given (e.g. ~/.zstd in CI), else a system libzstd.
ZSTD_PREFIX_FLAG=""
if [ -n "${ZSTD_HOME:-}" ] && [ -d "$ZSTD_HOME" ]; then
  ZSTD_PREFIX_FLAG="-DCMAKE_PREFIX_PATH=$ZSTD_HOME"
fi

if [ "$WITH_TOOLS" = 1 ]; then
  TOOLS_FLAGS=(-DWITH_TOOLS=ON -DWITH_CORE_TOOLS=ON -DWITH_BENCHMARK_TOOLS=ON)
else
  TOOLS_FLAGS=(-DWITH_TOOLS=OFF -DWITH_CORE_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF)
fi

# zstd is the only codec (WITH_ZSTD=ON); snappy/lz4/zlib default OFF, so the
# built lib needs only libzstd. cmake (not make): make force-detects bz2 with
# no opt-out. See the Makefile's grocksdb_clean_link tag for the link side.
# shellcheck disable=SC2086
cmake -S "$WORKDIR/rocksdb-${ROCKSDB_VERSION}" -B "$WORKDIR/build" \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PREFIX" \
  -DROCKSDB_BUILD_SHARED=ON \
  -DWITH_TESTS=OFF \
  -DWITH_BZ2=OFF \
  -DWITH_GFLAGS=OFF \
  -DWITH_ZSTD=ON \
  -DPORTABLE=1 \
  "${TOOLS_FLAGS[@]}" \
  $ZSTD_PREFIX_FLAG \
  $ARCH_FLAG

if [ "$WITH_TOOLS" = 1 ]; then
  ninja -C "$WORKDIR/build" -j"$JOBS"
  ninja -C "$WORKDIR/build" install
  # cmake's install target doesn't place the tool binaries; copy them out.
  mkdir -p "$PREFIX/bin"
  cp "$WORKDIR/build/tools/ldb" "$WORKDIR/build/tools/sst_dump" "$PREFIX/bin/"
  cp "$WORKDIR/build/db_bench" "$PREFIX/bin/"
else
  # Shared target only (halves the build vs cmake's always-on static target).
  # Install by hand since `ninja install` would pull in that static lib.
  ninja -C "$WORKDIR/build" -j"$JOBS" rocksdb-shared
  mkdir -p "$PREFIX/lib" "$PREFIX/include"
  # shellcheck disable=SC2086
  cp -a "$WORKDIR/build"/$LIB_GLOB "$PREFIX/lib/"
  cp -r "$WORKDIR/rocksdb-${ROCKSDB_VERSION}/include/rocksdb" "$PREFIX/include/"
fi
