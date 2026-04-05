#!/usr/bin/env bash
#
# Install RocksDB 10.9.1 (shared) from source on Linux, or via brew on macOS.
# Used by both CI (setup-go action) and developers.
#
# grocksdb v1.10.7 → RocksDB 10.9.1. Bump both together when upgrading.
#
# Usage:
#   ./scripts/install-rocksdb.sh                        # → /usr/local
#   PREFIX=$HOME/.rocksdb ./scripts/install-rocksdb.sh  # → ~/.rocksdb (CI)
#
set -euo pipefail

ROCKSDB_VERSION=10.9.1
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
    if command -v apt-get &>/dev/null; then
      sudo apt-get update -qq
      sudo apt-get install -y -qq cmake ninja-build \
        libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev
    fi

    WORKDIR=$(mktemp -d)
    trap 'rm -rf "$WORKDIR"' EXIT

    curl -sSfL -o "$WORKDIR/rocksdb.tar.gz" \
      "https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB_VERSION}.tar.gz"
    echo "${ROCKSDB_SHA256}  $WORKDIR/rocksdb.tar.gz" | sha256sum -c
    tar xzf "$WORKDIR/rocksdb.tar.gz" -C "$WORKDIR"

    # Cross-compilation: if CC is set, forward it to cmake.
    CMAKE_COMPILER_FLAGS=""
    if [ -n "${CC:-}" ]; then
      CXX="${CXX:-${CC/gcc/g++}}"
      CMAKE_COMPILER_FLAGS="-DCMAKE_C_COMPILER=$CC -DCMAKE_CXX_COMPILER=$CXX"
      if [[ "$CC" == *aarch64* ]]; then
        FIND_ROOT="/usr"
        if [ -n "${ZSTD_HOME:-}" ] && [ -d "$ZSTD_HOME" ]; then
          FIND_ROOT="$FIND_ROOT;$ZSTD_HOME"
        fi
        CMAKE_COMPILER_FLAGS="$CMAKE_COMPILER_FLAGS -DCMAKE_SYSTEM_NAME=Linux -DCMAKE_SYSTEM_PROCESSOR=aarch64"
        CMAKE_COMPILER_FLAGS="$CMAKE_COMPILER_FLAGS -DCMAKE_FIND_ROOT_PATH=$FIND_ROOT"
      fi
    fi

    # Use custom zstd if ZSTD_HOME is set (e.g. ~/.zstd in CI).
    ZSTD_PREFIX_FLAG=""
    if [ -n "${ZSTD_HOME:-}" ] && [ -d "$ZSTD_HOME" ]; then
      ZSTD_PREFIX_FLAG="-DCMAKE_PREFIX_PATH=$ZSTD_HOME"
    fi

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
      $ZSTD_PREFIX_FLAG \
      $CMAKE_COMPILER_FLAGS

    # Build only the shared target — RocksDB's cmake always builds static
    # too, and there's no option to disable it. This halves build time.
    ninja -C "$WORKDIR/build" -j"$(nproc)" rocksdb-shared

    # Manual install since 'ninja install' needs the static lib we skipped.
    mkdir -p "$PREFIX/lib" "$PREFIX/include"
    cp -a "$WORKDIR/build"/librocksdb.so* "$PREFIX/lib/"
    cp -r "$WORKDIR/rocksdb-${ROCKSDB_VERSION}/include/rocksdb" "$PREFIX/include/"
    ;;
  *)
    echo "error: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac
