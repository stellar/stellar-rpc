#!/usr/bin/env bash
#
# Install libzstd 1.5.7 (shared) from source on Linux, or via brew on macOS.
# Used by both CI (setup-go action) and the Dockerfile.
#
# Usage:
#   ./scripts/install-zstd.sh                        # → /usr/local
#   PREFIX=$HOME/.zstd ./scripts/install-zstd.sh     # user-local (CI)
#
set -euo pipefail

ZSTD_VERSION=1.5.7
ZSTD_SHA256=eb33e51f49a15e023950cd7825ca74a4a2b43db8354825ac24fc1b7ee09e6fa3
PREFIX="${PREFIX:-/usr/local}"

case "$(uname -s)" in
  Darwin)
    if command -v brew &>/dev/null; then
      brew install zstd
    else
      echo "error: homebrew not found, install zstd manually" >&2
      exit 1
    fi
    ;;
  Linux)
    if command -v apt-get &>/dev/null; then
      if ! command -v cmake &>/dev/null || ! command -v ninja &>/dev/null; then
        sudo apt-get update -qq
        sudo apt-get install -y -qq cmake ninja-build
      fi
    fi

    WORKDIR=$(mktemp -d)
    trap 'rm -rf "$WORKDIR"' EXIT

    curl -sSfL -o "$WORKDIR/zstd.tar.gz" \
      "https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz"
    echo "${ZSTD_SHA256}  $WORKDIR/zstd.tar.gz" | sha256sum -c
    tar xzf "$WORKDIR/zstd.tar.gz" -C "$WORKDIR"

    # Cross-compilation: if CC is set, forward it to cmake.
    CMAKE_COMPILER_FLAGS=""
    if [ -n "${CC:-}" ]; then
      CXX="${CXX:-${CC/gcc/g++}}"
      CMAKE_COMPILER_FLAGS="-DCMAKE_C_COMPILER=$CC -DCMAKE_CXX_COMPILER=$CXX"
      if [[ "$CC" == *aarch64* ]]; then
        CMAKE_COMPILER_FLAGS="$CMAKE_COMPILER_FLAGS -DCMAKE_SYSTEM_NAME=Linux -DCMAKE_SYSTEM_PROCESSOR=aarch64"
      fi
    fi

    # zstd's cmake lives in build/cmake/, not the repo root.
    # shellcheck disable=SC2086
    cmake -S "$WORKDIR/zstd-${ZSTD_VERSION}/build/cmake" -B "$WORKDIR/build" \
      -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="$PREFIX" \
      -DZSTD_BUILD_SHARED=ON \
      -DZSTD_BUILD_STATIC=OFF \
      -DZSTD_BUILD_PROGRAMS=OFF \
      $CMAKE_COMPILER_FLAGS

    ninja -C "$WORKDIR/build" -j"$(nproc)"
    ninja -C "$WORKDIR/build" install
    ;;
  *)
    echo "error: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac
