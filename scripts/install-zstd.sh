#!/usr/bin/env bash
#
# Install libzstd 1.5.7 for the packfile CGo wrapper and RocksDB.
#
# Works on Linux and macOS. Both build a shared library from source using
# cmake+ninja and install headers + lib to PREFIX (default /usr/local) —
# needed on macOS (not just pinning) because a plain `brew install zstd`
# only produces a library for the host's own architecture, which breaks
# the x86_64-apple-darwin build on Apple Silicon CI runners. Set
# MACOS_ARCH (e.g. "x86_64") to cross-compile for a different target.
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
      command -v cmake &>/dev/null || brew install cmake
      command -v ninja &>/dev/null || brew install ninja
    else
      echo "error: homebrew not found, install cmake/ninja manually" >&2
      exit 1
    fi

    WORKDIR=$(mktemp -d)
    trap 'rm -rf "$WORKDIR"' EXIT

    curl -sSfL -o "$WORKDIR/zstd.tar.gz" \
      "https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz"
    echo "${ZSTD_SHA256}  $WORKDIR/zstd.tar.gz" | shasum -a 256 -c

    tar xzf "$WORKDIR/zstd.tar.gz" -C "$WORKDIR"

    # MACOS_ARCH: target architecture to build for (e.g. "x86_64" when
    # cross-compiling from an arm64 runner). Defaults to the host's own
    # architecture when unset.
    ARCH_FLAG=""
    if [ -n "${MACOS_ARCH:-}" ]; then
      ARCH_FLAG="-DCMAKE_OSX_ARCHITECTURES=$MACOS_ARCH"
    fi

    # shellcheck disable=SC2086
    cmake -S "$WORKDIR/zstd-${ZSTD_VERSION}/build/cmake" -B "$WORKDIR/build" \
      -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="$PREFIX" \
      -DZSTD_BUILD_SHARED=ON \
      -DZSTD_BUILD_STATIC=OFF \
      -DZSTD_BUILD_PROGRAMS=OFF \
      $ARCH_FLAG

    ninja -C "$WORKDIR/build" -j"$(sysctl -n hw.ncpu)"
    ninja -C "$WORKDIR/build" install
    ;;
  Linux)
    # cmake + ninja are needed to build. Install only if missing — in CI
    # they may already be present from an earlier step or the runner image.
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

    # zstd's cmake root is in build/cmake/, not the repo root.
    # ZSTD_BUILD_SHARED=ON, ZSTD_BUILD_STATIC=OFF: shared-only.
    # ZSTD_BUILD_PROGRAMS=OFF: skip the zstd CLI (not needed).
    cmake -S "$WORKDIR/zstd-${ZSTD_VERSION}/build/cmake" -B "$WORKDIR/build" \
      -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX="$PREFIX" \
      -DZSTD_BUILD_SHARED=ON \
      -DZSTD_BUILD_STATIC=OFF \
      -DZSTD_BUILD_PROGRAMS=OFF

    ninja -C "$WORKDIR/build" -j"$(nproc)"
    ninja -C "$WORKDIR/build" install
    ;;
  *)
    echo "error: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac
