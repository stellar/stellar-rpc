#!/usr/bin/env bash
#
# Install libzstd 1.5.7 for the packfile CGo wrapper and RocksDB.
# Works on Linux and macOS. On Linux, builds a shared library (.so)
# from source using cmake+ninja and installs headers + lib to PREFIX
# (default /usr/local).
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
