#!/usr/bin/env bash
#
# Build libzstd 1.5.7 from source (shared lib) and install to PREFIX
# (default /usr/local). From source on macOS so it can target a cross-compile
# arch via MACOS_ARCH (e.g. "x86_64"), which brew can't do.
#
#   PREFIX=$HOME/.zstd ./scripts/install-zstd.sh
#
set -euo pipefail

ZSTD_VERSION=1.5.7
ZSTD_SHA256=eb33e51f49a15e023950cd7825ca74a4a2b43db8354825ac24fc1b7ee09e6fa3
PREFIX="${PREFIX:-/usr/local}"

# Per-OS: checksum tool, parallel jobs, cmake/ninja install, cross arch flag.
ARCH_FLAG=""
case "$(uname -s)" in
  Darwin)
    SHASUM=(shasum -a 256 -c)
    JOBS="$(sysctl -n hw.ncpu)"
    command -v brew &>/dev/null || { echo "error: homebrew not found, install cmake/ninja manually" >&2; exit 1; }
    command -v cmake &>/dev/null || brew install cmake
    command -v ninja &>/dev/null || brew install ninja
    [ -n "${MACOS_ARCH:-}" ] && ARCH_FLAG="-DCMAKE_OSX_ARCHITECTURES=$MACOS_ARCH"
    ;;
  Linux)
    SHASUM=(sha256sum -c)
    JOBS="$(nproc)"
    # sudo only when not already root (CI is non-root; a root container isn't).
    if [ "$(id -u)" -ne 0 ] && command -v sudo &>/dev/null; then SUDO=(sudo); else SUDO=(); fi
    if command -v apt-get &>/dev/null && { ! command -v cmake &>/dev/null || ! command -v ninja &>/dev/null; }; then
      "${SUDO[@]}" apt-get update -qq
      "${SUDO[@]}" apt-get install -y -qq cmake ninja-build
    fi
    ;;
  *)
    echo "error: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

curl -sSfL -o "$WORKDIR/zstd.tar.gz" \
  "https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz"
echo "${ZSTD_SHA256}  $WORKDIR/zstd.tar.gz" | "${SHASUM[@]}"
tar xzf "$WORKDIR/zstd.tar.gz" -C "$WORKDIR"

# zstd's cmake project lives under build/cmake/, not the repo root.
# shellcheck disable=SC2086
cmake -S "$WORKDIR/zstd-${ZSTD_VERSION}/build/cmake" -B "$WORKDIR/build" \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PREFIX" \
  -DZSTD_BUILD_SHARED=ON \
  -DZSTD_BUILD_STATIC=OFF \
  -DZSTD_BUILD_PROGRAMS=OFF \
  $ARCH_FLAG

ninja -C "$WORKDIR/build" -j"$JOBS"
ninja -C "$WORKDIR/build" install
