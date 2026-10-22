#!/usr/bin/env bash
#
# Build libzstd 1.5.7 from source as a static archive and install it to PREFIX
# (default /usr/local). Static so the stellar-rpc binary carries it and the
# Debian package stays a single file. From source on macOS so it can target a
# cross-compile arch via MACOS_ARCH (e.g. "x86_64"), which brew can't do.
#
#   PREFIX=$HOME/.zstd ./scripts/install-zstd.sh
#
set -euo pipefail

ZSTD_VERSION=1.5.7
PREFIX="${PREFIX:-/usr/local}"

# Duplicated in the other install script on purpose: each script stands alone,
# and CI hashes each file for its cache key. Root has no sudo in the
# quickstart builder image.
apt_install() {
  if [ "$(id -u)" = 0 ]; then
    apt-get update -qq && apt-get install -y -qq "$@"
  else
    sudo apt-get update -qq && sudo apt-get install -y -qq "$@"
  fi
}

# Per-OS: parallel jobs (JOBS overrides it), cmake/ninja install, cross arch flag.
ARCH_FLAG=""
case "$(uname -s)" in
  Darwin)
    JOBS="${JOBS:-$(sysctl -n hw.ncpu)}"
    command -v brew &>/dev/null || { echo "error: homebrew not found, install cmake/ninja manually" >&2; exit 1; }
    command -v cmake &>/dev/null || brew install cmake
    command -v ninja &>/dev/null || brew install ninja
    if [ -n "${MACOS_ARCH:-}" ]; then
      ARCH_FLAG="-DCMAKE_OSX_ARCHITECTURES=$MACOS_ARCH"
    fi
    # Same floor as the Makefile's -mmacosx-version-min.
    ARCH_FLAG="$ARCH_FLAG -DCMAKE_OSX_DEPLOYMENT_TARGET=13.0"
    ;;
  Linux)
    JOBS="${JOBS:-$(nproc)}"
    # Bare-machine fallback: install only what is missing.
    pkgs=()
    if ! command -v cmake &>/dev/null || ! command -v ninja &>/dev/null; then
      pkgs+=(cmake ninja-build)
    fi
    if ! command -v curl &>/dev/null; then
      pkgs+=(curl ca-certificates)
    fi
    if command -v apt-get &>/dev/null && [ "${#pkgs[@]}" -gt 0 ]; then
      apt_install "${pkgs[@]}"
    fi
    ;;
  *)
    echo "error: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac

# A shared libzstd next to the archive would win at link time. Refuse rather
# than delete: PREFIX may belong to other tools, such as /usr/local under brew.
shared=$(ls "$PREFIX"/lib/libzstd*.so* "$PREFIX"/lib/libzstd*.dylib 2>/dev/null || true)
if [ -n "$shared" ]; then
  echo "error: shared libzstd in $PREFIX/lib; remove it or pick another PREFIX:" >&2
  echo "$shared" >&2
  exit 1
fi

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

curl -sSfL -o "$WORKDIR/zstd.tar.gz" \
  "https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz"
tar xzf "$WORKDIR/zstd.tar.gz" -C "$WORKDIR"

# zstd's cmake project lives under build/cmake/, not the repo root.
# shellcheck disable=SC2086
cmake -S "$WORKDIR/zstd-${ZSTD_VERSION}/build/cmake" -B "$WORKDIR/build" \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PREFIX" \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DZSTD_BUILD_SHARED=OFF \
  -DZSTD_BUILD_STATIC=ON \
  -DZSTD_BUILD_PROGRAMS=OFF \
  $ARCH_FLAG

ninja -C "$WORKDIR/build" -j"$JOBS"
ninja -C "$WORKDIR/build" install
