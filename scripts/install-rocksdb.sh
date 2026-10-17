#!/usr/bin/env bash
#
# Build RocksDB 10.9.1 from source as a static archive for the grocksdb
# v1.10.7 CGo bindings and install it to PREFIX (default /usr/local). Static
# so the stellar-rpc binary carries it and the Debian package stays a single
# file. Pinned from source because brew/apt track a newer major than grocksdb
# v1.10.7 supports, and because from source it can target a cross-compile arch
# via MACOS_ARCH. Bump ROCKSDB_VERSION and grocksdb together.
#
#   PREFIX=$HOME/.rocksdb ./scripts/install-rocksdb.sh   # lib only (CI, dev)
#   WITH_TOOLS=1 ./scripts/install-rocksdb.sh            # + ldb/sst_dump/db_bench (Docker)
#
set -euo pipefail

ROCKSDB_VERSION=10.9.1
PREFIX="${PREFIX:-/usr/local}"
WITH_TOOLS="${WITH_TOOLS:-0}"

# apt-get as root where there is no sudo (the quickstart builder image), with
# sudo everywhere else.
apt_install() {
  if [ "$(id -u)" = 0 ]; then
    apt-get update -qq && apt-get install -y -qq "$@"
  else
    sudo apt-get update -qq && sudo apt-get install -y -qq "$@"
  fi
}

# Per-OS: parallel jobs, build deps, cross arch flag.
ARCH_FLAG=""
case "$(uname -s)" in
  Darwin)
    JOBS="$(sysctl -n hw.ncpu)"
    command -v brew &>/dev/null || { echo "error: homebrew not found, install cmake/ninja manually" >&2; exit 1; }
    command -v cmake &>/dev/null || brew install cmake
    command -v ninja &>/dev/null || brew install ninja
    if [ -n "${MACOS_ARCH:-}" ]; then
      ARCH_FLAG="-DCMAKE_OSX_ARCHITECTURES=$MACOS_ARCH"
    fi
    # Same floor as the Makefile's -mmacosx-version-min, so the archive and
    # the Go link agree on the deployment target.
    ARCH_FLAG="$ARCH_FLAG -DCMAKE_OSX_DEPLOYMENT_TARGET=13.0"
    ;;
  Linux)
    JOBS="$(nproc)"
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
      apt_install "${pkgs[@]}"
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
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DROCKSDB_BUILD_SHARED=OFF \
  -DWITH_TESTS=OFF \
  -DWITH_BZ2=OFF \
  -DWITH_GFLAGS=OFF \
  -DWITH_ZSTD=ON \
  -DPORTABLE=1 \
  "${TOOLS_FLAGS[@]}" \
  $ZSTD_PREFIX_FLAG \
  $ARCH_FLAG

ninja -C "$WORKDIR/build" -j"$JOBS"
# Installs librocksdb.a and the headers. The tools are not part of the install
# target, so copy them out by hand.
ninja -C "$WORKDIR/build" install
if [ "$WITH_TOOLS" = 1 ]; then
  mkdir -p "$PREFIX/bin"
  cp "$WORKDIR/build/tools/ldb" "$WORKDIR/build/tools/sst_dump" "$PREFIX/bin/"
  cp "$WORKDIR/build/db_bench" "$PREFIX/bin/"
fi

# A shared library left by an older install would win over the archive at
# link time, and the binary would load it at run time.
rm -f "$PREFIX"/lib/librocksdb*.so* "$PREFIX"/lib/librocksdb*.dylib
