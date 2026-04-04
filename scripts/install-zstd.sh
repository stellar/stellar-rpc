#!/usr/bin/env bash
#
# Install libzstd 1.5.7 for the packfile CGo wrapper and RocksDB.
# Works on Linux and macOS. On Linux, builds from source and installs
# headers + shared library to PREFIX (default /usr/local).
#
# Usage:
#   ./scripts/install-zstd.sh                                  # install to /usr/local (needs write access)
#   PREFIX=$HOME/.zstd ./scripts/install-zstd.sh                 # user-local install (no root needed)
#   CC=aarch64-linux-gnu-gcc-10 PREFIX=$HOME/.zstd/aarch64 ./scripts/install-zstd.sh   # cross-compile
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
    WORKDIR=$(mktemp -d)
    trap 'rm -rf "$WORKDIR"' EXIT

    curl -sSfL -o "$WORKDIR/zstd.tar.gz" \
      "https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz"
    echo "${ZSTD_SHA256}  $WORKDIR/zstd.tar.gz" | sha256sum -c

    tar xzf "$WORKDIR/zstd.tar.gz" -C "$WORKDIR"
    make -j"$(nproc)" -C "$WORKDIR/zstd-${ZSTD_VERSION}" lib-release
    # Shared only — no .a. Both the Go binary (-lzstd from zstd.go) and
    # RocksDB (.so) link dynamically against libzstd.so.
    make -C "$WORKDIR/zstd-${ZSTD_VERSION}/lib" install-shared install-includes install-pc PREFIX="$PREFIX"
    ;;
  *)
    echo "error: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac
