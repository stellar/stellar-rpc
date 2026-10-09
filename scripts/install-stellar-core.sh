#!/usr/bin/env bash
#
# Install stellar-core from SDF's apt repo (stable channel), plus the
# apt.llvm.org repo for the libc++ its package links. Pass a package version
# to pin it, else the newest release is installed. Run as root.
#
#   ./scripts/install-stellar-core.sh                              # newest
#   ./scripts/install-stellar-core.sh 28.0.1-3508.947aad841.noble  # pinned
#
set -euo pipefail

# Bump when core's package moves to a newer libc++ major.
LLVM_VERSION=20
CORE_VERSION="${1:-}"

CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")

curl -fsSL https://apt.stellar.org/SDF.asc -o /etc/apt/trusted.gpg.d/SDF.asc
curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key -o /etc/apt/trusted.gpg.d/apt.llvm.org.asc
echo "deb https://apt.stellar.org $CODENAME stable" > /etc/apt/sources.list.d/SDF.list
echo "deb http://apt.llvm.org/$CODENAME/ llvm-toolchain-$CODENAME-$LLVM_VERSION main" > /etc/apt/sources.list.d/llvm.list

apt-get update -qq -o Acquire::Retries=3
# core's postinst calls adduser without depending on it (bare images lack it)
apt-get install -y -qq --no-install-recommends -o Acquire::Retries=3 \
  adduser "stellar-core${CORE_VERSION:+=$CORE_VERSION}"
stellar-core version
