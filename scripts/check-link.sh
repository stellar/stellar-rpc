#!/usr/bin/env bash
#
# Fail if a stellar-rpc binary loads librocksdb or libzstd at run time. Both
# archives are linked statically so the Debian package stays a single binary.
# With MAX_GLIBC set, also fail on Linux if the binary needs a newer glibc.
# The Dockerfile's build stage sets 2.35: built on Ubuntu 22.04, one package
# runs on jammy and noble.
#
#   ./scripts/check-link.sh stellar-rpc
#   MAX_GLIBC=2.35 ./scripts/check-link.sh stellar-rpc
#
set -euo pipefail

BINARY="${1:?usage: $0 BINARY}"
MAX_GLIBC="${MAX_GLIBC:-}"

case "$(uname -s)" in
  Linux)  deps="$(ldd "$BINARY")" ;;
  Darwin) deps="$(otool -L "$BINARY")" ;;
  *)
    echo "error: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac
echo "$deps"

if grep -qE 'librocksdb|libzstd' <<<"$deps"; then
  echo "error: $BINARY loads librocksdb or libzstd dynamically" >&2
  exit 1
fi

if [ "$(uname -s)" = Linux ] && [ -n "$MAX_GLIBC" ]; then
  # Versions have two or three parts: GLIBC_2.2.5 on amd64, GLIBC_2.17 on arm64.
  needed="$(objdump -T "$BINARY" | { grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' || true; } | sort -uV | tail -1)"
  needed="${needed#GLIBC_}"
  if [ -z "$needed" ]; then
    echo "error: no GLIBC_ symbol version found in $BINARY" >&2
    exit 1
  fi
  # sort -C exits 1 when the two lines are out of order, i.e. needed > MAX.
  if ! printf '%s\n%s\n' "$needed" "$MAX_GLIBC" | sort -VC; then
    echo "error: $BINARY needs glibc $needed, newer than $MAX_GLIBC" >&2
    exit 1
  fi
  echo "glibc needed: $needed (ceiling $MAX_GLIBC)"
fi
