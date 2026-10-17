#!/usr/bin/env bash
#
# Fail if a stellar-rpc binary loads librocksdb or libzstd at run time, or on
# Linux if it needs a glibc newer than MAX_GLIBC. Both archives are linked
# statically so the Debian package stays a single binary, and the binary is
# built on Ubuntu 22.04 (glibc 2.35) so that one package runs on jammy and
# noble.
#
#   ./scripts/check-link.sh stellar-rpc
#
set -euo pipefail

BINARY="${1:?usage: $0 BINARY}"
MAX_GLIBC="${MAX_GLIBC:-2.35}"

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

if [ "$(uname -s)" = Linux ]; then
  needed="$(objdump -T "$BINARY" | grep -oE 'GLIBC_[0-9]+\.[0-9]+' | sort -uV | tail -1)"
  needed="${needed#GLIBC_}"
  newest="$(printf '%s\n%s\n' "$needed" "$MAX_GLIBC" | sort -V | tail -1)"
  if [ "$newest" != "$MAX_GLIBC" ]; then
    echo "error: $BINARY needs glibc $needed, newer than $MAX_GLIBC" >&2
    exit 1
  fi
  echo "glibc needed: $needed (ceiling $MAX_GLIBC)"
fi
