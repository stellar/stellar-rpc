#!/usr/bin/env bash
#
# Fail if a stellar-rpc binary loads librocksdb or libzstd at run time. Both
# are linked statically so the Debian package stays a single binary.
#
#   ./scripts/check-link.sh stellar-rpc
#
set -euo pipefail

BINARY="${1:?usage: $0 BINARY}"

case "$(uname -s)" in
  Linux)  deps="$(ldd "$BINARY")" ;;
  Darwin) deps="$(otool -L "$BINARY" | tail -n +2)" ;;
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
