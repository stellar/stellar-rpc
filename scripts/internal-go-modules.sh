#!/usr/bin/env bash
# Lets Go fetch github.com/stellar/go-stellar-sdk versions that aren't public
# yet. Go asks the module proxy first and falls back to git for versions the
# proxy doesn't have, so this points that git fetch at
# stellar/go-stellar-sdk-internal, with a token that can read it. It also sets
# GONOSUMDB for the SDK, because Google's checksum database doesn't know those
# versions; go.sum still checks every download. Both SDK repos have the same
# commits and tags, so go.mod and go.sum stay as they are in public.
#
#   internal-go-modules.sh github-actions      for the rest of a GitHub Actions job,
#                                              with INTERNAL_GO_MODULES_TOKEN
#   internal-go-modules.sh run <command>...    for one command, for example
#                                              go get github.com/stellar/go-stellar-sdk@v0.8.1
#                                              (the token defaults to `gh auth token`)
set -euo pipefail

module=github.com/stellar/go-stellar-sdk
public=https://$module
internal=https://github.com/stellar/go-stellar-sdk-internal

# write_gitconfig <file> <token>
write_gitconfig() {
  local auth
  auth=$(printf 'x-access-token:%s' "$2" | base64 | tr -d '\n')
  # insteadOf matches by prefix, so the internal repo's own URL maps to itself
  # instead of becoming .../go-stellar-sdk-internal-internal.
  (
    umask 077
    cat > "$1" << EOF
[url "$internal"]
	insteadOf = $public
	insteadOf = $internal
[http "$internal"]
	extraheader = AUTHORIZATION: basic $auth
EOF
  )
}

usage() {
  echo "usage: $0 github-actions | run <command>..." >&2
  exit 2
}

case "${1:-}" in
  github-actions)
    file=$RUNNER_TEMP/internal-go-modules.gitconfig
    write_gitconfig "$file" "${INTERNAL_GO_MODULES_TOKEN:?}"
    git config --global --add include.path "$file"
    echo "GONOSUMDB=${GONOSUMDB:+$GONOSUMDB,}$module" >> "$GITHUB_ENV"
    echo "gitconfig=$file" >> "$GITHUB_OUTPUT"
    ;;
  run)
    shift
    [ $# -gt 0 ] || usage
    if ! GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=probe.ok GIT_CONFIG_VALUE_0=1 git config probe.ok > /dev/null; then
      echo "$0 run needs git 2.31 or newer, and $(git version) is first on PATH." >&2
      exit 1
    fi
    file=$(mktemp)
    trap 'rm -f "$file"' EXIT
    write_gitconfig "$file" "${INTERNAL_GO_MODULES_TOKEN:-$(gh auth token)}"
    nosumdb=$(go env GONOSUMDB)
    n=${GIT_CONFIG_COUNT:-0}
    env GONOSUMDB="${nosumdb:+$nosumdb,}$module" GIT_CONFIG_COUNT=$((n + 1)) \
      "GIT_CONFIG_KEY_$n=include.path" "GIT_CONFIG_VALUE_$n=$file" "$@"
    ;;
  *)
    usage
    ;;
esac
