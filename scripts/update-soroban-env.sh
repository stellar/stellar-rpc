#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CARGO_TOML="${ROOT_DIR}/Cargo.toml"
CARGO_LOCK="${ROOT_DIR}/Cargo.lock"
SOROBAN_ENV_GIT_URL="${SOROBAN_ENV_GIT_URL:-https://github.com/stellar/rs-soroban-env}"

if ! command -v tomlq >/dev/null 2>&1; then
  echo "tomlq is required but was not found in PATH" >&2
  exit 1
fi

semver_sort='sort_by(capture("(?<major>\\d+)\\.(?<minor>\\d+)\\.(?<patch>\\d+)") | [.major, .minor, .patch] | map(tonumber)) | last'

cd "$ROOT_DIR"

tomlq -t '
  .workspace.dependencies["soroban-env-host-curr"] = {
    "package": "soroban-env-host",
    "git": env.SOROBAN_ENV_GIT_URL,
    "branch": "main"
  }
  | .workspace.dependencies["soroban-simulation-curr"] = {
    "package": "soroban-simulation",
    "git": env.SOROBAN_ENV_GIT_URL,
    "branch": "main"
  }
' "$CARGO_TOML" > "${CARGO_TOML}.updated"
mv "${CARGO_TOML}.updated" "$CARGO_TOML"

host_version="$(tomlq -r ".package | map(select(.name == \"soroban-env-host\") | .version) | ${semver_sort}" "$CARGO_LOCK")"
simulation_version="$(tomlq -r ".package | map(select(.name == \"soroban-simulation\") | .version) | ${semver_sort}" "$CARGO_LOCK")"

if [[ -z "$host_version" || -z "$simulation_version" || "$host_version" == "null" || "$simulation_version" == "null" ]]; then
  echo "failed to resolve soroban-env package IDs from Cargo.lock" >&2
  exit 1
fi

cargo update \
  -p "soroban-env-host@${host_version}" \
  -p "soroban-simulation@${simulation_version}"
