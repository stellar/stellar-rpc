#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CARGO_TOML="${ROOT_DIR}/Cargo.toml"
CARGO_LOCK="${ROOT_DIR}/Cargo.lock"
SOROBAN_ENV_GIT_URL="${SOROBAN_ENV_GIT_URL:-https://github.com/stellar/rs-soroban-env}"
SOROBAN_ENV_GIT_BRANCH="${SOROBAN_ENV_GIT_BRANCH:-main}"

if ! command -v tomlq >/dev/null 2>&1; then
  echo "tomlq is required but was not found in PATH" >&2
  exit 1
fi

cd "$ROOT_DIR"

SOROBAN_ENV_GIT_REVISION="${SOROBAN_ENV_GIT_REVISION:-$(git ls-remote "$SOROBAN_ENV_GIT_URL" "refs/heads/${SOROBAN_ENV_GIT_BRANCH}" | cut -f1)}"

# Export everything to env so tomlq sees it.
export SOROBAN_ENV_GIT_URL
export SOROBAN_ENV_GIT_BRANCH
export SOROBAN_ENV_GIT_REVISION

if [[ -z "$SOROBAN_ENV_GIT_REVISION" ]]; then
  echo "failed to resolve soroban-env git revision from ${SOROBAN_ENV_GIT_URL} ${SOROBAN_ENV_GIT_BRANCH}" >&2
  exit 1
fi

tomlq -t '
  .workspace.dependencies["soroban-env-host-curr"] = {
    "package": "soroban-env-host",
    "git": env.SOROBAN_ENV_GIT_URL,
    "rev": env.SOROBAN_ENV_GIT_REVISION
  }
  | .workspace.dependencies["soroban-simulation-curr"] = {
    "package": "soroban-simulation",
    "git": env.SOROBAN_ENV_GIT_URL,
    "rev": env.SOROBAN_ENV_GIT_REVISION,
    "features": ["unstable-next-api"]
  }
' "$CARGO_TOML" > "${CARGO_TOML}.updated"
mv "${CARGO_TOML}.updated" "$CARGO_TOML"

# Select the highest locked version so cargo update targets the current "-curr" lane.
target_host_version="$(tomlq -r '.package[] | select(.name == "soroban-env-host") | .version' "$CARGO_LOCK" | sort -V | tail -n1)"
target_simulation_version="$(tomlq -r '.package[] | select(.name == "soroban-simulation") | .version' "$CARGO_LOCK" | sort -V | tail -n1)"

if [[ -z "$target_host_version" || -z "$target_simulation_version" || "$target_host_version" == "null" || "$target_simulation_version" == "null" ]]; then
  echo "failed to resolve soroban-env package IDs from Cargo.lock" >&2
  exit 1
fi

cargo update \
  -p "soroban-env-host@${target_host_version}" \
  -p "soroban-simulation@${target_simulation_version}"

resolved_host_version="$(tomlq -r '.package[] | select(.name == "soroban-env-host") | .version' "$CARGO_LOCK" | sort -V | tail -n1)"
resolved_simulation_version="$(tomlq -r '.package[] | select(.name == "soroban-simulation") | .version' "$CARGO_LOCK" | sort -V | tail -n1)"

if [[ -z "$resolved_host_version" || -z "$resolved_simulation_version" || "$resolved_host_version" == "null" || "$resolved_simulation_version" == "null" ]]; then
  echo "failed to resolve updated soroban-env versions from Cargo.lock" >&2
  exit 1
fi

export SOROBAN_ENV_HOST_CURR_VERSION="=${resolved_host_version}"
export SOROBAN_SIMULATION_CURR_VERSION="=${resolved_simulation_version}"

tomlq -t '
  .workspace.dependencies["soroban-env-host-curr"].version = env.SOROBAN_ENV_HOST_CURR_VERSION
  | .workspace.dependencies["soroban-simulation-curr"].version = env.SOROBAN_SIMULATION_CURR_VERSION
' "$CARGO_TOML" > "${CARGO_TOML}.updated"
mv "${CARGO_TOML}.updated" "$CARGO_TOML"
