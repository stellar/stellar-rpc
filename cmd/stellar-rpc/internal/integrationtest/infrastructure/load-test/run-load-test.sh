#!/usr/bin/env bash
# Bootstraps the ephemeral load-test box (EC2 user-data): installs the toolchain,
# checks out TARGET_SHA, then hands off to `runner instantiate`, which streams the
# corpus from S3 and runs the ingest benchmark. The other half, `runner
# orchestrate`, polls for results over SSM from the GHA runner.
#
# Marker protocol shared with the runner half:
#   /tmp/download-complete  instance: corpus fetched; benchmark running
#   /tmp/results.md         instance: result body (presentation only)
#   /tmp/done               instance: machine-readable verdict ("ok"/"fail")

set -euo pipefail
log() { echo "[$(date -u +%FT%TZ)] $*"; }

exec > >(tee -a /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

TARGET_SHA="${TARGET_SHA:-}"
RUN_ID="${RUN_ID:-manual}"
REPO="${REPO:-stellar/stellar-rpc}"
WORK_DIR="${WORK_DIR:-/data}"
RESULTS_FILE="${RESULTS_FILE:-/tmp/results.md}"
DEFAULT_BRANCH=apply-load

# bail writes a failure verdict the runner half can read, then stops. It guards
# only the pre-Go bootstrap phase; once the Go runner starts it owns the verdict.
bail() {
  log "FATAL: $*"
  { printf '❌ **Ingest load test failed** (run %s on `%s`)\n\n```\n' "$RUN_ID" "$TARGET_SHA"
    printf '%s\n' "$*"
    printf '```\n'; } > "$RESULTS_FILE"
  echo fail > /tmp/done
  exit 1
}
trap 'bail "unhandled error at line $LINENO while running: $BASH_COMMAND"' ERR

log "clearing stale run state"
rm -f /tmp/done /tmp/download-complete \
      /tmp/bench-results.json /tmp/load-test-ledgers-*.xdr.zstd \
      "$RESULTS_FILE"
rm -rf "$WORK_DIR/stellar-rpc"

log "installing deps"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
# jq is required by `make build-libs` (compile-time version stamping), not just tooling.
apt-get install -y -qq --no-install-recommends \
  curl git jq build-essential ca-certificates \
  libpq5 libsodium23 libunwind8 libc++1-14

GO_VERSION=1.25.11
curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" | tar -xz -C /usr/local
export HOME="${HOME:-/root}"
export GOPATH="${GOPATH:-$HOME/go}"
export GOMODCACHE="${GOMODCACHE:-$GOPATH/pkg/mod}"
export GOCACHE="${GOCACHE:-$HOME/.cache/go-build}"
export CARGO_HOME=/root/.cargo
export RUSTUP_HOME=/root/.rustup
export PATH="/usr/local/go/bin:${CARGO_HOME}/bin:$PATH"
mkdir -p "$GOMODCACHE" "$GOCACHE" "$GOPATH/bin"

command -v cargo >/dev/null || curl -fsSL https://sh.rustup.rs \
  | sh -s -- -y --profile minimal --default-toolchain stable

# build-libs needs real git metadata, so this is a git tree not a source archive.
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"
mkdir -p stellar-rpc && cd stellar-rpc
git init -q
git remote add origin "https://github.com/$REPO.git"
if [ -z "$TARGET_SHA" ]; then
  log "TARGET_SHA unset; shallow fetching origin/$DEFAULT_BRANCH"
  git fetch --depth 1 origin "$DEFAULT_BRANCH" || bail "failed to fetch origin/$DEFAULT_BRANCH"
  git checkout --detach FETCH_HEAD
  TARGET_SHA=$(git rev-parse HEAD)
elif git fetch --depth 1 origin "$TARGET_SHA"; then
  git checkout --detach FETCH_HEAD
else
  log "direct commit fetch failed; falling back to full clone"
  cd "$WORK_DIR" && rm -rf stellar-rpc
  git clone "https://github.com/$REPO.git" stellar-rpc && cd stellar-rpc
  git fetch origin "+refs/pull/*:refs/remotes/origin/pr/*" 2>/dev/null || true
  git checkout "$TARGET_SHA"
fi
log "checked out $TARGET_SHA; handing off to the Go runner"

# The runner owns the verdict markers from here; release the bootstrap trap. The
# fallback below only covers a runner that dies before emitting one (e.g. compile error).
trap - ERR
export TARGET_SHA RUN_ID REPO WORK_DIR RESULTS_FILE
if ! go run ./cmd/stellar-rpc/internal/integrationtest/infrastructure/load-test/runner instantiate; then
  [ -f /tmp/done ] || bail "go runner exited without writing a verdict"
fi
