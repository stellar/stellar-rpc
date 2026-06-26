#!/usr/bin/env bash
# Bootstraps the ephemeral load-test box (EC2 user-data): installs the toolchain,
# checks out TARGET_SHA, then hands off to `runner instantiate`, which streams the
# corpus from S3 and runs the ingest benchmark.
# The other half, `runner orchestrate`, polls S3 for the result object.
#
# Result protocol: the box publishes one object to s3://$BUCKET/$RESULT_KEY holding
# {schemaVersion, verdict, markdown, bench, runId, targetSha}. The Go runner
# publishes the success object; this script publishes the fail object (it has the
# AWS CLI even when Go never starts), so the orchestrator always sees a verdict.

set -euo pipefail
log() { echo "[$(date -u +%FT%TZ)] $*"; }

exec > >(tee -a /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

# Hard self-terminate ceiling, independent of the GHA runner for if the runner is
# force-cancelled or crashes and skips its Terminate step.
# Sits above the workflow's 225min job timeout so it never pre-empts a healthy run.
SELF_TERMINATE_MINUTES="${SELF_TERMINATE_MINUTES:-240}"
shutdown -P "+${SELF_TERMINATE_MINUTES}" "load-test self-terminate ceiling" \
  || log "WARN: could not schedule self-terminate ceiling"

TARGET_SHA="${TARGET_SHA:-}"
RUN_ID="${RUN_ID:-manual}"
REPO="${REPO:-stellar/stellar-rpc}"
WORK_DIR="${WORK_DIR:-/data}"
RESULTS_FILE="${RESULTS_FILE:-/tmp/results.md}"
BUCKET="${BUCKET:-stellar-rpc-ci-load-test}"
RESULT_KEY="${RESULT_KEY:-}"
DEFAULT_BRANCH=main

# Install the AWS CLI + jq up front (before the bail trap) so any later failure
# can publish a fail result to S3. A failure during this step itself can't be
# uploaded and degrades to the orchestrator's timeout path.
log "installing aws cli + jq"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq --no-install-recommends awscli jq curl ca-certificates

# upload_result publishes {verdict, markdown} as the run's result object so the
# orchestrator (polling S3) sees a verdict instead of waiting out its timeout. The
# Go runner publishes the rich success object; this covers the fail paths.
upload_result() {
  local verdict="$1" body_file="$2"
  if [ -z "$BUCKET" ] || [ -z "$RESULT_KEY" ]; then
    log "WARN: BUCKET/RESULT_KEY unset; cannot publish $verdict result"
    return 0
  fi
  [ -s "$body_file" ] || printf 'Load test failed before producing a result body.\n' > "$body_file"
  jq -n --arg v "$verdict" --rawfile md "$body_file" \
        --arg run "$RUN_ID" --arg sha "$TARGET_SHA" \
        '{schemaVersion: 1, verdict: $v, markdown: $md, runId: $run, targetSha: $sha}' > /tmp/result.json
  aws s3api put-object --bucket "$BUCKET" --key "$RESULT_KEY" \
        --content-type application/json --body /tmp/result.json >/dev/null
}

# bail publishes a fail result the orchestrator can read, then stops. It guards
# only the pre-Go bootstrap phase; once the Go runner starts it owns the verdict.
bail() {
  log "FATAL: $*"
  { printf '❌ **Ingest load test failed** (run %s on `%s`)\n\n```\n' "$RUN_ID" "$TARGET_SHA"
    printf '%s\n' "$*"
    printf '```\n'; } > "$RESULTS_FILE"
  upload_result fail "$RESULTS_FILE" || log "WARN: fail result upload failed"
  exit 1
}
trap 'bail "unhandled error at line $LINENO while running: $BASH_COMMAND"' ERR

# temporary scaffolding: before merge to main, remove this section
# (debug-only: persists the full box log to S3 so a terminated instance is still
# debuggable; best-effort, never alters the real exit status)
upload_box_log() {
  [ -n "$BUCKET" ] && [ -n "$RESULT_KEY" ] || return 0
  aws s3 cp /var/log/user-data.log "s3://$BUCKET/${RESULT_KEY%/*}/user-data.log" >/dev/null 2>&1 || true
}
trap upload_box_log EXIT
# end temporary scaffolding section

log "clearing stale run state"
rm -f /tmp/bench-results.json /tmp/load-test-ledgers-*.xdr.zstd \
      "$RESULTS_FILE"
rm -rf "$WORK_DIR/stellar-rpc"

log "installing build deps"
apt-get install -y -qq --no-install-recommends \
  git build-essential \
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

# build-libs needs real git metadata, so this is a git tree/not a source archive.
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

# The Go runner owns the verdict from here -> release the bootstrap trap. On
# success it publishes the result object itself; on any non-zero exit it has
# written the failure body to RESULTS_FILE (or died before doing so), so we
# publish the fail result it couldn't.
trap - ERR
export TARGET_SHA RUN_ID REPO WORK_DIR RESULTS_FILE BUCKET RESULT_KEY
if ! go run ./cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/ingest-load-test/runner instantiate; then
  log "go runner exited non-zero; publishing fail result"
  upload_result fail "$RESULTS_FILE"
  exit 1
fi
