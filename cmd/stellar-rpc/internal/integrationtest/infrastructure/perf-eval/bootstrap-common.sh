# Generic EC2-leg bootstrap, shared by every perf-eval leg. It is NOT run on its
# own: ec2-leg.yml renders the box user-data as
#   <preamble exports>  +  bootstrap-common.sh  +  run-<leg>.sh
# so this file has no shebang. It installs the toolchain and exposes helpers; the
# leg script calls bootstrap_box then hands off to its runner via run_leg.
#
# Result protocol: the box publishes one object to s3://$BUCKET/$RESULT_KEY holding
# {schemaVersion, verdict, markdown, bench, runId, targetSha}. The Go runner
# publishes the success object; bail() here publishes the fail object so the
# gatherer always sees a verdict.

set -euo pipefail
log() { echo "[$(date -u +%FT%TZ)] $*"; }

exec > >(tee -a /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

# Hard self-terminate ceiling, independent of the GHA runner for if the runner is
# force-cancelled or crashes and skips its Terminate step.
# Sits above the workflow's job timeout so it never pre-empts a healthy run.
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

# Install the AWS CLI + jq early so any later failure can still publish a result to S3.
log "installing aws cli + jq"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq --no-install-recommends awscli jq curl ca-certificates

# upload_result publishes {verdict, markdown} as the run's result object so the
# gatherer (polling S3) sees a verdict. Covers the fail paths.
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

# bail publishes a fail result the gatherer can read, then stops. It guards
# only the pre-Go bootstrap phase. LEG_TITLE (set by the leg) titles the body.
bail() {
  log "FATAL: $*"
  { printf '❌ **%s failed** (run %s on `%s`)\n\n```\n' "${LEG_TITLE:-Perf eval}" "$RUN_ID" "$TARGET_SHA"
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

# bootstrap_box installs the build toolchain and checks out TARGET_SHA into
# $WORK_DIR/stellar-rpc, leaving the shell cd'd at the repo root. Generic across
# legs; the leg clears any of its own stale artifacts before calling this.
bootstrap_box() {
  log "clearing stale run state"
  rm -f "$RESULTS_FILE"
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
}

# run_leg hands off to a leg's runner package. The Go runner owns the
# verdict from here, so the bootstrap ERR trap is released first. On a non-zero
# exit the runner has written the failure body to RESULTS_FILE (or died before
# doing so), so we publish the fail result it couldn't.
run_leg() {
  local runner_pkg="$1"
  trap - ERR
  export TARGET_SHA RUN_ID REPO WORK_DIR RESULTS_FILE BUCKET RESULT_KEY
  if ! go run "$runner_pkg"; then
    log "go runner exited non-zero; publishing fail result"
    upload_result fail "$RESULTS_FILE"
    exit 1
  fi
}
