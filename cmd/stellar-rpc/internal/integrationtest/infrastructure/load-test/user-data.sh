#!/bin/bash
# Runs as root via EC2 user-data on the ephemeral CI box.
# Substituted at orchestrator step (GHA): TARGET_SHA, PR_NUMBER, RUN_ID.

set -euo pipefail
exec > >(tee -a /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

# === Templated by orchestrator =====================================
TARGET_SHA="${TARGET_SHA:-__TARGET_SHA__}"   # full SHA of commit to test
PR_NUMBER="${PR_NUMBER:-__PR_NUMBER__}"      # empty string on push-to-main
RUN_ID="${RUN_ID:-__RUN_ID__}"               # cross-reference back to workflow run
# ===================================================================
# This script writes results to /tmp/results.md and touches /tmp/done
# when finished, then exits. It does NOT self-terminate — the GHA job
# pulls the results via SSM Run Command and then calls TerminateInstances.
# The backgrounded watchdog (90 min) is the only termination path inside
# the box; it's the safety net for runs the GHA workflow fails to clean up.

BUCKET="${BUCKET:-stellar-rpc-ci-load-test}"
REGION="${REGION:-us-east-1}"
REPO="${REPO:-stellar/stellar-rpc}"
WORK_DIR="${WORK_DIR:-/data}"
GOLDEN_DB="${GOLDEN_DB:-${WORK_DIR}/golden.sqlite}"
RESULTS_FILE="${RESULTS_FILE:-/tmp/results.md}"
MANUAL_VALIDATION="${MANUAL_VALIDATION:-0}"
WATCHDOG_ENABLED="${WATCHDOG_ENABLED:-1}"
WAIT_FOR_THROTTLE_SIGNAL="${WAIT_FOR_THROTTLE_SIGNAL:-1}"

# Fallback branch used when TARGET_SHA is empty or still the literal template
# marker. The load-test code lives only on `apply-load` until that branch is
# merged to main; until then, manual / unparameterized runs must default here.
DEFAULT_BRANCH="apply-load"

if [ "$RUN_ID" = "__RUN""_ID__" ]; then
  RUN_ID="manual-validation"
fi

if [ "$MANUAL_VALIDATION" = "1" ]; then
  WATCHDOG_ENABLED=0
  WAIT_FOR_THROTTLE_SIGNAL=0
fi

# --- Helpers --------------------------------------------------------
log() { echo "[$(date -u +%FT%TZ)] $*"; }

imds_token() {
  curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 300' \
    http://169.254.169.254/latest/api/token
}
my_instance_id() {
  curl -fsS -H "X-aws-ec2-metadata-token: $(imds_token)" \
    http://169.254.169.254/latest/meta-data/instance-id
}

self_terminate() {
  # Only called by the watchdog as a last resort. Normal cleanup is done
  # by the GHA workflow after it fetches results via SSM.
  aws ec2 terminate-instances --region "$REGION" --instance-ids "$(my_instance_id)"
}

bail() {
  log "FATAL: $*"
  {
    printf '❌ **Ingest load test failed** (run %s on `%s`)\n\n```\n' \
      "$RUN_ID" "$TARGET_SHA"
    printf '%s\n' "$*"
    printf '```\n'
  } > "$RESULTS_FILE"
  # Signal GHA that we're done (with a failure result body). GHA fetches
  # $RESULTS_FILE via SSM, posts to the PR, and terminates us.
  touch /tmp/done
  exit 1
}
trap 'FAILED_LINE=$LINENO; FAILED_COMMAND=$BASH_COMMAND; bail "unhandled error at line $FAILED_LINE while running: $FAILED_COMMAND"' ERR

# --- Watchdog -------------------------------------------------------
# Force-terminate the instance after 90 minutes if user-data hangs.
# Self-contained: a backgrounded subshell calls terminate-instances
# directly, so we don't depend on InstanceInitiatedShutdownBehavior.
# Clean self_terminate normally fires first; if so, the box (and this
# sleeper) goes away well before the watchdog wakes up.
if [ "$WATCHDOG_ENABLED" = "1" ]; then
  if WATCHDOG_INSTANCE_ID=$(my_instance_id 2>/dev/null); then
    (
      sleep 5400  # 90 minutes
      aws ec2 terminate-instances --region "$REGION" \
        --instance-ids "$WATCHDOG_INSTANCE_ID" >/dev/null 2>&1 || true
    ) </dev/null >/dev/null 2>&1 &
    disown
    log "watchdog scheduled: instance will terminate ~90 minutes from now"
  else
    log "watchdog not scheduled: could not determine instance id from IMDS"
  fi
else
  log "watchdog disabled"
fi

# --- Bootstrap ------------------------------------------------------
log "clearing stale run state"
rm -f /tmp/done /tmp/download-complete /tmp/volume-throttle-requested \
      /tmp/bench-results.json /tmp/core.sha256 /tmp/observed.sha256 \
      "$RESULTS_FILE"
rm -rf "$WORK_DIR/stellar-rpc"

log "installing deps"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq --no-install-recommends zstd awscli jq curl git build-essential ca-certificates \
                       libpq5 libsodium23 libunwind8 libc++1-14

# Go (pinned)
GO_VERSION=1.22.7
curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" | tar -xz -C /usr/local
export HOME="${HOME:-/root}"
export GOPATH="${GOPATH:-$HOME/go}"
export GOMODCACHE="${GOMODCACHE:-$GOPATH/pkg/mod}"
export GOCACHE="${GOCACHE:-$HOME/.cache/go-build}"
export CARGO_HOME="/root/.cargo"
export RUSTUP_HOME="/root/.rustup"
export PATH="/usr/local/go/bin:${CARGO_HOME}/bin:$PATH"
mkdir -p "$GOMODCACHE" "$GOCACHE" "$GOPATH/bin"

if ! command -v cargo >/dev/null 2>&1; then
  log "installing Rust toolchain"
  curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
fi
rustup target add wasm32-unknown-unknown
rustup component add rustfmt clippy rust-src

# --- Fetch golden DB with fallback (no ListBucket needed) ----------
mkdir -p "$WORK_DIR"

GOLDEN_KEY=""
EXPECTED_SHA=""
for PFX in current prev1 prev2; do
  CANDIDATE_KEY="$PFX/golden.sqlite.zst"
  EXPECTED_SHA=""

  if GOLDEN_HEAD=$(aws s3api head-object --region "$REGION" \
      --bucket "$BUCKET" --key "$CANDIDATE_KEY" 2>/dev/null); then
    EXPECTED_SHA=$(printf '%s' "$GOLDEN_HEAD" | jq -r '.Metadata["sha256-raw"] // empty')
    if [ -z "$EXPECTED_SHA" ]; then
      log "no sha256-raw metadata on s3://$BUCKET/$CANDIDATE_KEY; skipping golden DB checksum verification"
    fi
  else
    log "head-object failed for s3://$BUCKET/$CANDIDATE_KEY; attempting download without checksum metadata"
  fi

  log "streaming download + decompress + hash from s3://$BUCKET/$CANDIDATE_KEY"
  START=$(date +%s)
  if aws s3 cp --region "$REGION" "s3://$BUCKET/$CANDIDATE_KEY" - \
    | zstd -d \
    | tee >(sha256sum | awk '{print $1}' > /tmp/observed.sha256) \
    > "$GOLDEN_DB"; then
    GOLDEN_KEY="$CANDIDATE_KEY"
    DURATION=$(( $(date +%s) - START ))
    log "found golden at s3://$BUCKET/$GOLDEN_KEY"
    log "golden DB ready in ${DURATION}s ($(du -h "$GOLDEN_DB" | cut -f1))"
    break
  fi

  rm -f "$GOLDEN_DB" /tmp/observed.sha256
done
[ -n "$GOLDEN_KEY" ] || bail "no golden.sqlite.zst in current/, prev1/, or prev2/"

OBSERVED_SHA=$(cat /tmp/observed.sha256)
if [ -n "$EXPECTED_SHA" ] && [ "$EXPECTED_SHA" != "$OBSERVED_SHA" ]; then
  bail "hash mismatch: expected $EXPECTED_SHA, got $OBSERVED_SHA"
fi
if [ -n "$EXPECTED_SHA" ]; then
  log "golden DB hash OK ($OBSERVED_SHA)"
else
  log "golden DB hash computed ($OBSERVED_SHA); verification skipped"
fi

# --- Fetch stellar-core (BUILD_TESTS build, with apply-load support) ----
# Stock SDF apt-package stellar-core does NOT include apply-load (it's
# #ifdef'd by BUILD_TESTS in src/main/CommandLine.cpp), so we ship a
# pre-built binary alongside the golden DB. Update cadence is independent
# of the golden DB, hence the separate `core/` prefix.
CORE_KEY="core/stellar-core.zst"
CORE_EXPECTED_SHA=""
if CORE_HEAD=$(aws s3api head-object --region "$REGION" \
  --bucket "$BUCKET" --key "$CORE_KEY" 2>/dev/null); then
  CORE_EXPECTED_SHA=$(printf '%s' "$CORE_HEAD" | jq -r '.Metadata["sha256-raw"] // empty')
  if [ -z "$CORE_EXPECTED_SHA" ]; then
    log "no sha256-raw metadata on s3://$BUCKET/$CORE_KEY; skipping stellar-core checksum verification"
  fi
else
  log "head-object failed for s3://$BUCKET/$CORE_KEY; fetching without checksum metadata"
fi

log "fetching stellar-core"
aws s3 cp --region "$REGION" "s3://$BUCKET/$CORE_KEY" - \
  | zstd -d \
  | tee >(sha256sum | awk '{print $1}' > /tmp/core.sha256) \
  > /usr/local/bin/stellar-core || bail "failed to download stellar-core from s3://$BUCKET/$CORE_KEY"
chmod +x /usr/local/bin/stellar-core

CORE_OBSERVED_SHA=$(cat /tmp/core.sha256)
if [ -n "$CORE_EXPECTED_SHA" ] && [ "$CORE_EXPECTED_SHA" != "$CORE_OBSERVED_SHA" ]; then
  bail "stellar-core hash mismatch: expected $CORE_EXPECTED_SHA, got $CORE_OBSERVED_SHA"
fi
if [ -n "$CORE_EXPECTED_SHA" ]; then
  log "stellar-core hash OK ($CORE_OBSERVED_SHA)"
else
  log "stellar-core hash computed ($CORE_OBSERVED_SHA); verification skipped"
fi
if CORE_VERSION=$(/usr/local/bin/stellar-core version 2>&1 | head -1); then
  log "$CORE_VERSION"
else
  [ -n "${CORE_VERSION:-}" ] || CORE_VERSION="stellar-core version unavailable"
  log "stellar-core version probe failed; continuing with: $CORE_VERSION"
fi

# Signal to the workflow that the large download/decompress stage is done.
# The workflow will best-effort request a root-volume throughput reduction via
# SSM, then drop /tmp/volume-throttle-requested so we can continue.
log "download complete"
touch /tmp/download-complete

cd "$WORK_DIR"
git clone "https://github.com/$REPO.git" stellar-rpc
cd stellar-rpc
# Fetch PR refs in case target SHA isn't reachable from branch tips
git fetch origin "+refs/pull/*:refs/remotes/origin/pr/*" 2>/dev/null || true

# If the orchestrator didn't substitute TARGET_SHA (manual / unparameterized
# run), fall back to the apply-load branch's tip — the load-test code lives
# only on that branch until it's merged to main.
if [ -z "$TARGET_SHA" ] || [ "$TARGET_SHA" = "__TARGET_SHA__" ]; then
  log "TARGET_SHA unset; falling back to origin/$DEFAULT_BRANCH"
  git checkout "origin/$DEFAULT_BRANCH"
  TARGET_SHA=$(git rev-parse HEAD)
else
  git checkout "$TARGET_SHA"
fi
log "checked out $TARGET_SHA"
log "building rpc libs"
make build-libs

if [ "$WAIT_FOR_THROTTLE_SIGNAL" = "1" ]; then
  THROTTLE_SIGNAL_DEADLINE=$(( $(date +%s) + 900 ))
  while [ ! -f /tmp/volume-throttle-requested ] && [ $(date +%s) -lt $THROTTLE_SIGNAL_DEADLINE ]; do
    sleep 5
  done
  if [ -f /tmp/volume-throttle-requested ]; then
    log "volume throttle request received"
    sleep 5  # Give the throttle a moment to take effect before we start the benchmark.
  else
    log "volume throttle request not received within 900s; continuing"
  fi
else
  log "volume throttle wait disabled"
fi

# --- Run the ingest perf benchmark ---------------------------------
# TestApplyLoadThenIngest regenerates the synthetic ledger bundle from the
# checked-in apply-load.cfg profile (so it always matches the current
# APPLY_LOAD_NUM_LEDGERS) and then replays it through the daemon's ingest
# path. The JSON consumed below measures only the ingest phase.
log "running ingest perf benchmark"
BENCH_START=$(date +%s)
ERR_TRAP_STATE=$(trap -p ERR || true)
trap - ERR
set +e
(
  LOADTEST_SQLITE_PATH="$GOLDEN_DB" \
  PERF_RESULTS_PATH=/tmp/bench-results.json \
  STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true \
  STELLAR_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN=/usr/local/bin/stellar-core \
  go test -run TestApplyLoadThenIngest \
    -timeout 60m \
    -v \
    ./cmd/stellar-rpc/internal/integrationtest/...
) > >(tee /tmp/benchmark.log) 2>&1
BENCH_STATUS=$?
set -e
if [ -n "$ERR_TRAP_STATE" ]; then
  eval "$ERR_TRAP_STATE"
fi
if [ "$BENCH_STATUS" -ne 0 ]; then
  BENCH_TAIL=$(tail -n 40 /tmp/benchmark.log 2>/dev/null || true)
  if [ -n "$BENCH_TAIL" ]; then
    bail "$(printf 'benchmark failed:\n%s' "$BENCH_TAIL")"
  fi
  bail "benchmark failed; see /var/log/user-data.log"
fi
BENCH_DURATION=$(( $(date +%s) - BENCH_START ))

# --- Format and post results --------------------------------------
LEDGER_COUNT=$(jq -r '.ledger_count' /tmp/bench-results.json)
THROUGHPUT=$(jq -r '.ledgers_per_second' /tmp/bench-results.json)
WALL_CLOCK=$(jq -r '.ingest_wall_clock_seconds' /tmp/bench-results.json)
LAT_P50=$(jq -r '.per_ledger_latency_ms.p50' /tmp/bench-results.json)
LAT_P95=$(jq -r '.per_ledger_latency_ms.p95' /tmp/bench-results.json)
LAT_P99=$(jq -r '.per_ledger_latency_ms.p99' /tmp/bench-results.json)

cat > "$RESULTS_FILE" <<MD
### 📈 Ingest load test — \`${TARGET_SHA:0:7}\`

| Metric | Value |
|---|---|
| Ledgers replayed | $LEDGER_COUNT |
| Throughput | ${THROUGHPUT} ledgers/sec |
| Ingest wall-clock | ${WALL_CLOCK}s |
| Per-ledger p50 / p95 / p99 | ${LAT_P50} / ${LAT_P95} / ${LAT_P99} ms |
| Golden DB fetch+decompress | ${DURATION}s |
| Total benchmark time | ${BENCH_DURATION}s |
| stellar-core | \`${CORE_VERSION}\` |
| Workflow run | [#${RUN_ID}](https://github.com/${REPO}/actions/runs/${RUN_ID}) |
MD

# --- Signal readiness to GHA --------------------------------------
# GHA polls /tmp/done via SSM Run Command, then pulls $RESULTS_FILE
# and posts to the PR. We just exit; the box stays up until GHA
# terminates it (or the watchdog fires at 90 min as a backstop).
log "results ready; signalling /tmp/done and exiting (GHA will terminate)"
touch /tmp/done
