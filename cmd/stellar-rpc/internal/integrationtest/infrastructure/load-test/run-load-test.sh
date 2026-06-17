#!/usr/bin/env bash
# Drives the ephemeral load test. Two entry points:
#   run-load-test.sh              — runs on the EC2 box as user-data; bootstraps
#                                   and runs the ingest benchmark.
#   run-load-test.sh orchestrate  — runs on the GHA runner; polls the box via
#                                   SSM and drives the gp3 throughput handshake.
# Marker protocol shared across both halves:
#   /tmp/download-complete                  instance: assets fetched, ready to throttle
#   /tmp/volume-throttle-{requested,failed} runner:   throttle outcome (gate for the benchmark)
#   /tmp/results.md                         instance: result body (presentation only)
#   /tmp/done                               instance: machine-readable verdict ("ok" or
#                                           "fail"); results.md is ready to fetch

set -euo pipefail
log() { echo "[$(date -u +%FT%TZ)] $*"; }

# Both halves of the gp3 throttle handshake key off this deadline; they must
# agree for the protocol to work.
THROTTLE_TIMEOUT=900

orchestrate() {
  : "${INSTANCE_ID:?}" "${AWS_REGION:?}" "${BENCH_VOLUME_THROUGHPUT:?}"
  : "${RESULTS_TIMEOUT:?}" "${POLL_INTERVAL:?}" "${GITHUB_OUTPUT:?}"
  : "${DEBUG_LOG_LINES:?}" "${DEBUG_LOG_EVERY_POLLS:?}"
  ROOT_VOLUME_ID="${ROOT_VOLUME_ID:-}"

  ssm_send() {
    local attempt=1 id
    while [ "$attempt" -le "${2:-3}" ]; do
      if id=$(aws ssm send-command --instance-ids "$INSTANCE_ID" \
          --document-name AWS-RunShellScript --parameters "$1" \
          --query 'Command.CommandId' --output text 2>/dev/null); then
        printf '%s\n' "$id"; return 0
      fi
      log "ssm send-command attempt $attempt failed" >&2
      attempt=$((attempt + 1)); sleep 5
    done
    return 1
  }
  # ssm_invoke PARAMS QUERY [SEND_ATTEMPTS] — send a command, wait for it, and
  # print the requested invocation field.
  ssm_invoke() {
    local id; id=$(ssm_send "$1" "${3:-3}") || return 1
    aws ssm wait command-executed --command-id "$id" --instance-id "$INSTANCE_ID" 2>/dev/null || true
    aws ssm get-command-invocation --command-id "$id" --instance-id "$INSTANCE_ID" \
      --query "$2" --output text 2>/dev/null || true
  }
  ssm_capture() { ssm_invoke "$1" StandardOutputContent; }
  # Success only on confirmed Status=Success: the handshake must never advance on a silently-dropped marker.
  ssm_touch() {
    [ "$(ssm_invoke "$(jq -cn --arg m "$1" '{commands:["touch \($m)"]}')" Status 2)" = Success ]
  }
  fetch_debug_tail() {
    ssm_capture "$(jq -cn --argjson n "$DEBUG_LOG_LINES" \
      '{commands:["if [ -f /var/log/user-data.log ]; then tail -n \($n|tostring) /var/log/user-data.log; else echo __NO_DEBUG_LOG__; fi"]}')" \
      || echo "__DEBUG_TAIL_UNAVAILABLE__"
  }

  local THROTTLE_ATTEMPTED=0 THROTTLE_SIGNALLED=0 THROTTLE_STARTED_AT=0
  signal_throttle() {
    ssm_touch "/tmp/volume-throttle-$1" && THROTTLE_SIGNALLED=1
  }
  reconcile_throttle() {
    [ "$THROTTLE_SIGNALLED" -eq 1 ] && return 0
    if [ -z "$ROOT_VOLUME_ID" ]; then
      signal_throttle failed
      return 0
    fi
    if [ "$THROTTLE_ATTEMPTED" -eq 0 ]; then
      if aws ec2 modify-volume --region "$AWS_REGION" --volume-id "$ROOT_VOLUME_ID" \
          --throughput "$BENCH_VOLUME_THROUGHPUT" >/dev/null; then
        log "requested gp3 downshift to $BENCH_VOLUME_THROUGHPUT MiB/s on $ROOT_VOLUME_ID"
        THROTTLE_ATTEMPTED=1; THROTTLE_STARTED_AT=$(date +%s)
      else
        signal_throttle failed
      fi
      return 0
    fi
    local current
    current=$(aws ec2 describe-volumes --region "$AWS_REGION" --volume-ids "$ROOT_VOLUME_ID" \
      --query 'Volumes[0].Throughput' --output text 2>/dev/null || echo "")
    log "throttle status current=${current:-?} target=$BENCH_VOLUME_THROUGHPUT"
    if [ "$current" = "$BENCH_VOLUME_THROUGHPUT" ]; then
      signal_throttle requested
    elif [ "$(date +%s)" -ge $((THROTTLE_STARTED_AT + THROTTLE_TIMEOUT)) ]; then
      log "throttle convergence timed out after ${THROTTLE_TIMEOUT}s"
      signal_throttle failed
    fi
  }

  # When done, the first output line is the instance verdict ("ok"/"fail"),
  # followed by the results.md payload.
  local POLL_PARAMS='commands=["if [ -f /tmp/done ]; then cat /tmp/done /tmp/results.md; elif [ -f /tmp/download-complete ]; then echo __DOWNLOAD_COMPLETE__; else echo __NOT_READY__; fi"]'
  local DEADLINE=$(($(date +%s) + RESULTS_TIMEOUT)) POLL_COUNT=0 OUT FIRST

  while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    POLL_COUNT=$((POLL_COUNT + 1))
    if ! OUT=$(ssm_capture "$POLL_PARAMS"); then
      log "ssm poll dispatch failed; retrying"; sleep "$POLL_INTERVAL"; continue
    fi
    FIRST=${OUT%%$'\n'*}
    if [ -z "$OUT" ] || [ "$FIRST" = __NOT_READY__ ]; then
      log "still waiting for /tmp/done"
    elif [ "$FIRST" = __DOWNLOAD_COMPLETE__ ]; then
      reconcile_throttle
      log "download stage complete; waiting for /tmp/done"
    else
      log "result payload from instance (verdict: $FIRST)"
      OUT=${OUT#*$'\n'} # strip the verdict line; the rest is the markdown body
      printf '%s\n' "$OUT"
      printf '%s' "$OUT" > /tmp/results.md
      {
        echo "found=true"
        if [ "$FIRST" = ok ]; then echo "passed=true"; else echo "passed=false"; fi
      } >> "$GITHUB_OUTPUT"
      return 0
    fi
    [ $((POLL_COUNT % DEBUG_LOG_EVERY_POLLS)) -eq 0 ] && { log "debug tail"; fetch_debug_tail; }
    sleep "$POLL_INTERVAL"
  done

  fetch_debug_tail > /tmp/debug-tail.log
  {
    echo "❌ Load test did not produce results within ${RESULTS_TIMEOUT}s."
    echo
    echo "Instance: \`${INSTANCE_ID}\`"
    [ -n "${GITHUB_SERVER_URL:-}" ] && [ -n "${GITHUB_REPOSITORY:-}" ] && [ -n "${GITHUB_RUN_ID:-}" ] \
      && echo "Workflow run: ${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
    if [ -s /tmp/debug-tail.log ]; then
      echo; echo "Last ${DEBUG_LOG_LINES} lines of /var/log/user-data.log:"; echo
      echo '```'; cat /tmp/debug-tail.log; echo '```'
    fi
  } > /tmp/timeout-comment.md
  echo "found=false" >> "$GITHUB_OUTPUT"
}

instance() {
  exec > >(tee -a /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

  # The workflow passes these via an exported user-data preamble; manual runs
  # may leave them unset.
  TARGET_SHA="${TARGET_SHA:-}"
  RUN_ID="${RUN_ID:-manual}"

  BUCKET="${BUCKET:-stellar-rpc-ci-load-test}"
  REGION="${REGION:-us-east-1}"
  REPO="${REPO:-stellar/stellar-rpc}"
  WORK_DIR="${WORK_DIR:-/data}"
  GOLDEN_DB="${GOLDEN_DB:-${WORK_DIR}/golden.sqlite}"
  RESULTS_FILE="${RESULTS_FILE:-/tmp/results.md}"
  DEFAULT_BRANCH=apply-load

  bail() {
    log "FATAL: $*"
    { printf '❌ **Ingest load test failed** (run %s on `%s`)\n\n```\n' "$RUN_ID" "$TARGET_SHA"
      printf '%s\n' "$*"
      printf '```\n'; } > "$RESULTS_FILE"
    echo fail > /tmp/done
    exit 1
  }
  trap 'bail "unhandled error at line $LINENO while running: $BASH_COMMAND"' ERR

  asset_expected_sha() {
    local head expected=""
    if head=$(aws s3api head-object --region "$REGION" --bucket "$BUCKET" --key "$1" 2>/dev/null); then
      expected=$(printf '%s' "$head" | jq -r '.Metadata["sha256-raw"] // empty')
      [ -z "$expected" ] && log "no sha256-raw on s3://$BUCKET/$1; skipping $2 checksum" >&2
    else
      log "head-object failed for s3://$BUCKET/$1; fetching $2 without checksum" >&2
    fi
    printf '%s' "$expected"
  }
  # stream_with_sha KEY OUT MODE SHA_OUT — MODE = zstd | raw
  stream_with_sha() {
    local filter="cat"
    if [ "${3:-raw}" = zstd ]; then filter="zstd -d"; fi
    aws s3 cp --region "$REGION" "s3://$BUCKET/$1" - | $filter \
      | tee >(sha256sum | awk '{print $1}' > "$4") > "$2"
  }
  verify_sha() {
    if [ -n "$2" ] && [ "$2" != "$3" ]; then
      bail "$1 hash mismatch: expected $2, got $3"
    fi
    log "$1 hash $([ -n "$2" ] && echo OK || echo "computed (unverified)") ($3)"
  }
  # fetch_verified KEY OUT MODE LABEL SHA_FILE — expected-sha lookup, download,
  # and checksum verification; bails on download failure or hash mismatch.
  fetch_verified() {
    local expected
    expected=$(asset_expected_sha "$1" "$4")
    log "fetching $4"
    stream_with_sha "$1" "$2" "$3" "$5" || bail "failed to download $4"
    verify_sha "$4" "$expected" "$(cat "$5")"
  }

  log "clearing stale run state"
  rm -f /tmp/done /tmp/download-complete \
        /tmp/volume-throttle-requested /tmp/volume-throttle-failed \
        /tmp/bench-results.json \
        /tmp/golden.sha256 /tmp/core.sha256 /tmp/ledger-bundle-*.sha256 \
        /tmp/load-test-ledgers-*.xdr.zstd \
        "$RESULTS_FILE"
  rm -rf "$WORK_DIR/stellar-rpc"

  log "installing deps"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq --no-install-recommends \
    zstd awscli jq curl git build-essential ca-certificates \
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

  mkdir -p "$WORK_DIR"

  GOLDEN_KEY=""
  GOLDEN_EXPECTED_SHA=""
  for PFX in current prev1 prev2; do
    CANDIDATE_KEY="$PFX/golden.sqlite.zst"
    GOLDEN_EXPECTED_SHA=$(asset_expected_sha "$CANDIDATE_KEY" "golden DB")
    log "streaming s3://$BUCKET/$CANDIDATE_KEY"
    START=$(date +%s)
    if stream_with_sha "$CANDIDATE_KEY" "$GOLDEN_DB" zstd /tmp/golden.sha256; then
      GOLDEN_KEY="$CANDIDATE_KEY"
      DURATION=$(( $(date +%s) - START ))
      log "golden DB ready in ${DURATION}s ($(du -h "$GOLDEN_DB" | cut -f1))"
      break
    fi
    rm -f "$GOLDEN_DB" /tmp/golden.sha256
  done
  [ -n "$GOLDEN_KEY" ] || bail "no golden.sqlite.zst in current/, prev1/, or prev2/"
  verify_sha "golden DB" "$GOLDEN_EXPECTED_SHA" "$(cat /tmp/golden.sha256)"

  # Stock SDF apt-package stellar-core lacks apply-load (BUILD_TESTS-gated),
  # so we ship a pre-built binary under a separate `core/` prefix.
  fetch_verified core/stellar-core.zst /usr/local/bin/stellar-core zstd stellar-core /tmp/core.sha256
  chmod +x /usr/local/bin/stellar-core

  # Three apply-load scenarios (one bundle + one config each), ingested as a
  # single concatenated ledger stream. Bundle i is described by config i.
  LEDGER_SCENARIOS="oz sac soroswap"
  LEDGER_BUNDLE_PATHS=""
  for SCENARIO in $LEDGER_SCENARIOS; do
    BUNDLE_PATH="/tmp/load-test-ledgers-v27-$SCENARIO.xdr.zstd"
    fetch_verified "ledgers/load-test-ledgers-v27-$SCENARIO.xdr.zstd" "$BUNDLE_PATH" raw \
      "ledger bundle ($SCENARIO)" "/tmp/ledger-bundle-$SCENARIO.sha256"
    LEDGER_BUNDLE_PATHS="${LEDGER_BUNDLE_PATHS:+$LEDGER_BUNDLE_PATHS,}$BUNDLE_PATH"
  done

  log "download complete"
  touch /tmp/download-complete

  # build-libs needs real git metadata, so this is a git tree not a source archive.
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
  log "checked out $TARGET_SHA; building rpc libs"
  make build-libs

  # Configs ship with the checkout; config i describes downloaded bundle i.
  CONFIG_DIR="$WORK_DIR/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/load-test/testdata"
  LOADTEST_CONFIG_PATHS=""
  for SCENARIO in $LEDGER_SCENARIOS; do
    CFG="$CONFIG_DIR/apply-load-v27-$SCENARIO.cfg"
    [ -f "$CFG" ] || bail "missing apply-load config $CFG in checkout"
    LOADTEST_CONFIG_PATHS="${LOADTEST_CONFIG_PATHS:+$LOADTEST_CONFIG_PATHS,}$CFG"
  done

  # Refuse to bench without a confirmed throttle: an un-throttled volume produces wrong numbers.
  THROTTLE_DEADLINE=$(($(date +%s) + THROTTLE_TIMEOUT))
  while [ ! -f /tmp/volume-throttle-requested ] \
    && [ ! -f /tmp/volume-throttle-failed ] \
    && [ "$(date +%s)" -lt "$THROTTLE_DEADLINE" ]; do
    sleep 5
  done
  if [ -f /tmp/volume-throttle-requested ]; then
    log "volume throttle confirmed"
  elif [ -f /tmp/volume-throttle-failed ]; then
    bail "volume throttle could not be confirmed"
  else
    bail "volume throttle was not confirmed within ${THROTTLE_TIMEOUT}s"
  fi

  log "running ingest perf benchmark"
  trap - ERR
  set +e
  (
    LOADTEST_INGEST_LEDGER_PATH="$LEDGER_BUNDLE_PATHS" \
    LOADTEST_CONFIG_PATH="$LOADTEST_CONFIG_PATHS" \
    LOADTEST_INGEST_DEADLINE="${LOADTEST_INGEST_DEADLINE:-150m}" \
    LOADTEST_SQLITE_PATH="$GOLDEN_DB" \
    PERF_RESULTS_PATH=/tmp/bench-results.json \
    PERF_RESULTS_MD_PATH="$RESULTS_FILE" \
    PERF_TARGET_SHA="$TARGET_SHA" \
    PERF_RUN_ID="$RUN_ID" \
    PERF_REPO="$REPO" \
    PERF_GOLDEN_FETCH_SECONDS="$DURATION" \
    STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true \
    go test -run TestIngestSyntheticLedgers -timeout 170m -v \
      ./cmd/stellar-rpc/internal/integrationtest/
  ) > >(tee /tmp/benchmark.log) 2>&1
  BENCH_STATUS=$?
  set -e
  if [ "$BENCH_STATUS" -ne 0 ]; then
    bail "$(printf 'benchmark failed:\n%s' "$(tail -n 80 /tmp/benchmark.log 2>/dev/null)")"
  fi

  [ -s "$RESULTS_FILE" ] || bail "benchmark succeeded but did not emit $RESULTS_FILE"
  log "results ready; signalling /tmp/done"
  echo ok > /tmp/done
}

case "${1:-instance}" in
  instance)    instance ;;
  orchestrate) orchestrate ;;
  *) echo "usage: $0 [instance|orchestrate]" >&2; exit 64 ;;
esac
