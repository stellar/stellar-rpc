#!/usr/bin/env bash
# Renders a stellar-rpc-benchmarks campaign config from the bench-campaign.yml
# dispatch inputs, plus the launch parameters derived from it (budget, deadline,
# instance type). bench-campaign.yml renders its campaign config through this
# script rather than inline, so the phase matrix, the wall-clock estimate and
# every rejection are exercisable on a laptop:
#
#   PHASE=1 INGEST=both QUERY=no MACHINE=2x ./render-campaign-toml.sh
#
# Env in: PHASE INGEST QUERY RUNS WORKERS MACHINE TARGET_REF HOT_NUM_LEDGERS
# RUN_NAME BENCHMARKS_REF, with PUBLISH_URI / INPUTS_PREFIX / CAPACITY_MINUTES /
# SETUP_MARGIN_MINUTES / NOW_EPOCH available as overrides for tests.
# BENCHMARKS_REF produces no output key: it is validated here and consumed by the
# workflow's launch job straight from the dispatch input.
# Out: key=value lines appended to $GITHUB_OUTPUT (stdout when unset); the
# human-readable estimate goes to stderr so it never lands in the outputs.
#
# Also runs on developer macOS, i.e. bash 3.2: no associative arrays, no
# `base64 -w0`.
set -euo pipefail

die() {
  echo "::error::$*" >&2
  exit 1
}

# Empty string is not an integer, which is what the WORKERS default relies on.
is_uint() {
  case "${1:-}" in
    '' | *[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

# require_uint <name> <value> <min> <max>
require_uint() {
  is_uint "$2" || die "$1 must be an integer, got '$2'"
  if [ "$2" -lt "$3" ] || [ "$2" -gt "$4" ]; then
    die "$1 must be between $3 and $4, got '$2'"
  fi
}

# GNU date wants -d @epoch, BSD date wants -r epoch.
fmt_epoch() {
  date -u -r "$1" +%FT%TZ 2>/dev/null || date -u -d "@$1" +%FT%TZ 2>/dev/null || echo "epoch $1"
}

PHASE="${PHASE:-}"
INGEST="${INGEST:-both}"
QUERY="${QUERY:-no}"
RUNS="${RUNS:-1}"
WORKERS="${WORKERS:-}"
MACHINE="${MACHINE:-2x}"
TARGET_REF="${TARGET_REF:-feature/full-history}"
HOT_NUM_LEDGERS="${HOT_NUM_LEDGERS:-0}"
RUN_NAME="${RUN_NAME:-}"
BENCHMARKS_REF="${BENCHMARKS_REF:-main}"

PUBLISH_URI="${PUBLISH_URI:-s3://stellar-rpc-bench/results}"
INPUTS_PREFIX="${INPUTS_PREFIX:-s3://stellar-rpc-bench/inputs/synthetic-ledgers/2026-07-18-apply-load-20k}"
# 21 h: four 5.3 h poller windows, the most the relay chain can cover.
CAPACITY_MINUTES="${CAPACITY_MINUTES:-1260}"
# Toolchain install, build, dataset sync, cold legs, query legs, bundle upload.
SETUP_MARGIN_MINUTES="${SETUP_MARGIN_MINUTES:-120}"
NOW_EPOCH="${NOW_EPOCH:-$(date +%s)}"

case "$INGEST" in
  both | cold | hot | none) ;;
  *) die "ingest must be one of both|cold|hot|none, got '$INGEST'" ;;
esac
case "$QUERY" in
  yes | no) ;;
  *) die "query must be yes|no, got '$QUERY'" ;;
esac
case "$MACHINE" in
  2x) instance_type=m6id.2xlarge; default_workers=8 ;;
  8x) instance_type=c6id.8xlarge; default_workers=32 ;;
  *) die "machine must be 2x|8x, got '$MACHINE'" ;;
esac

# Phase = the pacing campaign each dataset was generated for. The profile names
# are the dataset names and the S3 directory names under INPUTS_PREFIX.
case "$PHASE" in
  1) close_interval=2s;    interval_ms=2000; profiles="sac-6000 custom_token-4000 soroswap-1500" ;;
  2) close_interval=1s;    interval_ms=1000; profiles="sac-5000 custom_token-4000 soroswap-1500" ;;
  3) close_interval=600ms; interval_ms=600;  profiles="sac-6000 custom_token-3600 soroswap-1800" ;;
  *) die "phase must be 1|2|3, got '$PHASE'" ;;
esac

require_uint runs "$RUNS" 1 20
# A campaign runs chunk 1 only, so a cap above the 10,000-ledger chunk is a typo.
require_uint hot_num_ledgers "$HOT_NUM_LEDGERS" 0 10000
if [ -n "$WORKERS" ]; then
  require_uint workers "$WORKERS" 1 128
  workers="$WORKERS"
else
  workers="$default_workers"
fi

if [ "$INGEST" = none ] && [ "$QUERY" = no ]; then
  die "ingest=none with query=no is an empty campaign; pick an ingest mode or set query=yes"
fi

# A leading `-` is rejected everywhere below: these values are passed as
# arguments to commands on the box (`git checkout "$ref"`, the runner CLI, the
# instance Name tag), where a value like `-f` is read as an option instead of a
# name and the campaign silently runs the wrong thing.
campaign_name="${RUN_NAME:-phase${PHASE}-${MACHINE}}"
case "$campaign_name" in
  -*) die "run_name must not start with '-' (it is read as a command option), got '$campaign_name'" ;;
  *[!A-Za-z0-9._-]*) die "run_name must match [A-Za-z0-9._-]+ (the runner rejects anything else), got '$campaign_name'" ;;
esac
# The ref is interpolated into the TOML, so keep it to git-ref characters.
case "$TARGET_REF" in
  -*) die "ref must not start with '-' (it is read as a command option), got '$TARGET_REF'" ;;
  *[!A-Za-z0-9._/-]*) die "ref must match [A-Za-z0-9._/-]+, got '$TARGET_REF'" ;;
esac
# The benchmarks ref never reaches the TOML: it is interpolated into the box
# user-data, which runs as root, so it is gated here rather than only quoted.
case "$BENCHMARKS_REF" in
  -*) die "benchmarks_ref must not start with '-' (it is read as a command option), got '$BENCHMARKS_REF'" ;;
  *[!A-Za-z0-9._/-]*) die "benchmarks_ref must match [A-Za-z0-9._/-]+, got '$BENCHMARKS_REF'" ;;
esac

# Wall clock is dominated by the paced hot legs: one per dataset per run, each
# replaying `ledgers` ledgers at the phase close interval. Cold and query legs
# are unpaced and fall inside the setup margin.
if [ "$INGEST" = hot ] || [ "$INGEST" = both ]; then
  if [ "$HOT_NUM_LEDGERS" -gt 0 ]; then
    ledgers="$HOT_NUM_LEDGERS"
  else
    ledgers=10000
  fi
  dataset_count=0
  for _p in $profiles; do
    dataset_count=$((dataset_count + 1))
  done
  hot_secs=$(((dataset_count * RUNS * ledgers * interval_ms + 999) / 1000))
else
  hot_secs=0
fi
budget_minutes=$(((hot_secs + 59) / 60 + SETUP_MARGIN_MINUTES))

if [ "$budget_minutes" -gt "$CAPACITY_MINUTES" ]; then
  die "estimated budget ${budget_minutes}m exceeds the ${CAPACITY_MINUTES}m relay-chain ceiling;" \
    "shrink runs (${RUNS}), cap hot_num_ledgers (${HOT_NUM_LEDGERS}), or split the phase into separate dispatches"
fi

deadline_epoch=$((NOW_EPOCH + budget_minutes * 60))
# The box outlives the relay chain so a stuck run still uploads its log.
self_terminate_minutes=$((budget_minutes + 30))

query_bool=false
[ "$QUERY" = yes ] && query_bool=true

toml_file="$(mktemp "${TMPDIR:-/tmp}/campaign-XXXXXX")"
{
  printf 'name = "%s"\n' "$campaign_name"
  printf 'ref = "%s"\n' "$TARGET_REF"
  printf 'ingest = "%s"\n' "$INGEST"
  printf 'query = %s\n' "$query_bool"
  printf 'close_interval = "%s"\n' "$close_interval"
  printf 'runs = %s\n' "$RUNS"
  printf 'workers = %s\n' "$workers"
  printf 'hot_num_ledgers = %s\n' "$HOT_NUM_LEDGERS"
  printf 'publish_uri = "%s"\n' "$PUBLISH_URI"
  for profile in $profiles; do
    printf '\n[[dataset]]\n'
    printf 'name = "%s"\n' "$profile"
    printf 'kind = "packs-s3"\n'
    printf 'location = "%s/%s/packs/cold"\n' "$INPUTS_PREFIX" "$profile"
    printf 'chunks = [1]\n'
  done
} > "$toml_file"

{
  echo "toml_b64=$(base64 < "$toml_file" | tr -d '\n')"
  echo "budget_minutes=$budget_minutes"
  echo "deadline_epoch=$deadline_epoch"
  echo "instance_type=$instance_type"
  echo "workers=$workers"
  echo "self_terminate_minutes=$self_terminate_minutes"
  echo "campaign_name=$campaign_name"
} >> "${GITHUB_OUTPUT:-/dev/stdout}"

{
  echo "campaign $campaign_name: phase $PHASE (close_interval $close_interval), ingest=$INGEST query=$QUERY"
  echo "datasets: $profiles"
  echo "machine $MACHINE = $instance_type, workers $workers, runs $RUNS, hot_num_ledgers $HOT_NUM_LEDGERS"
  echo "paced hot legs ~$((hot_secs / 60))m; budget ${budget_minutes}m of ${CAPACITY_MINUTES}m capacity"
  echo "deadline $(fmt_epoch "$deadline_epoch"); box self-terminates after ${self_terminate_minutes}m"
  echo "rendered config: $toml_file"
} >&2
