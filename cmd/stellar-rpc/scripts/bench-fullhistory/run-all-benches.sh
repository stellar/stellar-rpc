#!/usr/bin/env bash
#
# run-all-benches.sh — drive the full read + ingest bench suite in
# bench-fullhistory.
#
# Builds the bench binary once and runs:
#
# Read benches — cold + hot variants of:
#   - ledgers (sweeps --n=1,10,20: single-ledger fetch, mid-page, full page)
#   - txpage  (ledger-range transaction page lookup)
#   - txhash  (single-hash getTransaction lookup)
#   - events  (eventstore.Query)
# Each read bench does a 1,4,8,16 --query-concurrency sweep.
#
# Ingest benches (skip the whole section with RUN_INGEST=0):
#   - hot-ingest         (single chunk -> fresh RocksDB hot store)
#   - cold-ingest        (multi-chunk  -> fresh cold packfiles)
#   - build-txhash-index (phase 2: cold .bin files -> queryable .idx)
#
# By default ingest writes to scratch (INGEST_OUT_ROOT) and the reads use the
# prebuilt fixtures, so the two halves are independent. Set INGEST_FIRST=1 to
# instead ingest FIRST and point every read bench at the freshly-ingested
# stores — a self-contained build+read run needing no prebuilt data, only
# INGEST_SOURCE_COLD_DIR (the raw-ledger packfile seed).
#
# All write CSVs + logs under $OUT_DIR. Adjust the paths/values at the top
# before running.

set -euo pipefail

# ---------------------------------------------------------------------------
# config — edit these for your machine.
# ---------------------------------------------------------------------------

# Where bench data lives.
COLD_LEDGERS_DIR="/mnt/nvme/ledgers/cold"
COLD_TXHASH_MPHF="/mnt/nvme/ledgers/txhash-cold/00005000-00005999.idx"
# NB: cold-events wants the *bucket* dir (.../events/<5-digit bucket>), not
# the events root — it globs *-events.pack directly in this dir.
COLD_EVENTS_DIR="/mnt/nvme/bench-ingest/sweep-cold-2026-05-27/events/00005"

# Hot stores are tied to a single chunk; the sweep-hot dir is consistent
# across all three data types (ledgers/txhash/events) for chunk 5860.
HOT_CHUNK=5860
HOT_LEDGERS_DIR="/mnt/nvme/bench-ingest/sweep-hot-2026-05-27/ledgers"
HOT_TXHASH_DIR="/mnt/nvme/bench-ingest/sweep-hot-2026-05-27/txhash"
HOT_EVENTS_DIR="/mnt/nvme/bench-ingest/sweep-hot-2026-05-27/events/hot"

# Sweep + sizing knobs.
QUERY_CONCURRENCY="1,4,8,16"
LEDGER_NS=(1 10 20)            # --n values for {cold,hot}-ledgers
LEDGERS_ITERS=60
TXPAGE_ITERS=200
TXHASH_ITERS=1000
EVENTS_ITERS=500
PAGE_SIZE=20
SEED=1

# Ingest knobs. Ingest re-reads raw ledgers from a cold packfile *source*
# (INGEST_SOURCE_COLD_DIR) and writes fresh hot/cold stores under
# INGEST_OUT_ROOT/{hot,cold} (wiped each run).
#
#   RUN_INGEST=0    skip the ingest benches entirely.
#   INGEST_FIRST=1  ingest BEFORE the reads and point every hot/cold read
#                   bench at the freshly-ingested stores instead of the
#                   prebuilt fixtures above. Self-contained: the only input is
#                   INGEST_SOURCE_COLD_DIR (raw-ledger packfile seed). Implies
#                   the ingest section runs.
RUN_INGEST="${RUN_INGEST:-1}"
INGEST_FIRST="${INGEST_FIRST:-0}"
# Seed packfile for ingest. Captured from COLD_LEDGERS_DIR here, BEFORE the
# bootstrap block below may repoint COLD_LEDGERS_DIR at the ingest output.
INGEST_SOURCE_COLD_DIR="${INGEST_SOURCE_COLD_DIR:-${COLD_LEDGERS_DIR}}"
INGEST_TYPES="ledgers,txhash,events"
INGEST_CHUNK="${HOT_CHUNK}"                     # first chunk to ingest
COLD_INGEST_NUM_CHUNKS=16                       # cold-ingest consecutive chunks
COLD_INGEST_CHUNK_WORKERS=8                     # cold-ingest chunk concurrency
INGEST_OUT_ROOT="${INGEST_OUT_ROOT:-/mnt/nvme/bench-ingest-out}"
HOT_INGEST_OUT="${INGEST_OUT_ROOT}/hot"         # hot-ingest --hot-dir
COLD_INGEST_OUT="${INGEST_OUT_ROOT}/cold"       # cold-ingest --cold-out-dir
COLD_INGEST_IDX="${COLD_INGEST_OUT}/txhash/index.idx"  # build-txhash-index output

# Bootstrap mode: the reads consume what we're about to ingest, so repoint the
# read-bench dirs at the ingest output. hot-ingest/cold-ingest create per-type
# subdirs {ledgers,txhash,events}; the hot events store sits at <root>/events
# and the read bench opens it at that same level (no extra /hot segment).
if [[ "${INGEST_FIRST}" == "1" ]]; then
  RUN_INGEST=1
  HOT_LEDGERS_DIR="${HOT_INGEST_OUT}/ledgers"
  HOT_TXHASH_DIR="${HOT_INGEST_OUT}/txhash"
  HOT_EVENTS_DIR="${HOT_INGEST_OUT}/events"
  COLD_LEDGERS_DIR="${COLD_INGEST_OUT}/ledgers"
  COLD_TXHASH_MPHF="${COLD_INGEST_IDX}"
  # cold-events is bucket-scoped: --cold-events-dir must be the bucket dir
  # (<root>/events/<5-digit bucket>), NOT the events root — the bench globs
  # *-events.pack directly in that dir and opens it as the bucketDir. All
  # chunks in one cold-ingest run share a bucket (BucketID = chunk/1000) as
  # long as the range doesn't cross a 1000-chunk boundary.
  COLD_EVENTS_DIR="${COLD_INGEST_OUT}/events/$(printf '%05d' $((INGEST_CHUNK / 1000)))"
fi

# Output. Timestamped subdir so reruns don't clobber prior CSVs.
OUT_ROOT="${OUT_ROOT:-bench-out}"
OUT_DIR="${OUT_ROOT}/run-$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="${OUT_DIR}/logs"

# Go binary (override with GO=...). Falls back to `go` on PATH.
GO="${GO:-$(command -v go || echo /home/simon/go-toolchain/go/bin/go)}"

# grocksdb cgo deps. The system librocksdb (8.9) is too old for grocksdb
# v1.10.x; this points at a user-local v10.x build. Same story for zstd:
# packfile zstd codec needs >=1.5.7 at runtime but system has 1.5.5.
# Override either prefix if yours lives elsewhere.
ROCKSDB_PREFIX="${ROCKSDB_PREFIX:-/home/simon/.rocksdb}"
ZSTD_PREFIX="${ZSTD_PREFIX:-/home/simon/.zstd}"
export CGO_CFLAGS="${CGO_CFLAGS:-} -I${ROCKSDB_PREFIX}/include"
export CGO_LDFLAGS="${CGO_LDFLAGS:-} -L${ROCKSDB_PREFIX}/lib -lrocksdb"
export LD_LIBRARY_PATH="${ZSTD_PREFIX}/lib:${ROCKSDB_PREFIX}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

# ---------------------------------------------------------------------------
# build + setup
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${SCRIPT_DIR}/bench-fullhistory"

mkdir -p "${LOG_DIR}"

echo "==> building bench-fullhistory binary (using ${GO})"
(cd "${SCRIPT_DIR}" && "${GO}" build -o "${BIN}" .)

echo "==> output dir: ${OUT_DIR}"

# Run one bench: $1 = sub-command, remaining args = flags. Tees stdout to a
# per-run log; non-zero exit prints a marker but does not abort the suite,
# so a single missing data dir doesn't cancel the rest.
run_bench() {
  local cmd="$1"; shift
  local label="$1"; shift
  local log="${LOG_DIR}/${label}.log"
  echo
  echo "=============================================================="
  echo "==> ${label}: bench-fullhistory ${cmd} $*"
  echo "    log: ${log}"
  echo "=============================================================="
  if ! "${BIN}" "${cmd}" "$@" --out="${OUT_DIR}" --seed="${SEED}" \
        --query-concurrency="${QUERY_CONCURRENCY}" 2>&1 | tee "${log}"; then
    echo "!! ${label} FAILED (continuing)"
  fi
}

# Run one ingest bench: same teeing/continue-on-failure behavior as
# run_bench, but ingest commands take neither --seed nor --query-concurrency.
run_ingest() {
  local cmd="$1"; shift
  local label="$1"; shift
  local log="${LOG_DIR}/${label}.log"
  echo
  echo "=============================================================="
  echo "==> ${label}: bench-fullhistory ${cmd} $*"
  echo "    log: ${log}"
  echo "=============================================================="
  if ! "${BIN}" "${cmd}" "$@" --out="${OUT_DIR}" 2>&1 | tee "${log}"; then
    echo "!! ${label} FAILED (continuing)"
  fi
}

# Build fresh hot + cold stores from the cold packfile seed. Output dirs must
# be empty, so each is wiped first. In INGEST_FIRST mode this runs before the
# reads (feeding them); otherwise after, as an independent measurement.
do_ingest() {
  # hot tier — single chunk -> fresh RocksDB store.
  rm -rf "${HOT_INGEST_OUT}"
  run_ingest hot-ingest "hot-ingest" \
    --types="${INGEST_TYPES}" --source=pack \
    --cold-dir="${INGEST_SOURCE_COLD_DIR}" \
    --chunk="${INGEST_CHUNK}" --hot-dir="${HOT_INGEST_OUT}" \
    --xdr-views

  # cold tier — multi-chunk -> fresh packfiles.
  rm -rf "${COLD_INGEST_OUT}"
  run_ingest cold-ingest "cold-ingest" \
    --types="${INGEST_TYPES}" --source=pack \
    --cold-dir="${INGEST_SOURCE_COLD_DIR}" \
    --chunk="${INGEST_CHUNK}" --num-chunks="${COLD_INGEST_NUM_CHUNKS}" \
    --chunk-workers="${COLD_INGEST_CHUNK_WORKERS}" \
    --cold-out-dir="${COLD_INGEST_OUT}" --xdr-views

  # cold txhash phase 2 — merge the .bin files cold-ingest wrote into a
  # single queryable .idx (only runs if txhash was one of INGEST_TYPES).
  if [[ "${INGEST_TYPES}" == *txhash* ]]; then
    run_ingest build-txhash-index "build-txhash-index" \
      --in-dir="${COLD_INGEST_OUT}/txhash" \
      --idx-out="${COLD_INGEST_IDX}"
  fi
}

# ---------------------------------------------------------------------------
# ingest-first: build the stores the reads below will consume.
# ---------------------------------------------------------------------------

if [[ "${INGEST_FIRST}" == "1" ]]; then
  echo
  echo "==> INGEST_FIRST=1: ingesting into ${INGEST_OUT_ROOT} before reads"
  do_ingest
fi

# ---------------------------------------------------------------------------
# ledger reads — sweep --n across {1, 10, 20}
# ---------------------------------------------------------------------------

for n in "${LEDGER_NS[@]}"; do
  run_bench cold-ledgers "cold-ledgers-n${n}" \
    --cold-dir="${COLD_LEDGERS_DIR}" \
    --n="${n}" --iters="${LEDGERS_ITERS}"

  run_bench hot-ledgers "hot-ledgers-n${n}" \
    --hot-dir="${HOT_LEDGERS_DIR}" --chunk="${HOT_CHUNK}" \
    --n="${n}" --iters="${LEDGERS_ITERS}"
done

# ---------------------------------------------------------------------------
# transaction-page reads (ledger-range tx lookup)
# ---------------------------------------------------------------------------

run_bench cold-txpage "cold-txpage" \
  --cold-dir="${COLD_LEDGERS_DIR}" \
  --page-size="${PAGE_SIZE}" --iters="${TXPAGE_ITERS}"

run_bench hot-txpage "hot-txpage" \
  --hot-dir="${HOT_LEDGERS_DIR}" --chunk="${HOT_CHUNK}" \
  --page-size="${PAGE_SIZE}" --iters="${TXPAGE_ITERS}"

# ---------------------------------------------------------------------------
# single-hash getTransaction lookup
# ---------------------------------------------------------------------------

run_bench cold-txhash "cold-txhash" \
  --cold-dir="${COLD_LEDGERS_DIR}" \
  --txhash-cold-mphf="${COLD_TXHASH_MPHF}" \
  --iters="${TXHASH_ITERS}"

run_bench hot-txhash "hot-txhash" \
  --hot-dir="${HOT_LEDGERS_DIR}" \
  --txhash-hot="${HOT_TXHASH_DIR}" \
  --cold-dir="${COLD_LEDGERS_DIR}" \
  --chunk="${HOT_CHUNK}" \
  --iters="${TXHASH_ITERS}"

# ---------------------------------------------------------------------------
# eventstore.Query
# ---------------------------------------------------------------------------

run_bench cold-events "cold-events" \
  --cold-events-dir="${COLD_EVENTS_DIR}" \
  --iters="${EVENTS_ITERS}"

run_bench hot-events "hot-events" \
  --hot-dir="${HOT_EVENTS_DIR}" --chunk="${HOT_CHUNK}" \
  --iters="${EVENTS_ITERS}"

# ---------------------------------------------------------------------------
# ingest — in INGEST_FIRST mode it already ran above (feeding the reads);
# otherwise run it now as an independent measurement (RUN_INGEST=0 skips).
# ---------------------------------------------------------------------------

if [[ "${INGEST_FIRST}" == "1" ]]; then
  echo
  echo "==> ingest already ran before the reads (INGEST_FIRST=1)"
elif [[ "${RUN_INGEST}" != "0" ]]; then
  do_ingest
else
  echo
  echo "==> RUN_INGEST=0, skipping ingest benches"
fi

echo
echo "=============================================================="
echo "==> done. CSVs + logs under ${OUT_DIR}"
echo "=============================================================="
