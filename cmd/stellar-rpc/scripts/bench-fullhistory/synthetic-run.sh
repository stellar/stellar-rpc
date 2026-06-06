#!/usr/bin/env bash
#
# synthetic-run.sh — end-to-end driver: generate synthetic cold stores for one
# or more apply-load profiles, then run the read bench suite on them.
#
#   for each PROFILE:  apply-load-gen.sh  (apply-load -> meta -> cold packfiles)
#   then:              bench-suite.sh     (cold-* / hot-* read benches -> CSVs)
#
# Profiles run SEQUENTIALLY by default. This matters: each dense apply-load holds
# in-memory soroban state that GROWS with ledger count (~8.5 MB/ledger at
# 6000 SAC tx/ledger), so running profiles in parallel multiplies peak RAM and
# can OOM. See MEMORY below.
#
# ---- MEMORY (read this before picking NUM_LEDGERS) --------------------------
# Peak RSS ≈ density(tx/ledger) × NUM_LEDGERS × ~1.4 KB/tx of live state.
# Measured on c6id.8xlarge (61 GB): sac @6000 tx/ledger hit ~32 GB at 3,760
# ledgers and projected ~85 GB at 10,000 -> OOM. Rough guidance per profile:
#   RAM 61 GB  -> sac/token ~6,000 ledgers; soroswap ~20,000 (it's 1,500 tx/ledger)
#   RAM 128 GB -> sac/token ~14,000; soroswap full chunks easily
#   RAM 256 GB -> sac/token ~28,000 (3 chunks); etc.
# A full 10k-ledger chunk of 10k-TPS SAC needs ~96-128 GB RAM. Size NUM_LEDGERS
# to your box, or run PARALLEL=1 only when total peak RSS fits in RAM.
#
# ---- USAGE -----------------------------------------------------------------
#   CORE_BIN=/usr/bin/stellar-core \           # a BUILD_TESTS (~buildtests) core
#   OUT_ROOT=/mnt/nvme/synth \                 # work + cold output (use fast local disk)
#   PROFILES="sac token soroswap" \
#   NUM_LEDGERS=6000 \                         # per profile; size to RAM (see above)
#   PARALLEL=0 \                               # 1 = all profiles at once (watch RAM!)
#   GCS_DEST=gs://bucket/path \                # optional: upload cold stores + results
#     ./synthetic-run.sh
#
# BENCH_BIN is auto-built from this dir if unset (needs Go + the RocksDB cgo deps;
# see SYNTHETIC-LEDGERS.md). On Linux, export LD_LIBRARY_PATH to the RocksDB .so.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_BIN="${CORE_BIN:-$(command -v stellar-core || true)}"
OUT_ROOT="${OUT_ROOT:-./synthetic-out}"
PROFILES="${PROFILES:-sac token soroswap}"
NUM_LEDGERS="${NUM_LEDGERS:-6000}"
CLUSTERS="${CLUSTERS:-8}"
PARALLEL="${PARALLEL:-0}"
RUN_BENCH="${RUN_BENCH:-1}"
GCS_DEST="${GCS_DEST:-}"
export CLOSE_TIME_MS="${CLOSE_TIME_MS:-600}"
export KEEP_META="${KEEP_META:-0}"

log(){ printf '\033[1;36m[synthetic-run]\033[0m %s\n' "$*" >&2; }
die(){ printf '\033[1;31m[synthetic-run] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

[ -n "$CORE_BIN" ] || die "stellar-core not found; set CORE_BIN (must be a ~buildtests build with apply-load)"
mkdir -p "$OUT_ROOT"; OUT_ROOT="$(cd "$OUT_ROOT" && pwd)"

# Build the bench binary once if not provided.
if [ -z "${BENCH_BIN:-}" ]; then
  BENCH_BIN="$SCRIPT_DIR/bench-fullhistory"
  log "building bench-fullhistory -> $BENCH_BIN"
  ( cd "$SCRIPT_DIR" && go build -o "$BENCH_BIN" . ) || die "go build failed (see SYNTHETIC-LEDGERS.md for cgo/RocksDB setup)"
fi
export CORE_BIN BENCH_BIN OUT_ROOT CLUSTERS NUM_LEDGERS

gen_one(){
  local P="$1"
  log "generate $P (num_ledgers=$NUM_LEDGERS clusters=$CLUSTERS close_ms=$CLOSE_TIME_MS)"
  PROFILE="$P" "$SCRIPT_DIR/apply-load-gen.sh" > "$OUT_ROOT/$P.gen.log" 2>&1
  log "$P generate exit=$? (log: $OUT_ROOT/$P.gen.log)"
}

log "START $(date -u +%FT%TZ) profiles='$PROFILES' parallel=$PARALLEL out=$OUT_ROOT mem-free=$(free -g 2>/dev/null|awk '/Mem/{print $4}')G"
if [ "$PARALLEL" = "1" ]; then
  log "PARALLEL=1: ensure combined peak RSS fits in RAM (see MEMORY note)"
  pids=(); for P in $PROFILES; do gen_one "$P" & pids+=($!); done
  wait "${pids[@]}"
else
  for P in $PROFILES; do gen_one "$P"; done
fi
log "generation done $(date -u +%FT%TZ)"

if [ "$RUN_BENCH" = "1" ]; then
  RESULTS="${RESULTS:-$OUT_ROOT/bench-results/run-$(date -u +%Y%m%dT%H%M%SZ)}"
  log "bench suite -> $RESULTS"
  ROOT="$OUT_ROOT" RESULTS="$RESULTS" PROFILES="$PROFILES" BENCH_BIN="$BENCH_BIN" \
    bash "$SCRIPT_DIR/bench-suite.sh" || log "bench-suite returned nonzero"
fi

if [ -n "$GCS_DEST" ]; then
  command -v gsutil >/dev/null || die "GCS_DEST set but gsutil not found"
  log "uploading cold stores + results to $GCS_DEST"
  for P in $PROFILES; do
    [ -d "$OUT_ROOT/$P/cold" ] && gsutil -m cp -r "$OUT_ROOT/$P/cold" "$GCS_DEST/$P/cold"
    [ -f "$OUT_ROOT/$P/work/apply-load.cfg" ] && gsutil cp "$OUT_ROOT/$P/work/apply-load.cfg" "$GCS_DEST/$P/apply-load.cfg"
  done
  [ -d "${RESULTS:-}" ] && gsutil -m cp -r "$RESULTS" "$GCS_DEST/bench-results"
  log "upload done -> $GCS_DEST"
fi
log "DONE $(date -u +%FT%TZ)"
