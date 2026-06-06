#!/usr/bin/env bash
#
# bench-suite.sh — run the bench-fullhistory READ suite against one or more
# synthetic cold stores produced by apply-load-gen.sh (and a hot store built
# from each cold pack). Writes per-profile CSVs + logs under RESULTS.
#
# Layout expected (created by apply-load-gen.sh / synthetic-run.sh):
#   $ROOT/<profile>/cold/{ledgers,txhash.idx,events/00000}
#
# Iter counts are env-overridable so the same script does a quick smoke
# (small *_ITERS) or a full run (defaults below). cold-events is skipped for a
# profile whose events are not 4-topic (apply-load's custom_token); the bench
# itself errors out cleanly otherwise.
#
# Required env: BENCH_BIN (path to the bench-fullhistory binary). On Linux the
# caller must also export LD_LIBRARY_PATH to the RocksDB v10 .so dir.
set -uo pipefail

BENCH_BIN="${BENCH_BIN:?set BENCH_BIN=/path/to/bench-fullhistory}"
ROOT="${ROOT:?set ROOT=<dir containing <profile>/cold>}"
RESULTS="${RESULTS:-$ROOT/bench-results/run-$(date -u +%Y%m%dT%H%M%SZ)}"
PROFILES="${PROFILES:-sac token soroswap}"
QC="${QC:-1,4,8,16}"
LEDGER_NS="${LEDGER_NS:-1 10 20}"
LEDGERS_ITERS="${LEDGERS_ITERS:-60}"
TXPAGE_ITERS="${TXPAGE_ITERS:-200}"
TXHASH_ITERS="${TXHASH_ITERS:-1000}"
EVENTS_ITERS="${EVENTS_ITERS:-500}"
PAGE_SIZE="${PAGE_SIZE:-20}"
HOT="${HOT:-1}"                    # 1 = also build a hot store per profile and run hot-* benches
# Profiles whose events are NOT 4-topic (skip cold/hot-events). apply-load's
# custom_token emits non-4-topic events; sac/soroswap are fine.
NO_EVENTS="${NO_EVENTS:-token}"

mkdir -p "$RESULTS"
echo "bench-suite -> $RESULTS  (profiles: $PROFILES)"

skip_events() { case " $NO_EVENTS " in *" $1 "*) return 0;; *) return 1;; esac; }

for P in $PROFILES; do
  COLD="$ROOT/$P/cold"
  if [ ! -d "$COLD/ledgers" ]; then echo "skip $P (no cold store at $COLD)"; continue; fi
  O="$RESULTS/$P"; mkdir -p "$O"
  echo "================= $P ================="

  # ---- COLD read benches (auto-discover chunk range) ----
  for n in $LEDGER_NS; do
    "$BENCH_BIN" cold-ledgers --cold-dir="$COLD/ledgers" --n="$n" --iters="$LEDGERS_ITERS" \
       --query-concurrency="$QC" --out="$O" > "$O/cold-ledgers-n$n.log" 2>&1 || echo "  cold-ledgers n=$n FAILED"
  done
  for mode in "" "--xdr-views"; do
    tag=$([ -z "$mode" ] && echo roundtrip || echo xdrviews)
    "$BENCH_BIN" cold-txpage --cold-dir="$COLD/ledgers" --page-size="$PAGE_SIZE" --iters="$TXPAGE_ITERS" \
       --query-concurrency="$QC" $mode --out="$O" > "$O/cold-txpage-$tag.log" 2>&1 || echo "  cold-txpage $tag FAILED"
    "$BENCH_BIN" cold-txhash --cold-dir="$COLD/ledgers" --txhash-cold-mphf="$COLD/txhash.idx" --iters="$TXHASH_ITERS" \
       --query-concurrency="$QC" $mode --out="$O" > "$O/cold-txhash-$tag.log" 2>&1 || echo "  cold-txhash $tag FAILED"
  done
  if ! skip_events "$P"; then
    "$BENCH_BIN" cold-events --cold-events-dir="$COLD/events/00000" --iters="$EVENTS_ITERS" \
       --query-concurrency="$QC" --out="$O" > "$O/cold-events.log" 2>&1 || echo "  cold-events FAILED"
  fi

  # ---- HOT: build a hot store from the cold pack (chunk 1), then hot reads ----
  if [ "$HOT" = "1" ]; then
    H="$O/hot"
    "$BENCH_BIN" hot-ingest --types=ledgers,txhash,events --source=pack --cold-dir="$COLD/ledgers" \
       --chunk=1 --hot-dir="$H" --out="$O" > "$O/hot-ingest.log" 2>&1 || echo "  hot-ingest FAILED (skipping hot reads)"
    if [ -d "$H/ledgers" ]; then
      for n in $LEDGER_NS; do
        "$BENCH_BIN" hot-ledgers --hot-dir="$H/ledgers" --chunk=1 --n="$n" --iters="$LEDGERS_ITERS" \
           --query-concurrency="$QC" --out="$O" > "$O/hot-ledgers-n$n.log" 2>&1 || echo "  hot-ledgers n=$n FAILED"
      done
      for mode in "" "--xdr-views"; do
        tag=$([ -z "$mode" ] && echo roundtrip || echo xdrviews)
        "$BENCH_BIN" hot-txpage --hot-dir="$H/ledgers" --chunk=1 --page-size="$PAGE_SIZE" --iters="$TXPAGE_ITERS" \
           --query-concurrency="$QC" $mode --out="$O" > "$O/hot-txpage-$tag.log" 2>&1 || echo "  hot-txpage $tag FAILED"
        "$BENCH_BIN" hot-txhash --hot-dir="$H/ledgers" --txhash-hot="$H/txhash" --cold-dir="$COLD/ledgers" --chunk=1 \
           --iters="$TXHASH_ITERS" --query-concurrency="$QC" $mode --out="$O" > "$O/hot-txhash-$tag.log" 2>&1 || echo "  hot-txhash $tag FAILED"
      done
      if ! skip_events "$P"; then
        "$BENCH_BIN" hot-events --hot-dir="$H/events" --chunk=1 --iters="$EVENTS_ITERS" \
           --query-concurrency="$QC" --out="$O" > "$O/hot-events.log" 2>&1 || echo "  hot-events FAILED"
      fi
    fi
  fi
  echo "  $P done -> $O"
done
echo "ALL BENCHES DONE -> $RESULTS"
