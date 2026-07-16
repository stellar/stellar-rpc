#!/usr/bin/env bash
#
# run-e2e-latency-suite.sh — ingest→query LATENCY across synthetic profiles on
# REAL ledgers, via the TestServeE2E_ProfileLatency gate in the fullhistory
# package (prebuilt into fullhistory.test).
#
# For each profile it live-ingests a small window of the profile's real frozen
# ledgers through the daemon's hot path (no cold freeze — cheap, seconds not
# minutes), measures per-ledger ingest→query visibility, then runs a dense
# closed-loop query pass for a real p99 per endpoint. This is the fast,
# realistic-workload counterpart to the toy TestServeE2E_IngestToQueryLatency.
#
# IMPORTANT — warmup: the head of a profile chunk is warmup (contract deploys /
# account setup) and far lighter than steady state. Set WARMUP to skip past it
# so the measured window reflects representative load. Ingesting WARMUP+WINDOW
# heavy ledgers is the cost here (tens of ms each), so keep WARMUP modest.
#
# Env knobs: OUT, WINDOW(=20), WARMUP(=200), REPS(=500), FIRST_CHUNK(=1),
# PASSPHRASE, TESTBIN, LEDGERS_ROOTS (space-separated "name:ledgers-root" pairs;
# a ledgers-root is the dir whose <bucket>/<chunk>.pack tree holds the profile's
# ledgers — either a packs-root/<profile> or a served data dir's .../ledgers).
set -euo pipefail

WORK=/mnt/nvme/ledgers/fhbench-work
TESTBIN="${TESTBIN:-$WORK/bin/fullhistory.test}"
OUT="${OUT:-$WORK/results/e2e-latency-$(date +%F)}"
WINDOW="${WINDOW:-20}"
WARMUP="${WARMUP:-200}"
REPS="${REPS:-500}"
FIRST_CHUNK="${FIRST_CHUNK:-1}"
PASSPHRASE="${PASSPHRASE:-Public Global Stellar Network ; September 2015}"

export LD_LIBRARY_PATH="/home/simon/.zstd/lib:/home/simon/.rocksdb/lib:${LD_LIBRARY_PATH:-}"

# "name:ledgers-root" per profile. Default points at the apply-load pack trees;
# override LEDGERS_ROOTS to run against a different packs-root/<profile> tree.
# A ledgers-root is the dir whose <bucket>/<chunk>.pack tree holds the profile's
# ledgers (e.g. an apply-load out/<profile>/cold/ledgers, or a served data dir's
# .../ledgers). Only WARMUP+WINDOW ledgers are read, so a partial pack suffices.
APPLY_LOAD="${APPLY_LOAD:-/mnt/nvme/ledgers/apply-load-20k/out}"
DEFAULT_ROOTS="sac:$APPLY_LOAD/sac/cold/ledgers token:$APPLY_LOAD/token/cold/ledgers soroswap:$APPLY_LOAD/soroswap/cold/ledgers"
read -r -a ROOTS <<<"${LEDGERS_ROOTS:-$DEFAULT_ROOTS}"

mkdir -p "$OUT"
echo "e2e latency suite -> $OUT (window=$WINDOW warmup=$WARMUP reps=$REPS)"

for entry in "${ROOTS[@]}"; do
	IFS=: read -r name root <<<"$entry"
	if [ ! -f "$root/00000/$(printf '%08d' "$FIRST_CHUNK").pack" ]; then
		echo "SKIP $name: no chunk $FIRST_CHUNK pack under $root" >&2
		continue
	fi
	echo "=== $name: ingest→query latency (window $WINDOW, warmup $WARMUP) ==="
	FHBENCH_PROFILE_LEDGERS="$root" \
	FHBENCH_PROFILE_NAME="$name" \
	FHBENCH_FIRST_CHUNK="$FIRST_CHUNK" \
	FHBENCH_WINDOW_LEDGERS="$WINDOW" \
	FHBENCH_WARMUP_LEDGERS="$WARMUP" \
	FHBENCH_QUERY_REPS="$REPS" \
	FHBENCH_PASSPHRASE="$PASSPHRASE" \
		"$TESTBIN" -test.run '^TestServeE2E_ProfileLatency$' -test.v -test.timeout 0 \
		>"$OUT/$name.txt" 2>&1 || echo "  $name FAILED (see $OUT/$name.txt)"
	# Pull just the report lines into a combined summary.
	grep -E "visibility|span|full |ingest |visible |query-latency|endpoint|getLedgers|getTransaction|getEvents" \
		"$OUT/$name.txt" | grep -v "level=" >>"$OUT/summary.txt" || true
	echo >>"$OUT/summary.txt"
done

echo
echo "Done. Per-profile logs + summary.txt under: $OUT"
