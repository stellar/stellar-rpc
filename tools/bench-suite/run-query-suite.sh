#!/usr/bin/env bash
#
# run-query-suite.sh — full-history QUERY benchmark across synthetic profiles.
#
# For each profile it boots the serve-harness (the TestServeProfileForBench gate in
# the fullhistory package, prebuilt into fullhistory.test) which backfills the
# profile's ledger packs into a fresh servable data dir (cold tier) plus a
# synthetic hot tier, binds a [serve] port, and holds. Then it points fhbench at
# that server for every (endpoint, tier) and captures the latency tables + a
# /metrics snapshot.
#
# Runs ONE profile at a time (each cold freeze peaks ~20-30 GB) — do NOT overlap
# with the ingest campaign. See tools/fhbench/README.md for fhbench itself.
#
# Env knobs: OUT, PORT(=8100), DURATION(=45s), HOT_LEDGERS(=5200), CONCURRENCY(=8),
# PASSPHRASE(=Public Global Stellar Network ; September 2015), READY_TIMEOUT(=1800),
# TESTBIN, FHBENCH, PACKS_ROOT.
set -euo pipefail

WORK=/mnt/nvme/ledgers/fhbench-work
TESTBIN="${TESTBIN:-$WORK/bin/fullhistory.test}"
FHBENCH="${FHBENCH:-$WORK/bin/fhbench}"
PACKS_ROOT="${PACKS_ROOT:-$WORK/packs-root}"
OUT="${OUT:-$WORK/results/query-$(date +%F)}"
PORT="${PORT:-8100}"
DURATION="${DURATION:-45s}"
HOT_LEDGERS="${HOT_LEDGERS:-5200}"
CONCURRENCY="${CONCURRENCY:-8}"
EVENT_TERMS="${EVENT_TERMS:-1,4,8,15}"
PASSPHRASE="${PASSPHRASE:-Public Global Stellar Network ; September 2015}"
READY_TIMEOUT="${READY_TIMEOUT:-3600}"

export LD_LIBRARY_PATH="/home/simon/.zstd/lib:/home/simon/.rocksdb/lib:${LD_LIBRARY_PATH:-}"

# profile:last-chunk (packs live under $PACKS_ROOT/<profile>, chunk ids start at 1)
PROFILES=(
	"sac:1"
	"token:1"
	"soroswap:2"
)
ENDPOINTS=(getLedgers getTransactions getTransaction getEvents)

mkdir -p "$OUT"
echo "query suite -> $OUT (port $PORT, duration $DURATION, hot_ledgers $HOT_LEDGERS)"

for entry in "${PROFILES[@]}"; do
	IFS=: read -r name lastchunk <<<"$entry"
	packs="$PACKS_ROOT/$name"
	datadir="$WORK/serve-data/$name"
	pdir="$OUT/$name"
	mkdir -p "$pdir"
	rm -rf "$datadir"
	mkdir -p "$datadir"

	echo "=== $name: booting serve-harness (cold chunks 1..$lastchunk) ==="
	FHBENCH_PROFILE_LEDGERS="$packs" \
	FHBENCH_DATA_DIR="$datadir" \
	FHBENCH_SERVE_ADDR="127.0.0.1:$PORT" \
	FHBENCH_FIRST_CHUNK=1 \
	FHBENCH_LAST_CHUNK="$lastchunk" \
	FHBENCH_HOT_LEDGERS="$HOT_LEDGERS" \
	FHBENCH_HOLD_SEC=3600 \
	FHBENCH_PASSPHRASE="$PASSPHRASE" \
		"$TESTBIN" -test.run '^TestServeProfileForBench$' -test.v -test.timeout 0 \
		>"$pdir/harness.log" 2>&1 &
	harness_pid=$!

	# Wait for READY (cold freeze can take many minutes).
	base="http://127.0.0.1:$PORT"
	ready=0
	for _ in $(seq 1 "$READY_TIMEOUT"); do
		if ! kill -0 "$harness_pid" 2>/dev/null; then
			echo "ERROR: harness for $name exited before READY; see $pdir/harness.log" >&2
			tail -20 "$pdir/harness.log" >&2 || true
			break
		fi
		if curl -sf "$base/ready" >/dev/null 2>&1; then ready=1; break; fi
		sleep 1
	done
	if [ "$ready" -ne 1 ]; then
		echo "ERROR: $name never became ready" >&2
		kill "$harness_pid" 2>/dev/null || true
		wait "$harness_pid" 2>/dev/null || true
		continue
	fi
	echo "    $name READY; running fhbench"

	# Served-range snapshot + a pre-run metrics scrape.
	curl -s "$base/metrics" >"$pdir/metrics-before.txt" 2>/dev/null || true

	# fhbench per endpoint, both tiers; aggregate tables into one file.
	report="$pdir/fhbench.txt"
	: >"$report"
	for ep in "${ENDPOINTS[@]}"; do
		echo "### $name $ep (both tiers)" >>"$report"
		"$FHBENCH" --url "$base" --endpoint "$ep" --tier both \
			--concurrency "$CONCURRENCY" --duration "$DURATION" --limit 50 \
			>>"$report" 2>>"$pdir/fhbench.err" || echo "fhbench $ep failed (see fhbench.err)" >>"$report"
		echo >>"$report"
	done

	# getEvents index-term sweep: the SELECTIVE path (term-count -> latency curve),
	# vs the unselective type-filter worst case measured in the loop above.
	echo "### $name getEvents index-term sweep (--event-terms $EVENT_TERMS, both tiers)" >>"$report"
	"$FHBENCH" --url "$base" --endpoint getEvents --tier both \
		--concurrency "$CONCURRENCY" --duration "$DURATION" --limit 50 \
		--event-terms "$EVENT_TERMS" \
		>>"$report" 2>>"$pdir/fhbench.err" || echo "fhbench term-sweep failed (see fhbench.err)" >>"$report"
	echo >>"$report"

	curl -s "$base/metrics" >"$pdir/metrics-after.txt" 2>/dev/null || true

	echo "    $name done; stopping harness"
	kill "$harness_pid" 2>/dev/null || true
	wait "$harness_pid" 2>/dev/null || true
	rm -rf "$datadir"
	sleep 3 # let the port free
done

echo
echo "Query suite complete. Per-profile fhbench tables + metrics under: $OUT"
