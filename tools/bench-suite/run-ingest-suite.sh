#!/usr/bin/env bash
#
# run-ingest-suite.sh — run the full-history INGESTION benchmark across synthetic
# apply-load profiles (sac/token/soroswap), laying the CSVs out exactly as the
# stellar-rpc-benchmarks converter (convert.py, KIND=synthetic) expects:
#
#   <OUT>/synth-cold-<profile>-run<N>/  driver.csv ledgers.csv txhash.csv events.csv
#   <OUT>/synth-hot-<profile>-run<N>/   driver.csv hot.csv
#   <OUT>/machine-metadata.txt
#
# This is stage 2 of the pipeline (see README.md). Stage 1 (generating the pack
# trees per profile) is external; stage 3 (convert + publish) runs in the
# stellar-rpc-benchmarks repo.
#
# Prereqs:
#   - a built stellar-rpc binary   (STELLAR_RPC, default: `stellar-rpc` on PATH)
#   - one synthetic ledger pack tree per profile, {bucket:05d}/{chunk:08d}.pack
#
# Configure the profiles below (name:pack-dir:start-chunk), then:
#   PACKS_ROOT=/data/packs INSTANCE_TYPE=m6id.2xlarge ./run-ingest-suite.sh
#
# Env knobs: OUT, REPS(=5), WORKERS(=4), NUM_CHUNKS(=1), NUM_LEDGERS(=0=whole
# chunk; set e.g. 500 for a cheap smoke run), SCRATCH, STELLAR_RPC, PACKS_ROOT,
# INSTANCE_TYPE, and <PROFILE>_CHUNK per profile.
#
# NOTE on runtime: a full hot run ingests a whole 10k-ledger chunk with one
# synced WriteBatch per ledger — tens of minutes per run. 5 reps × 3 profiles ×
# (cold + hot) is a multi-hour campaign; that is why it runs on a devbox. Use
# NUM_LEDGERS for a quick shape check first.
set -euo pipefail

STELLAR_RPC="${STELLAR_RPC:-stellar-rpc}"
OUT="${OUT:-./results-in/synthetic-$(date +%F)}"
REPS="${REPS:-5}"
WORKERS="${WORKERS:-4}"
NUM_CHUNKS="${NUM_CHUNKS:-1}"
NUM_LEDGERS="${NUM_LEDGERS:-0}" # 0 = whole range (hot); >0 caps the run for a smoke test
SCRATCH="${SCRATCH:-$(mktemp -d)}"
PACKS_ROOT="${PACKS_ROOT:-/data/packs}"

# Profiles: "name:pack-dir:start-chunk". Point each pack-dir at that profile's
# {bucket:05d}/{chunk:08d}.pack tree and set the chunk id it holds.
PROFILES=(
	"sac:${PACKS_ROOT}/sac:${SAC_CHUNK:-0}"
	"token:${PACKS_ROOT}/token:${TOKEN_CHUNK:-0}"
	"soroswap:${PACKS_ROOT}/soroswap:${SOROSWAP_CHUNK:-0}"
)

mkdir -p "$OUT"

# machine-metadata.txt: convert.py globs *machine-metadata*.txt, keeps the whole
# file as machine.raw, and best-effort parses instance-type, lscpu, and a
# "repo: <sha> (branch)" line for build provenance.
{
	date -u
	echo "instance-type: ${INSTANCE_TYPE:-unknown}"
	uname -a
	lscpu 2>/dev/null || sysctl -n machdep.cpu.brand_string 2>/dev/null || true
	echo "repo: $(git rev-parse HEAD) ($(git rev-parse --abbrev-ref HEAD))"
} >"$OUT/machine-metadata.txt"

for entry in "${PROFILES[@]}"; do
	IFS=: read -r name packdir chunk <<<"$entry"
	if [ ! -d "$packdir" ]; then
		echo "ERROR: pack dir for profile '$name' not found: $packdir" >&2
		exit 1
	fi
	for r in $(seq 1 "$REPS"); do
		echo ">>> $name  cold  run $r"
		"$STELLAR_RPC" bench-ingest cold \
			--source=pack --pack-dir="$packdir" \
			--start-chunk="$chunk" --num-chunks="$NUM_CHUNKS" --workers="$WORKERS" \
			--cold-out-dir="$SCRATCH/cold-$name-$r" \
			--out="$OUT/synth-cold-$name-run$r"
		rm -rf "$SCRATCH/cold-$name-$r"

		echo ">>> $name  hot   run $r"
		"$STELLAR_RPC" bench-ingest hot \
			--source=pack --pack-dir="$packdir" \
			--start-chunk="$chunk" --num-ledgers="$NUM_LEDGERS" \
			--hot-dir="$SCRATCH/hot-$name-$r" \
			--out="$OUT/synth-hot-$name-run$r"
		rm -rf "$SCRATCH/hot-$name-$r"
	done
done

cat <<EOF

Done. Results laid out in: $OUT
Next (stage 3, in the stellar-rpc-benchmarks repo):

  make convert RESULTS=$OUT RUN_ID=synthetic-$(date +%F) \\
    RUN_NAME="Synthetic apply-load — sac/token/soroswap" \\
    KIND=synthetic RUN_DATE=$(date +%F) \\
    FACTS=converter/facts/<your-sidecar>.json
  make serve   # view at http://localhost:8000
EOF
