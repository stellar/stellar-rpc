#!/usr/bin/env bash
#
# Config-driven benchmark campaign runner for the full-history bench
# subcommands. It reads a campaign config, validates it, builds the requested
# ref into a versioned binary, prepares each dataset's cold pack tree, and
# runs the configured ingest and query loops. Every benchmark invocation is a
# fresh process with its own --out directory.
#
# Usage:
#   ./scripts/bench-devbox/campaign.sh <path/to/campaign.cfg> [--dry-run]
#
# --dry-run prints every command the campaign would execute, with resolved
# paths and flags. It performs no builds, downloads, or benchmark runs.
#
# Environment:
#   BENCH  storage root for datasets, scratch space, and results
#          (default /mnt/nvme/bench, the devbox NVMe; on other machines set
#          it to a writable path, e.g. BENCH=/tmp/bench)
#
# Results land in $BENCH/results/<NAME>-<sha>-<stamp>/ together with the
# campaign config, the benchmarked binary's identity (binary.txt),
# machine-metadata.txt, and metadata.json. The results directory is bundled to
# /tmp/bench-results-<NAME>-<sha>-<stamp>.tgz (the EBS root on the devbox,
# so the bundle survives an instance stop). When PUBLISH_URI is set the bundle
# is also uploaded to <PUBLISH_URI>/<NAME>-<sha>-<stamp>/ by publish.sh.
#
# Config keys (the config is a sourced bash fragment; set only these):
#   NAME             campaign name (required; charset [A-Za-z0-9._-])
#   REF              git ref to benchmark (default: the current checkout).
#                    The script builds REF into $BENCH/bin/stellar-rpc-<sha>,
#                    restores the original checkout, and uses only that
#                    binary. REF selects the binary under test, nothing else.
#   INGEST           cold | hot | both | none (required)
#   QUERY            yes | no (required). Query-cold runs against each
#                    dataset's frozen pack root. Query-hot needs the hot DB a
#                    hot ingest leaves behind, so it only runs when INGEST is
#                    hot or both.
#   CLOSE_INTERVAL   bench-ingest hot --close-interval (default 0 = unpaced
#                    catch-up; e.g. 2s, 1s, 600ms for phase pacing)
#   RUNS             repetitions per (dataset, chunk) cell (default 5)
#   QC               query concurrency sweep list (default 1,4,16)
#   COLD_ITERS       bench-query cold --iters (default 100)
#   HOT_ITERS        bench-query hot --iters (default 200)
#   WORKERS          bench-ingest cold --workers (default 1)
#   HOT_NUM_LEDGERS  bench-ingest hot --num-ledgers (default 0 = whole range)
#   PUBLISH_URI      object-storage root to publish the finished bundle to
#                    (default empty = no publish). Must be gs:// or s3://; the
#                    bundle lands at <PUBLISH_URI>/<NAME>-<sha>-<stamp>/.
#   DATASETS         bash array of "name|kind|location|chunks" entries.
#                    kind=packs-local: location is a local cold pack root
#                      (the directory that contains ledgers/, events/,
#                      txhash/).
#                    kind=packs-gs: location is a gs:// prefix of the same
#                      tree; fetched once into $BENCH/golden/<name>/.
#                    kind=bsb-s3: location is an S3 bucket path; an untimed
#                      cold backfill materializes $BENCH/golden/<name>/.
#                    kind=fixture: location is the per-chunk ledger count for
#                      bench-ingest fixture (0 = whole chunk; a partial chunk
#                      cannot be frozen, so the count must be 0 or >= 10000).
#                      A generated fixture pack plus an untimed cold ingest
#                      materialize $BENCH/golden/<name>/.
#                    chunks is a space-separated chunk-ID list.
#
# To force a re-fetch of a golden dataset: rm -rf $BENCH/golden/<name>.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
BENCH="${BENCH:-/mnt/nvme/bench}"

die() { echo "error: $*" >&2; exit 1; }
note() { echo "== [$(date -u +%H:%M:%S)] $*"; }

# run CMD...: print the command, then execute it (skipped under --dry-run).
run() {
  printf '  $ %s\n' "$*"
  if [ "$DRY" -eq 0 ]; then
    "$@"
  fi
}

# --- arguments -----------------------------------------------------------------
[ $# -ge 1 ] || die "usage: campaign.sh <path/to/campaign.cfg> [--dry-run]"
CFG_ARG=$1
shift
DRY=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY=1 ;;
    *) die "unknown argument: $arg" ;;
  esac
done
[ -f "$CFG_ARG" ] || die "config not found: $CFG_ARG"
CFG="$(cd "$(dirname "$CFG_ARG")" && pwd)/$(basename "$CFG_ARG")"

if [ "$BENCH" = /mnt/nvme/bench ] && command -v mountpoint >/dev/null 2>&1; then
  mountpoint -q /mnt/nvme || die "/mnt/nvme not mounted — run bootstrap.sh first, or set BENCH"
fi

# --- config: defaults, source, key validation -----------------------------------
NAME=
REF=
INGEST=
QUERY=
CLOSE_INTERVAL=0
RUNS=5
QC=1,4,16
COLD_ITERS=100
HOT_ITERS=200
WORKERS=1
HOT_NUM_LEDGERS=0
PUBLISH_URI=
DATASETS=()

_pre="$(compgen -v | sort)"
# shellcheck disable=SC1090
source "$CFG"
_post="$(compgen -v | sort)"
for v in $(comm -13 <(printf '%s\n' "$_pre") <(printf '%s\n' "$_post")); do
  case "$v" in
    _pre | _post) ;;
    *) die "config: unknown key '$v' (allowed: NAME REF INGEST QUERY CLOSE_INTERVAL RUNS QC COLD_ITERS HOT_ITERS WORKERS HOT_NUM_LEDGERS PUBLISH_URI DATASETS)" ;;
  esac
done

re_name='^[A-Za-z0-9._-]+$'
re_int='^[0-9]+$'
re_qc='^[0-9]+(,[0-9]+)*$'
re_chunks='^[0-9]+( [0-9]+)*$'
re_dur='^(0|([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+)$'

[ -n "$NAME" ] || die "config: NAME is required"
[[ $NAME =~ $re_name ]] || die "config: NAME must match [A-Za-z0-9._-]+ (got '$NAME')"
case "$INGEST" in
  cold | hot | both | none) ;;
  *) die "config: INGEST must be cold|hot|both|none (got '${INGEST:-<unset>}')" ;;
esac
case "$QUERY" in
  yes | no) ;;
  *) die "config: QUERY must be yes|no (got '${QUERY:-<unset>}')" ;;
esac
[[ $CLOSE_INTERVAL =~ $re_dur ]] || die "config: CLOSE_INTERVAL must be a Go duration or 0 (got '$CLOSE_INTERVAL')"
for k in RUNS COLD_ITERS HOT_ITERS WORKERS; do
  [[ ${!k} =~ $re_int ]] && [ "${!k}" -ge 1 ] || die "config: $k must be an integer >= 1 (got '${!k}')"
done
[[ $HOT_NUM_LEDGERS =~ $re_int ]] || die "config: HOT_NUM_LEDGERS must be an integer >= 0 (got '$HOT_NUM_LEDGERS')"
[[ $QC =~ $re_qc ]] || die "config: QC must be a comma-separated integer list (got '$QC')"
[ -z "$PUBLISH_URI" ] || [[ $PUBLISH_URI =~ ^(gs|s3):// ]] || die "config: PUBLISH_URI must be a gs:// or s3:// URI (got '$PUBLISH_URI')"
[ "${#DATASETS[@]}" -ge 1 ] || die "config: DATASETS must list at least one dataset"

# Parse "name|kind|location|chunks" entries into parallel arrays.
DS_NAME=()
DS_KIND=()
DS_LOC=()
DS_CHUNKS=()
DS_ROOT=()
for entry in "${DATASETS[@]}"; do
  IFS='|' read -r d_name d_kind d_loc d_chunks d_extra <<<"$entry"
  [ -z "${d_extra:-}" ] || die "config: dataset entry has more than 4 fields: '$entry'"
  [ -n "${d_chunks:-}" ] || die "config: dataset entry needs 4 pipe-separated fields (name|kind|location|chunks): '$entry'"
  [[ $d_name =~ $re_name ]] || die "config: dataset name must match [A-Za-z0-9._-]+ (got '$d_name')"
  case " ${DS_NAME[*]:-} " in
    *" $d_name "*) die "config: duplicate dataset name '$d_name'" ;;
  esac
  [[ $d_chunks =~ $re_chunks ]] || die "config: dataset '$d_name': chunks must be a space-separated chunk-ID list (got '$d_chunks')"
  case "$d_kind" in
    packs-local)
      d_root=$d_loc
      ;;
    packs-gs)
      [[ $d_loc == gs://* ]] || die "config: dataset '$d_name': packs-gs location must start with gs:// (got '$d_loc')"
      d_root=$BENCH/golden/$d_name
      ;;
    bsb-s3)
      [ -n "$d_loc" ] || die "config: dataset '$d_name': bsb-s3 location must be an S3 bucket path"
      d_root=$BENCH/golden/$d_name
      ;;
    fixture)
      [[ $d_loc =~ $re_int ]] || die "config: dataset '$d_name': fixture location must be the per-chunk ledger count (got '$d_loc')"
      [ "$d_loc" -eq 0 ] || [ "$d_loc" -ge 10000 ] || die "config: dataset '$d_name': fixture ledger count must be 0 or >= 10000 — the cold freeze streams the whole 10,000-ledger chunk (got '$d_loc')"
      d_root=$BENCH/golden/$d_name
      ;;
    *)
      die "config: dataset '$d_name': kind must be packs-local|packs-gs|bsb-s3|fixture (got '$d_kind')"
      ;;
  esac
  DS_NAME+=("$d_name")
  DS_KIND+=("$d_kind")
  DS_LOC+=("$d_loc")
  DS_CHUNKS+=("$d_chunks")
  DS_ROOT+=("$d_root")
done

QUERY_COLD=0
QUERY_HOT=0
if [ "$QUERY" = yes ]; then
  QUERY_COLD=1
  case "$INGEST" in
    hot | both) QUERY_HOT=1 ;;
    *) note "QUERY=yes with INGEST=$INGEST leaves no hot DB — running the cold query suite only" ;;
  esac
fi
if [ "$INGEST" = none ] && [ "$QUERY" = no ]; then
  note "INGEST=none and QUERY=no — this campaign only prepares datasets"
fi

# --- binary under test -----------------------------------------------------------
cd "$REPO"
if [ -n "$REF" ]; then
  git rev-parse --verify --quiet "$REF^{commit}" >/dev/null || die "REF '$REF' does not resolve to a commit"
  BUILT_COMMIT=$(git rev-parse "$REF^{commit}")
  SHA=$(git rev-parse --short=8 "$BUILT_COMMIT")
else
  BUILT_COMMIT=$(git rev-parse HEAD)
  SHA=$(git describe --always --dirty --abbrev=8)
fi
BIN=$BENCH/bin/stellar-rpc-$SHA
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
RES=$BENCH/results/$NAME-$SHA-$STAMP
TARBALL=/tmp/bench-results-$NAME-$SHA-$STAMP.tgz

# Without this the AWS SDK signs S3 requests with the box's IAM role and the
# public bucket 403s; with no creds it falls back to anonymous access.
export AWS_EC2_METADATA_DISABLED=true

build_binary() {
  if [ "$DRY" -eq 0 ] && [ -n "$REF" ] && [ -x "$BIN" ]; then
    note "binary $BIN already built — skipping build"
    return
  fi
  note "build $([ -n "$REF" ] && echo "REF=$REF" || echo "current checkout") → $BIN"
  if [ -z "$REF" ]; then
    run make build-libs
    run go build -o "$BIN" ./cmd/stellar-rpc
    return
  fi
  local orig
  orig=$(git symbolic-ref -q --short HEAD || git rev-parse HEAD)
  if [ "$DRY" -eq 0 ]; then
    [ -z "$(git status --porcelain | grep -v '^??' || true)" ] ||
      die "work tree has uncommitted changes — commit or stash before benchmarking REF=$REF"
    trap 'git checkout -q "'"$orig"'" || true' EXIT
  fi
  run git -c advice.detachedHead=false checkout -q "$BUILT_COMMIT"
  run make build-libs
  run go build -o "$BIN" ./cmd/stellar-rpc
  run git checkout -q "$orig"
  if [ "$DRY" -eq 0 ]; then
    trap - EXIT
  fi
}

# --- dataset preparation: converge every kind on a local cold pack root ---------
golden_present() { # golden_present DIR: true if DIR exists and is non-empty
  [ -d "$1" ] && [ -n "$(find "$1" -mindepth 1 -print -quit 2>/dev/null)" ]
}

prepare_dataset() { # prepare_dataset INDEX
  local name=${DS_NAME[$1]} kind=${DS_KIND[$1]} loc=${DS_LOC[$1]} root=${DS_ROOT[$1]}
  local chunks c stage
  read -r -a chunks <<<"${DS_CHUNKS[$1]}"
  case "$kind" in
    packs-local)
      note "dataset $name: local cold pack root $root"
      if [ "$DRY" -eq 0 ]; then
        [ -d "$root/ledgers" ] || die "dataset '$name': $root/ledgers not found — location must be a cold pack root"
      fi
      ;;
    packs-gs)
      if golden_present "$root"; then
        note "dataset $name: golden packs already at $root — skipping fetch"
      else
        note "dataset $name: fetch $loc"
        run mkdir -p "$root.partial"
        run gcloud storage rsync -r "$loc" "$root.partial"
        run mv "$root.partial" "$root"
      fi
      ;;
    bsb-s3)
      if golden_present "$root"; then
        note "dataset $name: golden packs already at $root — skipping backfill"
      else
        run rm -rf "$root.partial"
        for c in "${chunks[@]}"; do
          note "dataset $name: golden backfill of chunk $c from S3 (untimed)"
          run "$BIN" bench-ingest cold \
            --source=bsb --datastore-type=S3 --region=us-east-2 \
            --bucket-path="$loc" \
            --start-chunk="$c" --num-chunks=1 \
            --cold-out-dir="$root.partial" \
            --out="$RES/golden-$name-c$c"
        done
        run mv "$root.partial" "$root"
      fi
      ;;
    fixture)
      if golden_present "$root"; then
        note "dataset $name: golden packs already at $root — skipping generation"
      else
        stage=$BENCH/fixture/$name/ledgers
        note "dataset $name: generate a fixture pack tree"
        run rm -rf "$BENCH/fixture/$name" "$root.partial"
        for c in "${chunks[@]}"; do
          note "dataset $name: generate fixture chunk $c ($loc ledgers)"
          run "$BIN" bench-ingest fixture \
            --pack-dir="$stage" --chunk="$c" --num-ledgers="$loc" --seed=1
        done
        for c in "${chunks[@]}"; do
          note "dataset $name: freeze fixture chunk $c into golden packs (untimed)"
          run "$BIN" bench-ingest cold \
            --source=pack --pack-dir="$stage" \
            --start-chunk="$c" --num-chunks=1 \
            --cold-out-dir="$root.partial" \
            --out="$RES/golden-$name-c$c"
        done
        run mv "$root.partial" "$root"
      fi
      ;;
  esac
  if [ "$DRY" -eq 0 ]; then
    [ -d "$root/ledgers" ] || die "dataset '$name': $root/ledgers missing after preparation"
  fi
}

# --- benchmark loops: one fresh process and one fresh --out dir per run ---------
run_ingest_cold() {
  local i c r name root chunks
  for i in "${!DS_NAME[@]}"; do
    name=${DS_NAME[$i]} root=${DS_ROOT[$i]}
    read -r -a chunks <<<"${DS_CHUNKS[$i]}"
    for c in "${chunks[@]}"; do
      for r in $(seq 1 "$RUNS"); do
        note "ingest-cold $name chunk $c run $r/$RUNS"
        run rm -rf "$BENCH/scratch/$name/$c"
        run "$BIN" bench-ingest cold \
          --source=pack --pack-dir="$root/ledgers" \
          --start-chunk="$c" --num-chunks=1 --workers="$WORKERS" \
          --cold-out-dir="$BENCH/scratch/$name/$c" \
          --out="$RES/ingest-cold-$name-c$c-run$r"
      done
    done
  done
}

# The hot DB is deleted before each run; the last run's DB is kept because
# the hot query suite reads it.
run_ingest_hot() {
  local i c r name root chunks cmd
  for i in "${!DS_NAME[@]}"; do
    name=${DS_NAME[$i]} root=${DS_ROOT[$i]}
    read -r -a chunks <<<"${DS_CHUNKS[$i]}"
    for c in "${chunks[@]}"; do
      for r in $(seq 1 "$RUNS"); do
        note "ingest-hot $name chunk $c run $r/$RUNS"
        run rm -rf "$BENCH/hot/$name/$c"
        cmd=("$BIN" bench-ingest hot
          --source=pack --pack-dir="$root/ledgers"
          --start-chunk="$c" --hot-dir="$BENCH/hot/$name/$c"
          --close-interval="$CLOSE_INTERVAL")
        if [ "$HOT_NUM_LEDGERS" -gt 0 ]; then
          cmd+=(--num-ledgers="$HOT_NUM_LEDGERS")
        fi
        cmd+=(--out="$RES/ingest-hot-$name-c$c-run$r")
        run "${cmd[@]}"
      done
    done
  done
}

run_query_cold() {
  local i c r name root chunks
  for i in "${!DS_NAME[@]}"; do
    name=${DS_NAME[$i]} root=${DS_ROOT[$i]}
    read -r -a chunks <<<"${DS_CHUNKS[$i]}"
    for c in "${chunks[@]}"; do
      for r in $(seq 1 "$RUNS"); do
        note "query-cold $name chunk $c run $r/$RUNS"
        run "$BIN" bench-query cold \
          --cold-dir="$root" --start-chunk="$c" --num-chunks=1 \
          --types=ledgers,txpage,txhash,events \
          --query-concurrency="$QC" --iters="$COLD_ITERS" \
          --out="$RES/query-cold-$name-c$c-run$r"
      done
    done
  done
}

run_query_hot() {
  local i c r name chunks cmd
  for i in "${!DS_NAME[@]}"; do
    name=${DS_NAME[$i]}
    read -r -a chunks <<<"${DS_CHUNKS[$i]}"
    for c in "${chunks[@]}"; do
      for r in $(seq 1 "$RUNS"); do
        note "query-hot $name chunk $c run $r/$RUNS"
        cmd=("$BIN" bench-query hot
          --hot-dir="$BENCH/hot/$name/$c" --chunk="$c"
          "--types=ledgers,txpage,txhash,events"
          --query-concurrency="$QC" --iters="$HOT_ITERS" --warmup=20)
        # A capped hot ingest leaves a truncated DB; keep the query sampler
        # inside what was ingested.
        if [ "$HOT_NUM_LEDGERS" -gt 0 ]; then
          cmd+=(--sample-ledgers="$HOT_NUM_LEDGERS")
        fi
        cmd+=(--out="$RES/query-hot-$name-c$c-run$r")
        run "${cmd[@]}"
      done
    done
  done
}

# --- provenance and machine metadata ---------------------------------------------
write_binary_info() {
  {
    echo "binary: $BIN"
    echo "commit: $BUILT_COMMIT"
    echo "ref:    ${REF:-<current checkout>}"
    "$BIN" version 2>&1 | head -3
  } >"$RES/binary.txt"
}

write_machine_metadata() {
  note "machine metadata"
  {
    date -u
    if TOKEN=$(curl -m 2 -sf -X PUT http://169.254.169.254/latest/api/token \
      -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null); then
      echo "instance-type: $(curl -m 2 -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-type)"
      echo "instance-id:   $(curl -m 2 -sH "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)"
    fi
    uname -a
    lsb_release -ds 2>/dev/null || true
    { lscpu | grep -E 'Model name|^CPU\(s\)'; } 2>/dev/null || true
    sysctl -n machdep.cpu.brand_string hw.memsize hw.ncpu 2>/dev/null || true
    { free -h | head -2; } 2>/dev/null || true
    lsblk -o NAME,SIZE,MODEL 2>/dev/null || true
    echo "repo: $(git -C "$REPO" rev-parse HEAD) ($(git -C "$REPO" rev-parse --abbrev-ref HEAD))"
    echo "binary: $BIN (commit $BUILT_COMMIT)"
    "$BIN" version 2>&1 | head -3
    go version 2>/dev/null || true
    { rustc --version || "$HOME/.cargo/bin/rustc" --version; } 2>/dev/null || true
    echo "campaign: $NAME · ingest: $INGEST · query: $QUERY · runs: $RUNS · concurrency: $QC"
    echo "cold-iters: $COLD_ITERS · hot-iters: $HOT_ITERS · close-interval: $CLOSE_INTERVAL · workers: $WORKERS · hot-num-ledgers: $HOT_NUM_LEDGERS"
    echo -n "fsync probe: "
    if probe=$(dd if=/dev/zero of="$BENCH/.fsync-probe" bs=4k count=2000 oflag=dsync 2>&1); then
      echo "$probe" | tail -1
    else
      echo "unavailable (dd has no oflag=dsync on this platform)"
    fi
    rm -f "$BENCH/.fsync-probe"
  } >"$RES/machine-metadata.txt" 2>&1
}

# write_campaign_metadata emits metadata.json, the machine-readable campaign
# manifest: run identity, campaign config, datasets, and hardware facts.
# Per-invocation detail (resolved flags, binary identity, timings) lives in
# each --out directory's invocation.json; this file records what no single
# invocation knows.
write_campaign_metadata() {
  local i token itype iid cpus chunks datasets_json=
  local -a hw=()
  for i in "${!DS_NAME[@]}"; do
    chunks=${DS_CHUNKS[$i]// /, }
    [ -z "$datasets_json" ] || datasets_json+=$',\n'
    datasets_json+="    {\"name\": \"${DS_NAME[$i]}\", \"kind\": \"${DS_KIND[$i]}\", \"location\": \"${DS_LOC[$i]}\", \"chunks\": [$chunks]}"
  done
  if token=$(curl -m 2 -sf -X PUT http://169.254.169.254/latest/api/token \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null); then
    itype=$(curl -m 2 -sH "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || true)
    iid=$(curl -m 2 -sH "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || true)
    [ -z "$itype" ] || hw+=("\"instance_type\": \"$itype\"")
    [ -z "$iid" ] || hw+=("\"instance_id\": \"$iid\"")
  fi
  hw+=("\"uname\": \"$(uname -srm)\"")
  if cpus=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null); then
    hw+=("\"cpus\": $cpus")
  fi
  if [ -f /proc/meminfo ]; then
    hw+=("\"mem_total_kb\": $(awk '/^MemTotal:/ {print $2}' /proc/meminfo)")
  fi
  local hardware_json
  hardware_json=$(printf '    %s,\n' "${hw[@]}")
  cat >"$RES/metadata.json" <<EOF
{
  "schema_version": 1,
  "run_id": "$NAME-$SHA-$STAMP",
  "campaign": {
    "name": "$NAME",
    "config_file": "$(basename "$CFG")",
    "ref": "$REF",
    "built_commit": "$BUILT_COMMIT",
    "ingest": "$INGEST",
    "query": "$QUERY",
    "close_interval": "$CLOSE_INTERVAL",
    "runs": $RUNS,
    "query_concurrency": "$QC",
    "cold_iters": $COLD_ITERS,
    "hot_iters": $HOT_ITERS,
    "workers": $WORKERS,
    "hot_num_ledgers": $HOT_NUM_LEDGERS
  },
  "datasets": [
$datasets_json
  ],
  "hardware": {
${hardware_json%,}
  },
  "hostname": "$(hostname)",
  "started_at": "$STARTED_AT",
  "finished_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
}

# --- campaign --------------------------------------------------------------------
note "campaign $NAME → $RES"
if [ "$DRY" -eq 1 ]; then
  note "dry run: printing commands only — nothing is built, downloaded, or executed"
else
  mkdir -p "$BENCH"/bin "$BENCH"/golden "$BENCH"/scratch "$BENCH"/hot "$BENCH"/fixture "$RES"
  cp "$CFG" "$RES/"
fi

build_binary
if [ "$DRY" -eq 0 ]; then
  write_binary_info
fi

for i in "${!DS_NAME[@]}"; do
  prepare_dataset "$i"
done

case "$INGEST" in cold | both) run_ingest_cold ;; esac
case "$INGEST" in hot | both) run_ingest_hot ;; esac
if [ "$QUERY_COLD" -eq 1 ]; then
  run_query_cold
fi
if [ "$QUERY_HOT" -eq 1 ]; then
  run_query_hot
fi

if [ "$DRY" -eq 1 ]; then
  if [ -n "$PUBLISH_URI" ]; then
    run "$SCRIPT_DIR/publish.sh" "$RES" "$PUBLISH_URI"
  fi
  note "dry run complete"
  exit 0
fi

write_machine_metadata
write_campaign_metadata
tar -C "$BENCH/results" -czf "$TARBALL" "$NAME-$SHA-$STAMP"
note "campaign done: $TARBALL"

# Publishing is a separate final step: the data is already safe in $RES and
# $TARBALL, so a publish failure is not a benchmark failure — it exits 1 with
# the exact retry command rather than corrupting the "campaign done" signal.
if [ -n "$PUBLISH_URI" ]; then
  if ! "$SCRIPT_DIR/publish.sh" "$RES" "$PUBLISH_URI"; then
    note "publish failed — data is safe in $RES and $TARBALL; retry with: publish.sh $RES $PUBLISH_URI"
    exit 1
  fi
  note "published: ${PUBLISH_URI%/}/$NAME-$SHA-$STAMP/"
fi
