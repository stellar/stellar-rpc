# shellcheck shell=bash
# shellcheck disable=SC2154  # env + helpers (log, bail, upload_result, upload_box_log,
# RUN_ID, BUCKET, RESULT_KEY, RESULTS_FILE, ...) come from bootstrap-common.sh, which
# the workflow concatenates ABOVE this fragment.
#
# Bench-campaign leg. Concatenated after bootstrap-common.sh in the rendered EC2
# user-data, so it inherits that file's env, helpers, and the already-active
# `set -euo pipefail` + ERR-trap-that-bails. Runs as root.
#
# It hands the whole campaign to the benchmarks repo's campaign CLI: this box
# clones stellar-rpc-benchmarks, lets its runner/bootstrap.sh provision the
# machine, and runs `campaign run` against the TOML the workflow rendered. The
# runner owns the legs, the bundle, the tarball, and the publish; this fragment
# only translates its exit code into the verdict object the poller reads.
LEG_TITLE="Bench campaign"

[ -n "${BENCH_TOML_B64:-}" ] || bail "BENCH_TOML_B64 unset; the workflow must render the campaign TOML into the user-data preamble"
BENCH_REPO_REF="${BENCH_REPO_REF:-main}"

# runner/bootstrap.sh uses sudo, $USER, and $HOME under `set -u`, and cloud-init
# user-data has none of them: root's shell here is not a login shell.
export HOME=/root USER=root

# bootstrap-common only installs awscli/jq/curl, and the benchmarks clone needs
# git. We deliberately do NOT call bootstrap_box: this box never builds
# stellar-rpc itself — the campaign CLI clones and builds it from the ref in the
# TOML, into its own build clone under $BENCH_ROOT/src.
log "installing git (for the benchmarks clone)"
apt-get install -y -qq --no-install-recommends git

# The basename lands verbatim in the bundle (the runner copies the config it was
# given), so keep it descriptive.
log "decoding campaign config"
printf '%s' "$BENCH_TOML_B64" | base64 -d > /root/bench-campaign.toml
log "campaign config:"
cat /root/bench-campaign.toml

# Full clone, not shallow: BENCH_REPO_REF may be a SHA.
log "cloning stellar-rpc-benchmarks at $BENCH_REPO_REF"
rm -rf /root/stellar-rpc-benchmarks
git clone https://github.com/stellar-experimental/stellar-rpc-benchmarks.git /root/stellar-rpc-benchmarks
git -C /root/stellar-rpc-benchmarks checkout "$BENCH_REPO_REF"

# The benchmarks bootstrap is authoritative for this machine: NVMe discovery and
# mount, the fsync-honesty probe (it exits non-zero on absorbed fsync rather than
# letting the box produce fiction), the AWS CLI, the pinned Go/Rust, and the
# native libs. A non-zero exit hits our still-active ERR trap -> bail -> fail
# verdict, which is what we want: none of those failures are recoverable here.
# SRC_REF points its native-lib install scripts (and their version pins) at the
# ref this campaign benchmarks, taken from the TOML the workflow rendered.
SRC_REF=$(sed -n 's/^ref = "\(.*\)"$/\1/p' /root/bench-campaign.toml | head -1)
log "running runner/bootstrap.sh (SRC_REF=${SRC_REF:-unset})"
SRC_REF="$SRC_REF" bash /root/stellar-rpc-benchmarks/runner/bootstrap.sh

# bootstrap.sh appends these to .bashrc for future shells; this shell is not one,
# so mirror them exactly as it writes them.
export PATH=/usr/local/go/bin:$HOME/go/bin:$HOME/.cargo/bin:$PATH
export CGO_CFLAGS="-I$HOME/.zstd/include -I$HOME/.rocksdb/include"
export CGO_LDFLAGS="-L$HOME/.zstd/lib -L$HOME/.rocksdb/lib"
export LD_LIBRARY_PATH="$HOME/.zstd/lib:$HOME/.rocksdb/lib"

# upload_bundle copies a results tarball and a run-info.json sidecar to the run's
# own prefix in S3, next to the result object. The verdict object carries only
# markdown, so the notify job reads the sidecar for the bundle's run id and
# destination as data, and it ingests the tarball into the results site from this
# bucket because its role can read this bucket but not the results bucket.
#
# Every step is best-effort: a failed upload must not change the campaign's
# verdict, so each command is either an `if` condition or is followed by `|| log`
# and so cannot reach the ERR trap. The notify job falls back to the result key
# when the sidecar is missing, and skips the site ingest when tarballKey is empty.
#
# It sets TARBALL_KEY to the key it wrote, or to an empty string when no tarball
# reached S3, for the caller's verdict markdown.
#
# usage: upload_bundle <tarball> <bench_run_id> <results_uri>
upload_bundle() {
  local tarball="$1" bench_run_id="$2" results_uri="$3"
  TARBALL_KEY=""
  if [ -z "$BUCKET" ] || [ -z "$RESULT_KEY" ]; then
    log "WARN: BUCKET/RESULT_KEY unset; skipping the tarball and run-info.json"
    return 0
  fi
  local prefix="${RESULT_KEY%/*}"
  TARBALL_KEY="$prefix/$(basename "$tarball")"
  if ! aws s3api put-object --bucket "$BUCKET" --key "$TARBALL_KEY" \
         --content-type application/gzip --body "$tarball" >/dev/null; then
    log "WARN: tarball upload failed; notify skips the results-site ingest"
    TARBALL_KEY=""
  fi
  if ! jq -n --arg run "$RUN_ID" --arg bench "$bench_run_id" \
        --arg uri "$results_uri" --arg tar "$tarball" --arg tarkey "$TARBALL_KEY" \
        '{schemaVersion: 1, runId: $run, benchRunId: $bench, resultsUri: $uri, tarball: $tar, tarballKey: $tarkey}' \
        > /tmp/run-info.json; then
    log "WARN: could not write run-info.json; notify falls back to the result key"
    return 0
  fi
  aws s3api put-object --bucket "$BUCKET" --key "$prefix/run-info.json" \
        --content-type application/json --body /tmp/run-info.json >/dev/null \
    || log "WARN: run-info.json upload failed; notify falls back to the result key"
}

# newest_bundle_tarball prints the path of the most recently modified
# /tmp/bench-results-*.tgz, or nothing when the runner produced none. The runner
# names one tarball per campaign run id, and a resumed or retried campaign on the
# same box can leave more than one behind.
newest_bundle_tarball() {
  local newest="" candidate
  for candidate in /tmp/bench-results-*.tgz; do
    [ -f "$candidate" ] || continue
    if [ -z "$newest" ] || [ "$candidate" -nt "$newest" ]; then
      newest="$candidate"
    fi
  done
  printf '%s' "$newest"
}

# The runner keeps its own campaign.log inside the bundle; this tee is for the
# lines this fragment parses (`published:`) and the tail it puts in a failure
# comment. pipefail is active so the `if` sees the runner's exit code, and the
# `if` keeps the ERR trap off a failed campaign — the failure path below is more
# useful than bail's.
log "running campaign"
if (cd /root/stellar-rpc-benchmarks/runner \
    && BENCH_ROOT=/mnt/nvme/bench go run ./cmd/campaign run /root/bench-campaign.toml) \
    2>&1 | tee /tmp/campaign-console.log; then

  # Exit 0 means every leg, the tarball, and the publish all succeeded, so the
  # runner printed `published: <dest>`. A missing line is defensive only.
  PUBLISHED_LINE=$(grep '^published: ' /tmp/campaign-console.log | tail -1 || true)
  [ -n "$PUBLISHED_LINE" ] || bail "campaign exited 0 but printed no 'published:' line; cannot locate the published bundle"
  RESULTS_URI="${PUBLISHED_LINE#published: }"
  RESULTS_URI="${RESULTS_URI%/}"
  BENCH_RUN_ID="${RESULTS_URI##*/}"
  TARBALL="/tmp/bench-results-${BENCH_RUN_ID}.tgz"
  log "published bundle $BENCH_RUN_ID to $RESULTS_URI"

  # The complete bundle and its sidecar, for the notify job to ingest into the
  # results site.
  upload_bundle "$TARBALL" "$BENCH_RUN_ID" "$RESULTS_URI"

  # shellcheck disable=SC2016  # the backticks below are markdown code spans, not shell expansion
  {
    printf '✅ **%s passed** (run `%s`)\n\n' "$LEG_TITLE" "$RUN_ID"
    printf -- '- bench run id: `%s`\n' "$BENCH_RUN_ID"
    printf -- '- results: `%s`\n' "$RESULTS_URI"
    printf -- '- tarball on the box: `%s`\n' "$TARBALL"
    printf -- '- benchmarks ref: `%s`\n' "$BENCH_REPO_REF"
    printf -- '- campaign config: `%s`\n' /root/bench-campaign.toml
  } > "$RESULTS_FILE"
  upload_result ok "$RESULTS_FILE"

  # The EXIT trap would do this, but poweroff can outrun a normal script exit, so
  # push the box log first. shutdown-behavior=terminate turns this poweroff into
  # a terminate, which is how a passing campaign releases the instance without
  # waiting for the GHA runner (the poll chain may still be between windows).
  upload_box_log
  log "campaign complete: $BENCH_RUN_ID — powering off (terminates the instance)"
  poweroff

else
  # Non-zero: a leg failed, the tarball failed, or the publish failed. The
  # runner's epilogue runs after a failed leg — it writes the bundle's metadata,
  # tars /mnt/nvme/bench/results/<run id> into /tmp/bench-results-<run id>.tgz,
  # and publishes it when the campaign config names a publish URI — so a bundle
  # holding every leg that did finish usually exists here.
  #
  # Upload it before the verdict: the box self-terminates at its ceiling, and
  # without this the good legs die with it and only an SSM rescue can recover
  # them. A campaign that failed before the tarball step leaves no tarball, and
  # a campaign whose publish step failed leaves no `published:` line, so both
  # are treated as missing rather than as errors.
  TARBALL=$(newest_bundle_tarball)
  TARBALL_KEY=""
  if [ -n "$TARBALL" ]; then
    BENCH_RUN_ID=$(basename "$TARBALL" .tgz)
    BENCH_RUN_ID="${BENCH_RUN_ID#bench-results-}"
    PUBLISHED_LINE=$(grep '^published: ' /tmp/campaign-console.log | tail -1 || true)
    RESULTS_URI="${PUBLISHED_LINE#published: }"
    RESULTS_URI="${RESULTS_URI%/}"
    log "uploading the bundle a failed campaign left behind: $BENCH_RUN_ID"
    upload_bundle "$TARBALL" "$BENCH_RUN_ID" "$RESULTS_URI"
  else
    log "WARN: no /tmp/bench-results-*.tgz on the box; there is no bundle to upload"
  fi

  # shellcheck disable=SC2016  # the backticks below are markdown code spans, not shell expansion
  {
    printf '❌ **%s failed** (run `%s`)\n\n' "$LEG_TITLE" "$RUN_ID"
    printf 'Last 60 lines of the campaign console:\n\n```\n'
    tail -n 60 /tmp/campaign-console.log
    printf '```\n\n'
    if [ -n "$TARBALL_KEY" ]; then
      printf 'Uploaded bundle: `s3://%s/%s`, holding the legs that finished before the failure.\n' "$BUCKET" "$TARBALL_KEY"
    else
      printf 'No bundle uploaded: an SSM rescue is the only way to reach the results.\n'
    fi
    printf 'Rescue: the bundle is under `/mnt/nvme/bench/results/`, the tarball at `/tmp/bench-results-*.tgz`.\n'
    printf 'The box is left running until its self-terminate ceiling (%s minutes after boot), so an operator can SSM in.\n' "$SELF_TERMINATE_MINUTES"
  } > "$RESULTS_FILE"
  upload_result fail "$RESULTS_FILE"

  # Deliberately no poweroff. On a publish failure the results exist only on this
  # box, and the NVMe bundle dies with it, so the box is left to the
  # self-terminate ceiling (plus the reaper) instead of being released here. The
  # tradeoff is instance-hours for a rescuable bundle.
  log "campaign failed; leaving the box up for rescue until the self-terminate ceiling"
  exit 1
fi
