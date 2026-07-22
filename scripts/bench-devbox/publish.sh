#!/usr/bin/env bash
#
# Safeguard a finished benchmark campaign bundle to object storage. It uploads
# a campaign results directory to <dest-root>/<run_id>/, where run_id is the
# bundle's basename (the same run_id recorded in the bundle's metadata.json).
# The uploader is idempotent, but published runs are immutable: it refuses to
# write into a destination that already holds objects unless --force is given.
#
# Usage:
#   ./scripts/bench-devbox/publish.sh <results-dir> [<dest-root-uri>] [--dry-run] [--force]
#
# Arguments:
#   <results-dir>    a campaign bundle ($BENCH/results/<run_id>/). Its basename
#                    is the run_id and the last path component of the upload.
#   <dest-root-uri>  object-storage root to publish under (default: $PUBLISH_URI).
#                    gs://… uploads with `gcloud storage rsync -r`; s3://… with
#                    `aws s3 sync`. No other scheme is supported.
#   --dry-run        print every cloud command that would run, then exit 0
#                    without executing any of them.
#   --force          overwrite a non-empty destination, skipping the
#                    immutability check. Published runs are otherwise immutable.
#
# Environment:
#   PUBLISH_URI  used as <dest-root-uri> when the argument is omitted.
#
# The upload lands at <dest-root-uri>/<run_id>/. On a real upload the final
# line printed is `published: <dest-root-uri>/<run_id>/` (machine-greppable).
set -euo pipefail

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
DRY=0
FORCE=0
RESULTS_DIR=
DEST_ROOT=
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY=1 ;;
    --force) FORCE=1 ;;
    -*) die "unknown argument: $arg" ;;
    *)
      if [ -z "$RESULTS_DIR" ]; then RESULTS_DIR=$arg
      elif [ -z "$DEST_ROOT" ]; then DEST_ROOT=$arg
      else die "unexpected extra argument: $arg"
      fi
      ;;
  esac
done

[ -n "$RESULTS_DIR" ] || die "usage: publish.sh <results-dir> [<dest-root-uri>] [--dry-run] [--force]"
RESULTS_DIR=${RESULTS_DIR%/}
[ -d "$RESULTS_DIR" ] || die "results dir not found: $RESULTS_DIR"
DEST_ROOT=${DEST_ROOT:-${PUBLISH_URI:-}}
[ -n "$DEST_ROOT" ] || die "no destination: pass <dest-root-uri> or set PUBLISH_URI"

RUN_ID=$(basename "$RESULTS_DIR")
[ -f "$RESULTS_DIR/metadata.json" ] || note "warning: $RESULTS_DIR/metadata.json missing — pre-manifest bundle"

DEST="${DEST_ROOT%/}/$RUN_ID/"

# --- scheme dispatch -------------------------------------------------------------
case "$DEST" in
  gs://*) ls_cmd=(gcloud storage ls "$DEST"); sync_cmd=(gcloud storage rsync -r "$RESULTS_DIR" "$DEST") ;;
  s3://*) ls_cmd=(aws s3 ls "$DEST"); sync_cmd=(aws s3 sync "$RESULTS_DIR" "$DEST") ;;
  *) die "unsupported destination scheme: $DEST (supported: gs://, s3://)" ;;
esac

# --- immutability check ----------------------------------------------------------
# Published runs are immutable. A nonexistent prefix errors on both CLIs, so a
# failed listing counts as empty; any output means the destination is occupied.
if [ "$FORCE" -eq 0 ]; then
  printf '  $ %s\n' "${ls_cmd[*]}"
  if [ "$DRY" -eq 0 ] && out=$("${ls_cmd[@]}" 2>/dev/null) && [ -n "$out" ]; then
    die "destination already has objects: $DEST — published runs are immutable; pass --force to overwrite"
  fi
fi

# --- upload ----------------------------------------------------------------------
note "publish $RUN_ID → $DEST"
run "${sync_cmd[@]}"

if [ "$DRY" -eq 1 ]; then
  note "dry run complete"
  exit 0
fi
echo "published: $DEST"
