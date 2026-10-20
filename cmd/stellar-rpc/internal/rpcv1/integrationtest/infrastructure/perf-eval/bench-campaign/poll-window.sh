#!/usr/bin/env bash
# Sizes one relay poll window so that it ends inside its own job: the window
# runs from now to the job's start plus its timeout, less a margin for the
# handoff and the steps after the relay. bench-campaign.yml calls it just before
# the relay, so time the job spent before that (checkout, toolchain, a cold
# cache) shortens the window instead of running it past the job's timeout.
#
#   JOB_START_EPOCH=$(date +%s) JOB_TIMEOUT_MINUTES=330 ./poll-window.sh
#
# Env in: JOB_START_EPOCH, JOB_TIMEOUT_MINUTES, with MARGIN_MINUTES (10),
# MIN_WINDOW_SECONDS (120) and NOW_EPOCH available as overrides for tests.
# Out: window_seconds=<n> and handoff=true|false appended to $GITHUB_OUTPUT
# (stdout when unset). handoff=true means the window is too short to poll: the
# job hands the campaign to the next poll job at once.
set -euo pipefail

die() {
  echo "::error::$*" >&2
  exit 1
}

JOB_START_EPOCH="${JOB_START_EPOCH:-}"
JOB_TIMEOUT_MINUTES="${JOB_TIMEOUT_MINUTES:-}"
MARGIN_MINUTES="${MARGIN_MINUTES:-10}"
MIN_WINDOW_SECONDS="${MIN_WINDOW_SECONDS:-120}"
NOW_EPOCH="${NOW_EPOCH:-$(date +%s)}"

for v in JOB_START_EPOCH JOB_TIMEOUT_MINUTES MARGIN_MINUTES MIN_WINDOW_SECONDS NOW_EPOCH; do
  case "${!v}" in
    '' | *[!0-9]*) die "$v must be a non-negative integer, got '${!v}'" ;;
  esac
done

window_end=$((JOB_START_EPOCH + (JOB_TIMEOUT_MINUTES - MARGIN_MINUTES) * 60))
window=$((window_end - NOW_EPOCH))
handoff=false
if [ "$window" -lt "$MIN_WINDOW_SECONDS" ]; then
  handoff=true
  window=0
fi

{
  echo "window_seconds=$window"
  echo "handoff=$handoff"
} >> "${GITHUB_OUTPUT:-/dev/stdout}"
echo "job started $((NOW_EPOCH - JOB_START_EPOCH))s ago; poll window ${window}s; handoff=$handoff" >&2
