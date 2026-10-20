#!/usr/bin/env bash
# Tests poll-window.sh: a poll window ends at least the margin before its job's
# timeout, whatever the job spent before the relay started.
#
#   ./poll-window_test.sh
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
fails=0

# window <start> <now> prints poll-window.sh's outputs for a 330 min job.
window() {
  JOB_START_EPOCH="$1" NOW_EPOCH="$2" JOB_TIMEOUT_MINUTES=330 GITHUB_OUTPUT=/dev/stdout \
    "$here/poll-window.sh" 2>/dev/null
}

expect() {
  local name="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    echo "ok   $name"
  else
    echo "FAIL $name: got '$got', want '$want'"
    fails=$((fails + 1))
  fi
}

start=1792465718 # 2026-10-20T03:08:38Z, poll1 of run 39362655592
timeout_at=$((start + 330 * 60))

out=$(window "$start" "$start")
expect "no delay: the full 320 min window" "$out" "$(printf 'window_seconds=19200\nhandoff=false')"

# Run 39362655592: a cold cache delayed the relay to 03:18:46. The old fixed
# 19200 s window then ended at the job's timeout and the job was cancelled.
now=$((start + 608))
w=$(window "$start" "$now" | sed -n 's/^window_seconds=//p')
expect "608 s delay: the window shrinks by the delay" "$w" "18592"
expect "608 s delay: the window ends 10 min before the timeout" "$((timeout_at - (now + w)))" "600"

now=$((start + 330 * 60 - 600 - 119))
expect "119 s left: hand off without polling" "$(window "$start" "$now")" "$(printf 'window_seconds=0\nhandoff=true')"

now=$((start + 330 * 60))
expect "past the timeout: hand off" "$(window "$start" "$now" | sed -n 's/^handoff=//p')" "true"

if JOB_START_EPOCH=x JOB_TIMEOUT_MINUTES=330 "$here/poll-window.sh" >/dev/null 2>&1; then
  echo "FAIL a non-integer start is rejected"
  fails=$((fails + 1))
else
  echo "ok   a non-integer start is rejected"
fi

[ "$fails" -eq 0 ]
