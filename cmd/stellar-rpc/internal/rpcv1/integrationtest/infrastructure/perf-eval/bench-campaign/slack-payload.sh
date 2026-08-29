#!/usr/bin/env bash
# Builds the Slack webhook payload (Block Kit) for the bench-campaign and
# bench-reaper notifications. The workflows call this instead of templating the
# payload inline so the whole message layout is renderable on a laptop:
#
#   MODE=campaign STATE=ok CAMPAIGN_NAME=phase3-c6id8xl PHASE=3 ... \
#       ./slack-payload.sh | jq .
#
# Env in, MODE=campaign:
#   STATE (ok|fail), REASON, CAMPAIGN_NAME, PHASE, INGEST, QUERY, RUNS, WORKERS,
#   INSTANCE_TYPE, HOT_NUM_LEDGERS, BUDGET_MINUTES, ELAPSED_MINUTES,
#   DEADLINE_EPOCH, TARGET_REF, TARGET_SHA, BENCH_RUN_ID, VIEWER_URL,
#   RESULTS_URI, RUN_URL, RUN_JSON (path to the converted results-site run JSON,
#   for the ingestion-vs-target recap), INGEST_STATE (ingested|skipped|failed,
#   empty when the ingest was never attempted) and INGEST_REASON (one line, why
#   the run is not on the site), EXCERPT (verdict-markdown lines, fail),
#   BOX_ID, BOX_RESCUED (true when the box is left up), RUN_ID_TAG, BUCKET,
#   RESULT_KEY, AWS_REGION, HARNESS_SHA, REPO_URL.
# Env in, MODE=reaper:
#   REAPED_JSON ('[{"id":"i-..","runId":"123..","overdueMin":42}, ...]'),
#   UNTAGGED (space-separated instance ids), RUN_URL, REPO_URL.
# Out: the payload JSON on stdout — {attachments: [{color, fallback, blocks}]}.
# One status-colored attachment (the full-height stripe), with the one-line
# summary in its fallback: Slack uses that for the notification preview and
# old clients without rendering it (a top-level text field would display above
# the card). Slack folds a tall attachment behind "Show more", so the blocks
# are ordered important-first — header, lead, key metadata, BUTTONS, recap —
# and only the secondary fields and the context footer risk the fold.
#
# Also runs on developer macOS, i.e. bash 3.2 + BSD date.
set -euo pipefail

MODE="${MODE:-campaign}"
REPO_URL="${REPO_URL:-https://github.com/stellar/stellar-rpc}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# 252 -> "4 h 12 m"; 420 -> "7 h"; 45 -> "45 m"; junk -> "".
fmt_minutes() {
  case "${1:-}" in '' | *[!0-9]*) echo ""; return ;; esac
  if [ "$1" -ge 60 ] && [ "$(($1 % 60))" -eq 0 ]; then
    echo "$(($1 / 60)) h"
  elif [ "$1" -ge 60 ]; then
    echo "$(($1 / 60)) h $(($1 % 60)) m"
  else
    echo "$1 m"
  fi
}

# GNU date wants -d @epoch, BSD date wants -r epoch.
fmt_epoch_hm() {
  case "${1:-}" in '' | *[!0-9]*) echo ""; return ;; esac
  date -u -r "$1" +%H:%M 2>/dev/null || date -u -d "@$1" +%H:%M 2>/dev/null || echo ""
}

# The S3 console object view for bucket $1, key $2 — an https link Slack can
# open, unlike a raw s3:// URI.
s3_object_url() {
  [ -n "$1" ] && [ -n "$2" ] || { echo ""; return; }
  echo "https://${AWS_REGION}.console.aws.amazon.com/s3/object/$1?region=${AWS_REGION}&prefix=$2"
}

# The S3 console browse view for an s3://bucket/prefix URI.
s3_prefix_url() {
  case "${1:-}" in s3://*) ;; *) echo ""; return ;; esac
  local rest="${1#s3://}"
  local bucket="${rest%%/*}"
  local prefix="${rest#*/}"
  [ "$prefix" != "$rest" ] || prefix=""
  echo "https://${AWS_REGION}.console.aws.amazon.com/s3/buckets/${bucket}?region=${AWS_REGION}&prefix=${prefix}/"
}

if [ "$MODE" = "reaper" ]; then
  REAPED_JSON="${REAPED_JSON:-[]}"
  UNTAGGED="${UNTAGGED:-}"
  jq -n \
    --argjson reaped "$REAPED_JSON" \
    --arg untagged "$UNTAGGED" \
    --arg run_url "${RUN_URL:-}" \
    --arg repo "$REPO_URL" \
    '
    def button($t; $u): {type: "button", text: {type: "plain_text", text: $t, emoji: true}, url: $u};
    ($reaped | length) as $n
    | ($untagged | split(" ") | map(select(. != ""))) as $loose
    | {
        attachments: [{
          color: "#ecb22e",
          fallback: "⚠️ bench reaper terminated past-deadline box(es): \($reaped | map(.id) | join(" ")) · \($run_url)",
          blocks: (
            [{type: "header", text: {type: "plain_text",
              text: "⚠️ Reaper terminated \($n) past-deadline box\(if $n == 1 then "" else "es" end)", emoji: true}}]
            + [{type: "section", text: {type: "mrkdwn", text: (
                ( $reaped | map(
                    (if .overdueMin >= 90 then "\((.overdueMin / 6 | round) / 10) h" else "\(.overdueMin) min" end) as $late
                    | (if (.runId // "") != "" then "campaign run <\($repo)/actions/runs/\(.runId)|\(.runId)>" else "campaign run unknown" end) as $src
                    | "• `\(.id)` — \($src), deadline passed *\($late)* ago"
                  ) )
                + ( $loose | map("• `\(.)` has no deadline tag — left alone (hand-launched?)") )
                | join("\n")
              )}}]
            + (if $run_url != "" then
                [{type: "actions", elements: [button("Reaper run"; $run_url)]}] else [] end)
            + [{type: "context", elements: [{type: "mrkdwn",
                text: "A reap means a campaign box outlived every in-band ceiling — worth a human look."}]}]
          )
        }]
      }
    '
  exit 0
fi

[ "$MODE" = "campaign" ] || { echo "MODE must be campaign or reaper, got '$MODE'" >&2; exit 1; }
STATE="${STATE:-}"
[ "$STATE" = "ok" ] || [ "$STATE" = "fail" ] || { echo "STATE must be ok or fail, got '$STATE'" >&2; exit 1; }

CAMPAIGN_NAME="${CAMPAIGN_NAME:-<unnamed>}"
REASON="${REASON:-}"
PHASE="${PHASE:-}"
INGEST="${INGEST:-}"
QUERY="${QUERY:-}"
RUNS="${RUNS:-}"
WORKERS="${WORKERS:-}"
INSTANCE_TYPE="${INSTANCE_TYPE:-}"
HOT_NUM_LEDGERS="${HOT_NUM_LEDGERS:-0}"
TARGET_REF="${TARGET_REF:-}"
TARGET_SHA="${TARGET_SHA:-}"
BENCH_RUN_ID="${BENCH_RUN_ID:-}"
VIEWER_URL="${VIEWER_URL:-}"
RESULTS_URI="${RESULTS_URI:-}"
RUN_URL="${RUN_URL:-}"
RUN_JSON="${RUN_JSON:-}"
INGEST_STATE="${INGEST_STATE:-}"
INGEST_REASON="${INGEST_REASON:-}"
EXCERPT="${EXCERPT:-}"
BOX_ID="${BOX_ID:-}"
BOX_RESCUED="${BOX_RESCUED:-false}"
RUN_ID_TAG="${RUN_ID_TAG:-}"
BUCKET="${BUCKET:-}"
RESULT_KEY="${RESULT_KEY:-}"
HARNESS_SHA="${HARNESS_SHA:-}"

ELAPSED_H=$(fmt_minutes "${ELAPSED_MINUTES:-}")
BUDGET_H=$(fmt_minutes "${BUDGET_MINUTES:-}")
DEADLINE_HM=$(fmt_epoch_hm "${DEADLINE_EPOCH:-}")

BOXLOG_URL=""
VERDICT_URL=""
if [ -n "$BUCKET" ] && [ -n "$RESULT_KEY" ]; then
  BOXLOG_URL=$(s3_object_url "$BUCKET" "${RESULT_KEY%/*}/user-data.log")
  VERDICT_URL=$(s3_object_url "$BUCKET" "$RESULT_KEY")
fi
BUNDLE_URL=$(s3_prefix_url "$RESULTS_URI")

# The ingest emits the detail-viewer URL (https://<site>/?run=<id>); the card
# links the run's summary page (https://<site>/summary.html?run=<id>) instead.
SUMMARY_URL=""
if [ -n "$VIEWER_URL" ]; then
  BASE="${VIEWER_URL%%\?*}"
  URL_QUERY="${VIEWER_URL#"$BASE"}"
  SUMMARY_URL="${BASE%/}/summary.html${URL_QUERY}"
fi

# The ingestion-p99-vs-target recap, from the converted run JSON the ingest step
# leaves in its benchmarks checkout. Per profile: p99 = driver.ingest_total.p99
# aggregated median (ns); the display name comes from the phase workload with
# the same tx/ledger as the unit name encodes. Tiers use the run's own reference
# lines: over the block time, over the phase target, or under it. null (no file,
# no hot ingest section, no target) just drops the block.
RESULTS_BLOCK=null
if [ "$STATE" = "ok" ] && [ -n "$RUN_JSON" ] && [ -r "$RUN_JSON" ]; then
  RESULTS_BLOCK=$(jq -c '
    def commafy: tostring | . as $s | ($s | length) as $l
      | if $l <= 3 then $s else ($s[0:$l-3] | commafy) + "," + $s[$l-3:] end;
    . as $root
    | .campaign.phase as $ph
    | ((.campaign.phase_targets // []) | map(select(.phase == $ph)) | first) as $t
    | ($t.ingest_p99_target_ns // null) as $target
    | ($t.block_time_ns // $root.checks.interval_ns // null) as $block
    | ($root.ingest_hot // {}) as $ih
    | if $ph == null or $target == null or $block == null or ($ih | length) == 0 then null else
        ( [ ($root.dataset.unit_order // ($ih | keys))[]
            | . as $u
            | ($ih[$u].driver.ingest_total.p99.m // null) as $p99
            | select($p99 != null)
            | (try ($u | capture("-(?<txpl>[0-9]+)-c[0-9]+$").txpl | tonumber) catch null) as $txpl
            | ((($t.workloads // []) | map(select(.tx_per_ledger == $txpl)) | first | .name) // $u) as $name
            | {name: $name, txpl: $txpl, p99: $p99}
          ] | sort_by(-.p99) ) as $rows
        | if ($rows | length) == 0 then null else
            (($target / 1e6) | round) as $target_ms
            | (($block / 1e6) | round) as $block_ms
            | {
                header: "*Ingestion p99 vs the Phase \($ph) target (\($target_ms) ms)*",
                lines: [ $rows[]
                  | ((.p99 / 1e6) | round) as $ms
                  | (((.p99 / $target * 10) | round) / 10) as $mult
                  | (if $ms >= 1000 then "\((($ms / 100) | round) / 10) s" else "\($ms) ms" end) as $disp
                  | (if .txpl == null then "" else " · \(.txpl | commafy) tx/ledger" end) as $load
                  | (if .p99 > $block then "🔴 \(.name)\($load) — *\($disp)* (\($mult)× target · past the \($block_ms) ms block time)"
                     elif .p99 > $target then "🟠 \(.name)\($load) — *\($disp)* (\($mult)× target)"
                     else "🟢 \(.name)\($load) — *\($disp)* (under target)" end)
                ],
                footer: "_median of \($root.campaign.reps // 1) run\(if ($root.campaign.reps // 1) == 1 then "" else "s" end) · block time \($block_ms) ms_"
              }
          end
      end
  ' "$RUN_JSON" 2>/dev/null) || RESULTS_BLOCK=null
  [ -n "$RESULTS_BLOCK" ] || RESULTS_BLOCK=null
fi

# The one-line summary: notification preview, old-client fallback, and what the
# workflow step echoes into its summary.
if [ "$STATE" = "ok" ]; then
  TEXT="✅ bench campaign $CAMPAIGN_NAME passed — run_id=${BENCH_RUN_ID:-unknown} · ${SUMMARY_URL:-${RESULTS_URI:-s3://$BUCKET/$RESULT_KEY}} · $RUN_URL"
else
  TEXT="❌ bench campaign $CAMPAIGN_NAME failed: $REASON — box log s3://$BUCKET/${RESULT_KEY%/*}/user-data.log · $RUN_URL"
fi

jq -n \
  --arg state "$STATE" \
  --arg text "$TEXT" \
  --arg name "$CAMPAIGN_NAME" \
  --arg reason "$REASON" \
  --arg phase "$PHASE" \
  --arg ingest "$INGEST" \
  --arg query "$QUERY" \
  --arg runs "$RUNS" \
  --arg workers "$WORKERS" \
  --arg machine "$INSTANCE_TYPE" \
  --arg hot_cap "$HOT_NUM_LEDGERS" \
  --arg elapsed "$ELAPSED_H" \
  --arg budget "$BUDGET_H" \
  --arg deadline_hm "$DEADLINE_HM" \
  --arg ref "$TARGET_REF" \
  --arg sha "$TARGET_SHA" \
  --arg bench "$BENCH_RUN_ID" \
  --arg summary "$SUMMARY_URL" \
  --arg run_url "$RUN_URL" \
  --arg bundle_url "$BUNDLE_URL" \
  --arg boxlog_url "$BOXLOG_URL" \
  --arg verdict_url "$VERDICT_URL" \
  --arg excerpt "$EXCERPT" \
  --arg box "$BOX_ID" \
  --arg rescued "$BOX_RESCUED" \
  --arg runidtag "$RUN_ID_TAG" \
  --arg repo "$REPO_URL" \
  --arg harness "$HARNESS_SHA" \
  --arg ingest_state "$INGEST_STATE" \
  --arg ingest_reason "$INGEST_REASON" \
  --argjson results "$RESULTS_BLOCK" \
  '
  def button($t; $u): {type: "button", text: {type: "plain_text", text: $t, emoji: true}, url: $u};
  def pbutton($t; $u): button($t; $u) + {style: "primary"};
  def field($l; $v): {type: "mrkdwn", text: "*\($l)*\n\($v)"};

  (if $sha != "" then "\($ref) @ <\($repo)/commit/\($sha)|\($sha[0:8])>" else $ref end) as $reftxt
  | (if $elapsed != "" and $budget != "" then " in *\($elapsed)* of a \($budget) budget"
     elif $elapsed != "" then " in *\($elapsed)*" else "" end) as $took

  # The campaign passing and the run reaching the results site are two
  # outcomes; a green campaign whose ingest broke still says so, in the lead
  # (which never folds) as well as the footer. A viewer URL in hand means the
  # ingest published, whatever the state says. Slack caps a section at 3000
  # chars and a context element at 2000, hence the trimmed reason.
  | ($ingest_reason | .[0:300]) as $ireason
  | (if $summary != "" or $ingest_state == "" or $ingest_state == "ingested" then ""
     elif $ingest_state == "failed" then " ⚠️ *Results-site ingest failed:* \($ireason)."
     else " ℹ️ *Not ingested:* \($ireason)." end) as $ingest_note

  | (if $state == "ok" then
      {
        color: "#2eb67d",
        header: "✅ Bench campaign passed — \($name | .[0:100])",
        lead: "\(if $runs == "1" then "The run finished green" else "All *\($runs) runs* green" end)\($took).\($ingest_note)",
        fields: ([
          ["Phase", $phase],
          ["Machine", ($machine + (if $workers != "" then " · \($workers) workers" else "" end))],
          ["Benchmarked ref", $reftxt],
          ["Run id", (if $bench != "" then "`\($bench)`" else "" end)]
        ] | map(select(.[1] != "") | field(.[0]; .[1]))),
        fields2: ([
          ["Ingest", ($ingest + (if $hot_cap != "" and $hot_cap != "0" then " · capped at \($hot_cap) ledgers" else "" end))],
          ["Query", $query]
        ] | map(select(.[1] != "") | field(.[0]; .[1]))),
        quote: "",
        buttons: ([
          (if $summary != "" then pbutton("View results"; $summary) else empty end),
          (if $run_url != "" then button("GitHub run"; $run_url) else empty end),
          (if $bundle_url != "" then button("Bundle on S3"; $bundle_url) else empty end)
        ]),
        context: ([
          (if $summary != "" or $ingest_state == "ingested" then "Ingested into the results site"
           elif $ingest_state == "failed" then "Not on the results site — ingest failed: \($ireason)"
           elif $ingest_state == "skipped" then "Not on the results site — \($ireason)"
           else "Not on the results site — the ingest did not complete" end),
          (if $harness != "" then "harness \($harness | .[0:8])" else empty end)
        ] | join(" · "))
      }
    else
      {
        color: "#e01e5a",
        header: "❌ Bench campaign failed — \($name | .[0:100])",
        lead: "*Reason:* \($reason)\(if $elapsed != "" and $budget != "" then ", after \($elapsed) of a \($budget) budget" else "" end).",
        fields: ([
          ["Phase / Machine", ([$phase, $machine] | map(select(. != "")) | join(" · "))],
          ["Benchmarked ref", $reftxt],
          ["Box", (if $box != "" and $rescued == "true" then "`\($box)` — left up for rescue" else "" end)],
          ["Run-id tag", (if $runidtag != "" and $rescued == "true" then "`\($runidtag)`" else "" end)]
        ] | map(select(.[1] != "") | field(.[0]; .[1]))),
        fields2: [],
        quote: (if $excerpt != "" then ($excerpt | split("\n") | map(select(. != "") | "> " + .) | join("\n")) else "" end),
        buttons: ([
          (if $run_url != "" then pbutton("GitHub run"; $run_url) else empty end),
          (if $boxlog_url != "" then button("Box log"; $boxlog_url) else empty end),
          (if $verdict_url != "" then button("Verdict on S3"; $verdict_url) else empty end)
        ]),
        context: (
          if $box != "" and $rescued == "true" then
            "⚠️ The box stays up for debugging — terminate it by the run-id tag when done, or the reaper kills it at the deadline\(if $deadline_hm != "" then " (\($deadline_hm) UTC)" else "" end)."
          elif $box != "" then "Box \($box) was terminated."
          else "No box was launched." end)
      }
    end) as $m

  | {
      attachments: [{
        color: $m.color,
        fallback: $text,
        blocks: (
          [{type: "header", text: {type: "plain_text", text: $m.header, emoji: true}}]
          + [{type: "section", text: {type: "mrkdwn", text: $m.lead}}]
          + (if $m.quote != "" then [{type: "section", text: {type: "mrkdwn", text: $m.quote}}] else [] end)
          + (if ($m.fields | length) > 0 then [{type: "section", fields: $m.fields}] else [] end)
          + (if ($m.buttons | length) > 0 then [{type: "actions", elements: $m.buttons}] else [] end)
          + (if $results != null then
              [{type: "divider"},
               {type: "section", text: {type: "mrkdwn",
                 text: (([$results.header] + $results.lines + [$results.footer]) | join("\n"))}}]
             else [] end)
          + (if ($m.fields2 | length) > 0 then [{type: "section", fields: $m.fields2}] else [] end)
          + (if $m.context != "" then [{type: "context", elements: [{type: "mrkdwn", text: $m.context}]}] else [] end)
        )
      }]
    }
  '
