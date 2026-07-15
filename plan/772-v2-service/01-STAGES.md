# #772 v2 Service — Stages, Checkpoint Protocol, and Session Prompts

## The stages

| Stage | File | Builds | Depends on |
|---|---|---|---|
| 1 | `stage-1-ingestion-metrics.md` | `latencytrack` pkg, per-ledger ingest read/write/e2e timing, minimal admin HTTP (`/metrics`, `/latency.json`) | — |
| 2 | `stage-2-registry.md` | `fullhistory/registry`: View, Registry, Reaper, cold-reader caches, resolve, BuildFromCatalog | — (1 preferred first) |
| 3 | `stage-3-write-integration.md` | Hooks into hotloop/backfill/lifecycle/startup; hot-handle ownership; deferred destruction | 2 |
| 4 | `stage-4-ledger-tx-adapters.md` | `fullhistory/serve`: db.LedgerReader + db.TransactionReader veneers | 2, 3 |
| 5 | `stage-5-events-adapter.md` | db.EventReader veneer (multi-chunk stitching, cursors) | 2, 3 (4 preferred first — shares serve pkg plumbing) |
| 6 | `stage-6-service-assembly.md` | `[serving]` config, gate, JSON-RPC server, stubs, getLedgerEntries, v2 getHealth, `metrics` endpoint, latency middleware | 1, 3, 4, 5 |
| 7 | `stage-7-devbox-config.md` | Devbox TOML (+placeholders), RUNBOOK, smoke.sh | 6 |

- Run them strictly in order 1→7, one fresh Claude session each, in `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service`.

## Checkpoint protocol (how sessions hand off — SELF-ENFORCING)

- `CHECKPOINT.md` in this directory is the single living handoff document.
- **Entry gate (every session, first action):** read `CHECKPOINT.md` and verify every prior stage has an entry marked `COMPLETE`.
  - All prior entries COMPLETE → proceed with your own stage.
  - Any prior entry missing or `PARTIAL` → do NOT start your own stage. Resume THAT stage instead (its `stage-N-*.md` is the spec; a PARTIAL entry names the exact resume point), finish it INCLUDING its checkpoint entry, then STOP and tell the user to re-run the current stage's prompt in a fresh session.
- After the entry gate: read `00-DECISIONS.md` and your own `stage-N-*.md`, then the design docs/code files your stage doc lists.
- **Exit gate (every session, blocking):** a stage is NOT complete until its entry is appended to `CHECKPOINT.md` (template at the bottom of that file) — as-built exported APIs (exact signatures), files added/changed, deviations from the stage doc (with why), verification results, warnings for the next stage. Write it BEFORE your final summary. If you must stop early for ANY reason, still write the entry, marked `PARTIAL(resume: <exact next action>)`. Ending without an entry is a protocol violation that breaks the next session.
- **Commit gate (every session, after the exit gate):** never commit on your own initiative and NEVER push. After your final summary, wait. When the user says "commit" (or equivalent), make ONE commit on the current branch containing the stage's code changes AND its `CHECKPOINT.md` entry, with a `<stage-N>`-scoped descriptive message. If the user instead asks for changes, make them, update your checkpoint entry to match, and wait again.
- A session that discovers a stage doc is wrong about the code TRUSTS THE CODE, does the minimal sane adaptation, and records the correction in its checkpoint entry.

## Exact prompts (copy-paste one per fresh session)

### Session 1

```text
Open /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/ and follow
the checkpoint protocol in 01-STAGES.md: first run its entry gate against CHECKPOINT.md
(if a prior stage is missing/PARTIAL, resume that stage instead and stop after it). Then
read 00-DECISIONS.md and stage-1-ingestion-metrics.md plus the files it lists, and
implement Stage 1 exactly as scoped: the latencytrack package, per-ledger ingestion
instrumentation (read/write/e2e with p50/p75/p90/p99 + max), and the minimal admin HTTP
listener. Verify per the stage doc's Acceptance section (build, vet, tests). BLOCKING
exit gate: append your stage entry to CHECKPOINT.md (COMPLETE, or PARTIAL with exact
resume point — even if you stop early) BEFORE your final summary. Then follow the commit
gate: never commit uninvited, never push — when I say "commit", commit this stage's code
+ CHECKPOINT.md entry as one commit on the current branch.
```

### Session 2

```text
Open /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/ and follow
the checkpoint protocol in 01-STAGES.md: first run its entry gate against CHECKPOINT.md —
stage 1 must be COMPLETE; if not, resume stage 1 instead and stop after it. Then read
00-DECISIONS.md and stage-2-registry.md, design-docs/query-routing-design.md in full, and
the code files the stage doc lists, and implement Stage 2: the fullhistory/registry
package (View, Registry, admission, Reaper, cold-reader LRU caches, resolve,
BuildFromCatalog) exactly per the spec and stage doc. Verify per the stage doc's
Acceptance section. BLOCKING exit gate: append your stage entry to CHECKPOINT.md
(COMPLETE, or PARTIAL with exact resume point — even if you stop early) BEFORE your
final summary. Then follow the commit gate: never commit uninvited, never push — when I
say "commit", commit this stage's code + CHECKPOINT.md entry as one commit on the
current branch.
```

### Session 3

```text
Open /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/ and follow
the checkpoint protocol in 01-STAGES.md: first run its entry gate against CHECKPOINT.md —
stages 1-2 must be COMPLETE; if not, resume the unfinished stage instead and stop after
it. Then read 00-DECISIONS.md and stage-3-write-integration.md (plus stage 2's as-built
API in CHECKPOINT.md, design-docs/query-routing-design.md §View update points, and the
code files the stage doc lists), and implement Stage 3: wire the registry hooks through
hotloop, backfill, lifecycle, and startup; transfer hot DB ownership to the registry;
defer physical destruction through the reaper — exactly per the stage doc's hook table.
ALL existing fullhistory tests must stay green. BLOCKING exit gate: append your stage
entry to CHECKPOINT.md (COMPLETE, or PARTIAL with exact resume point — even if you stop
early) BEFORE your final summary. Then follow the commit gate: never commit uninvited,
never push — when I say "commit", commit this stage's code + CHECKPOINT.md entry as one
commit on the current branch.
```

### Session 4

```text
Open /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/ and follow
the checkpoint protocol in 01-STAGES.md: first run its entry gate against CHECKPOINT.md —
stages 1-3 must be COMPLETE; if not, resume the unfinished stage instead and stop after
it. Then read 00-DECISIONS.md and stage-4-ledger-tx-adapters.md (plus stages 2-3 as-built
in CHECKPOINT.md and the interface/consumer/primitive files the stage doc lists), and
implement Stage 4: the fullhistory/serve package's db.LedgerReader and
db.TransactionReader veneers over the registry (admission mapping via NewTx,
chunk-traversing BatchGetLedgers, TxReader-backed GetTransaction with floor gating).
Verify per the stage doc's Acceptance section. BLOCKING exit gate: append your stage
entry to CHECKPOINT.md (COMPLETE, or PARTIAL with exact resume point — even if you stop
early) BEFORE your final summary. Then follow the commit gate: never commit uninvited,
never push — when I say "commit", commit this stage's code + CHECKPOINT.md entry as one
commit on the current branch.
```

### Session 5

```text
Open /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/ and follow
the checkpoint protocol in 01-STAGES.md: first run its entry gate against CHECKPOINT.md —
stages 1-4 must be COMPLETE; if not, resume the unfinished stage instead and stop after
it. Then read 00-DECISIONS.md and stage-5-events-adapter.md (plus stages 2-4 as-built in
CHECKPOINT.md; db/event.go and methods/get_events.go are the behavioral contract — read
them carefully), and implement Stage 5: the db.EventReader veneer (filter cross-product
mapping, ledger-windowed per-chunk Query, exact v1 cursor semantics, eventTypes
post-filter, DiagnosticEvent wrapping). Verify per the stage doc's Acceptance section.
BLOCKING exit gate: append your stage entry to CHECKPOINT.md (COMPLETE, or PARTIAL with
exact resume point — even if you stop early) BEFORE your final summary. Then follow the
commit gate: never commit uninvited, never push — when I say "commit", commit this
stage's code + CHECKPOINT.md entry as one commit on the current branch.
```

### Session 6

```text
Open /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/ and follow
the checkpoint protocol in 01-STAGES.md: first run its entry gate against CHECKPOINT.md —
stages 1-5 must be COMPLETE; if not, resume the unfinished stage instead and stop after
it. Then read 00-DECISIONS.md and stage-6-service-assembly.md (plus stages 1-5 as-built
in CHECKPOINT.md; internal/jsonrpc.go and internal/daemon/daemon.go are the v1
templates), and implement Stage 6: the [serving] config section superimposing v1 knobs,
the always-up HTTP servers with the backfill gate, the v2 interfaces.Daemon,
unsupported-error stubs, getLedgerEntries via the captive core query server, the v2
getHealth, the per-endpoint latency middleware, and the custom `metrics` JSON-RPC method.
Verify per the stage doc's Acceptance section including the manual smoke. BLOCKING exit
gate: append your stage entry to CHECKPOINT.md (COMPLETE, or PARTIAL with exact resume
point — even if you stop early) BEFORE your final summary. Then follow the commit gate:
never commit uninvited, never push — when I say "commit", commit this stage's code +
CHECKPOINT.md entry as one commit on the current branch.
```

### Session 7

```text
Open /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/ and follow
the checkpoint protocol in 01-STAGES.md: first run its entry gate against CHECKPOINT.md —
stages 1-6 must be COMPLETE; if not, resume the unfinished stage instead and stop after
it. Then read 00-DECISIONS.md and stage-7-devbox-config.md, and produce the devbox
deliverables: deploy/devbox/full-history.toml with FILL-marked placeholders matching the
config keys that were ACTUALLY built, the captive-core template, RUNBOOK.md (verified by
doing a clean build yourself), and smoke.sh covering every served endpoint plus one stub.
BLOCKING exit gate: append your stage entry to CHECKPOINT.md marking the plan COMPLETE
(or PARTIAL with exact resume point) BEFORE your final summary. Then follow the commit
gate: never commit uninvited, never push — when I say "commit", commit this stage's
deliverables + CHECKPOINT.md entry as one commit on the current branch.
```

## Notes

- Prompts are intentionally short — the stage docs carry the detail, the checkpoint carries the state. If you add scope mid-flight, put it in the stage doc (or a new stage file) rather than a longer prompt, so the paper trail stays in one place.
- The entry/exit/commit gates make the protocol self-enforcing: you never need to inspect CHECKPOINT.md between sessions, and each stage lands as exactly one commit, cut only when you say "commit". Re-running a stage's prompt after any failure is always safe — the entry gate routes the session to the right resume point.
- Stages 4 and 5 could run in parallel worktrees if you ever want to; they were sequenced to share the serve package scaffolding without merge friction.
