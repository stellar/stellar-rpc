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
- Commit between stages yourself if you want checkpointed git history — sessions never commit (D9).

## Checkpoint protocol (how sessions hand off)

- `CHECKPOINT.md` in this directory is the single living handoff document.
- Every session, FIRST action: read `CHECKPOINT.md`, `00-DECISIONS.md`, and its own `stage-N-*.md`; then read the design docs its stage file lists.
- Every session, LAST action before its final summary: append its entry to `CHECKPOINT.md` using the template at the bottom of that file — as-built exported APIs (exact signatures), files added/changed, deviations from the stage doc (with why), verification results, and warnings for the next stage.
- A session that discovers a stage doc is wrong about the code TRUSTS THE CODE, does the minimal sane adaptation, and records the correction in its checkpoint entry.
- If a session runs out of runway mid-stage, it still writes its checkpoint entry marking the stage `PARTIAL` with an exact resume point; re-running the same stage prompt resumes it.

## Exact prompts (copy-paste one per fresh session)

### Session 1

```text
Read /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md,
then 00-DECISIONS.md and stage-1-ingestion-metrics.md in the same directory, and the files
the stage doc says to read. Then implement Stage 1 exactly as scoped: the latencytrack
package, per-ledger ingestion instrumentation (read/write/e2e with p50/p75/p90/p99 + max),
and the minimal admin HTTP listener. Verify per the stage doc's Acceptance section
(build, vet, tests). Do NOT commit or push. Finish by appending your entry to
CHECKPOINT.md per the protocol in 01-STAGES.md, then give me a short summary.
```

### Session 2

```text
Read /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md,
then 00-DECISIONS.md and stage-2-registry.md in the same directory, plus
design-docs/query-routing-design.md in full and the code files the stage doc lists.
Then implement Stage 2: the fullhistory/registry package (View, Registry, admission,
Reaper, cold-reader LRU caches, resolve, BuildFromCatalog) exactly per the spec and
stage doc. Verify per the stage doc's Acceptance section. Do NOT commit or push.
Finish by appending your entry to CHECKPOINT.md, then give me a short summary.
```

### Session 3

```text
Read /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md
(especially stage 2's as-built API), then 00-DECISIONS.md and stage-3-write-integration.md,
plus design-docs/query-routing-design.md §View update points and the code files the stage
doc lists. Then implement Stage 3: wire the registry hooks through hotloop, backfill,
lifecycle, and startup; transfer hot DB ownership to the registry; defer physical
destruction through the reaper — exactly per the stage doc's hook table. ALL existing
fullhistory tests must stay green. Do NOT commit or push. Finish by appending your entry
to CHECKPOINT.md, then give me a short summary.
```

### Session 4

```text
Read /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md
(stages 2–3 as-built), then 00-DECISIONS.md and stage-4-ledger-tx-adapters.md, plus the
interface/consumer/primitive files the stage doc lists. Then implement Stage 4: the
fullhistory/serve package's db.LedgerReader and db.TransactionReader veneers over the
registry (admission mapping via NewTx, chunk-traversing BatchGetLedgers, TxReader-backed
GetTransaction with floor gating). Verify per the stage doc's Acceptance section.
Do NOT commit or push. Finish by appending your entry to CHECKPOINT.md, then give me
a short summary.
```

### Session 5

```text
Read /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md
(stages 2–4 as-built), then 00-DECISIONS.md and stage-5-events-adapter.md, plus the
interface/consumer/engine files the stage doc lists (db/event.go and methods/get_events.go
are the behavioral contract — read them carefully). Then implement Stage 5: the
db.EventReader veneer (filter cross-product mapping, ledger-windowed per-chunk Query,
exact v1 cursor semantics, eventTypes post-filter, DiagnosticEvent wrapping). Verify per
the stage doc's Acceptance section. Do NOT commit or push. Finish by appending your entry
to CHECKPOINT.md, then give me a short summary.
```

### Session 6

```text
Read /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md
(stages 1–5 as-built), then 00-DECISIONS.md and stage-6-service-assembly.md, plus the v1
assembly files the stage doc lists (internal/jsonrpc.go and internal/daemon/daemon.go are
the templates). Then implement Stage 6: the [serving] config section superimposing v1
knobs, the always-up HTTP servers with the backfill gate, the v2 interfaces.Daemon,
unsupported-error stubs, getLedgerEntries via the captive core query server, the v2
getHealth, the per-endpoint latency middleware, and the custom `metrics` JSON-RPC method.
Verify per the stage doc's Acceptance section including the manual smoke. Do NOT commit
or push. Finish by appending your entry to CHECKPOINT.md, then give me a short summary.
```

### Session 7

```text
Read /Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md
(all prior stages), then 00-DECISIONS.md and stage-7-devbox-config.md. Then produce the
devbox deliverables: deploy/devbox/full-history.toml with FILL-marked placeholders
matching the config keys that were ACTUALLY built, the captive-core template, RUNBOOK.md
(verified by doing a clean build yourself), and smoke.sh covering every served endpoint
plus one stub. Do NOT commit or push. Finish by appending your entry to CHECKPOINT.md
marking the plan COMPLETE, then give me a short summary.
```

## Notes

- Prompts are intentionally short — the stage docs carry the detail, the checkpoint carries the state. If you add scope mid-flight, put it in the stage doc (or a new stage file) rather than a longer prompt, so the paper trail stays in one place.
- Stages 4 and 5 could run in parallel worktrees if you ever want to; they were sequenced to share the serve package scaffolding without merge friction.
