# Query Routing Implementation Plan

## Overview

Plan for implementing the [query routing design](./query-routing-design.md): catalog-snapshot admission, the router (watermark plus hot handle set), lifecycle-executed deferred deletion, and the per-method adapters, on top of the storage and lifecycle machinery on `feature/full-history`.

The work is split into two workstreams. WS1 is the foundation: every plumbing, admission, and lifecycle change, landed as a single PR built in reviewable stages, with pre-marked seams to split along if the diff grows too large. WS2 is the endpoint work: one PR per query method plus wiring and the end-to-end test. Nothing in WS1 changes user-visible behavior; the serving stack becomes reachable only in WS2.

- **Design doc:** [`design-docs/query-routing-design.md`](./query-routing-design.md)
- **Base branch:** `feature/full-history`. All the storage, catalog, lifecycle, and ingestion machinery the plan builds on is merged.
- **Estimated calendar time (solo, serial):** 4 to 6 weeks.
- **Out of scope:** the v1 to v2 handler cutover and SQLite removal (#772), the getEvents v2 API surface (#426), and feeding `getFeeStats` in v2 (an ingestion-workstream item, flagged there).

## How to use this doc

- **One PR per Claude Code session** (the foundation PR will take several sessions; work stage by stage). Each PR section contains a cold-start prompt; paste it into a fresh session to begin.
- **Update the Status field** when a PR transitions. Commit the status change with the PR's code.
- **Branch naming:** `feat/qr-<n>-<slug>`, for example `feat/qr-1-foundation`.
- **One commit per foundation stage.** The foundation PR is invariant-bearing code that would normally land as several small PRs. To keep it reviewable at this size, each stage is one self-contained commit with its tests, in the listed order, so review can proceed commit by commit. The stage boundaries double as the split seams.
- **Engineering guardrails:** metrics are plain counters and histograms registered the existing way, no emission choreography; test fakes are designed once in a shared `serving/servingtest` helper and imported, not copied per package; comments state contracts and non-obvious orderings and link to the design doc for rationale rather than restating it.

## Reading list (read once before starting)

1. `design-docs/query-routing-design.md`: the design this plan implements. The load-bearing parts are the admission load order (`latest`, then handles, then snapshot), the write-side transitions table with its three ordering notes, and the deferred-deletion sequence.
2. `design-docs/full-history-streaming-workflow.md`: the write side, for the catalog states and lifecycle stages the hooks attach to.
3. The landed shapes the plan targets: `storage/rocksdb/rocksdb.go` (the `Store` wrapper and its lifecycle-lock discipline), `hotloop.go` (`openHotDBForChunk`, `runIngestionLoop`), `lifecycle/lifecycle.go` (`runLifecycle`, `Loop`, `BoundarySignal`), `catalog/catalog_sweep.go` (`SweepChunkArtifacts`, `SweepTxHashIndexKey`, `DiscardHotChunk`).
4. `MEMORY.md` entries: auto-loaded into every session.

## Decision gates

Two decisions gate specific work. D-1 gates a foundation stage; settle it before starting stage 7.

- **D-1 (blocks foundation stage 7): hot-handle ownership at the chunk boundary.** Today `runIngestionLoop` closes the completed chunk's hot DB at the boundary, and the freeze path in `backfill/process.go` reopens it read-only on the assumption that the writer closed it cleanly. Serving requires the completed chunk's DB to stay open until discard (it is the chunk's only tx-hash home during the freeze-to-coverage gap). The design's resolution: the router owns each `*hotchunk.DB` from open to discard, the boundary releases the writer role without closing, and the freeze reads through the shared handle. This changes ingestion code another author owns, so agree on it first. Design reference: [Changes to the streaming workflow](./query-routing-design.md#changes-to-the-streaming-workflow), hot database ownership.
- **D-2 (blocks the v2 half of PR-4): getEvents v2 API finalization (#426).** PR-4 lands the v1 scope (ascending, TOID cursor) regardless; descending, opaque cursors, and TransactionQuery follow #426 in a separate PR.

## Status legend

- ⬜ planned
- 🟡 in progress
- 🟢 merged

---

# Workstream 1: foundation

Goal: everything the endpoints need, landed before any endpoint exists. The rocksdb primitives, the serving package (admission, resolution, bounds), and the write-side integration (publish hooks, handle ownership, deferred deletion). After this workstream the daemon maintains the router live against real ingestion and lifecycle activity, with zero user-visible behavior.

## PR-1: serving foundation

- **Status:** 🟡 (stages 1–6 done on `feat/qr-1-foundation`; stage 7 awaits D-1)
- **Depends on:** decision D-1 (stage 7 onward)
- **Stages, in order, one commit each:**

  1. **rocksdb snapshot support.** `NewSnapshot`, `ReleaseSnapshot`, snapshot-pinned point reads (`GetSnap`) and iteration, under the same lifecycle-lock discipline as the existing operations (read lock across every C call, `checkOpen` before each). The close path accounts for outstanding snapshots. Tests: repeatable reads under concurrent writes, release semantics, operations against a released snapshot fail cleanly. Also fold in or delete the untracked `storage/rocksdb/snapbench/` benchmark; it should not stay untracked.
  2. **rocksdb `CloseIfIdle`.** The non-blocking variant of `Close`: set the closed flag, close only when no operation is in flight, report failure otherwise. Tests: an in-flight operation makes it report failure without blocking; every operation after the flag is set fails with the closed error whether or not the close succeeded. This is the drain-barrier code the memory-safety argument rests on; review adversarially.
  3. **Catalog snapshot reads.** Snapshot-pinned variants of the read accessors admission and resolution need (`State`, `HotState`, `TxHashIndexKeys` reading through a supplied snapshot).
  4. **Router, HandleSet, Admission.** `serving/router.go`: `Router` (`latest` atomic, `handles atomic.Pointer[HandleSet]`, `mu` serializing handle-set updates, the catalog, `geometry.Retention`), handle publish and discard (clone the map under `mu`, publish atomically), `Admit` and `Admission.Release`. `Admit` performs the three loads in the design's order: `latest` first, handle set second, catalog snapshot last, then derives the floor from the snapshot (`Retention.FloorAt` anchored at the highest ready hot chunk minus one, the same anchor the lifecycle run uses). Plus `serving/servingtest/`: the shared fakes (catalog fixture builder, handle-set fixtures) every later stage and PR imports. Tests: the two skew cases from the design's admission section (snapshot shows a chunk demoted after the handle set was loaded; a hot chunk missing from the handle set is unreachable because `latest` cannot point into it), floor derivation matches the lifecycle anchor, release on error paths.
  5. **Resolution and cold opens.** `serving/resolve.go`: `resolve(chunk, kind)` reading artifact and hot states through the admission snapshot: frozen wins, ready-hot second, everything else `ErrUnavailable` (R1). Per-kind cold opens (`ledger` and `eventstore` cold readers) owned by the request; window `.idx` files are opened by the lookup path in PR-3, not here. Tests: the resolution matrix (frozen only, hot only, both with cold winning, freezing/pruning/transient invisible, no home), reader ownership.
  6. **Bounds, cursors, errors.** `serving/bounds.go`: validate and clamp against the admitted `[floor, latest]`: a leading edge below the floor is rejected with an error carrying the available range; a trailing edge beyond `latest` is truncated; descending scans terminate at the floor. `ErrUnavailable`, the expired-cursor error shape, and the JSON-RPC error-code mapping. `serving/cursor.go`: the five cursor rules from the design (ledger coordinates only, exclusive resume in scan direction, bounds and filters travel in the cursor while the floor does not, `latest` re-read per page, empty pages still advance). Tests: the ascending and descending edge matrix, cursor aging below the floor, the empty-page advance.
  7. **Publish hooks and hot-handle ownership (D-1).** `hotloop.go`: publish the handle into the handle set in `openHotDBForChunk`'s ready path, after the catalog flip and before the chunk's first ledger commits; `latest.Store(seq)` as the final step of the per-ledger cycle, after `IngestLedger` returns (`hotchunk.IngestLedger` completes the in-memory events apply before returning, so this placement satisfies the "latest advances last" rule); the boundary stops closing the completed chunk's DB and ownership transfers to the router, with the loop's defer-close applying only to clean shutdown of the live chunk. `backfill/process.go`: the freeze's hot source reads through the router's shared handle instead of reopening the closed chunk read-only. `daemon.go` / `startup.go`: construct the router. Discard call sites remove the handle from the handle set before the catalog demotion; destruction moves to stage 8.
  8. **Deferred deletion in the lifecycle run.** `lifecycle/deletion.go`: the run-local demoted-items list; stages append what they demote (superseded index generations, discarded hot chunks, pruned artifacts). When the stages finish and the list is non-empty, the run waits out the grace period once, then deletes each item: `CloseIfIdle` for hot databases, unlink for files, directory removal, catalog-entry removal. An item that cannot be deleted is alarmed and skipped; the next run's scans re-discover its demoted key and retry. Startup sweep: before `ServeReads`, re-discover pending demotions from the catalog and run the same deletion body without the grace wait (no query survives the process). Config: the grace margin (the only knob); T derived as the maximum request timeout plus the margin; boot validation rejecting a margin that makes T not exceed the maximum request timeout. Tests with a fake clock: nothing deletes before T; a blocked `CloseIfIdle` is alarmed, skipped, and retried by the next run; crash-restart re-discovers and completes leftover demotions.

- **New files:** `serving/` (router.go, resolve.go, bounds.go, cursor.go, servingtest/), `lifecycle/deletion.go`, rocksdb snapshot and close additions, tests throughout.
- **Touched files:** `storage/rocksdb/rocksdb.go`, `catalog/`, `hotloop.go`, `backfill/process.go`, `lifecycle/lifecycle.go`, `daemon.go`, `startup.go`, `config/config.go`, `config_validate.go`.
- **Split seams, if the PR grows too large.** The stage boundaries are ordered so any prefix is landable on its own:
  - **Seam A, after stage 2:** rocksdb primitives (snapshots, `CloseIfIdle`). Pure wrapper code, no consumers yet.
  - **Seam B, after stage 6:** the serving package (catalog snapshot reads, router, admission, resolution, bounds). Fixture-tested, no daemon changes.
  - **Seam C, the rest:** write-side integration (hooks, ownership, deletion). The only part that touches running daemon code, and the part D-1 gates.
- **Goal:** the router runs live against real ingestion and lifecycle activity; retired resources are deleted by the run that demoted them; no endpoint exists yet, so the orderings can be validated (audit assertions, live soak) before any query depends on them.
- **Reading:** design doc: [Admission](./query-routing-design.md#admission), [Deferred deletion](./query-routing-design.md#deferred-deletion), [Write-side transitions](./query-routing-design.md#write-side-transitions), [Open-handle management](./query-routing-design.md#open-handle-management).
- **Cold-start prompt:**
  > "Work on PR-1 from `design-docs/query-routing-implementation-plan.md`: the serving foundation. Check the branch `feat/qr-1-foundation` for completed stages (one commit each) and continue with the next stage in the plan's list. The admission load order, the CloseIfIdle drain barrier, and the demote-wait-delete sequence are the review-critical parts."

---

# Workstream 2: endpoints and wiring

Goal: the four read paths resolve through admission, the serving stack is reachable end to end, and the concurrency guarantees are exercised against the real daemon. Interfaces match what `internal/methods` handlers already consume, so the cutover (#772) is a wiring swap.

## PR-2: ledger range streamer, getLedgers and getTransactions adapters

- **Status:** ⬜
- **Depends on:** PR-1
- **New files:**
  - `serving/ledgers.go`: the per-chunk walk (`resolve`, stream the requested range from the cold reader or the hot store), multi-chunk concatenation in both directions, mid-ledger resume for transactions. Each page performs its own admission and releases its own snapshot.
  - Adapter types satisfying the read interfaces `internal/methods` consumes for getLedgers and getTransactions.
  - Tests: the design's boundary-straddling worked example (a request spanning the last cold chunk and the live chunk), descending traversal, per-page admission.
- **Touched files:** none
- **Goal:** the two range scans end to end against a real catalog fixture.
- **Reading:** design doc, [`getLedgers`](./query-routing-design.md#getledgers) and [`getTransactions`](./query-routing-design.md#gettransactions).
- **Cold-start prompt:**
  > "Implement PR-2 from the query-routing implementation plan: the ledger range streamer and the getLedgers/getTransactions adapters over admission, per the design doc walkthroughs. Include the boundary-straddling worked example as a test case. Branch: `feat/qr-2-ledgers`."

## PR-3: getTransaction adapter

- **Status:** ⬜
- **Depends on:** PR-2 (ledger source)
- **New files:**
  - `serving/txlookup.go`: per-request assembly of `txhash.NewTxReader(hot, cold, ledgerSource, passphrase)`: hot `HashIndex`es from the handle set newest first, window coverages iterated through the admission snapshot with each generation-named `.idx` opened as it is probed, and a ledger source backed by `resolve(chunk, Ledgers)`. A candidate is served only if `floor <= ledger <= latest`, both from admission.
  - Tests: the freeze-to-coverage gap (hot tx-hash home until index coverage publishes), the floor gate on a window index that still names pruned ledgers, the `latest` gate on a candidate above the admitted watermark.
- **Touched files:** none
- **Goal:** by-hash lookup across both tiers; `TxReader` itself is unchanged.
- **Reading:** design doc, [`getTransaction`](./query-routing-design.md#gettransaction).
- **Cold-start prompt:**
  > "Implement PR-3 from the query-routing implementation plan: the getTransaction adapter assembling txhash.TxReader from admission state. Test the freeze-to-coverage gap and both admission gates (floor and latest). Branch: `feat/qr-3-txlookup`."

## PR-4: getEvents adapter (v1 scope)

- **Status:** ⬜
- **Depends on:** PR-1; D-2 for the v2 half (separate follow-up PR)
- **New files:**
  - `serving/events.go`: page scan-window arithmetic, per-chunk `eventstore.Query` invocation over the common `eventstore.Reader` interface, cursor position plus `scannedLedger` progress. V1 scope: ascending, TOID cursor, the existing `db.EventReader` interface shape.
  - Tests: page termination on both conditions (limit reached, window exhausted), the empty-page cursor advance, a page spanning a hot and a cold chunk.
- **Touched files:** none
- **Goal:** filtered event queries over hot and cold chunks; the engine is unchanged.
- **Reading:** design doc, [`getEvents`](./query-routing-design.md#getevents) and [Cursors](./query-routing-design.md#cursors).
- **Cold-start prompt:**
  > "Implement PR-4 from the query-routing implementation plan: the getEvents adapter, v1 scope (ascending, TOID cursor) over eventstore.Query. Direction generality can be structured in but only ascending is exposed. Branch: `feat/qr-4-events`."

## PR-5: serveReads wiring, health surface, metrics

- **Status:** ⬜
- **Depends on:** PR-2 through PR-4
- **New files:** none
- **Touched files:**
  - `fullhistory/daemon.go` / `startup.go`: `ServeReads` builds the handle set from the catalog's ready hot keys, runs the startup deletion sweep from PR-1, and starts the read server (behind a flag until #772). Complete before the lifecycle goroutine starts and before queries are admitted.
  - getLatestLedger (sequence, hash, close time through admission) and getHealth (latest age, oldest ledger from the derived floor) mappings.
  - Metrics, plain counters and histograms only: admission count, open snapshot count and oldest snapshot age (the leak backstop the design's open questions ask for), `ErrUnavailable` count, deletion alarms, grace-wait duration.
- **Goal:** the serving stack is reachable end to end; the cutover itself (handler swap, SQLite removal) stays in #772.
- **Reading:** design doc, [Write-side transitions](./query-routing-design.md#write-side-transitions) (the startup row) and [Open questions](./query-routing-design.md#open-questions) (snapshot hygiene).
- **Cold-start prompt:**
  > "Implement PR-5 from the query-routing implementation plan: ServeReads wiring behind a flag (handle-set bootstrap plus startup deletion sweep), getLatestLedger/getHealth mappings, and the serving metrics listed in the plan. Metrics are plain counters and histograms registered the existing way. Branch: `feat/qr-5-serve`."

## PR-6: concurrent-lifecycle end-to-end test

- **Status:** ⬜
- **Depends on:** PR-2; extends as PR-3 and PR-4 land
- **New files:**
  - `fullhistory/serving_e2e_test.go` (or an extension of the existing e2e test): queries running continuously through ingest, freeze, index swap, discard, and prune; a paginated scan whose cursor crosses a hot-to-cold migration between pages; kill-and-restart completing leftover demotions before serving; an assertion that no admitted query ever observes `ErrUnavailable` for a chunk inside its admitted range.
- **Touched files:** none
- **Goal:** the design's four problem cases (H1 through H4) exercised against the real daemon, not fakes.
- **Reading:** design doc, [The problem](./query-routing-design.md#the-problem) and [Design summary](./query-routing-design.md#design-summary).
- **Cold-start prompt:**
  > "Implement PR-6 from the query-routing implementation plan: the concurrent-query lifecycle end-to-end test. Drive continuous queries through a full chunk lifecycle and assert the H1 through H4 guarantees from the design doc's problem table. Branch: `feat/qr-6-e2e`."

---

## Sequencing summary

```
PR-1 (foundation, stages 1..8; D-1 before stage 7)
  │
  ├──► PR-2 ──► PR-3
  │      └──────────► PR-6 (extends as PR-3, PR-4 land)
  ├──► PR-4 (v2 half after D-2)
  │
PR-2..4 ──► PR-5
Cutover (#772, out of plan): after PR-5
```

Stages 1 through 6 of the foundation have no dependency on D-1 and can proceed immediately. Raise D-1 while they are underway so stage 7 is not blocked. If the foundation splits, the seams are recorded in the PR-1 section; the split PRs land in seam order.

## Related documents

- [query-routing-design.md](./query-routing-design.md): the design this plan implements.
- [full-history-streaming-workflow.md](./full-history-streaming-workflow.md): the write-side machinery the hooks attach to.
- Issue #770: the design ask; #772: cutover; #426: getEvents v2 API.
