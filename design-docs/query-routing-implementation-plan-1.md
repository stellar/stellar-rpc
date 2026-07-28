# Query Routing Implementation Plan (main components)

## Overview

Implementation plan for the [query routing design](./query-routing-design.md), covering the main components only: the RocksDB and catalog primitives, the serving core (router, admission, resolution, bounds, cursors), the write-side integration, deferred deletion, and the router bootstrap. Per-method endpoint adapters are out of scope here (see [Out of scope](#out-of-scope)).

The plan is written from scratch against the code on the current base branch. It does not build on the `qr-*` branches.

Grounding references use `file:line` against `cmd/stellar-rpc/internal/fullhistory/`.

- **Design doc:** [`design-docs/query-routing-design.md`](./query-routing-design.md)

## Components at a glance

```
                     ┌─ read-side track ──────────────┐
1 (rocksdb) ─┬─► 3a (router/admission) ─► 3b/3c/3d (resolve/bounds/cursors) ─┐
2 (catalog) ─┘        │                                                      ├─► 6 (bootstrap)
                      └─ write-side track ─► 4 (D-1) ─► 5 (deferred deletion) ┘
```

The dependency graph is more granular than "3 → 4 → 5":

- **Component 4 depends on 3a only** — it needs the Router to publish handles and the watermark and to `DiscardHandle`. It does not need resolve/bounds/cursors (3b–3d).
- **Component 5's hot-DB deletion depends on Component 4** — only after ownership transfer does a live shared handle exist at discard time to `CloseIfIdle`. Its cold-file deletion half depends on neither 4 nor the read side; it is pure lifecycle sequencing.
- **3b–3d are read-side only** — the deletion path never calls `resolve`; the grace period protects readers, but the mechanism does not depend on reader code existing.

**Chosen order:** `3a → 4 → 5` (write-side track) first, to validate the write-side orderings live before any query depends on them, then back to `3b → 3c → 3d` (read-side) and `6` (bootstrap). Components 1 and 2 are pure plumbing with no consumers and land alone.

## Working method

Each component lands with its tests and is reviewed before the next begins. Larger components are built in reviewable stages (Component 3 is split into 3a–3d). Comments are trimmed after each stage: state the contract and the non-obvious "why", drop restatements and forward-references to consumers.

## Decision gates

- **D-1 (blocks Component 4): hot-handle ownership transfer.** Today the chunk boundary closes the completed chunk's hot DB (`hotloop.go:173`) and freeze reopens it read-only (`backfill/process.go:201`). That read-only open is ledgers-only, and `hotchunk.DB.Events()` panics on it, so serving events from a completed hot chunk requires keeping the writer-opened (events-warm) handle alive until discard. The design transfers ownership of each `*hotchunk.DB` to the router at the boundary instead of closing. This changes ingestion code another author owns; agree on it before starting Component 4.
- **Grace period T (needed for Component 5):** pick the safety margin and confirm `T = max request timeout + margin` exceeds the max request timeout.

## Status legend

- ⬜ planned
- 🟡 in progress
- 🟢 done (uncommitted)
- ✅ merged

---

## Component 1 — RocksDB primitives

- **Status:** 🟢
- **Depends on:** nothing (no consumers yet; lands alone)
- **Files:** `storage/rocksdb/rocksdb.go`, `storage/rocksdb/snapshot_test.go`

Done:

- **Snapshot support.** `NewSnapshot` / `ReleaseSnapshot` and the read methods `GetAsOf` / `IterateAsOf` on `*Snapshot`'s owning `Store`, wrapping grocksdb under the existing `RLock` + `checkOpen` discipline. Reads build a fresh `ReadOptions` with `SetSnapshot` (the `BatchMultiGet` pattern), never mutating the shared `s.ro`. `Get`/`Iterate` and their snapshot twins share one lock/checkOpen/resolveCF/C-call site (`getRO`/`iterateRO`).
- **Snapshot accounting.** `snapRefs atomic.Int64`; teardown logs a leak (`rocksdb: closing %s with %d unreleased snapshot(s)`) if it closes with any outstanding. `ReleaseSnapshot` skips the C release once the DB is torn down.
- **`CloseIfIdle`.** The non-blocking variant of `Close`, sharing an idempotent `teardownLocked` latched on `s.db == nil`. It sets the closed flag then `TryLock`s; on a busy store it returns `(false, nil)` without blocking and without un-poisoning, so the straggler's next op fails cleanly and a later run retries the teardown.

Review notes settled: naming is `GetAsOf`/`IterateAsOf` (verb-first, avoids "returns a snapshot"); the closed flag is never rolled back (safe because `CloseIfIdle` only runs on a resource being deleted); comments avoid the "pins compaction" framing (the cost is negligible for both the short-lived hot chunks and the sub-MB catalog — a leaked snapshot is a held-resource bug, not a compaction cost).

## Component 2 — Catalog snapshot reads

- **Status:** 🟢
- **Depends on:** Component 1
- **Files:** `catalog/kv.go`, `catalog/catalog.go`, `catalog/catalog_snapshot_test.go`

Done:

- **KV primitives** threaded with an optional snapshot: `getAsOf(snap, key)` and `prefixScanAsOf(snap, prefix)`; `nil` snap = live read, so every existing write-protocol and sweep call site is behavior-unchanged.
- **Snapshot lifecycle** on `Catalog`: `NewSnapshot` / `ReleaseSnapshot` pass-throughs returning the opaque `*rocksdb.Snapshot` (the raw store stays hidden — queries never touch it).
- **Typed `AsOf` twins**, each beside its live counterpart: `StateAsOf`, `HotStateAsOf` (shared `decodeState`/`decodeHotState`), `ReadyHotChunkKeysAsOf` (floor frontier; `hotChunkKeysWith` now takes a snapshot), `TxHashIndexKeysAsOf` and `FrozenTxHashIndexAsOf` (`txhashIndexKeysByPrefix` takes a snapshot; the INV-2 uniqueness assertion stays in one shared `frozenTxHashIndex` body).

## Component 3 — Serving core (Router, HandleSet, Admission, resolve, bounds, cursors)

- **Status:** 🟡 (stage 3a done)
- **Depends on:** Components 1 and 2
- **Files:** new `serving/` package (`router.go` + tests so far; `resolve.go`, `bounds.go`, `cursor.go`, `servingtest/` to come)

Built in reviewable stages:

- **3a — Router / HandleSet / Admission (done).** `Router` (catalog + retention + `latest atomic.Uint32` + `handles atomic.Pointer[HandleSet]` + `mu`). Watermark `SetLatest`/`Latest`; copy-on-write `PublishHandle`/`DiscardHandle`. `Admit` performs the three loads in order (latest, handle set, snapshot last) and derives the floor from `Retention.FloorAt(hotFrontier(snap))`, where `hotFrontier` is the highest ready hot chunk minus one (`-1` when none). `Admission.Release` returns the snapshot; `Admit` releases on its own error path. Tests: floor-derivation matrix, floor/handle-set/watermark pinned at the admission instant, copy-on-write isolation, and the leak/no-leak snapshot lifecycle at close.
- **3b — Resolution and cold opens.** `resolve(chunk, kind)` reading artifact and hot states through the snapshot: frozen wins → cold reader; ready-hot + handle present → hot facade; else `ErrUnavailable` (R1). Add a kind-dispatch accessor on `hotchunk.DB` (it exposes `Ledgers`/`Txhash`/`Events`, no `Store(kind)`); per-request cold opens for `KindLedgers` and `KindEvents`. Window `.idx` coverage opens belong to the getTransaction endpoint.
- **3c — Bounds.** Validate/clamp against `[floor, latest]`: leading edge below floor rejected with an available-range error; trailing edge beyond `latest` truncated; descending scans terminate at floor.
- **3d — Cursors.** The five rules: ledger coordinates only; exclusive resume in scan direction; bounds and filters travel in the cursor while the floor is gated per-page; `latest` re-read per page; empty pages still advance.
- **servingtest/** shared fakes, introduced when the first stage needs cross-package fixtures.

## Component 4 — Write-side integration and ownership transfer (D-1)

- **Status:** 🟢 (4a + 4b committed)
- **Depends on:** Component 3a; decision D-1 (agreed)
- **Files:** `hotloop.go`, `backfill/process.go`, `startup.go`
- **4a (committed):** router constructed in `run()`, threaded into the ingestion loop; watermark advanced at the commit site (nil-guarded for the bounded loop).
- **4b (committed):** ownership transfer at the boundary (router path keeps the completed handle open; nil-Router bounded loop keeps the old close-before-next-key fence), publish at open, freeze reads the shared handle (`HotHandle`) with read-only reopen as the no-writer fallback, discard demotes + destroys via `CloseIfIdle` at end of run. Fence/exclusion comments rewritten.

- **`latest` watermark is net-new** — no readable live-sequence field exists today (only the `metrics.LastCommitted` gauge and `healthState`'s close time). Drive `router.SetLatest(seq)` from the per-ledger commit site (`hotloop.go:157`), after `hotService.Ingest` returns, since `IngestLedger` completes the in-memory events apply (`PhaseApply`) before returning. This satisfies "latest advances last".
- **Publish the handle** in `openHotDBForChunk`'s ready path, after the catalog flip and before the chunk's first ledger commits.
- **D-1 — ownership transfer.** The boundary (`hotloop.go:167-192`) stops closing the completed chunk's DB (currently `hotloop.go:173`); ownership transfers to the router, and the loop's defer-close applies only to clean shutdown of the live chunk. Keeping the writer-opened handle alive preserves the events-warm state a read-only reopen cannot provide.
- **Freeze reads through the shared handle.** `resolveHotSource` (`backfill/process.go:201`) takes the router's handle instead of reopening the completed chunk read-only.
- **Discard** removes the handle from the handle set before the catalog demotion; destruction moves to Component 5.
- **Construct the router** in `daemon.go` / `startup.go`.

## Component 5 — Deferred deletion in the lifecycle run

- **Status:** 🟢 (cold-file deferral committed; startup sweep + grace moved to Component 6 / #772)
- **Depends on:** Component 1 (`CloseIfIdle`), Component 4
- **Files:** `lifecycle/` (`deletion.go`, `eligibility.go`, `lifecycle.go`), `catalog/catalog_sweep.go`
- **Done:** cold sweeps split into `Demote*` + `Destroy*`; the prune stage demotes and defers; one run-local list destroys hot + cold at end of run after one grace wait.
- **Grace:** placeholder `defaultGrace = 5m` (applied in `WithLifecycleDefaults`), overridable via the `lifecycleGrace` test seam (`daemonOptions` → `StartConfig`); the e2e sets it to 1ms. TODO(#772): derive `grace = max request timeout + margin` from the read server's request deadline and boot-validate `T` exceeds it.
- **Moved:** the startup re-discovery sweep lands with the Component-6 bootstrap.

- Today `DiscardHotChunk` does an unconditional synchronous `os.RemoveAll` (`catalog_sweep.go:112`); the cold sweeps delete immediately too. Change discard and sweep to demote-only and append each demoted item to a run-local list.
- At the end of the run, if the list is non-empty, wait the grace period **T** once, then delete: `CloseIfIdle` for hot DBs, unlink for files, directory removal, catalog-key removal. An item that cannot be deleted is alarmed, skipped, and re-discovered by the next run's scans.
- **Startup sweep** before `ServeReads`: re-discover pending demotions and run the same deletion body without the grace wait (no query survives the process).
- **Config:** the grace margin (the only knob); `T = max request timeout + margin`; boot validation rejecting a margin that does not make `T` exceed the max request timeout.

## Component 6 — Startup bootstrap

- **Status:** 🟢 (bootstrap + startup sweep committed)
- **Depends on:** Components 3a, 4, 5
- **Files:** `serving/router.go` (`BootstrapHandles`), `lifecycle/startup_sweep.go`, `startup.go`

Placed in `run()` (where the router is constructed), not the injected `ServeReads` closure — that stays the #772 read-server hook. Before the loops start: `StartupSweep` destroys leftover demotions from a crashed run (transient hot below the live chunk, pruning cold), then `Router.BootstrapHandles` opens and publishes handles for the ready hot chunks below the live one (opened read-write so events are warm; the loop publishes the live chunk).

**Deferred to #772:** the actual read server, the `getLatestLedger`/`getHealth` mappings, serving metrics, and wiring `grace = max request timeout + margin` (needs the request deadline). Until then the router runs live but no endpoint consumes it.

## Out of scope

Deferred to endpoint work, not part of this plan:

- The `getLedgers`, `getTransactions`, `getTransaction`, and `getEvents` adapters.
- The `getTransaction` `.idx` window-coverage lookup path.
- `getLatestLedger` / `getHealth` mappings.
- Serving metrics.
- The concurrent-lifecycle end-to-end test.
- The v1→v2 handler cutover and SQLite removal (#772).

## Related documents

- [query-routing-design.md](./query-routing-design.md): the design this plan implements.
- [full-history-streaming-workflow.md](./full-history-streaming-workflow.md): the write-side machinery the hooks attach to.
