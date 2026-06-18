# Unified Ingestion Workflow — Implementation Issue Breakdown

> **Where this fits in [#777 RPC v2 Roadmap](https://github.com/stellar/stellar-rpc/issues/777):**
> the **Unified ingestion workflow → "Live ingestion + freeze/prune"** track, design issue **#722**.
> The design lives in `design-docs/full-history-streaming-workflow.md` (the daemon) and
> `design-docs/gettransaction-full-history-design.md` (the tx-hash subsystem). This file breaks that
> design into implementation issues.

---

## Scope and boundaries

**In scope:** the daemon that *orchestrates* storage — catch-up on startup, live ingestion from captive
core, the freeze → rebuild → discard → prune lifecycle, the catalog (meta-store) and the one write
protocol, derived progress, recovery, and the streaming-specific cold tx-hash protocol.

**Builds on / composes (separately tracked — do not reimplement):**

| Capability | Issue / package | Relationship |
|---|---|---|
| Per-data-type store write primitives (LCM → hot CF / cold artifact) | #765 — `internal/fullhistory/ingest` | compose `HotService`/`ColdService`, `RunHot`/`RunCold`, `ChunkSource`; this design = "when/how/with what crash-safety" |
| Hot tx-hash store | #729 — `pkg/stores/txhash` | composed by the hot-DB lifecycle |
| Cold tx-hash streamhash index (single-index build + read) | #728 — `pkg/stores/txhash/cold_*` | the rolling-rebuild + coverage protocol (Issue 6) layers on `BuildColdIndex`; #728 owns the `.bin`/`.idx` formats |
| XDR view extractors (events, tx-hashes, tx-details, tx-pages) | #764 | composed by `processChunk` / ingestion |
| Hot + cold ledger / event stores | #695/#739, #740/#756 — `pkg/stores/{ledger,eventstore}` | composed by the hot-DB lifecycle + `processChunk` |
| Chunk geometry, RocksDB + metastore helpers | `pkg/chunk`, `pkg/rocksdb`, `pkg/stores/metastore` | the catalog, geometry, and hot DB build on these |
| Packfile library (`.pack`) | `internal/packfile` | composed by `processChunk` / ledger fetch |
| **Query serving / reader routing** across hot + cold | #770 (design), #772 (cutover), #774 (events v2) | the design defers all read-path dispatch here; the reader honors the retention-floor contract |
| Trust-min validation | #773 (P2) | the `audit` deep-mode overlaps; otherwise independent |
| EBS / historical tier | (P3, no issue) | future; the immutable-file layout is forward-compatible |

**New code path.** New orchestration code lands under `cmd/stellar-rpc/internal/fullhistory/`, composing the
merged stores. The v1 SQLite ingestion/backfill path (`internal/ingest`, `internal/backfill`,
`daemon.go`'s backfill-then-ingest flow) is **subsumed by `startStreaming` and retired during the cutover
(#772)**; the standalone `03-backfill-workflow.md` design is superseded by the streaming doc.

---

## Build order (dependency phases)

```
Phase 1  Foundations          1 ─ Geometry      2 ─ Catalog + write protocol      3 ─ Config + locking
                                    │                  │   │                            │
Phase 2  Storage primitives    └──► 4 ─ Hot-DB lifecycle ──┤                            │
                                     5 ─ processChunk / catchupSource ◄── #765 #764      │
                                     6 ─ Tx-hash rolling rebuild  ◄── #728               │
                                     7 ─ Key-driven sweeps                               │
Phase 3  Orchestration         8 ─ Derived progress   9 ─ Resolver + executor           │
                                    10 ─ Ingestion loop      11 ─ Lifecycle tick         │
Phase 4  Wiring                12 ─ Startup (startStreaming) ◄───────────────────────────┘
                                    13 ─ Daemon/CLI wiring + retire v1 backfill
Phase 5  Operability           14 ─ Retention/widen/shorten   15 ─ Surgical recovery
                                    16 ─ audit command   17 ─ Metrics + logging
Phase 6  Validation            18 ─ Crash/convergence suite   19 ─ E2E integration   20 ─ Bench alignment
```

**Critical path:** 1 → 2 → 4/5/6/7 → 9 → 11 → 12 → 13. Issues 8, 10 fan in to 11/12. 16–20 trail and parallelize.

---

# Phase 1 — Foundations

### 1. Geometry & layout primitives
- **Scope:** Build on `pkg/chunk` (chunk id, first/last ledger, bucket id, `LedgersPerChunk=10_000`, genesis). Add what the design's geometry needs beyond it: the **window / `indexID`** arithmetic (`chunks_per_txhash_index`, `chunksInIndex`, `windowFirstChunk`/`windowLastChunk`), `lastCompleteChunkAt`, `MaxChunksPerTxhashIndex = floor(2³²/10_000) = 429_496`, and **signed** chunk arithmetic for the sub-genesis watermark sentinel (`chunk −1` → `chunkLastLedger(-1) = 1`) — `pkg/chunk.ID` is `uint32` and panics below genesis, so the sentinel is handled in the orchestration layer.
- **Acceptance:** exhaustive table-driven tests incl. the sentinel, young-network inverted ranges, the geometry table, contiguity (`chunkLastLedger(c)+1 == chunkFirstLedger(c+1)`), and round-trips.
- **Design refs:** "Geometry"; gettransaction §4. **Size:** S.

### 2. Catalog: key schema + one write protocol
- **Scope:** The streaming catalog built on `pkg/stores/metastore`. Key families (`chunk:{c}:{ledgers|events|txhash}`, `hot:chunk:{c}`, `index:{w}:{lo}:{hi}` with coverage in the name, `config:*` pins) with a strict key↔path bijection; states `freezing|frozen|pruning` and `transient|ready`. Typed reads: `State`, `frozenCoverage`, `hotChunkKeys`, `readyHotChunkKeys`, `indexKeys`, `chunkArtifactKeys`. The **one write protocol** (mark-then-write): put `"freezing"` before any I/O → fsync file + parent dirent (+ grandparent on a new bucket dir) → flip `"frozen"` (single put for per-chunk; atomic commit batch for the index). Single-process `flock` LOCK file lives here (taken in #3).
- **Acceptance:** crash-safety tests with simulated power-loss between each ordered step; "every file on disk has its key" and "key absent ⟹ file gone" hold at every interruption; multi-key batch atomicity; `frozenCoverage` uniqueness (>1 frozen per window is detectable).
- **Design refs:** "Data model", "One write protocol", "Substrate assumptions". **Depends on:** 1. **Size:** L.

### 3. Config schema, validation & single-process locking
- **Scope:** TOML schema (`[service]`, `[backfill]`, `[backfill.bsb]`, `[immutable_storage.*]`, `[catalog]`, `[streaming]`, `[streaming.hot_storage]`, `[logging]`) with defaults. `validateConfig`: `chunks_per_txhash_index` ∈ [1, Max], `workers ≥ 1`, `max_retries ≥ 0`, `earliest_ledger` form (genesis/now/chunk-aligned), the two-pin **atomic** first-start commit, restart immutability, `"now"`/numeric resolution requiring a reachable + ready tip. `flock` on the catalog path **and** each configured immutable-storage root **and** the hot-storage root.
- **Acceptance:** accepts valid configs; rejects every malformed case (zero/over-max cpi, zero workers, negative retries, misaligned/sub-genesis floor, future numeric floor); two daemons sharing any storage root are blocked; immutability aborts on pin mismatch.
- **Design refs:** "Configuration", `validateConfig`, "Single-process enforcement". **Depends on:** 1, 2. **Size:** M.

---

# Phase 2 — Storage primitives

### 4. Per-chunk hot DB lifecycle
- **Scope:** **One per-chunk hot RocksDB** holding all data types as column families (`ledgers` + the events CFs + the txhash CFs), so a ledger commits as **one atomic synced `WriteBatch` across all CFs** — the merged per-type hot stores are composed into this single multi-CF DB. `openHotDB` (ready→open / transient|absent→wipe+recreate with dirent + grandparent fsync; **fatal on a `ready` key whose dir is missing**), `discardHotDBForChunk` (transient bracket → rmdir → delete key), a read-only view for freezing. The `transient`/`ready` state machine.
- **Acceptance:** a ledger is fully present or fully absent (atomicity); create/discard idempotent across mid-op crashes; `ready`-but-missing-dir fatals with the curated recovery instruction (no auto-heal); the read handle closes before any same-tick discard.
- **Design refs:** "The chunk hot DB", "Hot DB helpers", "Hot DB lifecycle". **Composes:** `pkg/stores/{ledger,eventstore,txhash}` hot stores + `pkg/rocksdb`. **Depends on:** 2. **Size:** M.

### 5. `processChunk` + `catchupSource`
- **Scope:** Single-pass materialization of a chunk's cold artifacts (`ledgers`/`.pack`, events segment, `txhash`/`.bin`) with per-kind idempotency (skip if `"frozen"`), applying the one write protocol. `catchupSource` preference order — ready + complete hot DB → frozen local `.pack` (when `ledgers` not requested) → bulk backend — with the loss-vs-staleness rule and a bounded `waitForBackendCoverage` (fatal on timeout) for backend-only chunks above a lagging tip. The `.bin` is the merged txhash cold ingester's sorted run.
- **Acceptance:** re-materialization overwrites at the canonical path and is byte-identical; widening re-derives covered chunks from local `.pack` with no download; the backend-lag wait fires only for genuinely backend-only chunks.
- **Design refs:** "Backfill" / "The primitives" (artifact rules, `processChunk`, `catchupSource`). **Composes:** #765 `ColdIngester`s, #764 extractors, `internal/packfile`. **Depends on:** 1, 2, 4. **Size:** L.

### 6. Cold tx-hash rolling-rebuild protocol
- **Scope:** `buildTxhashIndex(w, lo, hi)`: skip-check (against the window's frozen coverage); coverage **mark**; k-way merge of `.bin[lo..hi]` → coverage-named `.idx` via streamhash's `SortedBuilder` (`payloadWidth` from cpi, `MinLedger` from `lo`, fingerprint); the atomic **commit batch** (promote new coverage / demote predecessor / on a terminal build demote every in-window `txhash` key). `buildThenSweep` runs the eager window-local sweep. Add the `streamhash` dependency.
- **Acceptance:** the build crash points converge; the uniqueness invariant (≤1 frozen coverage per window) holds at every instant; a same-coverage rebuild is byte-identical; a same-window 16-byte-prefix collision fails loudly (`ErrDuplicateKey`), never silently drops.
- **Design refs:** gettransaction §6–§7; the streaming "rolling rebuild" rule. **Extends:** #728's `BuildColdIndex` (single-index build) — this layers the coverage keys + rolling rebuild + commit batch on top. **Depends on:** 1, 2. **Size:** L.

### 7. Key-driven sweeps
- **Scope:** `sweepChunkArtifacts` and `sweepIndexKey` — the system's only two deletion bodies. Shared mechanic: demote-if-`"frozen"` → unlink → `fsyncDir` → delete key, batched per family. The two sweep rules (index `"freezing"` = delete-never-salvage / `"pruning"` = finish; chunk `"pruning"` / past-retention / redundant-input-in-finalized-window).
- **Acceptance:** "key absent ⟹ file gone" holds at every crash point; unlink-before-key-delete ordering verified; window-local index sweeps touch disjoint keys under concurrency.
- **Design refs:** the key-driven-sweeps rule; the op bodies. **Depends on:** 2. **Size:** M.

---

# Phase 3 — Orchestration

### 8. Derived progress
- **Scope:** Recompute the resume point from durable state at startup (never stored): a cold term (the highest fully-durable chunk) and a positional term over **`ready`-only** hot keys, clamped by `earliest − 1`, with the sub-genesis sentinel; refined by reading the highest ready hot DB's max committed seq. A lost hot DB is detected on open. (Progress is never written to the catalog — the catalog stays a pure catalog.)
- **Acceptance:** a boundary crash is recovered by the refinement; a surgically demoted hot key regresses the resume point without manual edits; a fresh start yields the genesis sentinel, never a spurious chunk-0 bound.
- **Design refs:** "Progress is derived"; the startup derivation. **Depends on:** 2, 4. **Size:** M.

### 9. Postcondition resolver + executor
- **Scope:** `resolve` — a pure catalog diff producing a `Plan` (per-chunk `ledgers`/`events` rules; the per-window `txhash` rule comparing stored vs desired coverage, with the trailing-window cap and the `stored_hi` clause so a window that was current at shutdown doesn't strand its tail chunks). `executePlan` — one bounded worker pool; an index build waits on its in-coverage chunk builds' done-channels **before** acquiring a slot (no deadlock); done-channels signal **success** (a chunk build closes its channel only once its `.bin` is durable; a failed build leaves it open and returns an error that cancels the group, so dependents bail). `runBackfill` drives `resolve` + `executePlan`; producibility is enforced per-chunk by `catchupSource`'s bounded wait.
- **Acceptance:** the plan is a loggable/diffable value recomputed from durable keys (nothing to reconcile on restart); steady-state restart plans nothing; a window that crossed a boundary during downtime gets its tail built; no slot-starvation deadlock at `workers = 1`; a failed build aborts the run (restart re-plans).
- **Design refs:** "Postcondition-driven planning", "Execution model". **Depends on:** 5, 6, 7. **Size:** L.

### 10. Hot-DB ingestion loop
- **Scope:** Drive ledgers from captive core (indexed `GetLedger`) into the live chunk's hot DB, one **atomic synced `WriteBatch` per ledger** across all CFs. The boundary protocol: **close the write handle before creating the next chunk's `hot:chunk` key**, then notify the lifecycle (a `chan ChunkID`; the daemon fatals if the lifecycle falls too far behind). Clean shutdown vs. unexpected core exit is distinguished at the daemon top level. The loop keeps no progress variable — each synced batch is the durable commit.
- **Acceptance:** a ledger is fully present or absent; restart resumes at exactly the last synced batch + 1; a clean shutdown exits zero; an unexpected core exit exits non-zero (supervisor restarts).
- **Design refs:** "Hot DB ingestion", "Concurrency model". **Composes:** captive core (`ledgerbackend`), the hot stores. **Depends on:** 4, 8. **Size:** M.

### 11. Lifecycle goroutine (tick: plan → discard → prune)
- **Scope:** `lifecycleLoop` (event-driven; selects on the notification channel and on cancellation) and `runLifecycleTick`: one progress derivation per tick; plan-and-execute via #9 (the production range starts at existing storage — the floor is a retention boundary, never a production one); then the **discard** scan (retire hot DBs the cold artifacts + index now fully serve) and the **prune** scan (index + chunk key families, floor arithmetic, the redundant-input branch). `effectiveRetentionFloor` and its two-role split. Error policy: bounded retry → abort (startup is the recovery path). Cancellation is handled cleanly (no spurious non-zero exit, no goroutine leak).
- **Acceptance:** a boundary tick freezes the just-closed chunk, folds it into the window, and discards its hot DB; the quiescence postcondition (re-running the plan + scans yields nothing); pruning removes a chunk once it slides past the floor; a clean shutdown mid-tick exits cleanly.
- **Design refs:** "Lifecycle", "Eligibility", "Concurrency model". **Depends on:** 7, 8, 9. **Size:** L.

---

# Phase 4 — Top-level wiring

### 12. Startup orchestration (`startStreaming`)
- **Scope:** open the catalog → `validateConfig` → derive the resume point → the **catch-up loop** (`networkTip` with bounded backoff + readiness reject; re-pass guarded against a stalled tip; `anchor = max(tip, resumePoint)`; the watermark mid-chunk resume exclusion; first-start fatal when there is no tip *and* no local history) → the **serve + ingest handoff** (open the resume hot DB, start captive core at the resume ledger, launch the lifecycle goroutine, start serving, run the ingestion loop). The first lifecycle tick doubles as startup convergence.
- **Acceptance:** first-start (genesis/now/numeric), steady restart, long-downtime, and young-network paths all reach a served, quiescent state; no startup-only cleanup pass needed.
- **Design refs:** "Daemon flow → Startup", `networkTip`, `effectiveRetentionFloor`. **Depends on:** 3, 8, 9, 10, 11. **Size:** L.

### 13. Daemon/CLI wiring + retire v1 backfill path
- **Scope:** A runnable streaming-daemon entrypoint wired into `cmd/stellar-rpc` (load the TOML config → `validateConfig` → acquire locks → `startStreaming` with the production backend + captive-core boundaries); a `--config` loader. Retire the standalone `full-history-backfill` CLI and the v1 `ingest.BackfillMeta`/`ingest.Service` SQLite write path. **The SQLite ingestion/query removal is coordinated with the cutover (#772).**
- **Acceptance:** the daemon boots from a single TOML; the repo builds; the v1 backfill CLI is removed; CHANGELOG updated.
- **Design refs:** "Configuration → CLI"; "Related documents". **Depends on:** 12; coordinates with #772. **Size:** M.

---

# Phase 5 — Operability & correctness

### 14. Retention: pruning, widening, shortening
- **Scope:** Retention **widening** re-derivation (catch-up rebuilds a finalized window at a wider `[lo', last]` — local `.pack` for covered chunks, bulk refetch for fully-pruned; the terminal commit demotes the old coverage), which runs at the next startup (extending the bottom of storage is catch-up's job, not a tick's). **Shortening** (immediate, in the retention role). The redundant-input cleanup corner. The storage-side **reader-retention contract** the prune/sweep stages rely on (below-floor reads are not-found regardless of on-disk state; the read path itself is #770's).
- **Acceptance:** widen/shorten converge at the next startup; a window straddling the floor serves in-range and returns not-found below it; the redundant-input cleanup of a widened-then-narrowed window works.
- **Design refs:** "Reader contract", gettransaction §7.3, "Scenario coverage". **Depends on:** 9, 11. **Size:** M.

### 15. Surgical recovery + hot-volume-loss handling
- **Scope:** The recovery model — a single atomic catalog **key-demotion** batch (tainted cold artifacts → `"freezing"`; tainted/lost hot keys → `"transient"`), self-correcting resume point, no filesystem surgery. Hot-volume-loss detection (a `ready` hot key whose DB won't open → a clear, actionable error pointing at recovery). A small operator entrypoint to emit the demotion batch against a stopped daemon, plus a runbook note.
- **Acceptance:** re-running a demotion batch is a no-op; a demotion reaching the live chunk rewinds to the last frozen boundary and re-ingests forward; a missing-dir mount misconfiguration is not auto-healed.
- **Design refs:** "Scenario coverage" (tainted data; hot-volume loss). **Depends on:** 4, 8. **Size:** M.

### 16. `audit` admin command (INV-1…4)
- **Scope:** Walk catalog keys + the filesystem to verify the invariants at quiescence — single canonical state (INV-2), disk↔catalog correspondence both directions (INV-3), the retention bound (INV-4), with an optional deep mode that re-derives sampled artifacts and byte-compares (INV-1). Returns a structured report. Must not false-negative (never report clean when a violation exists).
- **Acceptance:** each "what a bug looks like" violation is detected; a clean quiescent store passes; the straddling-floor `.idx` carve-out is honored (a stale-`lo` `.idx` is not a violation, a genuinely below-floor stray key is).
- **Design refs:** "Correctness", "What a bug looks like". **Depends on:** 2, 12. **Size:** M.

### 17. Observability: metrics + structured logging
- **Scope:** Metrics through a sink interface — ingestion lag, catch-up progress, freeze/rebuild/discard/prune counts & durations, live hot-DB count, cold-tier disk footprint, the derived resume point + effective floor, rebuild burst throughput — plus structured logs at the phase boundaries. Register the Prometheus sink via the existing daemon convention.
- **Acceptance:** the sink receives the expected signals when driving ledgers / a tick; logs are structured.
- **Design refs:** operational notes (rebuild cadence, peak disk). **Depends on:** 10, 11, 12. **Size:** M.

---

# Phase 6 — Validation & performance

### 18. Crash-injection & convergence test suite
- **Scope:** Construct each crash / partial-completion state (the build crash points + the scenario list), run the convergence path (catch-up + a lifecycle tick), and assert convergence to INV-1 ∧ 2 ∧ 3 ∧ 4 via the `audit` command, plus idempotency of every op. Scenarios: boundary crash, mid-chunk resume, hot-volume loss, retention widen/shorten, downtime crossing a window boundary, young network.
- **Acceptance:** from every injected state the system reaches quiescence with a passing `audit`; the suite is deterministic and race-clean.
- **Design refs:** "Convergence", "Scenario coverage". **Depends on:** 2–13. **Size:** L.

### 19. End-to-end integration tests (streaming daemon)
- **Scope:** Drive the daemon end to end — first-start, steady-state ingest + freeze + prune, restart resume (a true re-derivation), retention slide, and **multi-window tx-hash lookup correctness** (probe every in-retention window; cross-window false-positive rejection). Use the existing integration-test harness against a test backend + captive core where infra allows; an in-process variant with synthetic ledgers covers the cycle otherwise.
- **Acceptance:** a hash from any in-retention ledger resolves; out-of-retention → not-found; restart loses no committed ledger.
- **Depends on:** 12, 13. **Size:** L.

### 20. Bench-harness alignment
- **Scope:** Confirm the production `.bin`/`.idx` formats and rebuild path are byte-format-identical to the merged cold tx-hash path (#728/#780), and record the expected performance figures (≈1-min dense-window rebuild, ≈4.2 B/tx index, the `.bin` floor) — the measurement harness `bench-fullhistory` lives on the `rpc-hack` branch and is the source of those figures.
- **Acceptance:** the format-identity test passes; the documented figures match the design's Part-4 numbers.
- **Design refs:** gettransaction §6, Part 4. **Depends on:** 6. **Size:** M.

---

## Suggested epic

**[Epic] Unified ingestion workflow — implementation** (child of #722; rolls up to #777). Tracks issues
1–20. **Definition of done:** the daemon boots from one TOML, catches up, ingests live, freezes / rebuilds
/ discards / prunes on the lifecycle tick, survives crash-injection with a passing `audit`, and the v1
SQLite backfill/ingestion path is retired (with #772).
