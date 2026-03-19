# Design Doc Revamp — Post-PR-617 Review Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Revamp all full-history design docs and matching code to retire the "range" construct, introduce `chunks_per_index` as a config parameter, clean up the meta store key schema, merge the streaming/transition docs, and ruthlessly prune duplication.

**Architecture:** The primary deliverable is the design docs. Code changes are secondary and mostly mechanical (rename, add config param, new key schema). Work phases strictly in dependency order — the meta store key schema (Phase 1) is the foundation everything else references.

**Tech Stack:** Go, RocksDB, design docs in Markdown.

**Context for new session:** All of this came out of PR #617 review (https://github.com/stellar/stellar-rpc/pull/617). Reviewer is @tamirms. Full discussion context lives in `full-history/`. The design docs live in `full-history/design-docs/`. The code lives in `full-history/all-code/backfill-workflow/`. Never touch `full-history/all-code/txhash-ingestion-workflow/` — legacy, off-limits.

---

## What changed and why (read this first)

### "range" → "index"
The old design called the grouping of N chunks a "range". The reviewer pointed out that the same concept in a DAG model is naturally called `index` (because it's exactly the set of chunks needed to produce one txhash index file). This is a cosmetic rename — `index_id = chunk_id / chunks_per_index` — but it touches every doc and significant code.

**The concept itself is NOT being eliminated.** You need a grouping unit above chunk level for:
- Knowing when to trigger the txhash index build (when all N chunks are done)
- Progress reporting (`index 0004 [INGESTING] 147/1000 chunks 14.7% ETA 29m`)
- Pruning granularity (you can only prune at index boundaries)
- getStatus API (summary + active indexes)

### `chunks_per_index` config param
New config field. Default 1000, valid values: 1 / 10 / 100 / 1000.
Controls: how many chunks group into one txhash index, AND pruning granularity.
Implication: `index_id = chunk_id / chunks_per_index`. With default 1000 this is identical to current behavior.

### Meta store key cleanup
Old schema (verbose, had range prefix everywhere):
```
range:{N:04d}:chunk:{C:06d}:lfs_done         = "1"
range:{N:04d}:chunk:{C:06d}:txhash_done      = "1"
range:{N:04d}:recsplit:cf:{XX}:done          = "1"    ← 16 per-CF keys, gone
range:{N:04d}:recsplit:state                 = "COMPLETE"
range:{N:04d}:state                          = "INGESTING|RECSPLIT_BUILDING|COMPLETE"
```

New schema (clean):
```
chunk:{C:06d}:lfs             = "1"    no range prefix, no _done suffix
chunk:{C:06d}:txhash          = "1"
index:{N:04d}:txhashindex     = "1"    single key, replaces 16 CF keys + state key
```

The `range:N:state` machine key is gone. State is derived at startup from chunk flags + index key presence. The `"1"` value is kept (not empty) for readability in ldb tooling.

### Range orchestrator → flat worker pool
The "range orchestrator" language is stale. The actual impl is already a flat worker pool + single scheduler goroutine (see `dag.go`, `orchestrator.go`). Both `parallel_ranges` and `instances_per_range` config params go away entirely — replaced by a single `workers` count. The DAG handles cross-index concurrency naturally.

### process_chunk: LFS-first streaming
If `chunk:{id}:lfs` key is present but `chunk:{id}:txhash` is absent (partial completion), stream from local LFS packfile instead of GCS. No BSB needed. Faster, no GCS dependency.

### flush_interval: remove from config entirely
`flush_interval` (currently `~100 ledgers`) is not a user-facing config param. Different store types (LFS, events) will have different natural write cadences. It becomes an internal constant inside each store's write path, not something exposed in the config file or config table in the docs.

### cleanup_txhash: delete meta keys too
Current design deletes the raw txhash files but leaves the `txhash_done` key as a "permanent record". This is wrong — key presence should mean file exists. cleanup_txhash must delete both file and key.

### Streaming + transition docs: merge into one
`04-streaming-workflow.md` and `06-streaming-transition-workflow.md` cover tightly coupled concerns. Merge into a single `04-streaming-and-transition.md`. While merging, replace all `range` references with `index`.

### getStatus API: summary + active-only
The API can't return an unbounded per-index array (with `chunks_per_index=1` there are 6000 indexes for a 60M run). Response shape: summary counts + active-only array (bounded by worker count).

### Final ruthless pruning
After all structural changes, take multiple passes and remove: repeated information, redundant sections across docs, stale references, anything that doesn't earn its place.

---

## Code design principles (apply throughout all code changes)

These are not nice-to-haves. Apply them in every code task.

**Interface-driven throughout.** Every major component — txhash index builder, LFS writer, events writer, ledger source — should be behind an interface. The concrete implementations (RecSplit, packfile, BSB) should be swappable without touching the orchestration logic. If tomorrow the txhash index changes from 16 RecSplit CF files to a single file or a different hash function, only the implementation changes — not the DAG, not the orchestrator, not the task.

**Composable, not monolithic.** process_chunk should not know about BSB internals. build_txhash_index should not know about RecSplit internals. Each component takes its inputs via interface and produces its outputs via interface. The wiring happens at the top level (orchestrator / main).

**Test-parameterizable constants.** `LedgersPerChunk` (10,000) is a real-world constant but should flow through `Geometry` as a field rather than being hardcoded in each function. This lets tests use small values (e.g., 10 ledgers per chunk) without fighting the production constants. Same applies to any other "large in prod, small in test" constant. Do not add a separate test-only parameter — just make sure `Geometry` is constructed with the right values and passed through.

**No magic numbers in logic.** If a number appears in more than one place, it belongs in `Geometry` or a named constant.

**Detailed comments everywhere, especially in DAG and task scheduling code.** A future reader should be able to understand the entire scheduling model from the comments alone, without needing to read the design docs. Specifically:

- `dag.go` — Each field in the `DAG` struct needs a comment explaining its role in the scheduling algorithm. The `Execute` function needs a block comment explaining: (1) how in-degree tracking works, (2) why the semaphore size equals `maxWorkers` not `dag.Len()`, (3) the exact shutdown sequence for the three cases: normal completion, first task error, context cancellation. Every `select` branch needs a comment explaining what it handles.
- `tasks.go` — Each task type needs a package-level comment explaining: what it produces, what its inputs are, what meta store keys it reads/writes, and the exact invariant that must hold before `Execute` is called. The dependency diagram (already exists as ASCII art) must be kept current with any new task types. The comment on `buildTxHashIndexTask.Execute` must explain why the DAG guarantee makes explicit "are all chunks done?" checks unnecessary.
- `geometry.go` — Every index method (`IndexID`, `LedgersPerIndex`, `IsLastChunkInIndex`, etc.) needs a comment that states: the formula, a concrete example with production values, and a concrete example with test values. `LedgersPerIndex` specifically must explain that this is the cadence at which `build_txhash_index` fires.
- `interfaces.go` — Every interface method must have a doc comment explaining the invariant (e.g., "MUST only be called after fsync"), not just what it does.
- `meta_store.go` — The key construction functions must show an example of the exact key string produced.

---

## Files touched

### Design docs (`full-history/design-docs/`)
| File | What changes |
|------|-------------|
| `02-meta-store-design.md` | **Phase 1** — New key schema. Foundation for everything. |
| `10-configuration.md` | **Phase 2** — Add `chunks_per_index`, remove `flush_interval` and `parallel_ranges` and `instances_per_range`; add `workers` |
| `03-backfill-workflow.md` | **Phase 3** — Major rework: new keys, LFS-first, no range orchestrator, chunks_per_index, getStatus |
| `08-query-routing.md` | **Phase 4** — Add getStatus API spec |
| `04-streaming-workflow.md` | **Phase 5** — Merged with 06, range→index sweep |
| `06-streaming-transition-workflow.md` | **Phase 5** — Deleted, content merged into 04 |
| `07-crash-recovery.md` | **Phase 6** — Simplify streaming crash section; backfill section already clean |
| `01-architecture-overview.md` | **Phase 7** — range→index sweep |
| `09-directory-structure.md` | **Phase 7** — range→index sweep, update path examples |
| `11-checkpointing-and-transitions.md` | **Phase 7** — range→index sweep |
| `12-metrics-and-sizing.md` | **Phase 7** — range→index sweep |
| `13-recommended-operator-approach.md` | **Phase 7** — range→index sweep |
| `14-open-questions.md` | **Phase 7** — range→index sweep, mark resolved items |
| `15-query-performance.md` | **Phase 7** — range→index sweep |
| `16-backfill-run-metrics.md` | **Phase 7** — range→index sweep |
| `FAQ.md` | **Phase 7** — range→index sweep |
| `README.md` | **Phase 7** — range→index sweep, update key reference tables |
| All docs | **Phase 8** — Final ruthless pruning pass |

### Code (`full-history/all-code/backfill-workflow/`)
| File | What changes |
|------|-------------|
| `pkg/geometry/geometry.go` | Add `ChunksPerIndex`, `LedgersPerChunk` fields; add `IndexID`, `IndexFirstChunk`, `IndexLastChunk`, `ChunksForIndex` methods |
| `config.go` | Add `ChunksPerIndex int`; remove `FlushInterval`, `ParallelRanges`, `NumInstancesPerRange` |
| `config_test.go` | Add validation tests for ChunksPerIndex |
| `meta_store.go` | New key functions: `ChunkLFSKey`, `ChunkTxHashKey`, `IndexTxHashIndexKey`; remove all range-prefixed key functions |
| `meta_store_test.go` | Update for new key schema |
| `interfaces.go` | Review and strengthen: TxHashIndexBuilder, LFSWriter, LedgerSource should all be clean interfaces; add EventsWriter interface stub for future |
| `tasks.go` | Update task IDs and comments to use index_id language; remove range-specific wiring |
| `orchestrator.go` | Rename range→index; remove range state machine writes; replace `parallel_ranges * instances_per_range` with `workers`; update buildDAG triage |
| `stats.go` | Rename `RangeProgress` → `IndexProgress`; update log strings from "Range XXXX" → "Index XXXX" |
| `resume.go` | Rewrite triage: derive index state from chunk flags + index key; remove GetRangeState dependency |
| `resume_test.go` | Update for new triage logic |
| `reconciler.go` | Update for new key schema |
| `reconciler_test.go` | Update for new key schema |
| `recsplit_flow.go` | Write single `index:{N}:txhashindex` key; remove 16 CF keys + state key; ensure RecSplitFlow implements TxHashIndexBuilder interface |
| `recsplit_flow_test.go` | Update for single key |
| `bsb_instance.go` | Add LFS-first path; remove hardcoded flush interval (use internal constant) |
| `chunk_writer.go` | Update key writes to new schema; flush interval becomes internal constant |
| `.skills/architecture.md` | Update meta store key reference and range→index terminology |

---

## Phase 1 — New meta store key schema (foundation)

**Must be done before anything else.** Every other doc and code change references these keys.

### Task 1.1: Rewrite `02-meta-store-design.md`

- [ ] Read the current `02-meta-store-design.md` in full
- [ ] Replace the key schema section with the new schema:
  ```
  chunk:{C:06d}:lfs          = "1"   set after LFS packfile fsynced
  chunk:{C:06d}:txhash       = "1"   set after raw txhash flat file fsynced
  index:{N:04d}:txhashindex  = "1"   set after all CF index files built + fsynced
  ```
- [ ] Remove all `range:{N:04d}:` prefixed keys
- [ ] Remove the 16 per-CF done keys (`range:{N}:recsplit:cf:{XX}:done`)
- [ ] Remove the range state machine keys (`range:{N}:state`, `range:{N}:recsplit:state`)
- [ ] Add a note: index_id = chunk_id / chunks_per_index (derived, not stored)
- [ ] Add a note: "1" value retained for ldb readability; key presence is the signal
- [ ] Add section: Startup triage (how to derive index state from keys without range:N:state):
  ```
  index key present                                               → COMPLETE (skip)
  all chunks in index have lfs + txhash keys, index key absent   → trigger build_txhash_index
  some chunks missing lfs or txhash key                          → INGESTING (process missing chunks)
  no chunk keys exist                                            → NEW
  ```
- [ ] Commit: `docs: rewrite meta store key schema — range→index, flatten keys`

---

## Phase 2 — Config and geometry

### Task 2.1: Update config

- [ ] Read `config.go` and `10-configuration.md`
- [ ] In `config.go`:
  - Add `ChunksPerIndex int` to BackfillConfig (default 1000, valid: 1/10/100/1000)
  - Remove `FlushInterval`, `ParallelRanges`, `NumInstancesPerRange`
  - Add `Workers int` (replaces `ParallelRanges * NumInstancesPerRange`; default 40)
- [ ] Write failing test: valid ChunksPerIndex values pass, invalid (e.g. 500) fails
- [ ] Run test, implement validation, pass
- [ ] Update `backfill-config.toml` — add `chunks_per_index`, `workers`; remove old params
- [ ] Update `10-configuration.md`:
  - Add `chunks_per_index` (default 1000, valid: 1/10/100/1000; also controls pruning granularity)
  - Add `workers` (default 40; total concurrent task slots)
  - Remove `flush_interval` (internal constant per store, not user-facing)
  - Remove `parallel_ranges` and `instances_per_range` (replaced by `workers`)
- [ ] Commit: `feat: config — chunks_per_index + workers, remove stale params`

### Task 2.2: Add index boundary primitive to geometry

This task adds the authoritative primitive that expresses "txhash index creation
happens at a cadence of `ChunkSize × ChunksPerIndex` ledgers." Everything else
in the pipeline derives from this.

- [ ] Read `pkg/geometry/geometry.go` and `pkg/geometry/geometry_test.go` in full
- [ ] Add `ChunksPerIndex uint32` to the `Geometry` struct
  - `ChunkSize` is already a field — keep it, but make sure it flows through all constructors rather than being read from the package-level constant in any logic
  - `TestGeometry()` should use `ChunkSize=10, ChunksPerIndex=5` so tests run at tiny scale
  - `DefaultGeometry()` should use `ChunksPerIndex=1000`
- [ ] Add these methods — these are the primitive:
  - `LedgersPerIndex() uint32` — returns `ChunkSize * ChunksPerIndex`; this is THE cadence: one txhash index builds every this many ledgers
  - `IndexID(chunkID uint32) uint32` — returns `chunkID / ChunksPerIndex`
  - `IndexFirstChunk(indexID uint32) uint32` — returns `indexID * ChunksPerIndex`
  - `IndexLastChunk(indexID uint32) uint32` — returns `(indexID+1)*ChunksPerIndex - 1`
  - `IsLastChunkInIndex(chunkID uint32) bool` — returns `(chunkID+1) % ChunksPerIndex == 0`; true when completing this chunk should trigger a build
  - `IndexFirstLedger(indexID uint32) uint32` — first ledger in the index group
  - `IndexLastLedger(indexID uint32) uint32` — last ledger in the index group
  - `ChunksForIndex(indexID uint32) []uint32` — returns all chunk IDs in the group (used by DAG builder)
- [ ] Write tests in `geometry_test.go` using `TestGeometry()` (ChunkSize=10, ChunksPerIndex=5):
  - `TestLedgersPerIndex`: verify `10 * 5 = 50`; verify production default is `10_000 * 1_000 = 10_000_000`
  - `TestIndexID`: verify chunks 0-4 map to index 0; chunks 5-9 to index 1; chunk 10 to index 2
  - `TestIndexBoundaries`: verify `IndexFirstChunk(1)=5`, `IndexLastChunk(1)=9`, `IndexFirstLedger(1)=52`, `IndexLastLedger(1)=101`
  - `TestIsLastChunkInIndex`: verify chunks 4,9,14 return true; chunks 0,3,5,8 return false
  - `TestChunksForIndex`: verify `ChunksForIndex(1) = [5,6,7,8,9]`
  - `TestIndexRoundTrip`: for every chunk in [0,29], verify `IndexID(c)` is consistent with `IndexFirstChunk`/`IndexLastChunk` boundaries
  - `TestIndexCadenceMatchesChunkCount`: verify `len(ChunksForIndex(i)) == ChunksPerIndex` for several index IDs
- [ ] Run, implement, pass: `cd full-history/all-code && make test PKG=./pkg/geometry/...`
- [ ] Commit: `feat: geometry — index boundary primitive (IndexID, LedgersPerIndex, IsLastChunkInIndex)`

---

## Phase 3 — Backfill workflow doc + code

### Task 3.1: Strengthen interfaces

- [ ] Read `interfaces.go` in full
- [ ] Verify these are clean interfaces (not concrete types leaked into the signature):
  - `LedgerSource` — produces ledgers; implementations: BSB, LFS packfile reader
  - `TxHashIndexBuilder` — takes raw txhash files, produces index; implementation: RecSplitFlow
  - `LFSWriter` — writes LFS packfile chunks; implementation: current chunk_writer
  - `BackfillMetaStore` — already an interface; verify it has all new key methods
- [ ] Add `EventsWriter` interface stub (no implementation yet; just the interface for future use)
- [ ] Ensure no concrete types (e.g. `*RecSplitFlow`, `*BSBInstance`) appear in task or orchestrator signatures — only interfaces
- [ ] Commit: `refactor: interfaces — strengthen boundaries, add EventsWriter stub`

### Task 3.2: Update meta store key functions

- [ ] Read `meta_store.go` in full
- [ ] Add:
  ```go
  func ChunkLFSKey(chunkID uint32) string         { return fmt.Sprintf("chunk:%06d:lfs", chunkID) }
  func ChunkTxHashKey(chunkID uint32) string      { return fmt.Sprintf("chunk:%06d:txhash", chunkID) }
  func IndexTxHashIndexKey(indexID uint32) string { return fmt.Sprintf("index:%04d:txhashindex", indexID) }
  ```
- [ ] Remove: all `range:{N}:*` key functions, per-CF functions, SetRangeState, GetRangeState
- [ ] Write failing tests for new key functions
- [ ] Run, implement, pass
- [ ] Commit: `refactor: meta store — new key schema, remove range state machine`

### Task 3.3: Update resume triage

- [ ] Read `resume.go` and `resume_test.go` in full
- [ ] Rewrite `ResumeIndex` (was `ResumeRange`) to derive state from chunk flags:
  ```
  1. Check IndexTxHashIndexKey → if present, COMPLETE
  2. Scan all chunk lfs+txhash keys for this index
     - All present → ResumeActionBuild (trigger build_txhash_index)
     - Some present → ResumeActionIngest (with skip set of already-done chunks)
     - None present → ResumeActionNew
  ```
- [ ] Write failing tests covering all four resume actions using small geometry (ChunksPerIndex=5)
- [ ] Run, implement, pass
- [ ] Commit: `refactor: resume — derive index state from chunk keys, no range state machine`

### Task 3.4: Update RecSplit flow — single index key

- [ ] Read `recsplit_flow.go` and `recsplit_flow_test.go`
- [ ] Replace writing 16 per-CF done keys + range state key with single `IndexTxHashIndexKey` write after all CFs verified
- [ ] Ensure `RecSplitFlow` satisfies `TxHashIndexBuilder` interface
- [ ] Write failing tests: after Run(), only `index:{N}:txhashindex` key exists; no CF keys, no state keys
- [ ] Run, implement, pass
- [ ] Commit: `refactor: recsplit — single txhashindex key, implement TxHashIndexBuilder interface`

### Task 3.5: Update cleanup_txhash — delete meta keys too

- [ ] Find cleanup logic
- [ ] After deleting each raw txhash file: `meta.Delete(ChunkTxHashKey(chunkID))`
- [ ] Write failing test: after cleanup, `chunk:{id}:txhash` keys are absent
- [ ] Run, implement, pass
- [ ] Commit: `fix: cleanup_txhash — delete meta keys alongside raw files`

### Task 3.6: Update process_chunk — LFS-first + flush interval as internal constant

- [ ] Read `bsb_instance.go` and `chunk_writer.go` in full
- [ ] Remove `FlushInterval` from all config threading; replace with a package-level constant inside chunk_writer (e.g. `const defaultFlushInterval = 100`)
- [ ] Update all `meta.Put(...)` calls to use new key functions
- [ ] Add LFS-first branch: if `chunk:{id}:lfs` present but `chunk:{id}:txhash` absent, construct a `LFSPackfileSource` (implements `LedgerSource`) instead of BSB
- [ ] Write failing tests for the LFS-first path using small geometry
- [ ] Run, implement, pass
- [ ] Commit: `feat: process_chunk — LFS-first streaming, flush interval as internal constant`

### Task 3.7: Update orchestrator and stats — range→index rename

- [ ] Read `orchestrator.go` and `stats.go` in full
- [ ] In `orchestrator.go`:
  - Remove all range state machine writes (SetRangeState calls)
  - Rename all `rangeID` → `indexID`
  - Replace `maxWorkers = ParallelRanges * NumInstances` with `maxWorkers = cfg.Workers`
  - Update `buildDAG` to call `ResumeIndex` (Task 3.3)
  - Remove `logRangeStates` / `logRecSplitRange` or rewrite using new key schema
  - Update `logConfig` to print `chunks_per_index`, `workers`
- [ ] In `stats.go`:
  - Rename `RangeProgress` → `IndexProgress`, `ProgressTracker.RegisterRange` → `RegisterIndex`
  - Update all log strings: "Range XXXX" → "Index XXXX"
- [ ] In `tasks.go`: rename task IDs and comments to use index_id language
- [ ] Run all tests: `make test`
- [ ] Commit: `refactor: orchestrator + stats — range→index, flat worker pool, no range state machine`

### Task 3.8: Rewrite `03-backfill-workflow.md`

Rewrite from scratch using the current doc as a base — don't line-edit.

- [ ] Read current `03-backfill-workflow.md` and new `02-meta-store-design.md` in full
- [ ] Rewrite with:
  - New meta store key schema throughout
  - `index_id` / `chunks_per_index` replacing `range_id` / hardcoded 1000
  - LFS-first streaming in process_chunk pseudocode
  - flush_interval removed from config table; noted as internal constant inline in process_chunk section
  - `workers` replacing `parallel_ranges` + `instances_per_range` in config table
  - Cleanup_txhash pseudocode deletes meta keys
  - Single `index:{N}:txhashindex` key in RecSplit section
  - "Pre-condition: wait for BSB goroutines" removed from build_txhash_index
  - All "range orchestrator" language gone — flat worker pool only
  - Startup triage flowchart uses key-derived state (no range:N:state)
  - getStatus API response section (see Phase 4 for spec)
- [ ] Commit: `docs: rewrite backfill workflow — index concept, chunks_per_index, clean keys`

---

## Phase 4 — getStatus API spec

### Task 4.1: Add getStatus spec to `08-query-routing.md`

- [ ] Read `08-query-routing.md` in full
- [ ] Add `getStatus` response structure for backfill mode:
  ```json
  {
    "mode": "BACKFILL",
    "chunks_per_index": 1000,
    "summary": {
      "total_indexes": 6,
      "complete": 0,
      "building": 0,
      "ingesting": 2,
      "queued": 4,
      "total_chunks": 6000,
      "chunks_done": 288,
      "pct": 4.8,
      "eta_seconds": 1820
    },
    "active": [
      {"index": 0, "state": "INGESTING", "chunks_done": 147, "chunks_total": 1000, "pct": 14.7},
      {"index": 1, "state": "INGESTING", "chunks_done": 141, "chunks_total": 1000, "pct": 14.1}
    ]
  }
  ```
- [ ] Note: `active` only contains INGESTING or BUILDING indexes; size bounded by `workers` regardless of `chunks_per_index`
- [ ] Show BUILDING entry shape: `{"index": 3, "state": "BUILDING", "cfs_done": 11, "cfs_total": 16}`
- [ ] Commit: `docs: add getStatus API spec for backfill mode`

---

## Phase 5 — Merge streaming + transition docs

### Task 5.1: Merge `04-streaming-workflow.md` and `06-streaming-transition-workflow.md`

- [ ] Read both docs in full
- [ ] Create `04-streaming-and-transition.md`:
  - Streaming mode overview first
  - Transition from backfill → streaming in same doc (operationally inseparable)
  - All `range` references replaced with `index`
  - `index_id = chunk_id / chunks_per_index` math used throughout
  - No range state machine references
- [ ] Delete `06-streaming-transition-workflow.md`
- [ ] Update all cross-references in other docs pointing to `06-*`
- [ ] Commit: `docs: merge streaming + transition into 04-streaming-and-transition.md`

---

## Phase 6 — Crash recovery streaming simplification

### Task 6.1: Update `07-crash-recovery.md`

- [ ] Read in full
- [ ] Update streaming crash section for range→index rename
- [ ] Verify backfill section matches Phase 3 output (new keys, single index key, no range state)
- [ ] Eliminate any remaining scenario enumeration that's redundant with the invariant-based reasoning
- [ ] Commit: `docs: crash recovery — align with new key schema, remove range state refs`

---

## Phase 7 — Pervasive range→index sweep across all remaining docs

Run in parallel if subagents available.

### Task 7.1: `01-architecture-overview.md` — range→index sweep + `index_id = chunk_id / chunks_per_index` one-liner
### Task 7.2: `09-directory-structure.md` — update path examples (`txhash/{rangeID}` → `txhash/{indexID}`)
### Task 7.3: `11-checkpointing-and-transitions.md` — range→index, verify transition logic with chunks_per_index
### Task 7.4: `12-metrics-and-sizing.md` — range→index, update memory/storage tables
### Task 7.5: `13-recommended-operator-approach.md` — range→index
### Task 7.6: `14-open-questions.md` — range→index; mark items resolved in this revamp as RESOLVED
### Task 7.7: `15-query-performance.md` — range→index
### Task 7.8: `16-backfill-run-metrics.md` — range→index; update metric names (e.g. `range_complete_total` → `index_complete_total`)
### Task 7.9: `FAQ.md` — range→index, update stale answers
### Task 7.10: `README.md` — range→index, update key reference tables
### Task 7.11: `.skills/architecture.md` — update meta store key reference and terminology

Each: read → edit → commit.

---

## Phase 8 — Final ruthless multi-pass pruning

Do NOT skip this phase.

### Task 8.1: Pass 1 — Remove duplication
- [ ] Read every doc in order: 01, 02, 03, 04, 07, 08, 09, 10, 11, 12, 13
- [ ] For each section that restates something said elsewhere: keep canonical location, replace duplicate with a one-line cross-reference or delete outright
- [ ] Commit: `docs: pass 1 — remove duplication`

### Task 8.2: Pass 2 — Remove sections that don't earn their place
- [ ] Re-read all docs; delete:
  - Tables that restate prose
  - Vague "future work" sections
  - Illustrative examples that add nothing over the prose
  - Scaffolding sections that were useful during design but are now noise
- [ ] Commit: `docs: pass 2 — cut sections that don't earn their place`

### Task 8.3: Pass 3 — Tighten language in the two longest docs
- [ ] Re-read `03-backfill-workflow.md` and `04-streaming-and-transition.md`
- [ ] Cut passive voice, hedging, over-explanation, verbose pseudocode
- [ ] A design doc is not a tutorial
- [ ] Commit: `docs: pass 3 — tighten language`

---

## Final verification

- [ ] `make test` — all tests pass
- [ ] `make build` — compiles clean; remove `bin/` after
- [ ] `grep -r "range:" full-history/design-docs/` — no live meta store key names (only acceptable in "old schema" historical notes)
- [ ] `grep -r "rangeID\|RangeID\|range_id\|parallel_ranges\|instances_per_range\|FlushInterval" full-history/all-code/backfill-workflow/` — should be zero
- [ ] `grep -ri "range orchestrator" full-history/` — should be zero
- [ ] Final commit: `docs: full-history design doc revamp complete`

---

## How to start the new session

1. Open a new Claude Code session in `/Users/karthik/WS/new-world/stellar-rpc`
2. Say: **"Execute the plan at `full-history/design-doc-revamp-plan.md` in full, from Phase 1 through Phase 8 including the final verification checklist. Use superpowers:subagent-driven-development to parallelize where possible. Before any compilation or testing, read `full-history/all-code/.skills/build-and-test.md` for the correct CGO environment. Do not stop between phases. After each code task, run `make test` and confirm it passes before committing. Only stop when the final verification commit is done."**
3. Superpowers engages automatically — no need to invoke it explicitly.
4. If subagents are available: Phase 7 tasks will be parallelized.
5. Gate each phase: do not start Phase 2 until Phase 1 is committed and verified.

## Before starting — checklist

- [ ] Add git/rm/grep permissions to `~/.claude/settings.json` (see below)
- [ ] Add execution directives to `full-history/all-code/CLAUDE.md`

### settings.json permissions to add
```json
"Bash(git add:*)",
"Bash(git commit:*)",
"Bash(git diff:*)",
"Bash(git status:*)",
"Bash(git log:*)",
"Bash(git checkout:*)",
"Bash(grep:*)",
"Bash(rm:*)"
```

### CLAUDE.md addition (top of `full-history/all-code/CLAUDE.md`)
```markdown
## Executing the revamp plan

If directed to execute `full-history/design-doc-revamp-plan.md`:
- Read `.skills/build-and-test.md` BEFORE running any build or test command
- Do NOT stop between phases — complete all phases to the end
- After every code change: run `make test` and fix failures before committing
- After every doc change: verify cross-references are consistent
- Never leave binaries in the repo (`make clean` after any build)
- Do not ask for confirmation between tasks unless a destructive git operation is needed
```
