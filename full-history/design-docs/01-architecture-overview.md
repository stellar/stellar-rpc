# Architecture Overview

## Overview

- The Stellar Full History RPC Service ingests and serves the complete Stellar blockchain history
- It operates in two **mutually exclusive, fully independent** modes:
  - **Backfill Mode** — offline historical ingestion. Writes directly to immutable formats (LFS chunks + raw txhash flat files) without RocksDB. No queries served. Operator re-runs the same command on failure until completion, then switches to streaming mode.
  - **Streaming Mode** — real-time ingestion via CaptiveStellarCore. Writes to an active RocksDB store, serves queries, and periodically transitions completed ranges to immutable storage.
- These two modes have **separate transition workflows** and **separate crash recovery semantics**. There is no unified transition path shared between them.

---

## Mental Model: How the Data Hierarchy Works

```mermaid
flowchart TD
    R["Index<br/>10M ledgers"] -->|"contains 1,000"| C["Chunk<br/>10K ledgers"]
    C -->|"contains ~100"| F["Flush<br/>~100 ledgers"]
```

`index_id = chunk_id / chunks_per_txhash_index`

- **Index** controls the lifecycle: state machine transitions (`INGESTING → RECSPLIT_BUILDING → COMPLETE` in backfill, `ACTIVE → TRANSITIONING → COMPLETE` in streaming), RecSplit index builds, and per-index meta store state.
- **Chunk** controls file I/O and crash recovery: each chunk produces one LFS file and one raw txhash file (backfill). A chunk is the atomic unit — if either file is incomplete on crash, the whole chunk is rewritten.
- **Flush** controls memory: during backfill, accumulated data is flushed every ~100 ledgers to prevent unbounded RAM growth. Flushes are invisible to the state machine.

See the [Glossary](./README.md#glossary-of-terms) for full term definitions and [11-checkpointing-and-transitions.md](./11-checkpointing-and-transitions.md#key-constants) for the exact boundary math.

---

## System Components

The system has four components:

- Backfill and Streaming are **mutually exclusive** ingestion modes
- The Meta Store is shared by both
- The Query Layer is active only in Streaming mode

### 1. Backfill Mode

- Offline historical ingestion
- No RocksDB, no queries served
- Writes directly to immutable file formats

```mermaid
flowchart TD
    BSB["BufferedStorageBackend (BSB)<br/>Flat worker pool (default 40 task slots)<br/>DAG scheduler dispatches as dependencies resolve"]
    BSB --> LFS["LFS Chunk Files<br/>immutable/ledgers/chunks/"]
    BSB --> TXRAW["Raw TxHash Flat Files<br/>immutable/txhash/XXXX/raw/<br/>(36 bytes/entry: hash[32]+seq[4])"]
    TXRAW -->|"all chunks_per_txhash_index chunks done"| RECSPLIT["RecSplit Index Files<br/>immutable/txhash/XXXX/index/"]
```

### 2. Streaming Mode

- Real-time ingestion via CaptiveStellarCore
- Writes to RocksDB, serves queries
- Transitions completed ranges to immutable storage

```mermaid
flowchart TD
    CORE["CaptiveStellarCore<br/>batch size = 1 ledger"]
    CORE --> ACTIVE["Active Store (RocksDB)<br/>Current range, mutable"]
    ACTIVE -->|"index complete → transition workflow"| IMMUTABLE["Immutable Stores<br/>LFS + RecSplit<br/>Completed indexes"]
```

### 3. Meta Store

- Single RocksDB instance shared by both modes
- Source of truth for crash recovery

```mermaid
flowchart TD
    META["Meta Store (RocksDB)"]
    META --- RANGE["Per-index ingestion state<br/>ACTIVE / COMPLETE"]
    META --- CHUNK["Per-chunk completion flags<br/>chunk:{C}:lfs + chunk:{C}:txhash"]
    META --- RS["RecSplit build state<br/>per-CF done flags (both modes)"]
    META --- CP["Checkpoint ledger<br/>(streaming crash recovery)"]
```

### 4. Query Layer (streaming mode only)

```mermaid
flowchart TD
    HTTP["HTTP Server<br/>getTransactionByHash / getLedgerBySequence"]
    HTTP --> ROUTER["Query Router"]
    ROUTER --> ACTIVE_Q["Active Stores<br/>(RocksDB)"]
    ROUTER --> IMMUTABLE_Q["Immutable Stores<br/>(LFS + RecSplit)"]
```

All store lookups are sub-millisecond (~100–700 μs). End-to-end query latency is dominated by LCM parsing (~12–13 ms XDR decode for ledgers with 300+ tx). `getLedgerBySequence` completes in ~15 ms; `getTransactionByHash` in ~17–18 ms (adds a txhash→ledgerSeq lookup + transaction extraction). The active vs immutable store path makes <1 ms difference. See [15-query-performance.md](./15-query-performance.md) for full breakdown and [08-query-routing.md](./08-query-routing.md#performance-characteristics) for routing logic.

---

## Two Pipelines, Two Designs

Backfill is a bulk import job — it reads historical ledger data, writes immutable files, and exits. Streaming is a live daemon — it ingests one ledger at a time, serves queries, and runs forever. They share the output format (LFS + RecSplit) and the meta store, but no code or state.

| Dimension | Backfill | Streaming |
|-----------|----------|-----------|
| Data source | BufferedStorageBackend (GCS) or CaptiveStellarCore | CaptiveStellarCore only |
| RocksDB for ingestion | **No** — writes directly to files | **Yes** — active store per index |
| WAL concern | **None** — no RocksDB during ingestion | Required — crash recovery depends on WAL |
| Parallelism | Flat worker pool (default 40 task slots); DAG scheduler | Single goroutine, 1 ledger/batch |
| Flush cadence | Every ~100 ledgers to file | Every ledger (checkpoint_interval = 1) |
| Queries | Not served | All endpoints available |
| Transition workflow | Direct-write (no active store to tear down) | Active RocksDB → LFS + RecSplit |
| Crash recovery | Chunk-level granularity; re-run from first incomplete chunk | Ledger-level; resume from `last_committed_ledger + 1` |
| Process lifecycle | Exits when all indexes complete | Long-running daemon |

---

## Store Types

### Active Store (streaming mode only)

**Two separate RocksDB instances** per index being ingested in streaming mode:

- **Ledger store** (`<active_stores_base_dir>/ledger-store-chunk-{chunkID:06d}/`) — default CF only. Key: `uint32BE(ledgerSeq)`, Value: `zstd(LedgerCloseMeta)`. One RocksDB instance per 10K-ledger chunk; transitions independently at every chunk boundary (active → transitioning → LFS flush → close + delete).
- **TxHash store** (`<active_stores_base_dir>/txhash-store-index-{indexID:04d}/`) — 16 column families, one per first hex character of the txhash (`0`–`f`). Key: `txhash[32]`, Value: `uint32BE(ledgerSeq)`. CF routing: first hex char of the 64-char hash string (equivalently `txhash[0] >> 4` on raw bytes). One RocksDB instance per 10M-ledger index; transitions at index boundary.

**Each sub-flow can have at most 1 active store and 1 transitioning store at any point in time:**

| Sub-flow | Transition cadence | Max active | Max transitioning | Max total |
|----------|-------------------|------------|-------------------|-----------|
| Ledger | Every 10K ledgers (chunk boundary) | 1 | 1 | 2 |
| TxHash | Every 10M ledgers (index boundary) | 1 | 1 | 2 |

The ledger store transitions at every 10K-ledger chunk boundary: `SwapActiveLedgerStore` moves the old store to `transitioningLedgerStore`, a background goroutine flushes it to LFS, then `CompleteLedgerTransition` closes and deletes it. The txhash store transitions at every 10M-ledger index boundary via `PromoteToTransitioning`. **The ledger store has no column families** — it uses only the default CF.

### Immutable Stores (both modes)

**LFS (Ledger File Store)** — chunk files, 10K ledgers each:
- Path: `immutable/ledgers/chunks/XXXX/YYYYYY.data` + `.index`
- Data: individually zstd-compressed `LedgerCloseMeta` records
- Written per chunk (10K ledgers) during ingestion

**RecSplit Index** — minimal perfect hash, 16 column family files sharded by the first hex character of the txhash (`0`–`f`):
- Path: `immutable/txhash/XXXX/index/cf-{0..f}.idx`
- Built once per index, after all 1000 chunk raw txhash flat files are written
- Build time: minutes per index (backfill, 4-phase parallel pipeline); longer for streaming (sequential per-CF from RocksDB)
- Space efficiency: **~4.5 bytes/entry** vs 36 bytes/entry in RocksDB (~90% reduction). See [12-metrics-and-sizing.md](./12-metrics-and-sizing.md#space-efficiency-rocksdb--immutable) for full compression ratios.

> **Open question — RecSplit sharding**: The 16-shard design is a parallelism optimization (each shard builds in ~45 minutes vs ~7 hours for a single index). Research is underway to make single-index builds fast enough to eliminate sharding entirely, which would simplify file management, query routing, and crash recovery. See [14-open-questions.md — OQ-5](./14-open-questions.md#oq-5-recsplit-sharding--16-files-vs-single-index).

**Raw TxHash Flat Files** (intermediate, backfill only — never created during streaming):
- Path: `immutable/txhash/XXXX/raw/YYYYYY.bin`
- Format: `[txhash[32] || ledgerSeq[4]]` repeated, 36 bytes per entry
- Written per chunk during backfill ingestion; consumed by RecSplit builder at index completion; **deleted immediately after all 16 RecSplit CFs are built and verified**
- An index in state `COMPLETE` has no `raw/` directory

### Meta Store

Single RocksDB instance tracking state for both modes. Stores:
- Per-index ingestion state (ACTIVE / COMPLETE)
- Per-chunk task completion flags
- RecSplit build state per index
- Checkpoint ledger for streaming crash recovery

See [02-meta-store-design.md](./02-meta-store-design.md) for full key hierarchy.

> **See also**: [09-directory-structure.md](./09-directory-structure.md) for the on-disk directory layout and path formulas. [12-metrics-and-sizing.md](./12-metrics-and-sizing.md) for storage estimates per index.

---

## Ingestion Hierarchy (Backfill)

```mermaid
flowchart TD
    POOL["Flat Worker Pool<br/>(default 40 task slots)"]
    DAG["DAG Scheduler<br/>dispatches tasks as dependencies resolve"]
    CHUNK["process_chunk task<br/>10K ledgers = 1 LFS chunk + 1 raw txhash file"]
    RECSPLIT2["build_txhash_index task<br/>1 per index, after all chunks complete"]
    CLEANUP["cleanup_txhash task<br/>deletes raw files + meta keys"]

    DAG --> POOL
    POOL --> CHUNK
    CHUNK -->|"all chunks_per_txhash_index done"| RECSPLIT2
    RECSPLIT2 --> CLEANUP
```

**Key numbers** (default config):
- Index size: `chunks_per_txhash_index` × 10K ledgers (default 1,000 × 10K = 10M)
- Chunks per index: `chunks_per_txhash_index` (default 1,000)
- Concurrent task slots: `workers` (default 40)
- DAG handles cross-index overlap naturally — no per-index orchestrators

---

## Data Flow Summary

### Backfill

```mermaid
flowchart TD
    A["GCS / CaptiveCore"] --> B["process_chunk task<br/>(10K ledgers per chunk, dispatched by DAG)"]
    B --> C["Fetch + process ledgers<br/>flush every ~100 ledgers"]
    C --> D["LFS chunk file<br/>(10K ledgers)"]
    C --> E["Raw txhash flat file<br/>(10K ledgers, 36B/entry)"]
    E -->|"all chunks_per_txhash_index chunks complete"| F["RecSplit builder<br/>(4-phase parallel pipeline)"]
    F --> G["RecSplit index files<br/>(16 CFs per index)"]
```

### Streaming

```mermaid
flowchart TD
    A["CaptiveStellarCore"] --> B["1 ledger"]
    B --> C["RocksDB active stores<br/>checkpoint every ledger"]
    C -->|"chunk boundary<br/>(every 10K ledgers)"| D["Ledger sub-flow transition<br/>(background goroutine per chunk)<br/>SwapActiveLedgerStore → LFS flush → CompleteLedgerTransition"]
    D --> E["LFS chunk files<br/>(10K ledgers each)"]
    C -->|"index boundary<br/>(every 10M ledgers)"| F["TxHash sub-flow transition<br/>(background goroutine per index)<br/>PromoteToTransitioning → RecSplit build → RemoveTransitioningTxHashStore"]
    F --> G["RecSplit index files<br/>(16 CFs per index)"]
```

---

## Hardware Requirements

See [12-metrics-and-sizing.md](./12-metrics-and-sizing.md#hardware-requirements) for CPU, RAM, disk, and network requirements.

---

## Recommended Reading Order

See [README.md — Recommended Reading Order](./README.md#recommended-reading-order) for the full grouped reading order across all 15 documents.

---

## Backfill Transition (Summary)

The backfill transition is a **RecSplit index build**, not a store conversion. There is no active RocksDB to tear down — the raw txhash flat files written per-chunk during ingestion are the sole input.

```mermaid
flowchart TD
    DONE1000(["All chunks complete for index N<br/>(chunk:{C}:lfs + chunk:{C}:txhash set for every chunk)"]) --> SET_RS["build_txhash_index fires<br/>(DAG dependency satisfied)"]
    SET_RS --> BUILD_CFS["4-phase parallel pipeline:<br/>1. Count (100 workers)<br/>2. Add keys (100 workers, per-CF mutex)<br/>3. Build indexes (16 workers)<br/>4. Verify lookups (100 workers, optional)"]
    BUILD_CFS --> RS_COMPLETE["Set index:{N:010d}:txhash = COMPLETE"]
    RS_COMPLETE --> DELETE_RAW["Delete raw txhash flat files + tmp/<br/>immutable/txhash/{N:04d}/raw/*.bin"]
    DELETE_RAW --> INDEX_DONE(["Index N complete"])

    SET_RS -->|"DAG overlap"| INGEST_NEXT["Index N+1 process_chunk tasks<br/>run concurrently with RecSplit build"]
```

**Key facts**:
- Input: `immutable/txhash/{N:04d}/raw/{chunkID:06d}.bin` (36 bytes/entry: hash[32]+seq[4])
- Crash recovery: **all-or-nothing** — delete partial indexes + tmp + stale per-CF done flags, rerun full 4-phase pipeline. Both modes write per-CF done flags, but backfill does not consult them on resume — they serve as permanent bookkeeping records.
- Raw files are NOT deleted until all 4 phases complete
- Duration: minutes per index (parallelized 4-phase pipeline with 100 workers)

See [03-backfill-workflow.md](./03-backfill-workflow.md#build_txhash_indexindex_id--index-cadence-10m-ledgers) for full details.

---

## Streaming Transition (Summary)

The streaming transition operates as **two independent sub-flow transitions at different cadences**, not a single combined workflow at the index boundary.

**Ledger sub-flow** (every 10K ledgers — chunk boundary): During ACTIVE, at each chunk boundary `SwapActiveLedgerStore` moves the old ledger store to `transitioningLedgerStore`. A background goroutine reads 10K ledgers from it, writes LFS `.data` + `.index` files, fsyncs, sets `chunk:{C:010d}:lfs`, then calls `CompleteLedgerTransition` to close and delete it. By the time the index boundary is reached, all 1,000 ledger stores have been individually transitioned to LFS and deleted.

**TxHash sub-flow** (every 10M ledgers — index boundary): At the index boundary, the system waits for the last chunk's ledger transition to complete (`waitForLedgerTransitionComplete`), verifies all 1,000 `chunk:{C:010d}:lfs` flags, then promotes the txhash store to `transitioningTxHashStore` via `PromoteToTransitioning`. A background goroutine builds 16 RecSplit CFs from the transitioning txhash store, verifies, sets `COMPLETE`, and calls `RemoveTransitioningTxHashStore` to close and delete it.

```mermaid
flowchart TD
    BOUNDARY(["Index N last ledger committed\nledger == indexLastLedger(N)"]) --> WAIT["waitForLedgerTransitionComplete()\nEnsure last chunk's LFS flush is done"]
    WAIT --> VERIFY_LFS["Verify all 1000 chunk:C:lfs flags\n(safety check — set during ACTIVE)"]
    VERIFY_LFS --> PROMOTE["PromoteToTransitioning(N)\n(moves ONLY txhash store)"]
    PROMOTE --> SPAWN["AddActiveStore(N+1)\nSpawn RecSplit build goroutine"]
    SPAWN --> INGEST_NEXT["Index N+1 ingestion continues\n(main goroutine)\nLedger sub-flow transitions at chunk boundaries"]
    SPAWN --> RECSPLIT["RecSplit build\nScan transitioning txhash store per nibble CF (0–f)\n→ build MPH → write cf-N.idx → fsync\n(16 CFs; no raw flat files)\nSet index:{N}:txhash after all 16 CFs"]
    RECSPLIT --> VERIFY_RS["Verify: spot-check 1,000 samples\n(minimum 1 per chunk) of ledgers +\n1,000 samples of txhashes\nagainst immutable files"]
    VERIFY_RS -->|pass| DELETE["RemoveTransitioningTxHashStore(N)\nSet index:{N:010d}:txhash = COMPLETE"]
    VERIFY_RS -->|fail| ABORT["ABORT — do NOT delete txhash store\nLog error; operator intervention"]

    BOUNDARY -.->|"index N ledger queries\nserved from LFS\n(all chunks already flushed)"| WAIT
    BOUNDARY -.->|"index N txhash queries\nserved from transitioning\ntxhash store until COMPLETE"| PROMOTE
```

**Key facts**:
- **No "Phase 1" at index boundary** — all LFS chunk files are written at their individual chunk boundaries during ACTIVE, not at transition time
- RecSplit is built directly from the transitioning txhash store (no raw flat files produced)
- The transitioning txhash store remains open for queries throughout the RecSplit build; not deleted until verification passes
- Crash recovery: `chunk:{C:010d}:lfs` flags (set during ACTIVE) + `index:{N}:txhash` (all-or-nothing RecSplit); WAL ensures txhash store survives crash

See [04-streaming-and-transition.md](./04-streaming-and-transition.md) for full details.

---

## Transition Workflow Comparison

| Dimension | Backfill Transition | Streaming Transition |
|-----------|---------------------|----------------------|
| Trigger | All 1000 chunks complete (`chunk:{C:010d}:lfs` + `chunk:{C:010d}:txhash` set for all) | Index boundary ledger committed to active store |
| Input | Raw txhash flat files (`immutable/txhash/{N}/raw/`) | Two active RocksDB stores: ledger store (default CF) + txhash store (16 CFs) |
| Execution context | Index worker goroutine (sequential per index, then frees slot) | Background goroutine (concurrent with next index ingestion) |
| LFS chunks | Written by `process_chunk` tasks during ingestion | Flushed individually at each chunk boundary during ACTIVE (via `SwapActiveLedgerStore` + background LFS flush + `CompleteLedgerTransition`); all 1,000 chunks complete before index boundary |
| RecSplit input | 1000 raw flat files scanned per nibble | Transitioning txhash store CF scan per nibble (no raw flat files) |
| Raw txhash flat files | Produced, consumed, then deleted post-RecSplit | Not produced |
| Active store teardown | Not applicable (no active store in backfill) | Only txhash store deleted after RecSplit verification passes (ledger stores already deleted at chunk boundaries) |
| Live queries during transition | Not applicable (no query layer in backfill) | Ledger queries → LFS; txhash queries → transitioning txhash store until `index:{N}:txhash` is set |
| Crash recovery granularity | All-or-nothing (rerun full pipeline; single `index:{N}:txhash` key) | All-or-nothing (same as backfill — rebuild all 16 CFs from transitioning txhash store) |
| Duration | Minutes (4-phase parallel pipeline) | RecSplit build + verification; LFS chunk writes happen during ACTIVE, not at transition time |

> **Deep dives**: [03-backfill-workflow.md](./03-backfill-workflow.md#build_txhash_indexindex_id--index-cadence-10m-ledgers) covers the 4-phase parallel RecSplit pipeline, all-or-nothing crash recovery, and async overlap with the next index. [04-streaming-and-transition.md](./04-streaming-and-transition.md) covers the ledger and txhash sub-flow transitions, all-or-nothing RecSplit recovery, background goroutines, and store deletion.

---

## Crash & Recovery Level-Set

Both modes use the meta store as the source of truth for crash recovery. WAL is **never disabled** for meta store writes — this is a hard invariant.

### Backfill Crash Recovery

```mermaid
flowchart TD
    START(["Process restart"]) --> SCAN_RANGES["Scan all index:{N:010d}:txhash in meta store"]
    SCAN_RANGES --> CHECK_RANGE{"index:N:txhash?"}
    CHECK_RANGE -->|COMPLETE| SKIP_RANGE["Skip index N entirely"]
    CHECK_RANGE -->|absent| SCAN_CHUNKS["Scan all 1000 chunk flags for index N\n(chunk:C:lfs + chunk:C:txhash)"]
    SCAN_CHUNKS --> ALL_DONE{"All chunks\nboth flags set?"}
    ALL_DONE -->|yes| RESUME_RS["BUILD_READY — rerun full\n4-phase RecSplit pipeline\n(all-or-nothing)"]
    ALL_DONE -->|no| REDO
    SCAN_CHUNKS --> SKIP_DONE["Skip chunks where both flags = 1"]
    SCAN_CHUNKS --> REDO["Re-ingest chunks with any flag missing"]
```

**Resume rule**: restart from first incomplete chunk. Completed chunks (both `chunk:{C:010d}:lfs` and `chunk:{C:010d}:txhash` set after fsync) are never re-ingested. `process_chunk` tasks resume independently — non-contiguous completion is safe.

### Streaming Crash Recovery

```mermaid
flowchart TD
    START(["Process restart"]) --> READ_LCL["Read streaming:last_committed_ledger"]
    READ_LCL --> SCAN_RANGES2["Scan all index states"]
    SCAN_RANGES2 --> RESUME_INGEST["Resume ingestion from\nlast_committed_ledger + 1"]
    RESUME_INGEST --> CHECK_TRANS{"Any index with all\nchunk flags set but\nno txhash key?"}
    CHECK_TRANS -->|yes| RESUME_TRANS["Resume RecSplit build\n(all-or-nothing from\ntransitioning txhash store)"]
    CHECK_TRANS -->|no| CONTINUE["Continue normal ingestion"]
```

**Resume rule**: streaming always resumes from `last_committed_ledger + 1`. The active RocksDB stores are WAL-backed — all committed ledgers survive a crash. During ACTIVE, ledger stores are individually transitioned and deleted at chunk boundaries; the txhash store remains. During TRANSITIONING, only the txhash store exists (all ledger stores already deleted). If `index:{N}:txhash` is absent but all chunk flags are set, the RecSplit build is rerun from scratch (all-or-nothing); the transitioning txhash store is not deleted until RecSplit verification passes.

**Expectations**:
- Backfill: operator re-runs same command; idempotent by design. Expect ≤1 chunk (~10,000 ledgers) lost per in-flight task on crash.
- Streaming: daemon restarts; expect ≤1 ledger lost (the uncommitted ledger at crash time). No data loss for committed ledgers.

See [07-crash-recovery.md](./07-crash-recovery.md) for all 6 crash scenarios with detailed decision trees.

### RecSplit Crash Recovery: Backfill vs Streaming

The two pipelines use **different crash recovery strategies** for the RecSplit build phase:

| Dimension | Backfill | Streaming |
|-----------|----------|-----------|
| Recovery model | **All-or-nothing** — delete all `.idx` files + `tmp/`, rerun full 4-phase pipeline. `index:{N}:txhash` set only after all 16 CFs build and fsync. | **All-or-nothing** — same as backfill. Delete partial `.idx` files, rebuild all 16 CFs from the transitioning txhash store. |
| Completion signal | Single `index:{N}:txhash = "1"` key — written after all 16 CFs are built and fsynced | Same single key |
| Rationale | Backfill's parallelized 4-phase pipeline (100 workers) completes in minutes — fast enough that rerunning is simpler than partial recovery. | Streaming also uses all-or-nothing — the single index key eliminates per-CF tracking complexity. |

> **See also**: [02-meta-store-design.md](./02-meta-store-design.md#durability-guarantees) for the meta store durability guarantees and flag semantics. [11-checkpointing-and-transitions.md](./11-checkpointing-and-transitions.md) for the boundary formulas that determine when transitions trigger.

---

## getEvents — Placeholder

`getEvents` is **not yet designed**. Placeholders are maintained across all workflow documents ([03](./03-backfill-workflow.md), [04](./04-streaming-and-transition.md), [07](./07-crash-recovery.md), [02](./02-meta-store-design.md), [08](./08-query-routing.md)) to reserve implementation space.

**When `getEvents` is implemented**:
- Index state machine extends: `INGESTING → RECSPLIT_BUILDING → EVENTS_INDEX_BUILDING → COMPLETE`
- Transitioning txhash store (streaming) is NOT deleted until RecSplit + events index all complete
- Raw events files (backfill) are NOT deleted until events index is complete
- New meta store keys: `index:{N:010d}:events_index` and per-partition done flags

---

## Future Design Considerations

### RecSplit: Single Index vs 16 Shards

The current 16-shard RecSplit design exists for build parallelism (~45 minutes per shard vs ~7 hours for a single index of ~3 billion entries). If single-index build times can be reduced below ~1 hour, the design may pivot to one RecSplit file per index. This change is isolated to the txhash sub-workflow — it does not affect the two-pipeline architecture, the data hierarchy, the LFS ledger store, or the meta store key model for indexes and chunks. See [14-open-questions.md — OQ-5](./14-open-questions.md#oq-5-recsplit-sharding--16-files-vs-single-index) for the full trade-off analysis.

### Pre-Created Archives as Alternative Backfill Source

A potential third backfill mode: download pre-built immutable archives (LFS chunks + RecSplit indexes) from S3/GCS instead of ingesting from scratch. This would skip the entire ingestion + transition pipeline, reducing backfill from days/weeks to a network-bound download + verify operation (potentially 10–100x faster).

**Nothing changes for existing backfill.** The BSB and CaptiveCore backfill paths remain identical. Archive-based backfill would be a separate code path selected by configuration (e.g., `[backfill.archive]`). Meta store changes would be **additive only** — a simpler set of per-index download tracking keys, since there is no per-chunk ingestion to track. The existing key hierarchy (index state, chunk flags, RecSplit CF flags) is unaffected.

See [14-open-questions.md — OQ-6](./14-open-questions.md#oq-6-pre-created-archives-as-alternative-backfill-source) for the full trade-off analysis.

---

## Key Invariants

1. **No RocksDB during backfill ingestion** — data is written directly to LFS chunks and raw txhash flat files.
2. **Flush every ~100 ledgers** — never accumulate more than ~100 ledgers in RAM during backfill.
3. **RecSplit built at index granularity** — triggered only after all 1,000 `process_chunk` tasks for an index are complete.
4. **RecSplit runs async with next index** — while RecSplit builds, the worker moves on to ingest the next index.
5. **Backfill and streaming transitions are separate** — no shared transition workflow exists.
6. **No queries during backfill** — process exits when all requested indexes complete.
7. **Index boundaries inclusive** — Index N = ledgers `(N×10M)+2` to `((N+1)×10M)+1` inclusive.
8. **Chunk boundaries align to indexes** — Index N spans exactly chunks `N×1000` through `(N×1000)+999`.
9. **process_chunk tasks run concurrently** — up to 40 concurrent tasks (shared across all indexes) each with its own GCS connection; completed chunks are non-contiguous at crash time; recovery scans all 1,000 chunk flag pairs.
10. **WAL is never disabled for meta store writes** — the meta store WAL is required for crash recovery; `DisableWAL(true)` is forbidden for any meta store operation in either mode.
11. **Transitioning stores are never deleted until verification passes** — the transitioning txhash store remains open for queries and as a recovery source until `index:{N}:txhash` is set (after all 16 CFs are built, fsynced, and spot-check verification succeeds). Ledger stores are deleted individually at chunk boundaries during ACTIVE after their LFS flush completes and `chunk:{C:010d}:lfs` is set.
12. **`getEvents` is a placeholder everywhere** — no events indexing is implemented; all workflow docs carry an explicit placeholder section to track where implementation will hook in.
