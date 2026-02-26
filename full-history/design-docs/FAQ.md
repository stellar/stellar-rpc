# Frequently Asked Questions

> Consolidated Q&A for the Stellar Full History RPC Service (v2 design). Each question links to the document with the authoritative answer.

---

## Quick Answers

| Question | Short Answer | Full Answer |
|----------|-------------|-------------|
| What is a range? | 10M-ledger partition — the unit for RecSplit index and transition | [11](./11-checkpointing-and-transitions.md#range-boundary-formulas) |
| What is a chunk? | 10K-ledger LFS file unit — also one raw txhash flat file | [11](./11-checkpointing-and-transitions.md#chunk-boundary-formulas) |
| What is the difference between a chunk and a range? | Range = 10M ledgers (10,000 files). Chunk = 10K ledgers (1 file) | [11](./11-checkpointing-and-transitions.md#chunk-boundary-formulas) |
| Does backfill use RocksDB? | No — writes directly to LFS chunks + raw txhash flat files | [03](./03-backfill-workflow.md#design-principles) |
| Why no RocksDB during backfill? | Avoids WAL overhead; crash recovery is at chunk granularity, not ledger | [03](./03-backfill-workflow.md#design-principles) |
| When is the RecSplit index built? | Once per range, after all 1,000 chunk txhash files are complete | [05](./05-backfill-transition-workflow.md) |
| How long does RecSplit take? | ~4 hours per 10M-ledger range | [03](./03-backfill-workflow.md#bsb-configuration) |
| Does RecSplit block the next range? | No — runs async while the next range ingests | [03](./03-backfill-workflow.md#parallelism-model) |
| What happens on backfill crash? | Re-run same command; scans all chunk flags; skips both-done chunks, redoes rest (gaps from parallel BSB are normal) | [07](./07-crash-recovery.md#backfill-crash-scenarios) |
| What happens on streaming crash? | Restart; resumes from `last_committed_ledger + 1` | [07](./07-crash-recovery.md#streaming-crash-scenarios) |
| Is there a `transitioning/` directory? | No — transition state is tracked in meta store only | [02](./02-meta-store-design.md#design-decisions) |
| Is there a `global:mode` key in meta store? | No — mode is determined by `--mode` startup flag | [02](./02-meta-store-design.md#design-decisions) |
| Can I query during backfill? | No — only `getHealth` and `getStatus` are available | [03](./03-backfill-workflow.md#design-principles) |
| Can I query during streaming transition? | Yes — active RocksDB remains accessible until transition completes | [06](./06-streaming-transition-workflow.md) |
| How much RAM does backfill use? | TBD — not yet profiled end-to-end; see memory budget section | [12](./12-metrics-and-sizing.md#memory-budget--backfill-bsb-mode) |
| Why flush every ~100 ledgers? | Caps per-chunk RAM to <300KB regardless of throughput | [03](./03-backfill-workflow.md#memory-budget) |
| Are range boundaries inclusive? | Yes — both ends inclusive; no gaps, no overlaps | [11](./11-checkpointing-and-transitions.md#range-boundary-formulas) |
| What happens on crash mid-RecSplit? | Re-run same command; scans 16 CF done flags, skips built CFs, rebuilds the rest from raw flat files | [07](./07-crash-recovery.md#scenario-b3-crash-mid-recsplit-build) |
| Can two processes use the same data_dir? | No — RocksDB flock prevents it | [07](./07-crash-recovery.md#concurrent-access-prevention) |

---

## Backfill Mode

### Q: Why does backfill not use RocksDB for ingestion?

Backfill's crash recovery granularity is the chunk (10K ledgers), not the ledger. RocksDB with WAL would add ~1.7TB of active store for every 10M-ledger range only to discard it after the range transitions. Instead, backfill writes directly to the final immutable formats (LFS `.data`/`.index` files + raw txhash `.bin` files), which are fsynced at chunk boundaries. A crashed chunk is simply rewritten from scratch. See [03-backfill-workflow.md — Design Principles](./03-backfill-workflow.md#design-principles).

---

### Q: What is a BSB instance?

A BSB (BufferedStorageBackend) instance is one concurrent worker assigned a contiguous ledger sub-range within a 10M-ledger range. With `num_bsb_instances_per_range = 20` (default), each instance spans 500K ledgers (50 chunks). With `num_bsb_instances_per_range = 10`, each spans 1M ledgers (100 chunks). All instances within a range run **in parallel** — this is BSB parallelism. BSB instance boundaries always align to chunk boundaries (multiples of 10K). See [03-backfill-workflow.md — BSB Configuration](./03-backfill-workflow.md#bsb-configuration).

---

### Q: What are the valid values for `num_bsb_instances_per_range`?

Any positive integer that divides 1,000 evenly (`1000 % value == 0`). Default: `20`. Common values: 5, 10, 20, 25, 50. All produce BSB instance sizes that are exact multiples of the 10K chunk size:
- `50` → 200K ledgers per BSB instance (20 chunks/instance)
- `25` → 400K ledgers per BSB instance (40 chunks/instance)
- `20` → 500K ledgers per BSB instance (50 chunks/instance)
- `10` → 1M ledgers per BSB instance (100 chunks/instance)
- `5` → 2M ledgers per BSB instance (200 chunks/instance)

All instances within a range start simultaneously and run concurrently. See [10-configuration.md — backfill.bsb](./10-configuration.md#backfillbsb).

---

### Q: When does the RecSplit build start for a range?

After all 1,000 chunk sub-workflows for the range complete — meaning both `lfs_done` and `txhash_done` are set in the meta store for all 1,000 chunks. See [05-backfill-transition-workflow.md](./05-backfill-transition-workflow.md).

---

### Q: Why does RecSplit build async with the next range rather than sequentially?

RecSplit build takes ~4 hours. If it blocked the next range orchestrator, backfill throughput would be halved. Since RecSplit only reads the raw txhash flat files (which are immutable once written), it can safely run while the orchestrator slot is freed for the next range. See [03-backfill-workflow.md — Parallelism Model](./03-backfill-workflow.md#parallelism-model).

---

### Q: When are raw txhash flat files deleted?

After all 16 RecSplit CF index files for a range are built and verified (i.e., all `recsplit:cf:XX:done` flags are set). They must not be deleted before this — RecSplit must be able to resume from them on crash. See [05-backfill-transition-workflow.md](./05-backfill-transition-workflow.md) and [07-crash-recovery.md — Scenario B3](./07-crash-recovery.md#scenario-b3-crash-mid-recsplit-build).

---

### Q: What is the raw txhash flat file format?

36 bytes per entry, no header, append-only:
```
[txhash: 32 bytes][ledgerSeq: 4 bytes big-endian uint32]
```
One file per chunk: `immutable/txhash/{rangeID:04d}/raw/{chunkID:06d}.bin`. See [09-directory-structure.md — Raw TxHash Flat File Path Convention](./09-directory-structure.md#raw-txhash-flat-file-path-convention).

---

### Q: What happens on backfill crash?

Re-run the same command. The orchestrator scans **all 1,000 chunk flag pairs** for the in-progress range and skips any chunk where both `lfs_done = "1"` AND `txhash_done = "1"`. All other chunks are redone from scratch.

Because all 20 BSB instances run in parallel, the set of completed chunks at crash time is **non-contiguous** — some instances may be ahead, others behind. Gaps between completed chunks are normal and expected. For example, at crash time instance 3 may have completed chunks 150–199 while instance 7 has completed only 350–360. The resume logic handles this correctly: scan all chunk flags, skip the done ones, redo the rest.

See [07-crash-recovery.md — Backfill Crash Scenarios](./07-crash-recovery.md#backfill-crash-scenarios).

---

### Q: Does crash recovery require completed chunks to be contiguous?

No. Because BSB instances run in parallel, completed chunks at crash time are non-contiguous — gaps are expected. The resume rule scans ALL 1,000 chunk flag pairs for the range and skips only chunks where both flags are set. There is no assumption that completed chunks form a prefix. See [07-crash-recovery.md](./07-crash-recovery.md#core-invariants).

---

### Q: What happens if backfill crashes mid-RecSplit build?

Re-run the same command. The RecSplit recovery scans all 16 CF done flags. Any CF whose flag is set is skipped. Any CF whose flag is absent has its partial `.idx` file deleted and rebuilt from the raw txhash flat files (which are retained until all 16 CFs complete). Raw files are never deleted until the range reaches COMPLETE. See [07-crash-recovery.md — Scenario B3](./07-crash-recovery.md#scenario-b3-crash-mid-recsplit-build).

---

### Q: What is the minimum disk space required for backfill?

For a single range (10M ledgers): ~1.5 TB for LFS chunks + ~120 GB for raw txhash flat files + ~15 GB for RecSplit indexes + meta store overhead. With `parallel_ranges=2`, double the active storage. Raw txhash files are deleted after RecSplit completes, so peak usage is during RECSPLIT_BUILDING. See [12-metrics-and-sizing.md — Storage Estimates](./12-metrics-and-sizing.md#storage-estimates).

---

## Streaming Mode

### Q: What checkpoint granularity does streaming use?

Every single ledger. After each ledger is committed to the active RocksDB WriteBatch (with WAL), the meta store key `streaming:last_committed_ledger` is updated. On crash, the service resumes from `last_committed_ledger + 1`. See [11-checkpointing-and-transitions.md — Streaming Checkpoint Formula](./11-checkpointing-and-transitions.md#streaming-checkpoint-formula).

---

### Q: Can I query during a streaming transition?

Yes. During TRANSITIONING, ledger queries are served from LFS (all chunks were transitioned during ACTIVE), and txhash queries are served from the transitioning txhash store, which remains open until RecSplit completes and `RemoveTransitioningTxHashStore` is called. Once `COMPLETE`, all queries route to the immutable LFS + RecSplit stores. See [06-streaming-transition-workflow.md](./06-streaming-transition-workflow.md) and [08-query-routing.md](./08-query-routing.md).

---

### Q: Does streaming mode write raw txhash flat files?

No. Streaming mode builds RecSplit directly from the active txhash store (16 CFs, one per hex nibble) during the streaming transition workflow. Raw txhash flat files are a backfill-only artifact. See [06-streaming-transition-workflow.md](./06-streaming-transition-workflow.md).

---

### Q: What validates that there are no ledger gaps before streaming starts?

At startup in streaming mode, the service reads the meta store and verifies that all ranges preceding the start range are in `COMPLETE` state or a recoverable transition state (`TRANSITIONING` or `RECSPLIT_BUILDING`). Ranges in a transition state are automatically resumed (the RecSplit build goroutine is re-spawned). Any range in `INGESTING`, `ACTIVE`, or absent state causes a fatal startup error — this indicates a gap that requires backfill completion first. See [04-streaming-workflow.md](./04-streaming-workflow.md).

---

## Crash Recovery

### Q: What does startup reconciliation do?

On every startup, before any ingestion begins, the system compares on-disk artifacts against meta store state. It deletes orphaned files from previous crashes (e.g., raw txhash files left after a range completed, orphaned transitioning stores). This runs once, is synchronous, and typically completes in under a second. See [07-crash-recovery.md — Startup Reconciliation](./07-crash-recovery.md#startup-reconciliation).

---

### Q: How do I know if something went wrong during crash recovery?

The startup reconciliation pass logs all cleanup actions at WARN level. Look for log entries mentioning "orphaned", "deleting", or "FATAL". A FATAL log means unrecoverable inconsistency (e.g., a transitioning range whose input store is missing) — this requires operator investigation. Normal recovery (chunk rewrites, RecSplit CF rebuilds) logs at INFO level.

---

### Q: Can two processes accidentally run on the same data directory?

No. The meta store RocksDB instance uses kernel-level `flock()` on a LOCK file. Any second process attempting to open the same data directory will fail immediately. This lock is automatically released on process exit, including `kill -9`. No manual cleanup is ever required. See [07-crash-recovery.md — Concurrent Access Prevention](./07-crash-recovery.md#concurrent-access-prevention).

---

## Range and Chunk Math

### Q: What is the exact last ledger in Range 0?

Ledger **10,000,001** (inclusive). Range 0 spans ledgers 2–10,000,001 inclusive. The formula: `rangeLastLedger(0) = ((0+1) × 10,000,000) + 2 - 1 = 10,000,001`. See [11-checkpointing-and-transitions.md — Range Boundary Formulas](./11-checkpointing-and-transitions.md#range-boundary-formulas).

---

### Q: Which LFS chunk contains ledger 10,000,001?

Chunk **999**, file `immutable/ledgers/chunks/0000/000999.data`. Chunk 999 spans ledgers 9,990,002–10,000,001 (inclusive). It is the last chunk of Range 0. See [11-checkpointing-and-transitions.md — Chunk Boundary Examples](./11-checkpointing-and-transitions.md#chunk-boundary-examples).

---

### Q: Where does ledger 10,000,002 live?

It is the first ledger of Range 1 and Chunk 1000. Path: `immutable/ledgers/chunks/0001/001000.data`. See [11-checkpointing-and-transitions.md — Chunk Boundary Examples](./11-checkpointing-and-transitions.md#chunk-boundary-examples).

---

### Q: How many chunks are in a range?

Exactly 1,000 (= 10,000,000 / 10,000). Range N spans chunks `N×1000` through `(N×1000)+999` inclusive. See [11-checkpointing-and-transitions.md](./11-checkpointing-and-transitions.md#key-constants).

---

### Q: When does the streaming transition trigger?

At ledger `rangeLastLedger(N)` — i.e., 10,000,001 / 20,000,001 / 30,000,001 / … That ledger is written to the active store, the checkpoint is written, and only then is the background transition goroutine spawned. See [11-checkpointing-and-transitions.md — Transition Trigger](./11-checkpointing-and-transitions.md#transition-trigger-streaming).

---

## Meta Store and State

### Q: Is there a `global:mode` key in the meta store?

No. The v2 design eliminates it. Mode is determined entirely by the `--mode backfill` or `--mode streaming` flag at startup. The meta store never needs to know the current mode. See [02-meta-store-design.md — Design Decisions](./02-meta-store-design.md#design-decisions).

---

### Q: What meta store keys are written during backfill?

- `range:{rangeID:04d}:state` — INGESTING → RECSPLIT_BUILDING → COMPLETE
- `range:{rangeID:04d}:chunk:{chunkID:06d}:lfs_done` — set after each chunk LFS fsync
- `range:{rangeID:04d}:chunk:{chunkID:06d}:txhash_done` — set after each chunk txhash fsync
- `range:{rangeID:04d}:recsplit:state` — BUILDING → COMPLETE
- `range:{rangeID:04d}:recsplit:cf:{cfIndex:02d}:done` — set after each CF index file is built

See [02-meta-store-design.md](./02-meta-store-design.md).

---

### Q: What meta store keys are written during streaming?

- `range:{rangeID:04d}:state` — ACTIVE → TRANSITIONING → COMPLETE
- `streaming:last_committed_ledger` — updated every ledger
- `range:{rangeID:04d}:chunk:{chunkID:06d}:lfs_done` — set at each chunk boundary during ACTIVE (ledger sub-flow transition)
- `range:{rangeID:04d}:recsplit:cf:{cfIndex:02d}:done` — set during TRANSITIONING (RecSplit build from transitioning txhash store)

See [02-meta-store-design.md](./02-meta-store-design.md).

---

## Directory and Configuration

### Q: Is there a `transitioning/` directory?

No. The v2 design eliminates it. The RocksDB active store stays at `<active_stores_base_dir>/ledger-store-chunk-{chunkID:06d}/` throughout the transition — it is deleted in-place once the transition goroutine completes. Transition state is `range:{N}:state = "TRANSITIONING"` in the meta store. See [02-meta-store-design.md — Design Decisions](./02-meta-store-design.md#design-decisions) and [09-directory-structure.md](./09-directory-structure.md).

---

### Q: What is the path for a raw txhash file for range 3, chunk 3042?

`immutable/txhash/0003/raw/003042.bin`. See [09-directory-structure.md — Raw TxHash Flat File Path Convention](./09-directory-structure.md#raw-txhash-flat-file-path-convention).

---

### Q: What TOML key controls BSB instance count?

`[backfill.bsb].num_bsb_instances_per_range`. Valid values: any positive integer that divides 1,000 evenly (`1000 % value == 0`). Default: `20`. `[backfill.bsb]` and `[backfill.captive_core]` are mutually exclusive — exactly one must be present. See [10-configuration.md](./10-configuration.md#backfillbsb).

---

### Q: Can I put active stores and immutable stores on different disks?

Yes. Use `[active_stores].base_path` and `[immutable_stores].ledgers_base` / `[immutable_stores].txhash_base` to override the default sub-paths to absolute paths on separate volumes. See [10-configuration.md — Example 4](./10-configuration.md#example-4-multi-disk-layout).

---

## getEvents — Placeholder

> **Status**: Not yet designed. This section reserves space for future work.

### Q: Will `getEvents` be supported?

Yes, as a future work item. The current design explicitly reserves space for it in every relevant document. No timeline is committed.

### Q: Where would `getEvents` data be stored?

During streaming ingestion, a **separate active events RocksDB store** (its own RocksDB instance, independent of the ledger store and txhash store) would hold event data. During backfill, per-chunk events files would be written alongside LFS and txhash files. After a range completes, an events index would be built into `immutable/events/{rangeID:04d}/index/`.

### Q: How would crash recovery change for `getEvents`?

A third chunk completion flag (`events_done`) would be added alongside `lfs_done` and `txhash_done`. A chunk would only be skippable when ALL three flags are set. See [02-meta-store-design.md — getEvents Placeholder](./02-meta-store-design.md#getevents-immutable-store--placeholder).

### Q: How would query routing change for `getEvents`?

The QueryRouter would gain a `getEvents` routing path following the same ACTIVE→TRANSITIONING→COMPLETE pattern as existing endpoints. See [08-query-routing.md — getEvents Placeholder](./08-query-routing.md#getevents--placeholder).
