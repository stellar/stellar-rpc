# Metrics & Sizing Reference

> **This is the single source of truth for all concrete numbers, storage estimates, memory budgets, hardware requirements, and structural constants.** All other documents link here instead of duplicating these figures.

---

## Structural Constants

| Constant                                                          | Value              | Notes                                                                                   |
|-------------------------------------------------------------------|--------------------|-----------------------------------------------------------------------------------------|
| `FirstLedger`                                                     | 2                  | Ledger sequence of the first ledger in Stellar history                                  |
| `RangeSize`                                                       | 10,000,000 ledgers | One range = one RecSplit index                                                          |
| `ChunkSize`                                                       | 10,000 ledgers     | One chunk = one LFS file pair + one raw txhash flat file (during backfill mode)         |
| Chunks per range                                                  | 1,000              | = RangeSize (10M) ÷ ChunkSize (10K)                                                     |
| RecSplit column families                                          | 16                 | Sharded by first hex nibble of txhash (`0`–`f`)                                         |
| Default Buffered Storage Backend (BSB) instances per orchestrator | 20                 | `[backfill.bsb].num_bsb_instances_per_range`                                                          |
| Max parallel range orchestrators                                  | 2                  | `[backfill].parallel_ranges`                                                            |
| Max BSB instances in flight                                       | 40                 | 2 orchestrators × 20 BSB instances per orchestrator                                    |
| Ledgers per BSB instance (`num_bsb_instances_per_range=20`)                     | 500,000            | = RangeSize ÷ 20                                                                        |
| Ledgers per BSB instance (`num_bsb_instances_per_range=10`)                     | 1,000,000          | = RangeSize ÷ 10                                                                        |
| Chunks per BSB instance (`num_bsb_instances_per_range=20`)                      | 50                 | = 500K ÷ 10K                                                                            |
| Chunks per BSB instance (`num_bsb_instances_per_range=10`)                      | 100                | = 1M ÷ 10K                                                                            |
| BSB internal prefetch window                                      | 1,000 ledgers      | `[backfill.bsb].buffer_size`                                                            |
| BSB internal download workers                                     | 20                 | `[backfill.bsb].num_workers`                                                            |
| Flush interval                                                    | ~100 ledgers       | `[backfill].flush_interval`; max ledgers in RAM when trying to write a chunk on the LFS |
| Raw txhash entry size                                             | 36 bytes           | `txhash[32] \|\| ledgerSeq[4]` big-endian                                               |
| Average compressed LCM size                                       | ~150 KB            | Per ledger, after zstd                                                                  |
| Average raw ledger size                                           | ~1 MB              | Per ledger, uncompressed from BSB/GCS                                                   |
| Average transactions per ledger                                   | ~300               | Used for txhash buffer sizing                                                           |
| Approximate transactions per range                                | 3B+                | 10M ledgers × ~300 tx/ledger                                                            |

---

## Range Boundaries (First 5)

| Range | First Ledger | Last Ledger |
|-------|-------------|------------|
| 0 | 2 | 10,000,001 |
| 1 | 10,000,002 | 20,000,001 |
| 2 | 20,000,002 | 30,000,001 |
| 3 | 30,000,002 | 40,000,001 |
| 4 | 40,000,002 | 50,000,001 |

Formulas: `rangeFirstLedger(N) = (N × 10,000,000) + 2`, `rangeLastLedger(N) = ((N+1) × 10,000,000) + 1`.  
See [11-checkpointing-and-transitions.md](./11-checkpointing-and-transitions.md) for full math.

---

## Storage Estimates (Per 10M-Ledger Range)

| Component | Path | Size    | Notes |
|-----------|------|---------|-------|
| LFS chunk files | `immutable/ledgers/chunks/` | ~1.5 TB | 1,000 `.data` + `.index` pairs, zstd-compressed LCMs |
| RecSplit index | `immutable/txhash/{N:04d}/index/` | ~15 GB  | 16 CF files (`cf-0.idx`–`cf-f.idx`) |
| Raw txhash flat files | `immutable/txhash/{N:04d}/raw/` | ~120 GB | Temporary; deleted after all 16 CF indexes are built |
| Active ledger store | `<active_stores_base_dir>/ledger-store-chunk-{chunkID:06d}/` | ~1.7 TB | Streaming only; default CF; deleted post-transition |
| Active txhash store | `<active_stores_base_dir>/txhash-store-range-{rangeID:04d}/` | ~150 GB | Streaming only; 16 CFs by nibble; deleted post-transition |
| Meta store | `meta/rocksdb/` | ~100 MB | Shared across all ranges; grows slowly |

### Space Efficiency: RocksDB → Immutable

| Store | RocksDB (Active) | Immutable | Reduction | Notes |
|-------|------------------|-----------|-----------|-------|
| Ledger | ~1.7 TB | ~1.5 TB (LFS) | ~12% | LFS removes RocksDB overhead (WAL, MemTable, bloom filters); data itself is already zstd-compressed in both |
| TxHash | ~150 GB | ~15 GB (RecSplit) | **~90%** | RecSplit uses **~4.5 bytes/entry** vs 36 bytes/entry + bloom filter overhead in RocksDB |
| **Total per range** | **~1.85 TB** | **~1.52 TB** | **~18%** | RecSplit's 90% reduction on txhash is the dominant saving |

RecSplit's ~4.5 bytes/entry is a key design motivator: a minimal perfect hash maps every txhash to a unique slot with zero collisions, zero wasted space, and O(1) lookup with 2–3 disk seeks. Compared to RocksDB's 36 bytes/entry plus bloom filter and index block overhead, this is a ~90% reduction for the txhash store. See [05-backfill-transition-workflow.md](./05-backfill-transition-workflow.md#recsplit-index-construction) for RecSplit build mechanics.

**Peak disk required during backfill** (2 ranges in flight simultaneously): ~2 × (~1.5 TB LFS + ~120 GB raw) + ~100 MB meta ≈ **~3.2 TB**.

**Peak disk required during streaming**: active ledger store (~1.7 TB) + active txhash store (~150 GB) + prior immutable ranges + meta ≈ **~1.85 TB active + immutable history**.

---

## Memory Budget — Backfill (BSB Mode)

> **TBD** — Observed RSS with `num_bsb_instances_per_range=20` can reach ~40 GB in practice. Theoretical bottom-up estimates do not match observed usage; profiling needed before publishing numbers.

**`flush_interval` RAM cap**: `flush_interval=100` keeps per-chunk write buffer under ~250 KB. Never set above 10,000 (chunk size) — that would accumulate an entire chunk in RAM before any flush.

---

## Memory Budget — Backfill (CaptiveStellarCore Mode)

> **TBD** — CaptiveStellarCore process alone is ~8 GB per orchestrator; total budget not yet profiled.

---

## Memory Budget — Streaming Mode

> **TBD** — Not yet profiled end-to-end.

---

## Hardware Requirements

| Resource | Requirement | Notes |
|----------|-------------|-------|
| CPU | 32 cores | Parallelism: 40 BSB workers + RecSplit build threads |
| RAM | 128 GB | Headroom for OS, GCS client, RecSplit build working set |
| CaptiveStellarCore RAM | ~8 GB per instance | Streaming: 1 instance; backfill captive_core: 1 per orchestrator |
| Disk — active stores | SSD | Low-latency write path for streaming RocksDB |
| Disk — immutable stores | SSD | Large sequential writes during backfill; sequential reads at query time |
| Disk — meta store | SSD | Random reads/writes for chunk flag tracking |
| Network | High bandwidth | GCS/S3 backfill fetches up to 40 BSB instances × 20 workers |

---

## Durations

| Operation | Duration | Notes |
|-----------|----------|-------|
| RecSplit build per range | Minutes | 4-phase parallel pipeline with 100 workers (Count, Add, Build, Verify). Previously ~4 hours with 16-goroutine sequential design. |
| Chunk scan on resume | < 10 ms | At startup after a crash: reads `range:N:chunk:C:lfs_done` + `txhash_done` from meta store for ALL 1,000 chunks per range unconditionally — no early exit. Non-contiguous gaps from parallel BSB instances mean there is no "first incomplete chunk" concept. ~2,000 RocksDB `Get` calls per range — negligible. |
| Progress log interval | 1 minute | Wall-clock elapsed from process start |

---

## `num_bsb_instances_per_range` Trade-off

| `num_bsb_instances_per_range` | BSB span | Chunks/instance | Parallelism | RAM overhead |
|-------------------------------|----------|-----------------|-------------|-------------|
| `20` (default) | 500K ledgers | 50 | Higher | TBD |
| `10` | 1M ledgers | 100 | Lower | TBD |

Use `10` on memory-constrained machines; use `20` for maximum throughput.

---

## Transaction Density by Range

Transaction density varies dramatically across Stellar history:

| Range | Ledger Range | Approx. Tx/Ledger | Approx. Total Tx | Notes |
|-------|-------------|-------------------|-------------------|-------|
| 0 | 2 – 10M | ~1–50 | Sparse | Early network, very low activity |
| 1 | 10M – 20M | ~50–150 | Moderate | Growing adoption |
| 2 | 20M – 30M | ~150–250 | ~1.5B–2.5B | Increasing density |
| 3+ | 30M+ | ~300–325 | ~3B–3.25B | Steady-state high density |

Key implications:
- **RecSplit sizing**: For ranges 3+, each CF handles ~200M keys (~3.25B / 16 CFs). Index files are ~900 MB each (4.5 bytes/entry), ~14.4 GB total per range.
- **Memory during Verify**: With 16 CFs × 2 ranges in flight = 32 open indexes, mmap'd space is ~29 GB. Machines have 128 GB, so this fits comfortably.
- **Early ranges are fast**: Ranges 0–2 have significantly fewer transactions and complete RecSplit in seconds, not minutes.

---

## Related Documents

- [03-backfill-workflow.md](./03-backfill-workflow.md) — flush discipline, chunk write lifecycle
- [05-backfill-transition-workflow.md](./05-backfill-transition-workflow.md) — RecSplit build mechanics
- [09-directory-structure.md](./09-directory-structure.md) — on-disk paths for all stores
- [10-configuration.md](./10-configuration.md) — TOML knobs that drive these numbers
- [11-checkpointing-and-transitions.md](./11-checkpointing-and-transitions.md) — boundary math formulas
