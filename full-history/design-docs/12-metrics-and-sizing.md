# Metrics & Sizing Reference

> **This is the single source of truth for all concrete numbers, storage estimates, memory budgets, hardware requirements, and structural constants.** All other documents link here instead of duplicating these figures.

---

## Structural Constants

| Constant                                                          | Value              | Notes                                                                                   |
|-------------------------------------------------------------------|--------------------|-----------------------------------------------------------------------------------------|
| `FirstLedger`                                                     | 2                  | Ledger sequence of the first ledger in Stellar history                                  |
| `IndexSize`                                                       | 10,000,000 ledgers | One index = one RecSplit index build unit                                                |
| `ChunkSize`                                                       | 10,000 ledgers     | One chunk = one LFS file pair + one raw txhash flat file (during backfill mode)         |
| Chunks per index                                                  | 1,000              | = IndexSize (10M) ÷ ChunkSize (10K)                                                     |
| RecSplit column families                                          | 16                 | Sharded by first hex nibble of txhash (`0`–`f`)                                         |
| Max concurrent task slots                                         | 40                 | `[backfill].workers` (flat worker pool, DAG-scheduled)                                  |
| BSB internal prefetch window                                      | 1,000 ledgers      | `[backfill.bsb].buffer_size`                                                            |
| BSB internal download workers                                     | 20                 | `[backfill.bsb].num_workers`                                                            |
| `chunks_per_txhash_index` config default                                 | 1,000              | `[backfill].chunks_per_txhash_index`; must equal IndexSize ÷ ChunkSize                         |
| Raw txhash entry size                                             | 36 bytes           | `txhash[32] \|\| ledgerSeq[4]` big-endian                                               |
| Average compressed LCM size                                       | ~150 KB            | Per ledger, after zstd                                                                  |
| Average raw ledger size                                           | ~1 MB              | Per ledger, uncompressed from BSB/GCS                                                   |
| Average transactions per ledger                                   | ~300               | Used for txhash buffer sizing                                                           |
| Approximate transactions per index                                | 3B+                | 10M ledgers × ~300 tx/ledger                                                            |

---

## Index Boundaries (First 5)

| Index | First Ledger | Last Ledger |
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
| Active txhash store | `<active_stores_base_dir>/txhash-store-index-{indexID:04d}/` | ~150 GB | Streaming only; 16 CFs by nibble; deleted post-transition |
| Meta store | `meta/rocksdb/` | ~100 MB | Shared across all ranges; grows slowly |

### Space Efficiency: RocksDB → Immutable

| Store | RocksDB (Active) | Immutable | Reduction | Notes |
|-------|------------------|-----------|-----------|-------|
| Ledger | ~1.7 TB | ~1.5 TB (LFS) | ~12% | LFS removes RocksDB overhead (WAL, MemTable, bloom filters); data itself is already zstd-compressed in both |
| TxHash | ~150 GB | ~15 GB (RecSplit) | **~90%** | RecSplit uses **~4.5 bytes/entry** vs 36 bytes/entry + bloom filter overhead in RocksDB |
| **Total per range** | **~1.85 TB** | **~1.52 TB** | **~18%** | RecSplit's 90% reduction on txhash is the dominant saving |

RecSplit's ~4.5 bytes/entry is a key design motivator: a minimal perfect hash maps every txhash to a unique slot with zero collisions, zero wasted space, and O(1) lookup in ~100 μs (vs ~400 μs for RocksDB CF lookup). Compared to RocksDB's 36 bytes/entry plus bloom filter and index block overhead, this is a ~90% reduction for the txhash store. See [03-backfill-workflow.md](./03-backfill-workflow.md#build_txhash_indexrange_id--range-cadence-10m-ledgers) for RecSplit build mechanics and [15-query-performance.md](./15-query-performance.md) for query latency breakdown.

**Peak disk required during backfill** (2 ranges in flight simultaneously): ~2 × (~1.5 TB LFS + ~120 GB raw) + ~100 MB meta ≈ **~3.2 TB**.

**Peak disk required during streaming**: active ledger store (~1.7 TB) + active txhash store (~150 GB) + prior immutable ranges + meta ≈ **~1.85 TB active + immutable history**.

---

## Memory Budget — Backfill (BSB Mode)

> **TBD** — Observed RSS with `workers=40` can reach ~40 GB in practice. Theoretical bottom-up estimates do not match observed usage; profiling needed before publishing numbers.

**Per-chunk write buffer**: The chunk writer flushes incrementally; the in-RAM ledger buffer stays well under ~250 KB per chunk during normal operation.

---

## Memory Budget — Backfill (CaptiveStellarCore Mode)

> **TBD** — CaptiveStellarCore process alone is ~8 GB; total budget not yet profiled.

---

## Memory Budget — Streaming Mode

> **TBD** — Not yet profiled end-to-end.

---

## Hardware Requirements

| Resource | Requirement | Notes |
|----------|-------------|-------|
| CPU | 32 cores | Flat worker pool (40 task slots) + RecSplit build threads |
| RAM | 128 GB | Headroom for OS, GCS client, RecSplit build working set |
| CaptiveStellarCore RAM | ~8 GB per instance | Streaming: 1 instance; backfill captive_core: 1 |
| Disk — active stores | SSD | Low-latency write path for streaming RocksDB |
| Disk — immutable stores | SSD | Large sequential writes during backfill; sequential reads at query time |
| Disk — meta store | SSD | Random reads/writes for chunk flag tracking |
| Network | High bandwidth | GCS/S3 backfill fetches: up to 40 concurrent process_chunk tasks |

---

## Durations

| Operation | Duration | Notes |
|-----------|----------|-------|
| RecSplit build per range | Minutes | 4-phase parallel pipeline with 100 workers (Count, Add, Build, Verify). Previously ~4 hours with 16-goroutine sequential design. |
| Chunk scan on resume | < 10 ms | At startup after a crash: reads `chunk:{C}:lfs` + `chunk:{C}:txhash` from meta store for ALL 1,000 chunks per index unconditionally — no early exit. Non-contiguous gaps from concurrent `process_chunk` tasks mean there is no "first incomplete chunk" concept. ~2,000 RocksDB `Get` calls per index — negligible. |
| Progress log interval | 1 minute | Wall-clock elapsed from process start |

---

## Worker Count Trade-off

| `workers` | Concurrent tasks | GCS connections | RAM overhead |
|-----------|-----------------|-----------------|-------------|
| `40` (default) | Up to 40 process_chunk tasks | Up to 40 GCS connections | TBD |
| `20` | Lower parallelism | Fewer connections | Lower |
| `10` | Memory-constrained | Minimal | Minimal |

Reduce `workers` on memory-constrained machines; use the default for maximum throughput.

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

- [03-backfill-workflow.md](./03-backfill-workflow.md) — flush discipline, chunk write lifecycle, RecSplit build mechanics
- [09-directory-structure.md](./09-directory-structure.md) — on-disk paths for all stores
- [10-configuration.md](./10-configuration.md) — TOML knobs that drive these numbers
- [11-checkpointing-and-transitions.md](./11-checkpointing-and-transitions.md) — boundary math formulas
