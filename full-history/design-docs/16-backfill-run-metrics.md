# 16. Backfill Run Metrics — Full 60M-Ledger Production Run

> **Date**: 2026-03-02 to 2026-03-03
> **Duration**: 7 hours 4 minutes (wall clock)
> **Scope**: Ledgers 2 through 60,000,001 (6 indexes, 6,000 chunks, 60M ledgers)
> **Result**: Clean completion, zero crashes, zero restarts

This document captures the complete metrics from the first production backfill run of the Stellar Full History pipeline. The run ingested the entire Stellar pubnet history — 9.14 billion transactions across 60 million ledgers — and built RecSplit indexes for every index.

---

## Configuration

```toml
[backfill]
  start_ledger            = 2
  end_ledger              = 60000001
  parallel_indexes        = 2
  flush_interval          = 100

[backfill.bsb]
  bucket_path             = "sdf-ledger-close-meta/v1/ledgers/pubnet"
  buffer_size             = 1000
  num_workers             = 20
  num_instances_per_index = 20
```

**Key parameters**: 2 indexes process concurrently. Each index spawns 20 BSB (BufferedStorageBackend) instances, each responsible for 50 of the 1,000 chunks. BSB fetches ledger close metadata from GCS, and each instance has 20 download workers with a 1,000-ledger buffer.

---

## Overall Summary

| Metric | Value |
|--------|-------|
| Indexes | 6 (0000–0005) |
| Chunks | 6,000 |
| Ledgers | 60,000,000 |
| Transactions indexed | 9,140,500,170 |
| Wall clock time | 7h 4m 27s |
| Avg ingestion throughput | 2,355 ledgers/s \| 358,907 tx/s |
| Peak RSS | 178.19 GB |
| GC cycles | 41,333 |

---

## Per-Index Breakdown

The pipeline processes 2 indexes concurrently. When an index finishes ingestion, it releases its semaphore slot so the next queued index can begin ingesting. RecSplit runs outside the semaphore — so during peak load, 2 indexes ingest while 1-2 others build indexes simultaneously.

> Index 4 was the densest index in all of Stellar history: **3.2 billion transactions** in 10M ledgers, averaging **320 tx/ledger**. Its ingestion alone took 3h 41m and produced 1.8 TB of LFS data.

Each index covers 10M ledgers and contains 1,000 chunks of 10K ledgers each. **Ingestion** is the wall clock time from the index's first BSB fetch to the last chunk's fsync. **RecSplit** is the wall clock time for the full 4-phase index build (count + add + build + verify). **Total** is ingestion + RecSplit, but note these may overlap with other indexes — see [Pipeline Concurrency Timeline](#pipeline-concurrency-timeline). **LFS Size** is the total size of the zstd-compressed ledger data files. **Index Size** is the total size of the 16 RecSplit `.idx` files.

| Index | Ledger Span | Tx Count | Tx/Ledger | Ingestion | RecSplit | Total | LFS Size | Index Size |
|-------|------------|----------|-----------|-----------|----------|-------|----------|------------|
| 0000 | 2 – 10M | 475K | 0.05 | 35m 48s | 291ms | 35m 49s | 3.7 GB | 2.02 MB |
| 0001 | 10M – 20M | 43.3M | 4.3 | 35m 53s | 46s | 36m 39s | 23 GB | 220 MB |
| 0002 | 20M – 30M | 515.3M | 51.5 | 59m 5s | 9m 37s | 1h 9m | 209 GB | 2.56 GB |
| 0003 | 30M – 40M | 2.47B | 247.0 | 2h 43m | 54m 51s | 3h 38m | 868 GB | 12.28 GB |
| 0004 | 40M – 50M | 3.20B | 319.9 | 3h 41m | 40m 36s | 4h 22m | 1.8 TB | 15.90 GB |
| 0005 | 50M – 60M | 2.91B | 291.3 | 3h 27m | 19m 8s | 3h 46m | 1.4 TB | 14.49 GB |

### Raw .bin File Sizes (Deleted After RecSplit)

Intermediate `.bin` files (36 bytes/entry: 32-byte hash + 4-byte ledger seq) are created during ingestion and consumed by RecSplit. They are deleted after index building completes.

| Index | Raw .bin Size | Freed After RecSplit |
|-------|---------------|---------------------|
| 0000 | 16.31 MB | 16.31 MB |
| 0001 | 1.45 GB | 1.45 GB |
| 0002 | 17.28 GB | 17.28 GB |
| 0003 | 82.80 GB | 82.80 GB |
| 0004 | 107.24 GB | 107.24 GB |
| 0005 | 97.67 GB | 97.67 GB |

### Disk Usage Summary

Final on-disk sizes after RecSplit completes and raw `.bin` files are deleted. These are the permanent, immutable artifacts. Measured via `du -sh` on the output directories.

| Index | LFS (ledgers) | RecSplit Index | Total Immutable |
|-------|---------------|----------------|-----------------|
| 0000 | 3.7 GB | 2.1 MB | 3.7 GB |
| 0001 | 23 GB | 221 MB | 23.2 GB |
| 0002 | 209 GB | 2.6 GB | 211.6 GB |
| 0003 | 868 GB | 13 GB | 881 GB |
| 0004 | 1.8 TB | 16 GB | 1.816 TB |
| 0005 | 1.4 TB | 15 GB | 1.415 TB |
| **Total** | **~4.3 TB** | **~47 GB** | **~4.35 TB** |

---

## RecSplit 4-Phase Pipeline

RecSplit builds 16 perfect hash indexes (one per column family) for each index. The 4-phase pipeline is:

1. **Count** — 100 goroutines scan all `.bin` files to count entries per CF
2. **Add** — 100 goroutines re-read files and feed keys to 16 RecSplit builders (mutex-protected)
3. **Build** — 16 goroutines each build one CF's perfect hash index (CPU-bound)
4. **Verify** — 100 goroutines verify every key's lookup returns the correct ledger sequence

All times below are wall clock. **Count**, **Add**, and **Verify** each use 100 goroutines. **Build** uses 16 goroutines (one per CF) and is bounded by the slowest CF. **Total Keys** is the number of transaction hashes indexed across all 16 CFs. **Keys/CF** is Total Keys / 16 (keys are uniformly distributed across CFs by the first nibble of the transaction hash).

| Index | Total Keys | Keys/CF | Count | Add | Build | Verify | Total |
|-------|-----------|---------|-------|-----|-------|--------|-------|
| 0000 | 475K | ~30K | 30ms | 75ms | 94ms | 59ms | 291ms |
| 0001 | 43.3M | ~2.7M | 1.9s | 24.1s | 14.0s | 5.6s | 46.2s |
| 0002 | 515.3M | ~32.2M | 27.8s | 5m 2s | 2m 54s | 1m 10s | 9m 37s |
| 0003 | 2.47B | ~154.4M | 2m 23s | 26m 48s | 18m 26s | 7m 8s | 54m 51s |
| 0004 | 3.20B | ~199.9M | 2m 12s | 20m 40s | 12m 16s | 5m 23s | 40m 36s |
| 0005 | 2.91B | ~182.1M | 1m 18s | 6m 42s | 8m 6s | 2m 57s | 19m 8s |

### Why Index 4 RecSplit Was Faster Than Index 3 Despite More Keys

Index 4 has 29% more keys than Index 3 (3.2B vs 2.5B), yet its RecSplit took 26% less time (40m vs 55m). The Build phase was 33% faster (12m vs 18m). This is because Index 4 ran alone — Index 5 was still ingesting, so the 16 build goroutines had the full CPU to themselves. Index 3's build overlapped with Index 4's ingestion (40 BSB goroutines competing for CPU and I/O).

### RecSplit Index Efficiency

**Bytes/Key** = Index Size / Total Keys. **Per-CF Build Rate** = Keys/CF / Build phase wall time. This reflects the throughput of a single CF builder goroutine — during the Build phase, 16 such goroutines run in parallel, each building one CF's index independently. The rate varies because some indexes' Build phases overlap with another index's ingestion (competing for CPU), while others run alone.

| Index | Total Keys | Index Size | Bytes/Key | Per-CF Build Rate |
|-------|-----------|------------|-----------|-------------------|
| 0000 | 475K | 2.02 MB | 4.46 | 316K/s |
| 0001 | 43.3M | 220 MB | 5.33 | 193K/s |
| 0002 | 515.3M | 2.56 GB | 5.21 | 185K/s |
| 0003 | 2.47B | 12.28 GB | 5.22 | 140K/s |
| 0004 | 3.20B | 15.90 GB | 5.21 | 272K/s |
| 0005 | 2.91B | 14.49 GB | 5.22 | 375K/s |

The index is ~15% the size of the raw `.bin` data it replaces (e.g., Index 4: 15.9 GB index from 107 GB raw). Per-CF build rate varies 140–375K keys/s depending on CPU contention — Index 3's builder ran at 140K/s while competing with Index 4's ingestion, whereas Index 5's builder ran at 375K/s with the CPU to itself.

---

## Transaction Density Over Stellar History

> Stellar launched in 2015 with nearly zero traffic. Activity grew slowly through the first 30M ledgers (~8 years), then exploded — Index 4 (ledgers 40-50M) carries **6,729x** more transactions per ledger than Index 0.

| Index | Ledger Span | Tx Count | Avg Tx/Ledger | Index-over-Index Growth | Share of Total |
|-------|------------|----------|---------------|------------------------|----------------|
| 0000 | 2 – 10M | 475K | 0.05 | — (baseline) | 0.005% |
| 0001 | 10M – 20M | 43.3M | 4.3 | 91x | 0.5% |
| 0002 | 20M – 30M | 515.3M | 51.5 | 12x | 5.6% |
| 0003 | 30M – 40M | 2.47B | 247.0 | 4.8x | 27.0% |
| 0004 | 40M – 50M | 3.20B | 319.9 | 1.3x | 35.0% |
| 0005 | 50M – 60M | 2.91B | 291.3 | 0.9x | 31.9% |

**Observations:**

- 93.9% of all transactions are in Indexes 3–5 (the last 30M ledgers). Indexes 0–1 (the first 20M ledgers) contain 0.5%.
- The largest index-over-index jump is Index 1 → Index 2 (12x). After that, growth slows: 4.8x, 1.3x, then a slight decline (0.9x).
- Index 4 has the highest density at 320 tx/ledger. Index 5 is 9% lower at 291 tx/ledger.

### Densest Chunks — Top 10

Each chunk covers 10,000 ledgers. The densest chunks are concentrated in Index 3 (ledgers 30–40M), specifically in the 38–39M ledger band. Chunk 3863 (ledgers 38,630,002–38,640,001) holds the all-time record: **8.2M transactions in 10,000 ledgers** — an average of **820 tx/ledger**, which is 2.5x the index-level average and 3.3x the index-level average for Index 4.

| Rank | Chunk ID | Ledger Span | Tx Count | Avg Tx/Ledger |
|------|----------|-------------|----------|---------------|
| 1 | 3863 | 38,630,002 – 38,640,001 | 8,202,758 | 820.3 |
| 2 | 3862 | 38,620,002 – 38,630,001 | 7,107,270 | 710.7 |
| 3 | 3864 | 38,640,002 – 38,650,001 | 7,072,716 | 707.3 |
| 4 | 3869 | 38,690,002 – 38,700,001 | 6,919,792 | 692.0 |
| 5 | 3865 | 38,650,002 – 38,660,001 | 6,837,652 | 683.8 |
| 6 | 3849 | 38,490,002 – 38,500,001 | 6,809,205 | 680.9 |
| 7 | 3847 | 38,470,002 – 38,480,001 | 6,727,168 | 672.7 |
| 8 | 3844 | 38,440,002 – 38,450,001 | 6,615,158 | 661.5 |
| 9 | 3866 | 38,660,002 – 38,670,001 | 6,610,352 | 661.0 |
| 10 | 3341 | 33,410,002 – 33,420,001 | 6,600,555 | 660.1 |

Nine of the top 10 densest chunks fall in the 38.4M–38.7M ledger band (Index 3). This likely corresponds to a period of extremely high network activity in mid-2024. The lone outlier (chunk 3341, ledgers 33.4M) sits in the early part of Index 3.

---

## Pipeline Concurrency Timeline

The semaphore pattern (`parallel_indexes=2`) means at most 2 indexes ingest simultaneously. When an index finishes ingestion, it releases its slot so the next queued index can start ingesting. RecSplit runs outside the semaphore — it does not hold a slot while building indexes.

Each row below shows one index's lifecycle. Times are wall clock (UTC).

```
Index 0  22:02 ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 22:38 ░ 22:39                                         INGEST 36m | RECSPLIT <1s
Index 1  22:02 ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 22:38 ░░ 22:39                                        INGEST 36m | RECSPLIT 1s
Index 2        22:38 ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 23:38 ░░░░░░░░░ 23:47               INGEST 59m | RECSPLIT 10m
Index 3        22:38 ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 01:21 ░░░░░░░░░░░░░░░░░░░░░░░ 02:16    INGEST 2h43m | RECSPLIT 55m
Index 4              23:38 ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 03:19 ░░░░░░░░░░░░░░░ 03:59    INGEST 3h41m | RECSPLIT 41m
Index 5                    01:21 ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ 04:48 ░░░░░░░ 05:07    INGEST 3h27m | RECSPLIT 19m

▓ = ingesting    ░ = RecSplit
```

Note how ingestion slots hand off: Index 0 finishes at 22:38, Index 2 starts at 22:38. Index 3 finishes at 01:21, Index 5 starts at 01:21. The semaphore release is near-instant (<1 second).

### State Transition — Semaphore Release in Action

When Index 0 finishes ingestion, its semaphore slot is released *before* RecSplit starts. Index 2 immediately begins ingesting. This transition happens in a single second:

```
[22:38:36] [INDEX:0000] Ingestion complete — transitioning to RECSPLIT_BUILDING
[22:38:36] [INDEX:0002] Starting index 2
[22:38:36] [INDEX:0002] Starting ingestion: 20 BSB instances, 50 chunks each
```

Four seconds later, Index 1 does the same:

```
[22:38:40] [INDEX:0001] Ingestion complete — transitioning to RECSPLIT_BUILDING
[22:38:40] [INDEX:0003] Starting index 3
[22:38:40] [INDEX:0003] Starting ingestion: 20 BSB instances, 50 chunks each
```

---

## Progress Ticker Examples

The 1-minute progress ticker shows all 6 indexes at all times. Here are representative snapshots showing different pipeline states.

### Early run — 2 indexes ingesting, 4 queued

```
── Progress (1m elapsed) ────────────────────────────
  Index 0000 [INGESTING]: 22/1,000 chunks (2.2%) — ETA 44m 27s
    4,637 ledgers/s | 4 tx/s | 27.8 chunks/min
  Index 0001 [INGESTING]: 21/1,000 chunks (2.1%) — ETA 46m 39s
    3,443 ledgers/s | 7,024 tx/s | 20.7 chunks/min
  Index 0002 [QUEUED]
  Index 0003 [QUEUED]
  Index 0004 [QUEUED]
  Index 0005 [QUEUED]
  Memory: 31.76 GB RSS (peak 31.76 GB) | Go heap: 25.62 GB alloc
──────────────────────────────────────────────────────
```

### Mid-run — COMPLETE + RECSPLIT + INGESTING + QUEUED all visible

This snapshot at 1h 36m shows the full diversity of pipeline states. Index 2 is in the RecSplit Add phase (100 workers distributing keys across 16 indexes), Index 3 is 77% through ingestion at 438K tx/s, Index 4 just started loading BSB buffers, and Index 5 is still queued.

```
── Progress (1h 36m elapsed) ────────────────────────────
  Index 0000 [COMPLETE]
  Index 0001 [COMPLETE]
  Index 0002 [RECSPLIT:ADDING]: 100 workers, 16 indexes
  Index 0003 [INGESTING]: 766/1,000 chunks (76.6%) — ETA 18m 22s
    2,123 ledgers/s | 437,982 tx/s | 12.7 chunks/min
    LFS p50=307ms p90=699ms — BSB p50=2.7ms p90=7.8ms
  Index 0004 [INGESTING]: 0/1,000 chunks (0.0%) — ETA N/A
    0 ledgers/s | 0 tx/s | 0.0 chunks/min
  Index 0005 [QUEUED]
  Memory: 46.85 GB RSS (peak 46.85 GB) | Go heap: 27.36 GB alloc | 1108 goroutines
──────────────────────────────────────────────────────
```

---

## Latency Percentiles

### Sample Set

These percentiles come from the pipeline's built-in `LatencyStats` collector, which records every operation across the entire session. The sample sizes are:

| Operation | Measured Per | Sample Count |
|-----------|-------------|-------------|
| LFS write | Chunk (one write per 10K-ledger chunk) | 6,000 |
| TxHash write | Chunk (one write per chunk) | 6,000 |
| BSB GetLedger | Ledger (one call per ledger) | 60,000,000 |
| Chunk fsync | Chunk (one fsync per chunk) | 6,000 |

These are **session-wide** percentiles — they aggregate across all 6 indexes, all 6,000 chunks, and all 60M ledgers. They are not sampled or estimated; every operation is recorded. The final values are reported in the completion summary at the end of the run.

### What Each Metric Measures

| Metric | What It Measures |
|--------|-----------------|
| **LFS write** | Time to write one 10K-ledger chunk's worth of zstd-compressed `LedgerCloseMeta` to an LFS `.data` file. Includes zstd compression and the `write()` syscall. Varies widely by chunk density — a chunk with 8M transactions produces a much larger compressed payload than one with 500 transactions. |
| **TxHash write** | Time to write one chunk's raw transaction hashes (36 bytes/entry) to a `.bin` file. Proportional to transaction count in the chunk. |
| **BSB GetLedger** | Time for a single `GetLedger()` call to return one `LedgerCloseMeta` from the BSB in-memory buffer. Includes buffer wait time, GCS fetch (on cache miss), and XDR deserialization. |
| **Chunk fsync** | Time to `fsync()` a completed chunk's files (both the LFS `.data` file and the TxHash `.bin` file). |

### Percentile Values

| Operation | p50 | p90 | p95 | p99 |
|-----------|-----|-----|-----|-----|
| LFS write | 172ms | 1.14s | 2.21s | 13.89s |
| TxHash write | 63ms | 414ms | 639ms | 1.61s |
| BSB GetLedger | 3.0ms | 12.4ms | 24.4ms | 79.0ms |
| Chunk fsync | 11.2ms | 55.4ms | 69.9ms | 154ms |

The wide spread in LFS write latency (p50=172ms vs p99=13.9s) reflects the spread of chunk densities — early-index chunks with few transactions compress quickly, while the densest chunks (8M+ transactions) require significant CPU time for zstd compression.

---

## Memory Profile

The values below are observed ranges from the 1-minute progress ticker memory reports across the full 7-hour run. Each ticker line reports three values: **RSS** (resident set size from the OS), **Go Heap Alloc** (`runtime.MemStats.HeapAlloc` — live heap bytes in use), and **Go Heap Sys** (`runtime.MemStats.HeapSys` — total virtual address space requested from the OS by Go's allocator). The peak RSS of 178 GB corresponds to a Go Heap Sys of 172.8 GB at the same point in the run.

| Phase | Typical RSS | Go Heap Alloc | Go Heap Sys | Goroutines |
|-------|-------------|---------------|-------------|------------|
| 2 indexes ingesting | 30–47 GB | 25–45 GB | 7–63 GB | 1,100–1,700 |
| 1 index ingesting + 1 RecSplit | 30–44 GB | 20–39 GB | 63–166 GB | 650–1,000 |
| RecSplit Build (solo) | 3–8 GB | 1–7 GB | 172 GB | 21–64 |
| Peak (Index 5 RecSplit Build) | 178 GB | 61 GB | 172.8 GB | 21 |
| Final (idle) | 1.93 GB | 1.93 GB | 172.8 GB | 3 |

---

## Key Takeaways

1. **7 hours for all of Stellar history.** 60M ledgers, 9.14B transactions, 4.35 TB of immutable data. One machine, one process, zero restarts.

2. **Transaction density grew 6,729x** from genesis to the 40-50M ledger era. Index 0 (genesis) has 475K total transactions; Index 4 has 3.2 billion. The pipeline handles both extremes without configuration changes.

3. **RecSplit is fast when it runs alone.** Index 5's RecSplit (2.9B keys) took 19 minutes vs Index 3's 55 minutes (2.5B keys) because Index 5 ran without competing ingestion. CPU contention from concurrent ingestion is the main RecSplit bottleneck.

4. **The semaphore pattern works.** Indexes begin ingesting the instant a slot opens — the handoff takes <1 second. RecSplit runs outside the semaphore, so index building never blocks ingestion of the next index.

5. **The densest period in Stellar history** is the 38.4M–38.7M ledger band (mid-2024), with chunks exceeding 8M transactions each (820 tx/ledger). This is 2.5x the index-level average for Index 3.

6. **93.9% of all transactions are in the last 30M ledgers.** Indexes 0–1 (the first 20M ledgers) contain 0.5% of the 9.14B total. The pipeline spent 72 minutes on those two indexes and 6+ hours on the remaining four.

7. **RecSplit indexes are ~15% the size of raw data.** The 16 per-CF perfect hash indexes achieve a consistent 5.2 bytes/key, reducing 306 GB of raw `.bin` files (36 bytes/entry) to 47 GB of index. The raw files are deleted after indexing — net disk savings of 259 GB.

8. **Peak RSS was 178 GB.** Go Heap Sys reached 172.8 GB and stayed there for the remainder of the run (Go does not release virtual address space back to the OS). During ingestion, heap alloc was typically 25–45 GB. The 178 GB RSS spike occurred during Index 5's RecSplit Build phase.
