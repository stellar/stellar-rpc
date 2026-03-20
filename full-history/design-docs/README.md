# Stellar Full History RPC Service — Design Docs

> **Status**: In review (PR #617)
> **Scope**: Backfill pipeline design. Streaming pipeline design will follow in a separate PR.

---

## Document Index

| # | Document | What It Covers |
|---|----------|----------------|
| 01 | [01-architecture-overview.md](./01-architecture-overview.md) | Two-pipeline architecture, store types, high-level data flow |
| 03 | [03-backfill-workflow.md](./03-backfill-workflow.md) | **Complete backfill reference** — DAG task graph, meta store keys, directory structure, configuration, crash recovery, getStatus API |

`03-backfill-workflow.md` is self-contained. A reviewer can read it top-to-bottom to understand the entire backfill pipeline without needing any other document.

---

## Glossary

| Term | Definition |
|------|-----------|
| **Index** | 10M-ledger grouping (contains 1,000 Chunks). The RecSplit build boundary — one set of 16 index files per Index. `index_id = chunk_id / chunks_per_txhash_index`. |
| **Chunk** | 10K-ledger unit (1,000 per Index). The atomic crash recovery and file I/O boundary — each chunk produces one LFS file and one raw txhash file. |
| **LFS (Ledger File Store)** | Immutable chunk files storing zstd-compressed `LedgerCloseMeta` records. Path: `immutable/ledgers/chunks/XXXX/YYYYYY.data`. |
| **RecSplit** | Minimal perfect hash (MPH) index for txhash → ledger lookups. 16 index files per Index (one per CF, sharded by `txhash[0] >> 4`). |
| **Raw TxHash Flat File** | Intermediate backfill-only file: 36 bytes per entry (`hash[32] + seq[4]`). Consumed by RecSplit builder, then deleted. |
| **Meta Store** | Single RocksDB instance (WAL always enabled) tracking chunk flags and index completion keys. Source of truth for crash recovery. |
| **BSB (BufferedStorageBackend)** | GCS-backed ledger source. Each `process_chunk` task creates its own GCS connection via `BSBFactory`. |
| **Worker Pool** | Flat pool of `workers` goroutines (default 40) that execute DAG tasks. |
| **CF (Column Family)** | One of 16 RecSplit index files, sharded by the first hex character (nibble) of the txhash. |
| **WAL (Write-Ahead Log)** | RocksDB durability mechanism. Always enabled for the meta store. |

---

## What Changed from v1

### The Fundamental Break

In v1, backfill and streaming were structurally similar pipelines — both wrote through RocksDB as an intermediate store. The v2 redesign breaks this symmetry. **Backfill writes directly to immutable formats (LFS + flat files) without RocksDB active stores.** This eliminates WAL, compaction, and write amplification overhead for a workload that is purely sequential with no concurrent reads.

### Backfill Pipeline (v2)

- **No RocksDB during ingestion.** Ledgers → LFS chunk files. TxHashes → raw flat files. No WAL, no compaction.
- **Flat worker pool with DAG scheduling.** A single pool of `workers` goroutines (default 40) processes `process_chunk` and `build_txhash_index` tasks as dependencies are satisfied.
- **RecSplit index build is a separate async step.** After all 1,000 chunks complete, RecSplit runs concurrently with the next index's ingestion.
- **Crash recovery is chunk-atomic.** A chunk is complete when both meta store flags are set after fsync. No partial reuse.

### Streaming Pipeline (v2) — Future PR

The streaming pipeline retains RocksDB active stores (necessary for concurrent reads during live ingestion). Its design will be documented in a separate PR.
