# Stellar Full History RPC Service — Design Docs

> **Scope**: Backfill pipeline only. Streaming pipeline design follows in a separate PR.

## Documents

| Document | Description |
|----------|-------------|
| [03-backfill-workflow.md](./03-backfill-workflow.md) | Complete backfill design — geometry, meta store keys, directory layout, configuration, DAG task graph, execution model, crash recovery, getStatus API |

The backfill doc is self-contained. Read it top-to-bottom for the full picture.

## Quick Context

The Stellar Full History RPC Service ingests the complete blockchain and serves `getLedger` and `getTransaction` queries. It has two modes:

- **Backfill** (this PR) — offline bulk import. Writes directly to immutable files (LFS chunks + RecSplit indexes). No RocksDB, no queries during ingestion. DAG-scheduled with a flat worker pool.
- **Streaming** (future PR) — real-time ingestion via CaptiveStellarCore. Writes to RocksDB active stores, serves queries, transitions to immutable storage at index boundaries.

These modes are fully independent — separate code, separate crash recovery, separate transition workflows.
