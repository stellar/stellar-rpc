# Stellar Full History RPC Service — Design Docs

## Quick Context

The Stellar Full History RPC Service ingests the complete blockchain history and serves queries:

- `getLedger` — retrieve any ledger from history
- `getTransaction` — retrieve any transaction from history
- `getEvents` — retrieve events with filter matching from history

Two mutually exclusive modes:

- **Backfill** — offline bulk import
  - Writes directly to immutable files (LFS pack files + RecSplit indexes + events cold segments)
  - No RocksDB active stores, no queries during ingestion
  - DAG-scheduled with a flat worker pool
  - Exits when done
- **Streaming** (default) — real-time ingestion via CaptiveStellarCore
  - Writes to RocksDB active stores + events hot segment
  - Serves queries concurrently with ingestion
  - Transitions completed data to immutable storage in background
  - Long-running daemon
- Backfill typically runs first to populate historical data
- Streaming picks up where backfill left off

## Documents

| Doc | Title | Status |
|-----|-------|--------|
| [events](../../design-docs/getevents-full-history-design.md) | getEvents Full-History Design | Complete |
| [01](./01-backfill-workflow.md) | Backfill Workflow | Complete |
| [02](./02-streaming-workflow.md) | Streaming Workflow | Complete |
| 03 | Query Routing | **Not started** |
| 04 | Operator Guide | **Not started** |

**What each doc covers:**

- **Events** — hot/cold segments, roaring bitmap indexes, MPHF, freeze process, query path
- **01 Backfill** — geometry, directory layout, meta store keys, config, DAG tasks, execution model, crash recovery
- **02 Streaming** — startup, ingestion loop, three sub-flow transitions, crash recovery invariants, backfill-to-streaming migration
- **03 Query Routing** — routing `getLedger`/`getTransaction`/`getEvents` to correct store during active/transitioning/complete phases
- **04 Operator Guide** — end-to-end setup, hardware sizing, monitoring, troubleshooting

**What's folded into existing docs (no separate doc needed):**

- Architecture overview → split across 01 overview, 02 overview, this README
- Meta store keys → defined inline in 01 and 02
- Directory structure → inline in 01
- Configuration → inline in 01 and 02
- Checkpointing math → inline in 01, referenced by 02
- Crash recovery → inline in 01 (backfill) and 02 (streaming invariants)

## Reading Order

- Read events doc first — standalone, no prerequisites
- Read 01 (backfill) second — defines all shared concepts: geometry, meta store keys, directory layout, flag-after-fsync invariant
- Read 02 (streaming) third — assumes familiarity with 01

## Shared Concepts

Defined in the backfill doc, used by all documents:

- **Chunk** — 10_000 ledgers, atomic unit of ingestion and file I/O
- **Index** — `chunks_per_txhash_index` chunks (default 1_000 = 10_000_000 ledgers), unit of RecSplit build
- **Meta store** — single RocksDB instance, source of truth for crash recovery
  - `chunk:{C:08d}:lfs` — ledger pack file complete
  - `chunk:{C:08d}:txhash` — raw txhash `.bin` file complete (backfill only)
  - `chunk:{C:08d}:events` — events cold segment complete
  - `index:{N:08d}:txhash` — RecSplit index complete
  - `streaming:last_committed_ledger` — per-ledger checkpoint (streaming only)
  - `config:chunks_per_txhash_index` — immutable after first run
- **Flag-after-fsync** — meta store flags set only after durable file writes, core crash recovery invariant for both modes
