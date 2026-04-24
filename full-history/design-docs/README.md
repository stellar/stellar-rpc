# Stellar Full History RPC Service — Design Docs

## Documents

| Doc | Scope |
|-----|-------|
| [01-backfill-workflow.md](./01-backfill-workflow.md) | Backfill subroutine internals — DAG, per-chunk tasks, shared TOML config, meta-store key schema, crash recovery |
| [02-streaming-workflow.md](./02-streaming-workflow.md) | Unified daemon end-to-end — startup phases, live ingestion, freeze transitions, pruning, query contract |

## Reading Order

- Read **01 Backfill** first. It defines shared concepts used by both docs: geometry, meta-store key schema, shared TOML config, flag-after-fsync.
- Read **02 Streaming** second. It builds on 01's vocabulary and describes how the daemon invokes backfill as its Phase 1 (catchup) subroutine.

## See Also

- [packfile library design](../../design-docs/packfile-library.md) — binary format for immutable `.pack` files (ledger packs + events cold segments); consumed by both docs above.
- [getEvents full-history design](../../design-docs/getevents-full-history-design.md) — events hot/cold segment layout, roaring bitmap indexes, MPHF; consumed by both docs above.
