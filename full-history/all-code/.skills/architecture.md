# Architecture Reference

## Two Pipelines

| Dimension | Backfill | Streaming |
|-----------|----------|-----------|
| Backend | BSB (BufferedStorageBackend) or CaptiveStellarCore | CaptiveStellarCore only |
| Active store | None — writes to LFS + raw txhash flat files | Two RocksDB per index |
| Queries | Not served | getLedgerBySequence, getTransactionByHash |
| RecSplit trigger | All 1,000 chunks done | 10M-ledger index boundary |
| Lifecycle | Exits when indexes complete | Long-running daemon |
| Flush interval | ~100 ledgers (write buffer cap) | Per-ledger checkpoint |

BSB and CaptiveStellarCore are mutually exclusive.

## Constants & Formulas

```
FirstLedger=2  IndexSize=10M  ChunkSize=10K  ChunksPerIndex=1000

ledgerToIndexID(seq)  = (seq - 2) / 10_000_000
indexFirstLedger(N)   = (N * 10_000_000) + 2
indexLastLedger(N)    = ((N+1) * 10_000_000) + 1
ledgerToChunkID(seq)  = (seq - 2) / 10_000
chunkFirstLedger(C)   = (C * 10_000) + 2
chunkLastLedger(C)    = ((C+1) * 10_000) + 1
IndexID(chunkID)      = chunkID / ChunksPerIndex
```

| Index | First Ledger | Last Ledger | Chunk IDs |
|-------|-------------|------------|-----------|
| 0 | 2 | 10,000,001 | 0-999 |
| 1 | 10,000,002 | 20,000,001 | 1000-1999 |
| 2 | 20,000,002 | 30,000,001 | 2000-2999 |

### Chunk Boundaries (both ends inclusive)

Each chunk contains exactly 10,000 ledgers. First and last ledger are both **inclusive**.

**Index 0** (chunks 0-999, ledgers 2-10,000,001):

| Chunk | First Ledger | Last Ledger | File |
|-------|-------------|------------|------|
| 0 | 2 | 10,001 | `chunks/0000/000000.data` |
| 1 | 10,002 | 20,001 | `chunks/0000/000001.data` |
| 998 | 9,980,002 | 9,990,001 | `chunks/0000/000998.data` |
| 999 | 9,990,002 | 10,000,001 | `chunks/0000/000999.data` |

**Index 5** (chunks 5000-5999, ledgers 50,000,002-60,000,001):

| Chunk | First Ledger | Last Ledger | File |
|-------|-------------|------------|------|
| 5000 | 50,000,002 | 50,010,001 | `chunks/0005/005000.data` |
| 5001 | 50,010,002 | 50,020,001 | `chunks/0005/005001.data` |
| 5998 | 59,980,002 | 59,990,001 | `chunks/0005/005998.data` |
| 5999 | 59,990,002 | 60,000,001 | `chunks/0005/005999.data` |

No gaps: chunk 4999 ends at 50,000,001, chunk 5000 starts at 50,000,002.

## Directory Structure

```
{data_dir}/
├── meta/rocksdb/                           <- Meta store (WAL never disabled)
├── active/
│   ├── ledger-store-chunk-{chunkID:06d}/   <- Streaming only
│   └── txhash-store-index-{indexID:04d}/   <- Streaming only; 16 CFs
└── immutable/
    ├── ledgers/chunks/{XXXX}/{YYYYYY}.data <- LFS chunk files (+ .index)
    └── txhash/{indexID:04d}/
        ├── raw/{chunkID:06d}.bin           <- Backfill only; deleted after RecSplit
        └── index/cf-{nibble}.idx           <- RecSplit index, 16 per index
```

## Meta Store Keys

| Key | Value |
|-----|-------|
| `chunk:{C:06d}:lfs` | "1" |
| `chunk:{C:06d}:txhash` | "1" (backfill only) |
| `index:{N:04d}:txhashindex` | "1" |
| `streaming:last_committed_ledger` | uint32BE(ledgerSeq) |

## State Machines

- **Backfill**: State is key-derived — chunk completion inferred from `chunk:{C:06d}:lfs` + `chunk:{C:06d}:txhash` flags; index completion inferred from `index:{N:04d}:txhashindex`. No stored INGESTING/RECSPLIT_BUILDING/COMPLETE state values.
- **Streaming**: ACTIVE -> TRANSITIONING -> COMPLETE (inferred from presence of active/transitioning RocksDB stores and meta flags)
- **RecSplit sub-state**: derived from `index:{N:04d}:txhashindex` flag

## Crash Recovery Flags

Written AFTER fsync, never before. Permanent once set.

- `chunk:{C:06d}:lfs` — LFS .data+.index fsynced (both modes)
- `chunk:{C:06d}:txhash` — .bin fsynced (backfill only)
- `index:{N:04d}:txhashindex` — RecSplit index built for this index (written after all 16 CFs built + fsynced)

### Backfill resume (per-chunk)

Skip if BOTH flags are "1". Any other combination = full rewrite of both files. There is NO partial-rewrite path.

| chunk:{C:06d}:lfs | chunk:{C:06d}:txhash | Action |
|-------------------|----------------------|--------|
| "1" | "1" | Skip |
| any other combination | | Delete both files, full rewrite from scratch |

### RecSplit crash recovery (backfill vs streaming)

**Backfill**: All-or-nothing. When `index:{N:04d}:txhashindex` is absent, delete all `.idx` files in `index/` dir and `tmp/` dir, then rerun the entire 4-phase pipeline (Count → Add → Build → Verify) from scratch. `index:{N:04d}:txhashindex` is set only after all 16 CFs build and fsync successfully.

**Streaming**: Per-CF granularity. When the transitioning txhash store is present but `index:{N:04d}:txhashindex` is absent, scan individual CF progress and rebuild only incomplete CFs from the transitioning txhash RocksDB store. Per-CF tracking drives the recovery decision here because streaming builds sequentially from RocksDB (slower).

## Design Docs (authoritative)

- `01-architecture-overview.md` — two-pipeline overview
- `02-meta-store-design.md` — all meta keys and state enums
- `03-backfill-workflow.md` — backfill ingestion pipeline (incl. RecSplit build)
- `04-streaming-and-transition.md` — streaming ingestion loop + active RocksDB → LFS + RecSplit transition
- `07-crash-recovery.md` — all crash scenarios
- `09-directory-structure.md` — full on-disk file tree
- `11-checkpointing-and-transitions.md` — boundary math
