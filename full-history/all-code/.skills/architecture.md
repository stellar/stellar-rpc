# Architecture Reference

## Two Pipelines

| Dimension | Backfill | Streaming |
|-----------|----------|-----------|
| Backend | BSB (BufferedStorageBackend) or CaptiveStellarCore | CaptiveStellarCore only |
| Active store | None — writes to LFS + raw txhash flat files | Two RocksDB per range |
| Queries | Not served | getLedgerBySequence, getTransactionByHash |
| RecSplit trigger | All 1,000 chunks done | 10M-ledger range boundary |
| Lifecycle | Exits when ranges complete | Long-running daemon |
| Flush interval | ~100 ledgers (write buffer cap) | Per-ledger checkpoint |

BSB and CaptiveStellarCore are mutually exclusive.

## Constants & Formulas

```
FirstLedger=2  RangeSize=10M  ChunkSize=10K  ChunksPerRange=1000

ledgerToRangeID(seq)  = (seq - 2) / 10_000_000
rangeFirstLedger(N)   = (N * 10_000_000) + 2
rangeLastLedger(N)    = ((N+1) * 10_000_000) + 1
ledgerToChunkID(seq)  = (seq - 2) / 10_000
chunkFirstLedger(C)   = (C * 10_000) + 2
chunkLastLedger(C)    = ((C+1) * 10_000) + 1
chunkToRangeID(C)     = C / 1000
```

| Range | First Ledger | Last Ledger | Chunk IDs |
|-------|-------------|------------|-----------|
| 0 | 2 | 10,000,001 | 0-999 |
| 1 | 10,000,002 | 20,000,001 | 1000-1999 |
| 2 | 20,000,002 | 30,000,001 | 2000-2999 |

### Chunk Boundaries (both ends inclusive)

Each chunk contains exactly 10,000 ledgers. First and last ledger are both **inclusive**.

**Range 0** (chunks 0-999, ledgers 2-10,000,001):

| Chunk | First Ledger | Last Ledger | File |
|-------|-------------|------------|------|
| 0 | 2 | 10,001 | `chunks/0000/000000.data` |
| 1 | 10,002 | 20,001 | `chunks/0000/000001.data` |
| 998 | 9,980,002 | 9,990,001 | `chunks/0000/000998.data` |
| 999 | 9,990,002 | 10,000,001 | `chunks/0000/000999.data` |

**Range 5** (chunks 5000-5999, ledgers 50,000,002-60,000,001):

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
│   └── txhash-store-range-{rangeID:04d}/   <- Streaming only; 16 CFs
└── immutable/
    ├── ledgers/chunks/{XXXX}/{YYYYYY}.data <- LFS chunk files (+ .index)
    └── txhash/{rangeID:04d}/
        ├── raw/{chunkID:06d}.bin           <- Backfill only; deleted after RecSplit
        └── index/cf-{nibble}.idx           <- RecSplit index, 16 per range
```

## Meta Store Keys

| Key | Value |
|-----|-------|
| `range:{N:04d}:state` | INGESTING / RECSPLIT_BUILDING / ACTIVE / TRANSITIONING / COMPLETE |
| `range:{N:04d}:recsplit:state` | BUILDING / COMPLETE |
| `range:{N:04d}:recsplit:cf:{XX:02d}:done` | "1" |
| `range:{N:04d}:chunk:{C:06d}:lfs_done` | "1" |
| `range:{N:04d}:chunk:{C:06d}:txhash_done` | "1" (backfill only) |
| `streaming:last_committed_ledger` | uint32BE(ledgerSeq) |

## State Machines

- **Backfill**: INGESTING -> RECSPLIT_BUILDING -> COMPLETE
- **Streaming**: ACTIVE -> TRANSITIONING -> COMPLETE
- **RecSplit sub-state**: PENDING -> BUILDING -> COMPLETE

## Crash Recovery Flags

Written AFTER fsync, never before. Permanent once set.

- `lfs_done` — LFS .data+.index fsynced (both modes)
- `txhash_done` — .bin fsynced (backfill only)
- `recsplit:cf:XX:done` — per-CF RecSplit index built

### Backfill resume (per-chunk)

Skip if BOTH flags are "1". Any other combination = full rewrite of both files. There is NO partial-rewrite path.

| lfs_done | txhash_done | Action |
|----------|-------------|--------|
| "1" | "1" | Skip |
| any other combination | | Delete both files, full rewrite from scratch |

## Design Docs (authoritative)

- `01-architecture-overview.md` — two-pipeline overview
- `02-meta-store-design.md` — all meta keys and state enums
- `03-backfill-workflow.md` — backfill ingestion pipeline
- `05-backfill-transition-workflow.md` — RecSplit build after ingestion
- `07-crash-recovery.md` — all crash scenarios
- `09-directory-structure.md` — full on-disk file tree
- `11-checkpointing-and-transitions.md` — boundary math
