# Directory Structure

## Overview

The service organizes all data under a single configurable `data_dir`. Backfill mode writes directly to `immutable/` and never creates active stores. Streaming mode uses `<active_stores_base_dir>/` for the live range stores and `immutable/` for completed ranges. Meta store is always present under `meta/`. No `transitioning/` directory exists — transition state is tracked entirely in the meta store.

---

## Full File Tree

The tree below shows a **streaming snapshot at the range 5→6 boundary**: range 5 is `TRANSITIONING` (only the transitioning txhash store remains open for queries — all ledger stores were individually transitioned and deleted at their chunk boundaries during ACTIVE — while the background RecSplit build goroutine converts the txhash store to immutable), and range 6 is the newly `ACTIVE` range. Ranges 0–4 are `COMPLETE` and shown only by their immutable artifacts.

```
{data_dir}/
│
├── meta/
│   └── rocksdb/                              ← Single meta store RocksDB instance
│       ├── MANIFEST-*
│       ├── *.sst
│       ├── *.log                             ← WAL (required — never disable)
│       └── OPTIONS-*
│
├── active/                                   ← [active_stores].base_path (default: {data_dir}/active)
│   │                                           STREAMING MODE ONLY
│   │
│   │   ── TRANSITIONING: range 5 txhash store (open read-only; RecSplit being built) ──
│   │   (no ledger store — all ledger stores for range 5 were deleted at their chunk boundaries)
│   ├── txhash-store-index-0005/              ← TxHash store for index 5 (10M-ledger index);
│   │   ├── MANIFEST-*                          stays open until RecSplit build completes
│   │   ├── *.sst                               and RemoveTransitioningTxHashStore is called
│   │   ├── *.log
│   │   └── OPTIONS-*
│   │
│   │   ── ACTIVE: range 6 stores (being written to right now) ──
│   ├── ledger-store-chunk-006000/            ← First ledger store of range 6 (chunk 6000,
│   │   ├── MANIFEST-*                          ledgers 60,000,002–60,010,001)
│   │   ├── *.sst
│   │   ├── *.log
│   │   └── OPTIONS-*
│   └── txhash-store-index-0006/              ← TxHash store for index 6; 16 CFs (nibble 0–f)
│       ├── MANIFEST-*
│       ├── *.sst
│       ├── *.log
│       └── OPTIONS-*
│
└── immutable/
    ├── ledgers/
    │   └── chunks/
    │       ├── 0000/                         ← range 0: chunks 0–999
    │       │   ├── 000000.data               ← chunk 0:    ledgers 2–10,001
    │       │   ├── 000000.index
    │       │   ├── ...
    │       │   ├── 000999.data               ← chunk 999:  ledgers 9,990,002–10,000,001
    │       │   └── 000999.index
    │       ├── 0001/                         ← range 1: chunks 1000–1999
    │       │   ├── 001000.data               ← chunk 1000: ledgers 10,000,002–10,010,001
    │       │   ├── 001000.index
    │       │   ├── ...
    │       │   ├── 001999.data               ← chunk 1999: ledgers 19,990,002–20,000,001
    │       │   └── 001999.index
    │       ├── 0002/                         ← range 2: chunks 2000–2999
    │       │   └── ...
    │       ├── 0003/                         ← range 3: chunks 3000–3999
    │       │   └── ...
    │       ├── 0004/                         ← range 4: chunks 4000–4999
    │       │   └── ...
    │       └── 0005/                         ← range 5: chunks 5000–5999 (written at each chunk boundary during ACTIVE by per-chunk ledger transition goroutines)
    │           ├── 005000.data               ← chunk 5000: ledgers 50,000,002–50,010,001
    │           ├── 005000.index
    │           ├── ...
    │           ├── 005999.data               ← chunk 5999: ledgers 59,990,002–60,000,001
    │           └── 005999.index
    │
    └── txhash/
        ├── 0000/                             ← index 0 (COMPLETE: no raw/, only index/)
        │   └── index/
        │       ├── cf-0.idx
        │       ├── ...
        │       └── cf-f.idx
        ├── 0001/                             ← index 1 (COMPLETE)
        │   └── index/
        │       ├── cf-0.idx
        │       ├── ...
        │       └── cf-f.idx
        ├── 0002/                             ← index 2 (COMPLETE)
        │   └── index/ ...
        ├── 0003/                             ← index 3 (COMPLETE)
        │   └── index/ ...
        ├── 0004/                             ← index 4 (COMPLETE)
        │   └── index/ ...
        └── 0005/                             ← index 5 (TRANSITIONING → built by goroutine; may be partial)
            └── index/
                ├── cf-0.idx                  ← RecSplit CF 0; present once that CF's build completes
                ├── ...
                └── cf-f.idx
```

**Notes**:
- `active/` is created when streaming mode starts. At most **3 RocksDB stores** can exist simultaneously at a range boundary: the TRANSITIONING txhash store (range N — ledger stores already deleted at chunk boundaries) plus the two ACTIVE stores (range N+1's ledger-store + txhash-store). Briefly during a chunk boundary within the ACTIVE phase, a 4th store may exist: the transitioning ledger store (being flushed to LFS). Each sub-flow has at most 1 active + 1 transitioning store at any time.
- The TRANSITIONING txhash-store-index-NNNN is deleted in-place once RecSplit build and verification pass via `RemoveTransitioningTxHashStore` — it is never moved or renamed. Ledger stores are deleted at each chunk boundary via `CompleteLedgerTransition` after the LFS flush completes.
- `immutable/txhash/{indexID:04d}/raw/` exists **only during backfill ingestion** (state `INGESTING` or `RECSPLIT_BUILDING`). It is deleted immediately after all 16 RecSplit CFs for that index are built and verified. A COMPLETE index has no `raw/` directory — only `index/`.
- **Streaming mode never creates `raw/`**. RecSplit is built directly from the active txhash store (reading each of its 16 CFs by nibble); no flat files are written to disk.
- **Directory creation**: All immutable parent directories (`immutable/ledgers/chunks/{XXXX}/`, `immutable/txhash/{XXXX}/raw/`, `immutable/txhash/{XXXX}/index/`) are created on-demand via `os.MkdirAll` (equivalent to `mkdir -p`) before the first file write to that path. This is safe for concurrent BSB instances writing to different chunk files within the same index directory — `MkdirAll` is a no-op if the directory already exists and does not race with concurrent calls for the same path. Active store directories (`ledger-store-chunk-*`, `txhash-store-index-*`) are created automatically by RocksDB when the store is opened.

---

## LFS Chunk Path Convention

```
immutable/ledgers/chunks/{XXXX}/{YYYYYY}.data
immutable/ledgers/chunks/{XXXX}/{YYYYYY}.index
```

Where:
- `XXXX` = `chunkID / 1000` (4-digit zero-padded) — groups 1000 chunks per index directory
- `YYYYYY` = `chunkID` (6-digit zero-padded)

### Path Formulas

```go
func chunkDir(dataDir string, chunkID uint32) string {
    return filepath.Join(dataDir, "immutable", "ledgers", "chunks",
        fmt.Sprintf("%04d", chunkID/1000))
}

func chunkDataPath(dataDir string, chunkID uint32) string {
    return filepath.Join(chunkDir(dataDir, chunkID), fmt.Sprintf("%06d.data", chunkID))
}

func chunkIndexPath(dataDir string, chunkID uint32) string {
    return filepath.Join(chunkDir(dataDir, chunkID), fmt.Sprintf("%06d.index", chunkID))
}
```

### Chunk ID Examples

| Ledger Seq | Chunk ID | Dir | Data Path |
|-----------|----------|-----|-----------|
| 2 | 0 | `chunks/0000/` | `chunks/0000/000000.data` |
| 10,001 | 0 | `chunks/0000/` | `chunks/0000/000000.data` |
| 10,002 | 1 | `chunks/0000/` | `chunks/0000/000001.data` |
| 9,990,002 | 999 | `chunks/0000/` | `chunks/0000/000999.data` |
| 10,000,001 | 999 | `chunks/0000/` | `chunks/0000/000999.data` |
| 10,000,002 | 1000 | `chunks/0001/` | `chunks/0001/001000.data` |
| 20,000,002 | 2000 | `chunks/0002/` | `chunks/0002/002000.data` |
| 50,000,002 | 5000 | `chunks/0005/` | `chunks/0005/005000.data` |

---

## Raw TxHash Flat File Path Convention

> **Backfill mode only.** Raw txhash flat files are never created during streaming ingestion. They exist only while an index is in state `INGESTING` or `RECSPLIT_BUILDING` and are deleted immediately after all 16 RecSplit CFs for that index are built and verified. An index in state `COMPLETE` has no `raw/` directory.

```
immutable/txhash/{indexID:04d}/raw/{chunkID:06d}.bin
```

**Format**: Fixed-width, no header. Each entry is 36 bytes:

```
[txhash: 32 bytes][ledgerSeq: 4 bytes big-endian uint32]
```

Files are append-only during ingestion (flushed every ~100 ledgers), fsynced at chunk completion, and deleted once all 16 RecSplit CFs for the index are built and verified.

### Path Formulas

```go
func rawTxHashPath(dataDir string, indexID, chunkID uint32) string {
    return filepath.Join(dataDir, "immutable", "txhash",
        fmt.Sprintf("%04d", indexID), "raw",
        fmt.Sprintf("%06d.bin", chunkID))
}
```

### Raw TxHash File Examples

| Index ID | Chunk ID | Path |
|---------|----------|------|
| 0 | 0 | `immutable/txhash/0000/raw/000000.bin` |
| 0 | 999 | `immutable/txhash/0000/raw/000999.bin` |
| 1 | 1000 | `immutable/txhash/0001/raw/001000.bin` |
| 1 | 1999 | `immutable/txhash/0001/raw/001999.bin` |
| 5 | 5000 | `immutable/txhash/0005/raw/005000.bin` |
| 5 | 5999 | `immutable/txhash/0005/raw/005999.bin` |

---

## RecSplit Index Path Convention

```
immutable/txhash/{indexID:04d}/index/cf-{nibble}.idx
```

Where `nibble` is the first hex character of the txhash: `0`–`9`, `a`–`f` (16 files per index).

### Path Formulas

```go
func recSplitPath(dataDir string, indexID uint32, nibble string) string {
    return filepath.Join(dataDir, "immutable", "txhash",
        fmt.Sprintf("%04d", indexID), "index",
        fmt.Sprintf("cf-%s.idx", nibble))
}
```

### RecSplit Index Examples

| Index ID | Nibble | Path |
|---------|--------|------|
| 0 | 0 | `immutable/txhash/0000/index/cf-0.idx` |
| 0 | a | `immutable/txhash/0000/index/cf-a.idx` |
| 0 | f | `immutable/txhash/0000/index/cf-f.idx` |
| 1 | 0 | `immutable/txhash/0001/index/cf-0.idx` |
| 5 | f | `immutable/txhash/0005/index/cf-f.idx` |

---

## Active Store Path Convention (Streaming Mode Only)

```
<active_stores_base_dir>/ledger-store-chunk-{chunkID:06d}/   ← ledger store (default CF only)
<active_stores_base_dir>/txhash-store-index-{indexID:04d}/   ← txhash store (16 CFs, one per nibble 0–f)
```

**Two separate RocksDB instances per active index:**
- **Ledger store** (`ledger-store-chunk-{chunkID:06d}/`): default CF only. `key = uint32BE(ledgerSeq)`, `value = zstd(LedgerCloseMeta)`. One instance per 10K-ledger chunk; transitions at every chunk boundary (active → transitioning → LFS flush → close + delete via `CompleteLedgerTransition`; max 1 active + 1 transitioning per sub-flow).
- **TxHash store** (`txhash-store-index-{indexID:04d}/`): 16 column families, one per first hex character of the txhash (`0`–`f`). CF routing: first hex char of the 64-char hash string (equivalently `txhash[0] >> 4` on raw bytes). `key = txhash[32]`, `value = uint32BE(ledgerSeq)`. One instance per 10M-ledger index; transitions at every index boundary (active → transitioning via `PromoteToTransitioning` → RecSplit build → close + delete via `RemoveTransitioningTxHashStore`; max 1 active + 1 transitioning).

At most one active range exists at a time. During streaming transition (TRANSITIONING state), only the transitioning txhash store remains open for queries — all ledger stores have already been deleted at their chunk boundaries. Ledger queries are served from LFS. The transitioning txhash store is deleted after RecSplit build and verification complete.

---

## Meta Store Path

```
meta/rocksdb/
```

Single RocksDB instance. WAL must never be disabled. Present in both backfill and streaming modes.

---

## Multi-Disk Configuration

Path overrides via TOML config allow each subtree to live on a different volume:

```
meta/rocksdb/                  → [meta_store].path               (default: {data_dir}/meta/rocksdb)
<active_stores_base_dir>/      → [active_stores].base_path        (default: {data_dir}/active)
                                 (ledger-store-chunk-{chunkID:06d}/ and txhash-store-index-{indexID:04d}/ live under this base)
immutable/ledgers/             → [immutable_stores].ledgers_base  (default: {data_dir}/immutable/ledgers)
immutable/txhash/              → [immutable_stores].txhash_base   (default: {data_dir}/immutable/txhash)
```

**Typical layout**:

| Volume | Store | Rationale |
|--------|-------|-----------|
| SSD | `meta/rocksdb/` + `<active_stores_base_dir>/` | Low-latency write path; meta store requires fast random I/O |
| SSD | `immutable/ledgers/` + `immutable/txhash/` | Large sequential writes during backfill; query reads at range boundaries |

---

## Storage Estimates

See [12-metrics-and-sizing.md](./12-metrics-and-sizing.md#storage-estimates) for per-range storage estimates across all store types.

---

## Related Documents

- [01-architecture-overview.md](./01-architecture-overview.md) — store types and their roles
- [03-backfill-workflow.md](./03-backfill-workflow.md) — which files are created during backfill
- [04-streaming-workflow.md](./04-streaming-workflow.md) — active store lifecycle
- [10-configuration.md](./10-configuration.md) — TOML path overrides
- [11-checkpointing-and-transitions.md](./11-checkpointing-and-transitions.md) — chunk boundary math
- [12-metrics-and-sizing.md](./12-metrics-and-sizing.md) — storage estimates, memory budgets, hardware requirements
