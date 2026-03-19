# Meta Store Design

## Overview

The meta store is a single RocksDB instance that tracks completion state for both backfill and streaming modes. It is the authoritative source for: which chunks have been written, which indexes have been built, and the streaming checkpoint ledger for crash recovery.

The meta store does **not** track a global operating mode. Mode is determined by startup flags (`--mode backfill` vs `--mode streaming`), not meta store contents.

**WAL invariant**: The meta store RocksDB instance **always has WAL enabled**. This is non-negotiable. All writes to the meta store — chunk flags (`lfs`, `txhash`), index completion keys (`txhashindex`), and the streaming checkpoint key — are only considered durable after the WAL entry for that write has been fsynced to disk. A flag is never treated as set unless the WAL write succeeded. Disabling WAL for the meta store would invalidate the entire crash recovery model: flags that appear set in the RocksDB MemTable but not yet in WAL would be lost on a crash, causing chunks to be silently skipped on resume despite being only partially written.

---

## Key Schema

All keys are plain strings. Values are `"1"` (presence = done, absence = not done) unless otherwise noted. The `"1"` value is retained for ldb/sst_dump readability; key presence is the signal.

There are exactly three key types for backfill/index work, plus one streaming checkpoint key:

| Key Pattern | Value | Written By | Written When |
|-------------|-------|-----------|-------------|
| `chunk:{C:06d}:lfs` | `"1"` | BSB instance (backfill) or ledger sub-flow (streaming, at chunk boundaries) | After LFS `.data` + `.index` files for chunk C are fsynced to disk |
| `chunk:{C:06d}:txhash` | `"1"` | BSB instance (backfill only) | After txhash `.bin` flat file for chunk C is fsynced to disk |
| `index:{N:04d}:txhashindex` | `"1"` | RecSplit builder goroutine | After all CF index files for index N are built and fsynced to disk |
| `streaming:last_committed_ledger` | `uint32` big-endian 4 bytes | Streaming ingest loop | After every ledger is committed to the active RocksDB store and its WAL entry is synced |

### Derived relationship: index_id

```
index_id = chunk_id / chunks_per_index
```

This is derived arithmetic, not stored in the meta store. For the default configuration (`chunks_per_index = 1000`), index 0 covers chunks 0--999, index 1 covers chunks 1000--1999, etc.

---

## Chunk Keys

Two keys per chunk. Written independently after each respective file fsync.

**`chunk:{C}:txhash` is backfill-only.** In streaming mode, txhash data goes directly into the txhash store (RocksDB, 16 CFs) — no raw flat files are produced, and `chunk:{C}:txhash` is never set for streaming chunks.

**`chunk:{C}:lfs` in streaming mode**: at each chunk boundary (every 10K ledgers), the ledger sub-flow transitions the active ledger store: the old store moves to `transitioningLedgerStore`, a background goroutine flushes its 10K ledgers to LFS chunk files, fsyncs, sets the `lfs` flag, then closes and deletes the transitioning store via `CompleteLedgerTransition`. By the time the index boundary is reached, **all** `lfs` flags for that index's chunks are set.

**Constraints**:
- Absent means either not started or incomplete — treated identically on resume (full rewrite of both files).
- `lfs` and `txhash` are written independently; however, a chunk is only skippable on resume (backfill) when **both** flags are `"1"`. If either flag is absent, both files are rewritten from scratch.
- For streaming, only `lfs` is written; a streaming chunk is LFS-complete when `chunk:{C}:lfs = "1"`.
- Flags are **never deleted or reset** once set to `"1"`.

**Examples** (backfill, index 0 = chunks 000000--000999):
```
chunk:000000:lfs     →  "1"    ← chunk 0 LFS done
chunk:000000:txhash  →  "1"    ← chunk 0 txhash done
chunk:000001:lfs     →  "1"
chunk:000001:txhash  →  absent ← partial
chunk:000999:lfs     →  "1"
chunk:000999:txhash  →  "1"    ← chunk 999 (last of index 0) done
```

**Examples** (streaming — no txhash keys):
```
chunk:001000:lfs  →  "1"   ← first chunk of index 1, flushed at chunk boundary
chunk:001234:lfs  →  "1"   ← mid-index, set at chunk 1234's boundary
chunk:001235:lfs  →  absent ← not yet transitioned (current chunk still filling)
  (no txhash keys exist for streaming chunks)
```

> **getEvents placeholder**: A future `chunk:{C:06d}:events` flag will be added here when `getEvents` immutable store support is designed. See [getEvents Immutable Store — Placeholder](#getevents-immutable-store--placeholder).

---

## Index Key

One key per index. Written after all CF index files for that index are built and fsynced.

The index key replaces the old per-CF tracking (16 `recsplit:cf:XX:done` keys + `recsplit:state`). A single `index:{N}:txhashindex` key signals that the entire RecSplit build for index N is complete. There is no partial-CF tracking — the RecSplit build is all-or-nothing per index.

**Examples**:
```
index:0000:txhashindex  →  "1"    ← index 0 RecSplit build complete
index:0001:txhashindex  →  absent ← index 1 not yet built
```

**Constraints**:
- Written only after **all** CF index files are fsynced. If the process crashes mid-build, the key is absent and the entire build is rerun from scratch on resume.
- Never deleted once set.

---

## Streaming Checkpoint

One global key, written on every ledger commit.

| Key Pattern | Value Type | Written By | Written When |
|-------------|-----------|-----------|-------------|
| `streaming:last_committed_ledger` | `uint32` big-endian 4 bytes | Streaming ingest loop | After every ledger is committed to the active RocksDB store and its WAL entry is synced |

**Constraints**:
- Never deleted; only overwritten.
- On crash, resume from `last_committed_ledger + 1`.
- If absent (process never committed a single ledger), resume from `startLedger` per config.

**Example**:
```
streaming:last_committed_ledger  →  [0x00, 0x98, 0x96, 0x81]   ← ledger 10,000,001
```

---

## Durability Guarantees

| Operation | Durability Basis |
|-----------|-----------------|
| `chunk:{C}:lfs = "1"` | Written to meta store with WAL; only written **after** the LFS `.data` + `.index` files are fsynced to disk |
| `chunk:{C}:txhash = "1"` | Written to meta store with WAL; only written **after** the txhash `.bin` file is fsynced to disk |
| `index:{N}:txhashindex = "1"` | Written to meta store with WAL; only written **after** all CF index files are fsynced |
| `streaming:last_committed_ledger` | Written to meta store with WAL after the ledger's RocksDB WriteBatch (also WAL-backed) succeeds |

**Consequence**: Any crash between a file fsync and the subsequent meta store WAL write leaves the file on disk with no flag set. On resume, the absent flag triggers a full rewrite of that chunk (or full rebuild of that index). The file may be partial and is truncated/overwritten. This is safe and correct by design.

---

## Startup Triage

On startup, the orchestrator derives the state of each index from key presence alone — there is no stored state machine. For each index N (where `chunks_per_index = 1000`):

| Condition | Derived State | Action |
|-----------|--------------|--------|
| `index:{N}:txhashindex` present | **COMPLETE** | Skip entirely |
| All chunks in index have `lfs` + `txhash` keys, `index:{N}:txhashindex` absent | **BUILD_READY** | Trigger `build_txhash_index` |
| Some chunks missing `lfs` or `txhash` key | **INGESTING** | Process missing chunks, then trigger build |
| No chunk keys exist for any chunk in index | **NEW** | Start fresh ingestion |

The scan covers all `chunks_per_index` chunk flag pairs for the index. Completed chunks form non-contiguous islands (20 BSB instances run concurrently, each making independent progress). The scan never stops at the first gap — it examines every chunk to build the complete skip set.

**Streaming mode triage** uses only `lfs` keys (no `txhash`):

| Condition | Derived State | Action |
|-----------|--------------|--------|
| `index:{N}:txhashindex` present | **COMPLETE** | Skip entirely |
| All chunks in index have `lfs` key, `index:{N}:txhashindex` absent | **BUILD_READY** | Trigger RecSplit build from transitioning txhash store |
| Some chunks missing `lfs` key | **ACTIVE** | Continue streaming (chunks complete at boundaries) |
| No chunk keys exist for any chunk in index | **NEW** | Start fresh |

---

## ID Formulas

```go
const (
    FirstLedger    = 2
    IndexSize      = 10_000_000   // ledgers per index
    ChunkSize      = 10_000       // ledgers per chunk
    ChunksPerIndex = IndexSize / ChunkSize  // 1000
)

func ledgerToIndexID(ledgerSeq uint32) uint32 {
    return (ledgerSeq - FirstLedger) / IndexSize
}

func indexFirstLedger(indexID uint32) uint32 {
    return (indexID * IndexSize) + FirstLedger
}

func indexLastLedger(indexID uint32) uint32 {
    return ((indexID + 1) * IndexSize) + FirstLedger - 1
}

func ledgerToChunkID(ledgerSeq uint32) uint32 {
    return (ledgerSeq - FirstLedger) / ChunkSize
}

func chunkFirstLedger(chunkID uint32) uint32 {
    return (chunkID * ChunkSize) + FirstLedger
}

func chunkLastLedger(chunkID uint32) uint32 {
    return ((chunkID + 1) * ChunkSize) + FirstLedger - 1
}

func chunkToIndexID(chunkID uint32) uint32 {
    return chunkID / ChunksPerIndex
}
```

**Quick reference**:

| IndexID | First Ledger | Last Ledger | Chunks |
|---------|-------------|------------|--------|
| 0 | 2 | 10,000,001 | 0--999 |
| 1 | 10,000,002 | 20,000,001 | 1000--1999 |
| 2 | 20,000,002 | 30,000,001 | 2000--2999 |
| N | (N x 10M)+2 | ((N+1) x 10M)+1 | N x 1000 -- (N x 1000)+999 |

---

## Design Decisions

**No `global:mode` key**: Mode is determined by startup flags. The meta store never needs to know the current mode.

**Flat keys, no range prefix**: Chunk keys use a global chunk ID (`chunk:{C}:lfs`) with no range/index prefix. The index relationship is derived arithmetically (`index_id = chunk_id / chunks_per_index`). This avoids redundant hierarchy in the key namespace and makes prefix scans straightforward — `chunk:` lists all chunk state, `index:` lists all index state.

**Per-chunk flags (not per-BSB-instance flags)**: All 20 BSB instances run concurrently within an index. Each instance owns a slice of chunks, but instances make independent progress and crash independently. At crash time, completed chunks form non-contiguous islands. Per-chunk flags handle every possible completion pattern and enable selective skip on resume. The resume rule scans all `chunks_per_index` chunk flag pairs — not just up to the first incomplete one — because gaps are expected and normal.

**Single index key (not per-CF)**: The old schema tracked 16 per-CF done keys plus a `recsplit:state` key (18 keys per range for RecSplit). The new schema uses a single `index:{N}:txhashindex` key. The RecSplit build is all-or-nothing: if the process crashes mid-build, all partial index files are deleted and the build reruns from scratch. Per-CF tracking added complexity without recovery value — backfill already used all-or-nothing recovery, and the incremental-CF-resume path for streaming was never exercised.

**No stored state machine**: The old schema stored `range:{N}:state` with explicit states (INGESTING, RECSPLIT_BUILDING, COMPLETE). The new schema derives state from key presence at startup (see [Startup Triage](#startup-triage)). This eliminates a class of bugs where the stored state and actual key state diverge (e.g., all chunks done but state still says INGESTING due to a crash between the last chunk flag write and the state transition write).

**Chunk flags are never deleted**: Once `lfs` or `txhash` is set to `"1"`, it is permanent. This enables idempotent resumption.

**Checkpoint data is never deleted**: `streaming:last_committed_ledger` is only overwritten, never deleted. It represents the last safely durable ledger for streaming crash recovery.

---

## getEvents Immutable Store — Placeholder

> **Status**: Not yet designed. This section reserves space for future work.

When `getEvents` support is added, it will require:

- A new per-chunk completion flag alongside `lfs` and `txhash`:
  ```
  chunk:{C:06d}:events
  ```
  Value: `"1"` when the events index for this chunk is fsynced; absent otherwise.

- A new index-level events completion key:
  ```
  index:{N:04d}:eventsindex
  ```

- Extension of the chunk skip rule: a chunk is only skippable on resume when **all** flags are set (`lfs = "1"` AND `txhash = "1"` AND `events = "1"`). Until then, the incomplete flag's sub-workflow is redone from scratch.

- Same fsync-before-flag invariant as existing flags: events data must be durably written before `events` is set.

The existing key schema has been designed with this extension in mind. No schema migration to existing keys is expected.

---

## Related Documents

- [03-backfill-workflow.md](./03-backfill-workflow.md) — how the meta store is written during backfill
- [04-streaming-and-transition.md](./04-streaming-and-transition.md) — how `streaming:last_committed_ledger` is updated, ACTIVE to COMPLETE transition
- [07-crash-recovery.md](./07-crash-recovery.md) — how all state keys are used for recovery
- [08-query-routing.md](./08-query-routing.md) — how index state drives query dispatch
