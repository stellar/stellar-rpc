# getEvents: Full-History Design

## Summary

The Stellar network has emitted over 21 billion events across more than 60 million ledgers and continues to grow. The existing `getEvents` RPC was designed for a limited retention window and does not scale to full-history queries. This document proposes a purpose-built backend that stores and indexes the complete event history and serves filtered queries with low, predictable latency.

## How to Read This Document

This document is organized in four parts:

**Part 1: Problem and Scope** (Sections 1–3) covers the objective, event structure, and query model. Skip if you're already familiar with `getEvents`.

**Part 2: Architecture** (Sections 4–6) explains how the system is structured: its main components and how data flows through them.

**Part 3: Implementation Reference** (Sections 7–14) covers event IDs, the bitmap index, active and cold chunk storage, the freeze process, crash recovery, and backfill.

**Part 4: Performance and Scaling** will be added in a future revision with benchmarks, operational profiles, and scaling analysis.

---

# Part 1: Problem and Scope

## 1. Objective

Design a storage and indexing layer that lets RPC nodes serve filtered event queries across the full Stellar history.

Target characteristics:

* **p99 latency ≤ 500ms**
* Filtering by **contract ID and up to four topic fields**
* Boolean filter combinations
* Compatible with the proposed [getEvents v2 API](https://github.com/orgs/stellar/discussions/1872) (bidirectional ordering, stateless cursors)

Populating a new RPC node with full history (e.g., via snapshot distribution) is not covered here. The backfill write path for building cold segments from historical data is covered in Section 14.

## 2. Event Structure

Each stored event corresponds to a [ContractEvent XDR](https://github.com/stellar/go-stellar-sdk/blob/main/xdr/Stellar-ledger.x#L371-L390) emitted:

| Field | Indexed | Notes |
| :---- | :---- | :---- |
| contractId | Yes | Which contract emitted it (StrKey "C..." address) |
| topic0–3 | Yes | 1–4 typed labels categorizing the event (ScVal encoded). Additional topics beyond 4 are stored but not indexed. |
| type | No | `contract`, `system`, or `diagnostic` |
| value | No | The event payload (e.g., amount transferred) |

* The indexed fields (`contractId`, `topic0`–`topic3`) are the ones callers search on.
* There are three event types (`contract`, `system`, and `diagnostic`), but diagnostic events are debug-only and not stored, and system events make up 0.000025% of the total. So for practical purposes, almost every event is a contract event.
* Topics are encoded as ScVal, a binary format used across Stellar contracts. Topics are indexed as opaque byte sequences without decoding.

During ingestion, the system attaches metadata from the surrounding ledger context:

| Field | Notes |
| :---- | :---- |
| txHash | Transaction hash |
| transactionIndex | Position of the transaction within the ledger |
| operationIndex | Position of the operation within the transaction |
| ledger | Ledger sequence number |
| ledgerClosedAt | Ledger close timestamp |
| id | Computed event ID |

These metadata fields are stored with each event rather than derived during query execution.

## 3. Query Model

A query specifies:

* a **start ledger** and optionally an **end ledger**
* **Filters** on contract ID and up to 4 topic fields
* A result **limit** (up to 1,000 events)
* An **ordering** direction (ascending or descending by event ID)

If no end ledger is provided, the system internally caps the search range at 10,000 ledgers.

Only the first four topics are indexed to keep the index size bounded.

Filters can be combined using AND, OR, and combinations of both. The exact query syntax is defined in [this](https://github.com/orgs/stellar/discussions/1872) document. Internally, each indexed value maps to a set of matching event IDs and query evaluation is boolean operations over these sets.

---

# Part 2: Architecture

The architecture relies on the following key properties:

* **Range-bounded queries.** Every query specifies a ledger range, capped at 10,000 ledgers.
* **Append-only data.** Events are never updated or deleted once written.
* **Single writer per chunk.** During live ingestion, a single writer handles the active chunk; readers operate concurrently. During backfill, multiple workers can build separate cold chunks in parallel.

The system manages two types of data:

* **Events:** the raw event payloads (contract ID, topics, value) along with metadata for each event (ledger number, tx hash, timestamps).
* **Index:** a reverse mapping from query terms to matching events. Five fields are indexed: `contractId`, `topic0`, `topic1`, `topic2`, and `topic3`. For each unique (field, value) pair seen in the data, the index stores a [roaring bitmap](https://roaringbitmap.org/) of the event IDs that contain it. This allows answering "which events match this filter?" without scanning every event.

## 4. Chunks

The core organizational unit is the **chunk**, covering 10,000 consecutive ledgers — the same chunk granularity used by the ledger sub-flow. At current rates each chunk holds approximately 10 million events, representing about 15 hours of data.

With approximately 6,000 chunks in the current history, the chunk count increases daily. Since the query range cap is also 10,000 ledgers, any single query touches at most 2 chunks.

## 5. Active and Cold Chunks

Events are organized into two types of chunks based on their mutability:

* **Active chunk:** The single, currently mutable chunk receiving all new events. Event data lives in a RocksDB store; the bitmap index lives in memory for fast mutation and querying.
* **Cold chunks:** Immutable, with both events and index stored in optimized on-disk formats.

When the active chunk reaches its boundary (every 10K ledgers):

* A new active chunk immediately begins accepting writes.
* The previous active chunk transitions to cold via a background freeze goroutine.
* The previous chunk continues serving queries during the freeze.

This is the same transition pattern used by the ledger sub-flow: swap the active store, flush to immutable storage in the background, set the completion flag, close and delete the old store.

## 6. Data Flow

### Writing (Ingestion)

A single writer processes ledgers one at a time. For each ledger:

1. Assign sequential event IDs within the chunk
2. Write events, ledger offsets, and index deltas to RocksDB in a single atomic WriteBatch
3. Update the in-memory bitmap index
4. Advance the watermark, making new events visible to readers

### Reading (Queries)

1. For each chunk the query spans, load the bitmaps for all query terms
2. Combine bitmaps with AND/OR according to the filters
3. Iterate matching event IDs up to the limit
4. Fetch the actual events for those IDs
5. Post-filter events to verify all query terms match

---

# Part 3: Implementation Reference

## 7. Event IDs

Every event in a chunk is assigned a sequential ID from 0 to total\_events - 1. These are internal IDs used for indexing within a chunk.

A ledger offset array tracks the running total of events through each ledger:

```
offsets[0] = 1,042       (ledger 0: 1,042 events, IDs 0–1,041)
offsets[1] = 2,029       (ledger 1: 987 events, IDs 1,042–2,028)
offsets[2] = 4,529       (ledger 2: 2,500 events, IDs 2,029–4,528)
...
```

This array is directly indexable by ledger number in O(1).

Sequential IDs keep event IDs within the 32-bit range required for standard roaring bitmaps and produce densely packed containers. Alternative schemes like TOID (64-bit) or ledger-prefixed IDs produce sparse ID spaces that inflate bitmap sizes. The tradeoff is that sequential IDs require the ledger offset array to translate between ledger numbers and event ID ranges.

## 8. Bitmap Index

For each unique value seen in an indexed field, the system stores a roaring bitmap of matching event IDs. Each such (field, value) pair is called a term:

```
(contractId, 0x3a1f...)  → {0, 4, 17, 203, 8741200, ...}
(topic0, 0x7b2c...)      → {0, 1, 5, 12, 13, 14, 98, 102, ...}
```

Each term is identified by a 17-byte key: a 1-byte field type (contractId=0, topic0=1, topic1=2, topic2=3, topic3=4) followed by a 16-byte xxhash of its value bytes. The field type prefix ensures uniqueness across fields.

Roaring bitmaps are used because term density varies widely across the data. Most terms match very few events while common terms like "transfer" can match millions. Roaring bitmaps handle this efficiently across the full range: array containers for sparse terms (where a full bitmap would be mostly empty) and compressed bitmap containers for dense terms (where a sorted array would be too large).

### 8.1 Why Bitmaps Live in Memory

During ingestion, every ledger requires adding new event IDs to the relevant bitmaps (~4,000 adds per ledger assuming ~1,000 events with ~4 indexed fields each).

If bitmaps lived on disk or in the embedded DB, each add would require deserializing the bitmap, updating it, and re-serializing it back. For dense terms like "transfer" that can grow to millions of entries, this serialize/deserialize cycle on every ledger would be far too slow.

Keeping bitmaps in memory makes each add a simple `bitmap.Add` call with no I/O or serialization overhead. Queries also benefit since bitmap intersections are done directly on the in-memory structures.

Changes are persisted as per-ledger deltas in the active store (see Section 9.1), allowing the in-memory index to be reconstructed during crash recovery.

### 8.2 Bitmap Memory Footprint

The in-memory bitmap index is the primary memory cost of the events system. Here is a worst-case estimate for a single chunk (10K ledgers, ~10M events).

**Term count**: A term is a unique `(field, value)` pair across the 5 indexed fields (contractId, topic0–topic3). Estimated 200K–500K unique terms per chunk.

**Roaring bitmap sizing**: Roaring bitmaps partition the 32-bit ID space into chunks of 65,536 IDs. With 10M events, ~153 chunks are used. Each chunk is stored as an array container (2 bytes × entries, when < 4,096 entries) or a bitmap container (8KB fixed, when >= 4,096 entries).

| Term Category | Count | Size per Term | Subtotal |
|--------------|-------|--------------|----------|
| Dense (>1M matches, e.g., `transfer`) | ~20 | ~1.2MB (153 bitmap containers) | ~24MB |
| Medium (1K–1M matches) | ~2,000 | ~100KB avg | ~200MB |
| Sparse (<1K matches) | ~498,000 | ~500 bytes avg | ~250MB |
| Map overhead (17-byte key + pointer) | 500K entries | ~40 bytes | ~20MB |
| **Total (worst case)** | | | **~500MB** |

Realistic estimate: **100–300MB per chunk**.

During the freeze window, the old chunk's bitmaps are held for reads while the new chunk starts accumulating. Peak memory: ~2x single chunk. **Worst case peak: ~1GB. Realistic peak: 200–600MB.**

This footprint is the same regardless of active store implementation — bitmaps are in memory either way.

## 9. Active Chunk (Hot Path)

The active events store is a RocksDB instance, following the same pattern as the ledger and txhash sub-flows. One instance per chunk (10K ledgers), with three column families.

Path: `<active_stores_base_dir>/events-store-chunk-{chunkID:06d}/`

WAL: **always enabled** (never `DisableWAL`).

### 9.1 Column Families

#### CF `events` — Event Data

| Key | Value | Notes |
|-----|-------|-------|
| `uint32BE(eventID)` | Event XDR bytes + metadata (contractId, topics, value, txHash, transactionIndex, operationIndex, ledger, ledgerClosedAt, id) | Sequential eventIDs within the chunk, starting from 0 |

EventIDs are sequential integers assigned during ingestion (`0, 1, 2, ...`). The `uint32BE` encoding ensures lexicographic key order matches eventID order, enabling efficient range scans.

#### CF `offsets` — Ledger Offset Array

| Key | Value | Notes |
|-----|-------|-------|
| `uint32BE(ledgerSeq)` | `uint32BE(cumulativeEventCount)` | Running total of events through this ledger. `offsets[L] - offsets[L-1]` = number of events in ledger L. |

Used to translate between ledger numbers and eventID ranges during queries. Also kept in memory as an array for O(1) lookup during query evaluation.

#### CF `deltas` — Index Delta Log

| Key | Value | Notes |
|-----|-------|-------|
| `uint32BE(ledgerSeq)` | Serialized batch of `(term_key[17], event_id[4])` pairs | One entry per ledger. Each pair is a 17-byte term key hash + 4-byte eventID. |

The deltas CF is **only read during crash recovery / startup** to rebuild the in-memory bitmap index. It is not accessed during normal query serving.

#### Column Family Summary

| CF | Key | Value | Size per key | Read during queries? | Read during recovery? |
|----|-----|-------|-------------|---------------------|----------------------|
| `events` | `uint32BE(eventID)` | Event XDR + metadata | Variable (~200–500 bytes typical) | Yes — event fetch by ID | No |
| `offsets` | `uint32BE(ledgerSeq)` | `uint32BE(cumulativeEventCount)` | 4 bytes | No (in-memory copy used) | Yes — rebuild offset array |
| `deltas` | `uint32BE(ledgerSeq)` | Serialized `(term_key, event_id)` batch | Variable (~4KB per ledger typical, assuming ~1000 events × ~4 terms × 21 bytes) | No | Yes — rebuild bitmaps |

### 9.2 In-Memory State

Two data structures live entirely in memory during ingestion and query serving:

**Bitmap Index** — A concurrent map: `term_key[17] → *roaring.Bitmap`

- **Populated during ingestion**: for each event, `bitmap.Add(eventID)` for each of its indexed fields (contractId + up to 4 topics).
- **Populated during startup/recovery**: by replaying the deltas CF (see Section 12).
- **Used during queries**: bitmap AND/OR operations to find matching eventIDs.
- **Concurrency**: copy-on-write semantics. Readers get a consistent snapshot without blocking the writer.
- **Lifetime**: from store open until freeze completes and cold chunk is queryable.

**Ledger Offset Array** — An in-memory array: `[]uint32`, indexed by `(ledgerSeq - chunkFirstLedger)`.

- **Populated during ingestion**: append cumulative event count after each ledger.
- **Populated during startup/recovery**: by scanning the offsets CF.
- **Used during queries**: translate the query's ledger range into an eventID range for bitmap iteration.

### 9.3 Per-Ledger Write Path

```
For each incoming ledger:

1. Assign event IDs:
     start_id = next_event_id
     next_event_id += len(events)

2. Build WriteBatch across all three CFs:

     CF "events":
       for each event in ledger:
         Put(uint32BE(event_id), eventXDRBytes)

     CF "offsets":
       Put(uint32BE(ledgerSeq), uint32BE(next_event_id))    // cumulative count

     CF "deltas":
       Put(uint32BE(ledgerSeq), serializeDeltaBatch(events)) // (term_key, event_id) pairs

3. WriteBatch.Write()
     ← single WAL fsync
     ← atomic across all three CFs: either all writes land or none do

4. Update in-memory state:
     for each event:
       for each indexed field (contractId, topic0..topic3):
         bitmap[termKey(field, value)].Add(event_id)
     ledgerOffsets = append(ledgerOffsets, next_event_id)

5. Advance in-memory watermark:
     lastVisibleLedger = ledgerSeq
     (readers will now include this ledger's events in query results)
```

The per-ledger checkpoint remains `streaming:last_committed_ledger` in the meta store — written after the ledger's WriteBatches to the ledger store, txhash store, and events store all succeed. This maintains the existing checkpoint invariant: `streaming:last_committed_ledger` is never advanced past a ledger that isn't durable in all three stores.

```
Per ledger (streaming ingestion loop):
  1. WriteBatch to active ledger store (WAL)
  2. WriteBatch to active txhash store (WAL)
  3. WriteBatch to active events store (WAL)
  4. Update streaming:last_committed_ledger in meta store
       ← ONLY after all three WriteBatches succeed
```

## 10. Cold Chunk

### 10.1 Cold Event Storage

Events are grouped into fixed-size blocks, which are then compressed with zstd.

| File | Description |
| :---- | :---- |
| `events.pack` | Compressed event blocks with offset index and ledger offset array embedded in the packfile |

The offset index and ledger offset array are embedded in the packfile's app data and loaded asynchronously on chunk open, allowing parallel I/O with other chunk files opened during the same request. See the [packfile library](https://github.com/tamirms/event-analysis/blob/main/packfile-library.md) design for details.

### 10.2 Cold Index Storage

Bitmaps are serialized to disk in a single index file (`index.pack`) covering all indexed fields. To look up a bitmap by term key, a [Minimal Perfect Hash Function](https://en.wikipedia.org/wiki/Perfect_hash_function#Minimal_perfect_hash_function) (MPHF) stored in `index.hash` maps the term key to its position in `index.pack`, implemented using [streamhash](https://github.com/tamirms/streamhash).

An MPHF maps each known key to a unique slot in [0, N) with O(1) lookup and no collisions, making it a compact and efficient key-to-position mapping. The MPHF file is small enough to load in a single IOP at the start of a request and is loaded asynchronously.

| File | Description |
| :---- | :---- |
| `index.hash` | MPHF mapping term keys to slot positions in `index.pack` |
| `index.pack` | Serialized roaring bitmaps, one per term, each prefixed with a 4-byte fingerprint |

Since an MPHF maps any input to a valid slot — even keys not in the build set — a query for a non-existent term would resolve to a slot and retrieve whatever bitmap is stored there. Each bitmap record in `index.pack` is therefore prefixed with a 4-byte fingerprint to detect and reject these false positives.

A 4-byte fingerprint can still collide, so query results are post-filtered after event fetch to verify all terms match (see Section 11.3).

**Term Lookup:**

1. Hash the term key and query the MPHF in `index.hash` to obtain the slot index.
2. Read the record at that slot in `index.pack`.
3. If the fingerprint matches the hash prefix, deserialize the bitmap; otherwise the term is not present.

### 10.3 Cold Chunk On-Disk Layout

```
immutable/events/{chunkID:06d}/
├── events.pack         ← compressed event blocks + embedded offset index + ledger offset array
├── index.hash          ← MPHF mapping term keys to slots
└── index.pack          ← serialized roaring bitmaps, one per term, fingerprint-prefixed
```

## 11. Query Path

The system first identifies which chunks overlap the query's ledger range. Since each chunk covers 10,000 ledgers and the query range is capped at 10,000 ledgers, a query spans at most 2 chunks when the range crosses a chunk boundary. `streaming:last_committed_ledger` is read to limit the query to events from ledgers that have been fully processed and committed. Results from each chunk are merged and returned up to the limit.

### 11.1 Query Routing

For each chunk the query touches, the router determines the data source:

| Chunk State | Event data source | Bitmap source |
|-------------|------------------|---------------|
| Active (current chunk being ingested) | Active events RocksDB store | In-memory bitmap index |
| Transitioning (freeze in progress) | Transitioning events RocksDB store (still open for reads) | In-memory bitmap index (snapshot held until freeze completes) |
| Cold (`events_done` set, store deleted) | Cold files: `events.pack` | Cold files: `index.hash` + `index.pack` |

This follows the same routing pattern used by the ledger sub-flow (active ledger store vs. transitioning ledger store vs. LFS).

### 11.2 Active Chunk Read Path

```
1. Compute eventID range for the query's ledger range:
     start_event_id = ledgerOffsets[startLedger - chunkFirstLedger - 1]  (or 0 if first ledger)
     end_event_id   = ledgerOffsets[endLedger - chunkFirstLedger]

2. Look up bitmaps for all query terms from the in-memory concurrent map:
     contractId_bitmap = bitmaps[termKey(CONTRACT_ID, contractIdBytes)]
     topic0_bitmap     = bitmaps[termKey(TOPIC_0, topic0Bytes)]
     (bitmaps use copy-on-write — readers get a consistent snapshot)

3. Combine bitmaps per filter logic:
     result_bitmap = contractId_bitmap AND topic0_bitmap
     (for OR filters: result_bitmap = bitmap_A OR bitmap_B)

4. Iterate matching eventIDs within [start_event_id, end_event_id):
     iterator = result_bitmap.Iterator()
     advance iterator to start_event_id
     collected = []
     while iterator.HasNext() && iterator.Current() < end_event_id && len(collected) < limit:
       event_id = iterator.Next()

5. Fetch events from RocksDB:
       event_bytes = eventsStore.Get(CF "events", uint32BE(event_id))
       event = deserialize(event_bytes)

6. Post-filter:
       verify that the deserialized event actually matches all query terms
       (defense against bitmap false positives from hash collisions in term keys)
       if matches: collected = append(collected, event)

7. Return collected events (up to limit)
```

For active chunks (~10M events at most), the RocksDB block cache keeps the working set in memory. The Get is effectively a memory lookup. The dominant cost in the active query path is bitmap operations (steps 2–3) and post-filtering (step 6 — XDR deserialization to verify term matches). The event fetch step is a small fraction of total query time.

### 11.3 Cold Chunk Read Path

```
1. Load bitmaps for all query terms:
     Hash the term key and query the MPHF in index.hash to get a slot.
     Read the record at that slot in index.pack.
     Check the 4-byte fingerprint. If it matches, deserialize the bitmap.
     Otherwise the term has no matches and can be skipped.

2. Combine bitmaps with AND/OR according to the filters.

3. Iterate matching event IDs in order up to the remaining limit,
   using the event ID range derived from the ledger offset array
   embedded in events.pack to skip IDs outside the requested ledger range.

4. Fetch events by ID:
     block_index = event_id / block_size
     position    = event_id % block_size
     Decompress the block from events.pack.
     Extract the event at the computed position.

5. Post-filter to verify all terms match.
   The 4-byte fingerprint check in step 1 can still produce false positives,
   so post-filtering is necessary to ensure correctness.
```

## 12. Startup and Recovery

Both graceful shutdown and crash recovery follow the same startup sequence. The only difference is whether the RocksDB store opens cleanly or requires WAL replay — this is transparent to the application.

### 12.1 Startup Sequence

```
On startup:

1. Open meta store
     → read streaming:last_committed_ledger (e.g., 6,000,778)
     → read range:0000:state (e.g., "ACTIVE")
     → determine current chunk: chunkID = ledgerToChunkID(6,000,778)  (e.g., chunk 600)

2. Check for incomplete freeze (transitioning events store on disk):
     if events-store-chunk-{prevChunkID} directory exists AND events_done not set:
       → resume freeze for that chunk (see Section 13)

3. Open active events RocksDB store: events-store-chunk-{chunkID:06d}/
     → RocksDB opens the store
     → if unclean shutdown: WAL replay recovers all committed WriteBatches automatically
     → result: CF "events", CF "offsets", CF "deltas" contain exactly
       the data from all ledgers that completed step 3 of the write path

4. Rebuild in-memory ledger offset array:
     Scan CF "offsets" in key order:
       for each (ledgerSeq, cumulativeEventCount):
         ledgerOffsets = append(ledgerOffsets, cumulativeEventCount)
     → array has one entry per committed ledger in this chunk

5. Rebuild in-memory bitmap index:
     Scan CF "deltas" in key order:
       for each (ledgerSeq, deltaBatch):
         for each (term_key, event_id) in deserialize(deltaBatch):
           bitmaps[term_key].Add(event_id)
     → bitmaps are identical to what was in memory before shutdown/crash

6. Set next_event_id from last entry in ledger offset array
     → ingestion can resume assigning sequential eventIDs

7. Resume ingestion from streaming:last_committed_ledger + 1
```

### 12.2 Why Bitmap Rebuild Is Correct

The in-memory bitmaps are lost on any shutdown (graceful or crash). They are rebuilt from the deltas CF, which contains the exact sequence of `(term_key, event_id)` additions that were applied during ingestion. Because:

- The deltas CF is written in the same atomic WriteBatch as the events — if the events are in the store, the deltas are too
- `bitmap.Add(event_id)` is idempotent and order-independent for the same event_id
- The deltas cover every ledger in the store (no gaps — the WriteBatch is atomic)

The rebuilt bitmaps are identical to the pre-shutdown state.

### 12.3 Rebuild Cost

For a chunk with 777 committed ledgers (~800K events):

| Step | Work | Estimated time |
|------|------|---------------|
| RocksDB open + WAL replay | Automatic | < 1 second |
| Scan CF `offsets` (777 entries) | 777 key-value reads, 8 bytes each | < 10 ms |
| Scan CF `deltas` (777 entries) | 777 key-value reads, ~4KB each; ~3.2M bitmap.Add calls | 1–3 seconds |
| **Total** | | **< 5 seconds** |

For a full 10K-ledger chunk (~10M events, ~40M bitmap.Add calls): estimated 10–30 seconds. This is a one-time cost at startup.

### 12.4 Crash Scenarios

| Crash Point | Events in Store | Bitmaps | Recovery Action |
|-------------|----------------|---------|-----------------|
| Mid-WriteBatch | Up to previous ledger (WAL) | Rebuilt from deltas | Resume from last_committed + 1 |
| After WriteBatch, before bitmap update | Includes current ledger | Rebuilt from deltas (includes current ledger) | Resume from last_committed + 1 |
| During freeze | Transitioning store intact | Rebuilt from deltas for new chunk | Restart freeze from scratch |
| After cold files fsynced, before events_done | Transitioning store intact, cold files complete | Rebuilt for new chunk | Rerun freeze (idempotent) |
| After events_done, before store delete | Orphaned transitioning store | Rebuilt for new chunk | Delete orphaned store |

In all cases, recovery is: **open RocksDB (WAL replay automatic) → rebuild bitmaps from deltas CF → resume**. One model for all crash points.

**Detailed crash walkthroughs:**

**Crash during normal ingestion (mid-chunk):**
```
State at crash:
  streaming:last_committed_ledger = 6,000,778 (in meta store)
  Active events store: events-store-chunk-000600/
    CF "events":  800K events (ledgers 6,000,002–6,000,778)
    CF "offsets": 777 entries
    CF "deltas":  777 entries
  In-memory bitmaps: lost
  In-memory ledger offsets: lost

On restart:
  1. Open meta store → last_committed = 6,000,778, chunk = 600
  2. Open events-store-chunk-000600/ → WAL replay (if unclean)
     → all 777 ledgers' data intact across all CFs
  3. Rebuild offsets array from CF "offsets" → 777 entries
  4. Rebuild bitmaps from CF "deltas" → 777 ledger batches replayed
  5. Resume ingestion from ledger 6,000,779
```

**Crash mid-WriteBatch (ledger partially written):**
```
WriteBatch for ledger 6,000,779 was in progress but not committed to WAL.

On restart:
  WAL replay restores state up to ledger 6,000,778.
  Ledger 6,000,779's partial WriteBatch is discarded (never hit WAL).
  Bitmaps rebuilt from deltas → cover ledgers up to 6,000,778.
  Resume from 6,000,779 → re-ingest. Identical result.
```

**Crash during freeze:**
```
State at crash:
  Chunk 600 reached its last ledger (6,010,001).
  SwapActiveEventsStore moved chunk 600's store to transitioningEventsStore.
  New active events-store-chunk-000601/ opened for chunk 601.
  Background freeze goroutine was mid-way through building cold files.
  events_done for chunk 600: NOT set.

On restart:
  1. Open meta store → last_committed tells us we're in chunk 601
  2. Detect transitioning store: events-store-chunk-000600/ still on disk, events_done absent
     → discard any partial cold files for chunk 600
     → restart freeze: read from transitioning store, build cold segment from scratch
  3. Open active events-store-chunk-000601/ → WAL replay
  4. Rebuild bitmaps for chunk 601 from its CF "deltas"
  5. Freeze for chunk 600 completes:
     → cold files written + fsynced
     → events_done = "1" set in meta store
     → transitioning store closed + deleted
  6. Resume ingestion
```

## 13. Freeze (Transition) at Chunk Boundary

Events transition at chunk cadence (every 10K ledgers), the same cadence as the ledger sub-flow. This creates a third sub-flow alongside the existing two:

| Sub-flow | Active Store | Transition Cadence | Max Active | Max Transitioning | Max Total |
|----------|-------------|-------------------|------------|-------------------|-----------|
| Ledger | `ledger-store-chunk-{chunkID:06d}/` | Every 10K ledgers | 1 | 1 | 2 |
| Events | `events-store-chunk-{chunkID:06d}/` | Every 10K ledgers | 1 | 1 | 2 |
| TxHash | `txhash-store-range-{rangeID:04d}/` | Every 10M ledgers | 1 | 1 | 2 |

### 13.1 Freeze Trigger

```
ledgerSeq == chunkLastLedger(currentChunk)
```

Same trigger condition as the ledger sub-flow transition.

### 13.2 Freeze Workflow

```
Chunk boundary hit (e.g., ledger 6,010,001 — last ledger of chunk 600):

1. SwapActiveEventsStore(chunkID + 1):
     → current active events store (chunk 600) becomes transitioningEventsStore
     → transitioningEventsStore stays OPEN for reads during freeze
     → snapshot the in-memory bitmaps for chunk 600
     → snapshot the in-memory ledger offset array for chunk 600
     → new active events store opens for chunk 601 (events-store-chunk-000601/)
     → new empty in-memory bitmaps and ledger offset array for chunk 601

2. Background freeze goroutine (runs concurrently with ingestion of chunk 601):

   a. Build cold events.pack:
        iterate CF "events" in transitioningEventsStore in key order
        group events into fixed-size blocks
        compress each block with zstd
        write blocks to events.pack
        embed offset index and ledger offset array (from snapshot) into events.pack
        fsync events.pack

   b. Build cold index files:
        serialize bitmaps (from snapshot) with 4-byte fingerprint prefixes → index.pack
        build MPHF from term keys → index.hash
        fsync index.pack and index.hash

   c. Set events_done = "1" in meta store (WAL-backed)
        ← MUST be durable before step d

   d. Close transitioningEventsStore, delete its directory
        (events-store-chunk-000600/ removed from disk)

   e. Release bitmap snapshot and ledger offset snapshot for chunk 600

   f. Signal completion:
        set transitioningEventsStore = nil
        signal condition variable (waitForEventsTransitionComplete unblocks)
```

### 13.3 Critical Ordering Invariant

The `events_done` flag MUST be durable in the meta store BEFORE the transitioning events store is closed and deleted (step c before step d). This is the same invariant as `lfs_done` in the ledger sub-flow:

- If crash happens before `events_done` is set: transitioning store is still on disk. Freeze reruns from scratch.
- If crash happens after `events_done` but before store deletion: orphaned store cleaned up on startup. Cold files serve queries.
- If `events_done` were set AFTER store deletion: a crash between deletion and flag-set would leave no data source for that chunk — unrecoverable.

### 13.4 Query Routing During Freeze

While the freeze is in progress for chunk 600:

| Query target | Source |
|-------------|--------|
| Chunk 600 (transitioning) | Transitioning events store (still open) + in-memory bitmaps (snapshot) |
| Chunk 601 (active, being ingested) | Active events store + in-memory bitmaps (new, growing) |
| Chunk 599 and earlier (cold) | Cold files: `events.pack`, `index.hash`, `index.pack` |

Queries are never blocked. The transitioning store remains readable throughout the freeze.

### 13.5 Coordination at Chunk Boundary

At every chunk boundary, two transitions fire simultaneously: the ledger sub-flow (LFS flush) and the events sub-flow (freeze). Both are independent background goroutines:

```
Chunk boundary hit (ledger == chunkLastLedger):

  ┌─ SwapActiveLedgerStore(chunkID + 1)
  │    → background goroutine: read 10K ledgers → write LFS .data + .index
  │    → fsync → set lfs_done → close + delete transitioning ledger store
  │
  ├─ SwapActiveEventsStore(chunkID + 1)
  │    → background goroutine: read events → build cold segment
  │    → fsync → set events_done → close + delete transitioning events store
  │
  └─ Ingestion continues immediately on new active stores for chunk+1
       (ledger store, events store, and the same txhash store — txhash spans the full range)
```

The two freeze goroutines are independent — they don't wait for each other. They operate on different stores and produce different output files. The only shared dependency is that both must complete before the range boundary.

### 13.6 Coordination at Range Boundary

At the range boundary (every 10M ledgers), the existing invariant applies: **all lower-cadence sub-flow transitions must complete before the higher-cadence transition proceeds.**

```
Range boundary hit (ledger == rangeLastLedger):

1. waitForLedgerTransitionComplete()
     → block until the last chunk's ledger LFS flush goroutine finishes
     → all 1,000 lfs_done flags guaranteed durable

2. waitForEventsTransitionComplete()
     → block until the last chunk's events freeze goroutine finishes
     → all 1,000 events_done flags guaranteed durable

3. Verify all 1,000 lfs_done flags are set (safety check)

4. Verify all 1,000 events_done flags are set (safety check)

5. Proceed with txhash sub-flow transition:
     → PromoteToTransitioning(N) — moves txhash store
     → Atomic WriteBatch: range:N:state = TRANSITIONING, range:N+1:state = ACTIVE
     → Spawn RecSplit build goroutine
     → Create new active stores (ledger + events + txhash) for range N+1
     → Resume ingestion
```

## 14. Backfill

During backfill, event cold segments are written directly — no RocksDB events store is involved. Backfill has no concurrent readers, no per-ledger crash recovery, and no need for query routing — chunk-level restart on crash is sufficient.

### 14.1 Integration with the Backfill DAG

The existing `process_chunk` task gains a third output:

```
process_chunk(chunk_id)                    [chunk cadence — 10K ledgers]
  deps:    none
  outputs:
    - LFS chunk file (.data + .index)           → sets lfs_done
    - Raw txhash flat file (.bin)                → sets txhash_done
    - Cold events segment (events.pack +         → sets events_done
      index.hash + index.pack)

  atomic WriteBatch: lfs_done="1" + txhash_done="1" + events_done="1"
```

All three outputs are produced in a single pass over the ledger data. The chunk skip rule on resume becomes: all three flags must be `"1"` to skip.

### 14.2 Per-Chunk Write Flow

```
process_chunk(chunk_id):
  open events.pack for writing
  initialize in-memory bitmaps (empty)
  initialize in-memory ledger offset array (empty)
  block_buffer = []
  next_event_id = 0

  for each ledger (10K total, streamed via BSB):

    ... existing: append to LFS chunk file, accumulate txhash entries ...

    for each event in ledger:
      assign event_id = next_event_id++
      block_buffer = append(block_buffer, serialize(event))

      if len(block_buffer) == 128:                ← block full
        compress block with zstd
        append compressed block to events.pack
        write() to events.pack                    ← page cache only, no fsync
        block_buffer = []

      for each indexed field (contractId, topic0..topic3):
        bitmaps[termKey(field, value)].Add(event_id)

    ledgerOffsets = append(ledgerOffsets, next_event_id)

    every ~100 ledgers:
      write() LFS and txhash file handles         ← existing flush cadence

  at chunk completion (10K ledgers processed):
    flush final partial block (< 128 events → compress + append)
    embed offset index + ledger offset array into events.pack
    fsync events.pack

    serialize bitmaps with 4-byte fingerprint prefixes → index.pack
    build MPHF from term keys → index.hash
    fsync index.pack + index.hash

    fsync LFS .data + .index files                 ← existing
    fsync txhash .bin file                          ← existing

    atomic WriteBatch: lfs_done="1" + txhash_done="1" + events_done="1"
```

### 14.3 Memory Profile During Backfill

Event data flows through memory in 128-event blocks and is immediately flushed to disk. The bitmaps and ledger offset array must stay in memory for the full chunk because the MPHF index cannot be built until all terms and their bitmaps are complete.

| Data | Lifetime | Size |
|------|----------|------|
| Block buffer (128 events) | Flushed on every full block | 128 × ~250 bytes = **~32KB** |
| Ledger offset array (10K entries) | Full chunk | 10K × 4 bytes = **~40KB** |
| In-memory bitmaps (~200K–500K terms) | Full chunk | **~100–500MB** (see Section 8.2) |
| events.pack on disk (incremental) | Written incrementally | ~1.5–2.5GB final (compressed) |

The bitmap memory is the dominant cost. This is unavoidable — the MPHF and serialized bitmaps for the cold index files require the complete bitmap state for every term in the chunk.

With `parallel_ranges=2` and 20 BSB instances per range, up to 40 chunks can be in flight. However, each BSB instance processes chunks sequentially (one at a time), so at most 40 bitmap sets coexist. At ~100–500MB each, that's **4–20GB** of bitmap memory across all in-flight chunks. This should be factored into the memory budget alongside the existing LFS and txhash memory costs.

### 14.4 Crash Recovery During Backfill

If a crash occurs mid-chunk, `events_done` is absent. On resume:

- The chunk's partial `events.pack`, `index.hash`, and `index.pack` are overwritten from scratch
- Bitmaps are rebuilt in memory as events are re-processed
- No WAL, no truncation, no file-length tracking — just rewrite the chunk

This is identical to how `lfs_done` and `txhash_done` work during backfill: absent flag → full rewrite.

---

## Meta Store Integration

Events integrates into the existing meta store key hierarchy. No separate DB is needed.

### Per-Chunk Flag

| Key | Value | Written When |
|-----|-------|-------------|
| `range:{N:04d}:chunk:{C:06d}:events_done` | `"1"` or absent | After cold segment files (`events.pack`, `index.hash`, `index.pack`) are fsynced to disk |

Same semantics as `lfs_done` and `txhash_done`:
- Written only after fsync (flag-after-fsync invariant)
- Never deleted once set
- Absent means incomplete — freeze reruns from transitioning store

### Chunk Skip Rule

With events added, a chunk is only skippable on resume when ALL flags are set:

```
lfs_done = "1" AND txhash_done = "1" AND events_done = "1"
```

If any flag is absent, the entire chunk is reprocessed.

---

## Directory Structure

```
{data_dir}/
├── active/
│   ├── ledger-store-chunk-{chunkID:06d}/        ← active ledger store (existing)
│   ├── events-store-chunk-{chunkID:06d}/        ← active events store
│   │   ├── MANIFEST-*
│   │   ├── *.sst
│   │   ├── *.log                                ← WAL (required)
│   │   └── OPTIONS-*
│   └── txhash-store-range-{rangeID:04d}/        ← active txhash store (existing)
│
└── immutable/
    ├── ledgers/chunks/...                        ← LFS chunk files (existing)
    ├── txhash/...                                ← RecSplit index files (existing)
    └── events/                                   ← cold event segments
        └── {chunkID:06d}/
            ├── events.pack                       ← compressed event blocks + embedded offsets
            ├── index.hash                        ← MPHF for term key → slot lookup
            └── index.pack                        ← serialized bitmaps, fingerprint-prefixed
```

Maximum RocksDB stores open simultaneously at a chunk boundary (worst case — both ledger and events transitions in flight):
- 1 active ledger store + 1 transitioning ledger store = 2
- 1 active events store + 1 transitioning events store = 2
- 1 active txhash store = 1
- **Total: 5** (briefly; transitioning stores are closed quickly after freeze completes)

At range boundary, add 1 transitioning txhash store → **6 max**.
