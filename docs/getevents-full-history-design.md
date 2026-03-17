# RPC getEvents Full-History Design

## Summary

The Stellar network has emitted over 28 billion events across more than 60 million ledgers and continues to grow. The existing `getEvents` RPC was designed for a limited retention window and does not scale to full-history queries. This document proposes a purpose-built backend that stores and indexes the complete event history and serves filtered queries with low, predictable latency.

## How to Read This Document

This document is organized in four parts:

**Part 1: Problem and Scope** (Sections 1–3) covers the objective, event structure, and query model. Skip if you're already familiar with `getEvents`.

**Part 2: Architecture** (Sections 4–6) explains how the system is structured: its main components, how data flows through them.

**Part 3: Implementation Reference** (Sections 7–14) covers event IDs, the bitmap index, hot and cold segment storage, the freeze process, and recovery.

**Part 4: Capacity, Performance and Scaling** (Sections 15–22) covers storage estimates, memory, query performance, ingestion throughput, operational profile, observability, tiered storage, and scaling projections. Some sections are still in progress and will be updated as benchmarking data becomes available.

---

# Part 1: Problem and Scope

## 1\. Objective

Design a storage and indexing layer that lets RPC nodes serve filtered event queries across the full stellar history.

Target characteristics:

* **p99 latency ≤ 500ms**  
* Filtering by **contract ID and up to four topic fields**  
* Boolean filter combinations  
* Compatible with the proposed [getEvents v2 API](https://github.com/orgs/stellar/discussions/1872) (bidirectional ordering, stateless cursors)

Populating a new RPC node with full history (e.g., via snapshot distribution) is not covered here. The backfill write path for building cold segments from historical data is covered in Section 14\.

## 2\. Event Structure

Each stored event corresponds to a [ContractEvent XDR](https://github.com/stellar/go-stellar-sdk/blob/main/xdr/Stellar-ledger.x#L371-L390) emitted:

| Field | Indexed | Notes |
| :---- | :---- | :---- |
| contractId | Yes | Which contract emitted it ([StrKey](https://stellar.github.io/js-stellar-sdk/StrKey.html) "C..." address) |
| topic0–3 | Yes | 1–4 typed labels categorizing the event (ScVal encoded). Additional topics beyond 4 are stored but not indexed. |
| type | No | `contract`, `system`, or `diagnostic` |
| value | No | The event payload (e.g., amount transferred) |

* The indexed fields (`contractId`, `topic0`–`topic3`) are the ones callers search on.  
* There are three event types (`contract`, `system`, and `diagnostic`), but diagnostic events are debug-only and not stored, and system events make up 0.000025% of the total. So for practical purposes, almost every event is a contract event.  
* Topics are encoded as ScVal, a binary format used across the Stellar contracts. The topics are indexed as opaque byte sequences without decoding.

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

## 3\. Query Model

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
* **Single writer per segment.** During live ingestion, a single writer handles the hot segment; readers operate concurrently. During backfill, multiple workers can build separate cold segments in parallel.

The system manages two types of data:

* **Events:** the raw event payloads (contract ID, topics, value) along with metadata for each event (ledger number, tx hash, timestamps).  
* **Index:** a reverse mapping from query terms to matching events. Five fields are indexed: `contractId`, `topic0`, `topic1`, `topic2`, and `topic3`. For each unique (field, value) pair seen in the data, the index stores a [roaring bitmap](https://roaringbitmap.org/) of the event IDs that contain it. This allows answering "which events match this filter?" without scanning every event. 

### Architecture Overview

![][image1]

## 4\. Segments

The core organizational unit is the **segment**, which encompasses all events generated across a configurable range of 10,000 consecutive ledgers (configurable).

At current rates each segment currently holds approximately 10 million events, representing about 15 hours of data. These segments are stored in individual directories, named sequentially by segment number. With approximately 6,000 segments in the current history, this number is increasing daily.

With a segment size and query range cap both set to 10,000 ledgers, any single query touches at most 2 segments. Partitioning the event data into segments keeps each index manageable.

## 5\. Hot and Cold Segments

Events are organized into two types of segments based on their mutability:

* **Hot Segment:** This is the single, currently mutable segment that receives all new events. Its index lives in memory for fast mutation and querying.   
* **Cold Segments:** These segments are immutable, with both the events and their corresponding index stored on disk.

When the hot segment reaches capacity:

* A new hot segment immediately begins accepting writes.  
* The previous hot segment freezes into a cold segment.  
* The old hot segment continues serving queries during the freeze.

## 6\. Data Flow

### Writing (ingestion)

A single writer processes ledgers one at a time. For each ledger:

1. Assign sequential event IDs within the segment  
2. Append events to disk  
3. Update the bitmap index  
4. Atomically update the last processed ledger, making its events visible to readers

### Reading (queries)

1. For each segment the query spans, load the bitmaps for all query terms  
2. Combine bitmaps with AND/OR according to the filters  
3. Iterate matching event IDs up to the limit  
4. Fetch the actual events for those IDs  
5. Post-filter events to verify all query terms match

---

# Part 3: Implementation Reference

## 7\. Event Addressing

Every event in a segment is assigned a sequential ID from 0 to total\_events \- 1\. These are internal IDs used for indexing within a segment. 

A ledger offset array tracks the running total of events through each ledger:

```
offsets[0] = 1,042       (ledger 0: 1,042 events, IDs 0–1,041)
offsets[1] = 2,029       (ledger 1: 987 events, IDs 1,042–2,028)
offsets[2] = 4,529       (ledger 2: 2,500 events, IDs 2,029–4,528)
...
```

This array is directly indexable by ledger number in O(1).

Sequential IDs keep event IDs within the 32-bit range required for standard roaring bitmaps and produce densely packed containers. Alternative schemes like TOID (64-bit) or ledger-prefixed IDs produce sparse ID spaces that inflate bitmap sizes. The tradeoff is that sequential IDs require the ledger offset array to translate between ledger numbers and event ID ranges.

## 8\. Bitmap Index

For each unique value seen in an indexed field, the system stores a roaring bitmap of matching event IDs. Each such (field, value) pair is called a term:

```
(contractId, 0x3a1f...)  → {0, 4, 17, 203, 8741200, ...}
(topic0, 0x7b2c...)      → {0, 1, 5, 12, 13, 14, 98, 102, ...}
```

Each term is identified by a 17-byte key: a 1-byte field type (contractId, topic0–3) followed by a 16-byte hash (e.g., xxhash) of its value bytes. The field type prefix ensures uniqueness across fields. The index is stored as a map of these terms to roaring bitmaps.

Roaring bitmaps are used because term density varies widely across the data. Most terms match very few events while common terms like "transfer" can match millions. Roaring bitmaps handle this efficiently across the full range: array containers for sparse terms (where a full bitmap would be mostly empty) and compressed bitmap containers for dense terms (where a sorted array would be too large).

## 9\. Hot Segment

### 9.1 Hot Event Storage

Events are stored uncompressed as raw bytes (XDR payload \+ metadata), with direct access by event ID. The hot segment also maintains a ledger offset array (cumulative event counts per ledger) and index deltas (per-ledger term-to-event-ID mappings used for crash recovery).

The exact storage backend for hot event data is an implementation detail. Regardless of backend, events must be retrievable by event ID in O(1), and the ledger offset array must support O(1) lookup by ledger number.

### 9.2 Hot Index Storage

Bitmaps live entirely in memory as a single concurrent map of `17-byte term key hash → roaring bitmap pointer`. 

During ingestion, every ledger requires adding new event IDs to the relevant bitmaps (\~4,000 adds per ledger assuming \~1,000 events with \~4 indexed fields each). 

If bitmaps lived on disk or in the embedded DB, each add would require deserializing the bitmap, updating it, and re-serializing it back. For dense terms like "transfer" that can grow to millions of entries, this serialize/deserialize cycle on every ledger would be far too slow. 

Keeping bitmaps in memory makes each add a simple `bitmap.Add` call with no I/O or serialization overhead. Queries also benefit since bitmap intersections can be done directly on the in-memory bitmaps.

Changes are persisted as per-ledger deltas (in a flat file or the embedded DB, depending on the storage backend), allowing the in-memory index to be reconstructed during crash recovery.

### 9.3 Hot Write Path

```
For each incoming ledger:

1. Assign event IDs: start_id = next_event_id; next_event_id += len(events).

2. Persist events (XDR bytes + event metadata), keyed by event ID.

3. Persist (term_key, event_id) delta pairs for this ledger.

4. Update in-memory bitmaps (bitmap.Add for each event's contract + topics).

5. Persist cumulative event count for this ledger in the ledger offset array.

6. Atomically commit: last_committed_ledger and all data written in steps 2–5 must become durable together.
```

## 10\. Cold Segment

### 10.1 Cold Event Storage

Events are grouped into fixed-size blocks, which are then compressed with zstd.

| File | Description |
| :---- | :---- |
| `events.pack` | Compressed event blocks with offset index and ledger offset array embedded in the packfile |

The offset index and ledger offset array are embedded in the packfile's app data and loaded asynchronously on segment open, allowing parallel I/O with other segment files opened during the same request. See the [packfile library](https://github.com/tamirms/event-analysis/blob/main/packfile-library.md) design for details.

**On-disk Layout**

```
cold/
├── 0000/
│   ├── events.pack
│   ├── index.hash
│   └── index.pack
├── 0001/
│   ├── events.pack
│   ├── index.hash
│   └── index.pack
...
└── 6000/
    ├── events.pack
    ├── index.hash
    └── index.pack
```

### 10.2 Cold Index Storage

Bitmaps are serialized to disk in a single index file (`index.pack`) covering all indexed fields. To look up a bitmap by term key, we need a way to map a term key to its position in `index.pack`. This is done using a [Minimal Perfect Hash Function](https://en.wikipedia.org/wiki/Perfect_hash_function#Minimal_perfect_hash_function) (MPHF), stored in `index.hash`, implemented using [streamhash](https://github.com/tamirms/streamhash).

An MPHF maps each known key to a unique slot in \[0, N) with O(1) lookup and no collisions, making it a compact and efficient key-to-position mapping. The MPHF file is small enough to load in a single IOP at the start of a request and is loaded asynchronously.

| File | Description |
| :---- | :---- |
| `index.hash` | MPHF mapping term keys to slot positions in `index.pack` |
| `index.pack` | Serialized roaring bitmaps, one per term, each prefixed with a 4-byte fingerprint |

Since an MPHF maps any input to a valid slot, even keys not in the build set, a query for a non-existent term would still resolve to a slot and retrieve whatever bitmap is stored there. Each bitmap record in `index.pack` is therefore prefixed with a 4-byte fingerprint to detect and reject these false positives. 

A 4-byte fingerprint can still collide, so query results are post-filtered after event fetch to verify all terms match (see Section 10.3).

**Term Lookup:**

1. Hash the term key and query the MPHF in `index.hash` to obtain the slot index.  
2. Read the record at that slot in `index.pack`.  
3. If the fingerprint matches the hash prefix, deserialize the bitmap; otherwise the term is not present.

The resulting bitmap contains the event IDs matching the term.

## 11\. Freeze Process (Hot → Cold)

```
1. Start new hot segment immediately.

2. Read uncompressed events sequentially from the hot segment storage.

3. Compress into zstd blocks and write cold events.pack with offset index and ledger offset array embedded.

4. Build MPHF from term keys, serialize bitmaps with fingerprint prefixes into index.pack, and write index.hash.

5. Mark segment as frozen in DB; queries now served from cold files.

6. Discard in-memory index and hot files.
```

The old hot segment continues serving reads throughout. During freeze, two hot segments coexist briefly: the old segment being frozen (still serving reads) and the new segment accepting writes. Since the new segment has just started, its index is near-empty during freeze. Peak memory overhead is one full hot index plus a negligible new one.

## 12\. Query Path

The system first identifies which segments overlap the query's ledger range. Assuming each segment covers 10,000 ledgers and the query range is capped at 10,000 ledgers, a query spans at most 2 segments when the range crosses a segment boundary. `last_committed_ledger` is read to limit the query to events from ledgers that have been fully processed and committed. Results from each segment are merged and returned up to the limit.

### 12.1 Query Routing Flowchart

![][image2]

### 12.2 Hot Segment Read Path

```
1  Look up bitmaps for all query terms from the in-memory concurrent map. Bitmaps use copy-on-write so readers always get a consistent snapshot without blocking writes.

2. Combine bitmaps with AND/OR according to the query filters.

3. Iterate matching event IDs in order up to the remaining limit. Use the event ID range derived from the in-memory ledger offset array to skip IDs outside the requested ledger range.

4. Fetch raw events by ID from the hot segment storage.

5. Post-filter events to verify that all query terms match.
```

### 12.3 Cold Segment Read Path

```
1. Load bitmaps for all query terms:
* Hash the term key and query the MPHF in index.hash to get a slot.
* Read the record at that slot in index.pack.
* Check the 4-byte fingerprint. If it matches, deserialize the bitmap. Otherwise the term has no matches and can be skipped.

2. Combine bitmaps with AND/OR according to the filters.

3. Iterate matching event IDs in order up to the remaining limit, using the event ID range derived from the ledger offset array embedded in events.pack to skip IDs outside the requested ledger range.

4. Fetch raw events by ID: 
* Compute block index as (event_id / block_size) and position within the block as (event_id % block_size).
* Decompress the block from events.pack.
* Extract the event at the computed position.

5.  Post-filter to verify all terms match. The 4-byte fingerprint check in step 1 can still produce false positives, so post-filtering is necessary to ensure all terms match.
```

## 13\. Startup Procedure

The embedded key-value store (e.g., RocksDB) holds the state needed for atomic commits and crash recovery:

* `last_committed_ledger`: last fully committed ledger sequence number
* `current_hot_segment`: the segment currently accepting writes
* `freezing_segment`: the segment currently being frozen, if any
* Cold segment registry: `segment_start` and `segment_end` for each completed cold segment

```
On startup:
1. Read state from DB (last_committed_ledger, current_hot_segment, freezing_segment).

2. If freezing_segment is set, discard any partially written cold files and restart the freeze process.

3. Ensure no hot segment data exists beyond last_committed_ledger.

4. Replay persisted index deltas to rebuild in-memory bitmaps.

5. Load ledger offset array.

6. Resume accepting writes.
```

The DB is the source of truth for segment state. Marking a segment as frozen is done in a single atomic commit only after all cold files are fully written, so a crash before that commit leaves the segment in freezing state and the freeze restarts from scratch. A crash after the commit finds a complete cold segment and recovers immediately.

## 14\. Backfill Process

When populating cold segments from historical ledger data, the system writes cold segments directly, skipping the hot segment phase entirely. Since there are no concurrent reads during backfill, there is no need for per-ledger DB commits or crash recovery files.

```
For each segment (10,000 ledgers):
1. For each ledger:
   a. Append events to events.pack (block compression is handled internally by the packfile library).
   b. Update in-memory bitmaps.
   c. Update in-memory ledger offset array.

2. At segment completion (10,000 ledgers):
   a. Finalize events.pack (flush remaining data, embed offset index and ledger offset array).
   b. Build MPHF and serialize bitmaps into index.pack, write index.hash.
   c. Mark segment as available in DB.
```

The in-memory bitmaps and ledger offset array must be retained for the entire segment since they are needed to generate `index.hash`, `index.pack`, and the packfile metadata in step 2.

If a backfill worker fails mid-segment, the incomplete segment is discarded and restarted. Segments are independent so backfill can be parallelized across multiple workers. The cold segments produced are identical in format to those produced by freeze.

---

# Part 4: Capacity, Performance & Scaling

This section describes the system’s storage, memory, and performance characteristics, along with scaling projections under increased event load.

All calculations are based on current network observations and assume a segment size of 10,000 ledgers.

**Network Parameters:**  
These values are derived from recent ledgers and reflect current network behavior as of March 2026\.

| Parameter | Value |
| :---- | :---- |
| Ledger rate | \~1 ledger / 6 seconds (\~14,400 ledgers/day) |
| Events per ledger | \~1,000 |
| Total events (full history) | \~30 billion |
| Contract and system events | \~22 billion |

**Segment Estimates**:  
(segment size \= 10,000 ledgers)  
Earlier ledgers contain fewer events and are therefore smaller; the estimates below are based on current network behavior and are intended for capacity planning.

| Parameter | Value | Notes |
| :---- | :---- | :---- |
| Events per segment | \~10,000,000 | 10,000 ledgers × 1,000 events/ledger |
| Segment growth rate | \~1.44 segments/day | 14,400 ledgers/day ÷ 10,000 |
| Segments in full history | \~6,000 | As of March 2026 |

---

## 15\. Storage Estimates

Storage estimates are derived from measurements across 50 recent segments.

| Storage | Size |
| ----- | ----- |
| Average cold segment (\~8 million events) | \~430 MB |
| Hot segment | \~3 GB |
| **Total storage for 22B events** | **\~1.3 TB** |

### 15.1 Average Per-Event Storage Footprint

| Storage | Size (bytes) |
| ----- | ----- |
| Event (uncompressed) | \~250 |
| Event (compressed) | \~50 |
| Index overhead | \~10 |
| **Total storage per event** | **\~60** |

The average uncompressed event size (\~250 bytes) includes the raw event XDR and associated metadata.

## 16\. Memory Estimates

*To be added as benchmarking data becomes available. Will cover memory usage of the in-memory bitmap index and related structures.*

## 17\. Query Performance

*To be added as benchmarking data becomes available. Will cover query latency characteristics for hot and cold segment read paths.*

## 18\. Ingestion and Backfill Throughput

*To be added as benchmarking data becomes available. Will cover write throughput for live ingestion and historical backfill.*

## 19\. Operational Profile

*To be added as benchmarking data becomes available. Will cover the resource profile (IOPS, memory, CPU, disk) of the event storage and indexing layer.*

## 20\. Monitoring and Observability

*To be added. Will cover key metrics the system should expose for operational visibility.*

## 21\. Tiered Storage

*To be added. Will cover a tiered storage model where recent segments reside on faster storage (e.g., NVMe) and older segments migrate to cheaper storage (e.g., EBS), balancing query performance with cost.*

## 22\. Scaling Projections

*To be added as benchmarking data becomes available. Will cover system behavior under increased event volume.*

[image1]: architecture-overview.png
[image2]: query-routing-flowchart.png
