# RPC getTransaction Full-History Design

# Part 1: Problem and Scope

## 1. Objective

Serve `getTransaction(hash)` for any transaction whose ledger falls within the retention window (full history by default):

- **Complete.** Every transaction in every in-retention ledger is resolvable by its hash, with no gaps — across crashes, restarts, and retention changes alike. The one exception is a hash-prefix collision so rare (~10⁻²⁰ for a dense window) that it counts as negligible, and even then it fails loudly rather than silently. §8.2 has it.
- **Correct.** A lookup never returns the wrong transaction; a missing or out-of-retention one returns not-found.
- **No in-memory index.** The map lives in on-disk `.idx` files, read through the page cache — not a RAM structure sized to the transaction count. The daemon's memory does not grow with the number of transactions in history.
- **Cheap to maintain.** Ingestion adds negligible cost to the per-ledger write, and the cold index stays current with a rebuild that is small relative to how often it runs.

Out of scope: how a reader chooses which tier and window to consult and stays correct while files are added and removed (the query-routing design), and the storage of the transaction bytes themselves (the ledger store).

## 2. Lookup model

`getTransaction` takes a 32-byte transaction hash and returns the transaction's envelope, result, and meta, plus its ledger and close time. The data flow:

```
hash ──► seq ──► LCM for seq ──► extract the tx ──► verify hash ──► respond
      (this doc)  (ledger store)
```

Three properties of the transaction-hash key space shape the design:

- **Point lookups only.** Every query is for one specific hash, never a range or prefix — exactly what a perfect hash is built for.
- **Hashes are uniform and immutable.** A transaction hash is never updated, and corresponds to at most one applied transaction (the network's replay protection). The map is append-only: one batch of entries per ledger.
- **The full transaction is always fetched anyway.** The response needs the envelope, result, and meta, so the read path always ends by fetching the transaction and checking its full 32-byte hash. That means the map needn't be exact — only *complete*, never missing a hash that is really there. False positives are harmless: a fingerprint screens most of them, and the final hash check catches the rest.

---

# Part 2: Architecture

## 3. The two tiers

Each in-retention transaction lives in exactly one place — one tier, one window, never copied. But a hash on its own doesn't say which place, so a lookup checks them all, and at most one answers (none, if the hash isn't stored). The two places a transaction can live:

| Tier | Structure | Serves |
|---|---|---|
| **Hot** | `txhash` CF of the per-chunk hot RocksDB | the live chunk, plus any frozen chunk the window index doesn't cover yet |
| **Cold** | one streamhash `.idx` per window, covering chunks `[lo, hi]` | every chunk in `[lo, hi]` (at/below the frozen `hi`, at/above the floor chunk `lo`) |

```
                     window w
  chunks:   [lo ···························· hi] [hi+1 ···] [live]
  served by: └──────── {lo}-{hi}.idx ─────────┘  hot DBs    hot DB
                                                 (awaiting    (being
                                                  coverage)   written)
```

The two tiers hand off with no gap. A chunk's hot table is dropped only *after* the cold index covers that chunk. So a freshly frozen chunk keeps being answered from its hot table until the index can answer for it, and only then does the hot table go away. Every transaction is findable in exactly one tier at all times.

## 4. Geometry

Two units organize the map. Every structure below is named by them:

- **Chunk** — 10,000 ledgers (hardcoded). The unit of the hot DB and of the sorted runs.
- **Window** — 1,000 chunks = 10,000,000 ledgers (hardcoded). The unit of the cold index.

```
chunkID(seq)        = (seq - 2) / 10_000
chunkFirstLedger(c) = c * 10_000 + 2
chunkLastLedger(c)  = (c + 1) * 10_000 + 1
indexID(c)          = c / 1000                          # takes a CHUNK id
chunksInIndex(w)    = [w*1000, (w+1)*1000 - 1]
```

Window 0 spans ledgers 2–10,000,001 (chunks 0–999), window N spans N×10M+2 – (N+1)×10M+1 (chunks N×1000 – (N+1)×1000−1). All ids zero-pad `%08d`.

---

# Part 3: Implementation Reference

## 5. Hot tier

### 5.1 Storage

The hot tier is a plain key-value table, one per chunk, stored as a `txhash` column family in that chunk's RocksDB:

- **Key**: the full 32-byte transaction hash.
- **Value**: the 4-byte ledger sequence.

Storing the full hash makes the hot tier **exact**: a lookup either finds the hash or it doesn't. There are no false positives to screen out and nothing to verify. The table is tuned for point lookups.

### 5.2 Write path

Writing is straightforward. As each ledger is ingested, one `(hash, seq)` entry is added for every transaction in it, in the same atomic write that stores the rest of the ledger. So a ledger's hashes are written all-or-nothing, together with the rest of the ledger.

### 5.3 Lifetime

A chunk's hot table lives from the moment the chunk starts ingesting until the cold index covers it. Coverage can lag the chunk's freeze by a while; until it lands, the chunk is simply answered from its hot table.

## 6. Cold artifacts

The cold tier has two kinds of file: a per-chunk sorted run (`.bin`) and the per-window index (`.idx`).

### 6.1 The per-chunk sorted run: `.bin`

The `.bin` lives at `txhash/raw/{bucket:05d}/{chunk:08d}.bin`, with catalog key `chunk:{chunk:08d}:txhash`. It is produced once, when the chunk is frozen: as the chunk's ledgers are read, each transaction's `(hash, seq)` is collected, and at the end they are **sorted in memory** (~3M entries ≈ 60 MB for a dense chunk — negligible) and written out.

**Format** (the streamhash merge format):

```
uint64 LE        entry count
entry × count    20 bytes each: [key: 16][seq: 4 LE]
```

- `key` is the **first 16 bytes of the transaction hash**. The index uses only these 16 bytes to place and find a transaction; what happens when two hashes share a 16-byte prefix is in §8.2.
- Entries are sorted ascending by `key`, **bytewise over all 16 bytes** — a total order, so the same entries always produce byte-identical files (rebuilds are deterministic).

The `.bin` is a pre-sorted file, and a lookup never reads it directly. It is sorted because streamhash builds an index **much faster, and with much less memory, when its keys arrive already sorted** — its *sorted-builder mode*.

A `.bin` is kept while it is still a rebuild input — every rebuild re-merges the `.bin` files for the chunks its window currently covers. Once the window is complete and its final index is built, the `.bin` files are no longer needed, and are deleted — or, if retention is narrower than a window so its chunks age out before the window completes, retention pruning deletes them first.

### 6.2 The per-window index: `.idx`

The `.idx` lives at `txhash/index/{window:08d}/{lo:08d}-{hi:08d}.idx`, tracked by the catalog key `index:{window:08d}:{lo:08d}:{hi:08d}`. There is one minimal-perfect-hash file per **coverage** — a coverage being the chunk range `[lo, hi]` the file actually hashes. Streamhash's `SortedBuilder` builds it from the k-way merge of `.bin[lo..hi]`. The index carries two per-entry fields:

- **Payload (3 bytes): the answer the hash maps to — a ledger seq.** It is stored as an offset from the coverage's first ledger (`MinLedger = chunkFirstLedger(lo)`) rather than as a full seq, to save bytes. A window spans 10,000,000 ledgers, so the largest offset (`10_000_000 - 1`) fits in a 24-bit field. Streamhash writes the payload width into the index file's header; the coverage's ledger range `[MinLedger, MaxLedger]`, which streamhash does not model itself, rides in the file's user-metadata slot as two 4-byte little-endian values. Both are read back when the index is opened, so there is no separate sidecar file.
- **Fingerprint (1 byte, fixed by the format): screens out wrong hashes** before the expensive fetch-and-verify. One byte rejects ~255/256 of foreign hashes per window probe (§8.2), costing one byte per transaction.

All-in, the index costs ≈4.2 bytes per transaction (MPHF structure + payload + fingerprint) — ≈12.5 GB for a dense full window, versus the ≈60 GB of `.bin` runs it consumes.

### 6.3 Coverage and the live index

An index file is named by its **coverage** — the chunk range `[lo, hi]` it hashes:

- **`lo`** — the lowest chunk the index covers. It is the window's first chunk, unless the retention floor has cut into the window, in which case it rises to the first chunk still retained.
- **`hi`** — the highest chunk the index covers. While the window is the current one (the network tip is in it), each rebuild advances `hi` to the highest frozen chunk: by one in steady state, by many when catching up after downtime. Once the window is complete, `hi` is its last chunk and the index is final.

A window has exactly **one live index** at a time, and a lookup resolves "the window's index" to that one file. A rebuild builds a new index at a wider coverage and replaces the live one; the replacement is atomic, so a lookup always sees one complete index, never a half-built one. (How that swap stays atomic across a crash is the daemon's write protocol, in the streaming doc.)

So the index hashes exactly the transactions in chunks `[lo, hi]`. Chunks below `lo` are out of scope — cut off by the floor. Chunks above `hi` aren't folded in yet, and are served from their hot tables until the next rebuild advances `hi`.

**Example** (1,000 chunks per window): the tip is in chunk 5350, so window 5 (chunks 5000–5999) is the current window, and the floor is at chunk 5100. The live index covers chunks 5100–5349, in the file `txhash/index/00000005/00005100-00005349.idx`; chunk 5350 is still in its hot table, and chunks 5000–5099 are below the floor. At the next boundary the index is rebuilt to cover 5100–5350, and the old file is deleted.

## 7. The rolling rebuild

### 7.1 Rebuild cadence and cost

The current window's index is **rebuilt from scratch on every chunk boundary**, to fold in the chunk that just froze; it grows until the window is complete. Only the current window is ever rebuilt — a finalized window's index never changes.

This is affordable because the rebuild is cheap relative to its cadence: a full-window build takes ≈1 minute, against a boundary only every ~14 hours at mainnet rates (Part 4). Rebuilding the whole index each time keeps every `.idx` on disk a complete index for its coverage, with no half-updated state.

### 7.2 The rebuild

To rebuild window `w`'s index over coverage `[lo, hi]`:

1. **Skip if already done.** If the live index already covers `[lo, hi]`, there is nothing to do. (A live index *wider* than desired also counts — a risen floor never forces a narrowing rebuild; the below-floor slice is masked by the retention gate, §8.2.)
2. **Merge.** Merge the sorted `.bin` files for chunks `[lo, hi]` into a new index file, with streamhash's sorted-builder. (Every chunk in `[lo, hi]` must have a `.bin`; a missing one fails the merge.)
3. **Swap in.** Make the new file the window's live index, replacing the previous one.

```go
// rebuild window w's index over [lo, hi]
sb := streamhash.NewSortedBuilder(newIndexFile, sortedBuilderOpts)
for entry := range kWayMerge(binFiles(lo, hi)) { // sorted .bin files → one stream
    sb.Add(entry)
}
sb.Finish()
// then make newIndexFile the window's live index, replacing the old one
```

Because a rebuild writes a whole new file and only swaps it in at the end, the live index is never partially updated: a lookup sees either the old index or the new one, never something in between.

### 7.3 Finalization

When a window's last chunk is folded in, its index is final: it covers the whole window and is not rebuilt again — unless retention later widens to include older chunks, when it is rebuilt wider to cover them. The window's `.bin` files have done their job as rebuild inputs, and are deleted.

### 7.4 Disk use during a rebuild

A rebuild writes a whole new index file before the old one is removed, so a window directory briefly holds ~2× the index size (~25 GB at the end of a dense window). The window's `.bin` files are also all on disk together, since the rebuild merges them at once — about 60 GB for a dense window. Both are transient.

The window-end rebuild writes ~12.5 GB in ~1 minute (~200 MB/s burst) — trivial on instance NVMe, but worth provisioning for on throughput-capped volumes like EBS gp3.

## 8. Query path

### 8.1 Routing

A hash names no ledger, so the reader cannot know which home holds it in advance — it **probes them all**, and the hash resolves in exactly one:

| Tier | Probe set | How |
|---|---|---|
| cold — one `.idx` per window | **every in-retention window** | MPHF + fingerprint + verify (§8.2) |
| hot — `txhash` CF per chunk | the chunks above any window's `hi` (live, or frozen awaiting coverage) | exact full-key get (§8.3) |

The hot tier is a few chunks at most — one window's tail, normally just the live chunk — so the probe set is `≈ (in-retention windows) + (a handful of chunks)`. How the reader learns current coverage and stays consistent across rebuilds is the query-routing design's concern. This document requires only two things: that the two tiers together cover the whole retention window (the gap-free hot→cold handoff, §5.3), and that each transaction lives in exactly one of them. So **at most one probe confirms**: the verify runs on every fingerprint hit but succeeds for at most one.

### 8.2 Cold lookup

The cold tier **probes every in-retention window's `.idx`**. A hash gives no hint about which window it's in — to know the window you'd compute `chunkID(seq) / 1000`, and `seq` is the very thing the lookup is trying to find. So there is nothing to pre-select, and each window is probed in turn:

```
for each in-retention window (its live index → {lo}-{hi}.idx):
  → MPHF probe on the hash's 16-byte prefix
  → fingerprint check (1 byte)                    — miss ⇒ skip this window
  → on a fingerprint hit:
       seq = MinLedger + payload (3 bytes)
       retention gate: seq ≥ floor?               — else skip this window
       fetch the LCM for seq, extract the tx
       verify the full 32-byte hash               — confirms, or rejects a false positive
respond on the confirmed hit; not-found if no window confirms
```

Because the hash belongs to at most one window, **at most one window confirms**; a not-found lookup — a non-existent or not-yet-ingested hash — confirms none and must rule out every in-retention window.

The final verification is essential: a minimal perfect hash returns a slot for *any* input, including a hash it doesn't contain, so every hit must be confirmed. The fingerprint screens out most foreign hashes cheaply, and the fetch-and-verify rejects the rest.

A **16-byte prefix collision between two distinct in-retention transactions** has two cases, and only one bounds completeness. The cold index keys on streamhash's 128-bit routing key (§6.1), so two hashes sharing their first 16 bytes are indistinguishable *to a single window's build*.

*Different windows* — the more likely of the two, since a shared prefix is far more apt to straddle two of history's windows than to fall inside one. Each transaction keys into its own window's `.idx`, so neither build sees a duplicate and both resolve normally. The collision shows up only as a fingerprint false-positive when a lookup probes the *other* window. That window's MPHF maps the shared prefix to its own resident transaction, and the fingerprint (also derived from those 16 bytes) matches — but the fetch-and-verify rejects it, because the full 32-byte hashes differ. This is exactly the foreign-key path the verify already exists for: one wasted ledger fetch, no wrong answer and no false negative.

*Same window* — the genuine residual. The two are a single key to that window's builder, so streamhash rejects the duplicate at build time (`ErrDuplicateKey`) and the build fails **loudly**: it never silently drops a transaction, and the verify ensures it never returns a wrong one. This is the only bound on completeness, and it is tiny — the birthday probability over a dense window's ~3×10⁹ keys against 2¹²⁸ is ~10⁻²⁰ per window, a cryptographic-scale risk accepted as negligible.

**Probe ordering, parallelism, early-stop, and the resulting latency and I/O are the query-routing design's concern** (§8.1), out of scope here.

### 8.3 Hot lookup

Chunks above `hi` are probed in their hot DBs' `txhash` column family — an exact, full-key point get. A miss here is a real miss, with none of the cold tier's verification subtleties (the fetch-and-verify still runs, since the response needs the transaction anyway). In steady state this tier is just the live chunk, plus briefly the one chunk in the freeze-to-coverage gap. After catch-up or a crash it can be several chunks, shrinking as rebuilds advance `hi`.

---

# Part 4: Capacity & Performance

## 9. Storage footprint

Per dense chunk (~3M transactions) and dense window (1,000 chunks, ~3×10⁹ transactions):

| Structure | Unit cost | Dense chunk | Dense window | Lifetime |
|---|---|---|---|---|
| hot `txhash` CF | 36 B/tx raw (32 key + 4 value), before RocksDB overhead | ~110 MB raw | — (per-chunk) | chunk ingestion → index coverage |
| `.bin` sorted run | 20 B/tx exactly | ~60 MB | ~60 GB | chunk freeze → window finalization, or retention floor |
| `.idx` | ≈4.2 B/tx (3-byte payload) | — (per-window) | ~12.5 GB | build → superseded next boundary, or retention |

Transient peaks: ~2× the index size in the window dir during each rebuild (~25 GB at window end); the `.bin` files for the in-flight window total ~60 GB. Both are transient (§7.4). The steady-state durable cost of the cold tier is the `.idx` files alone: ≈4.2 bytes per transaction across all retained history.

## 10. Performance

- **Ingest, hot**: one `(hash, seq)` put per transaction, inside the ledger's existing write.
- **Ingest, cold**: the in-memory sort of ~3M entries is negligible against the chunk's streaming pass; the `.bin` write is sequential.
- **Rebuild**: a full dense window merges ~60 GB of sorted `.bin` files into a ~12.5 GB `.idx` in ≈1 minute (~200 MB/s write burst), measured in the `bench-fullhistory` harness. Mid-window rebuilds scale with `hi − lo`. Against a ~14-hour boundary cadence at mainnet rates, the rebuild is a ~0.1% duty cycle.
- **Lookup, cold**: one MPHF probe per in-retention window — fingerprint screen, then fetch-and-verify on a hit. The hash is in at most one window, so at most one fetch confirms; fingerprint false positives (~1/256 per window, §6.2) are rejected by the full-hash verify. Probe ordering, parallelism, and the resulting latency/throughput are the query-routing design's concern (§8.1).
- **Lookup, hot**: one RocksDB point get in a bloom-filtered CF, then the same ledger fetch.

---

## Related documents

- [full-history-streaming-workflow.md](./full-history-streaming-workflow.md) — the daemon this subsystem lives in: geometry, the catalog and one write protocol, `processChunk`, the resolver and executor, the lifecycle run (freeze → rebuild → discard → prune), and the correctness invariants (INV-1 … INV-4) with their audits.
- The reader / query-routing design — how readers obtain current coverage and dispatch between hot DBs and frozen files across transitions.
- [getevents-full-history-design.md](./getevents-full-history-design.md) — the sibling subsystem (events), same hot/cold architecture over the same chunk geometry.
- [packfile-library.md](./packfile-library.md) — the `.pack` format the read path's ledger fetch lands on.
- `bench-fullhistory` — the measurement harness behind every figure in Part 4.
