# RPC getTransaction Full-History Design

## Summary

How the full-history daemon ingests and serves transactions for the tx-by-hash endpoint (`getTransaction`). A transaction lookup is a two-step read: resolve the hash to a ledger sequence, then fetch the transaction from that ledger's stored LCM. This document covers the resolution structure: the hot tier (a column family in the per-chunk hot RocksDB) and the cold tier (per-chunk sorted runs merged into a per-window minimal-perfect-hash index), the file formats, the rolling rebuild that keeps the cold tier current, the read path, and the capacity numbers.

The daemon context — chunk geometry, the meta store, the one write protocol, catch-up, and the lifecycle tick — is defined in [full-history-streaming-workflow.md](./full-history-streaming-workflow.md) (the streaming doc). That document references this one for everything tx-hash-specific and restates only what its own protocols depend on.

---

# Part 1: Problem and Scope

## 1. Objective

Serve `getTransaction(hash)` for any transaction whose ledger falls within the retention window (full history by default):

- **Complete.** Every transaction in every in-retention ledger is resolvable by hash — no gaps, including across crashes, restarts, and retention changes — with one quantified residual: the cold index keys on streamhash's 128-bit routing key (the hash's first 16 bytes, §6.1), so two distinct in-retention hashes that share a 16-byte prefix *and fall in the same window* collide as one index key. That is a ~10⁻²⁰-per-dense-window event, and it is *fail-stop* rather than silent — the window's index build fails loudly rather than dropping or mis-resolving a transaction. (A shared prefix across *different* windows is harmless: the two never meet in one build, and the read path's verify rejects the resulting cross-window false positive — §8.2.) The guarantee is otherwise absolute.
- **Correct.** A lookup never returns the wrong transaction; a missing or out-of-retention one returns not-found.
- **No in-memory index.** The hash→seq map is on-disk `.idx` files (read through the page cache), not a RAM-resident structure sized to the transaction count — so the daemon holds no memory proportional to the number of transactions in history. (A lookup probes one `.idx` per in-retention window — a hash carries no window hint; that probe set and its cost are the query-routing design's concern.)
- **Cheap to maintain.** Ingestion adds negligible cost to the per-ledger write path, and the cold index stays current with a rebuild that is small relative to its cadence.

Out of scope: how readers obtain the daemon's current coverage and dispatch between tiers across rebuild/freeze/discard transitions (the query-routing design), and the storage of the transactions themselves (the ledger store — `.pack` files and the hot `ledgers` CF — covered by the streaming doc and the packfile library doc).

## 2. Lookup model

`getTransaction` takes a 32-byte transaction hash and returns the transaction's envelope, result, and meta, plus its ledger and close time. The data flow:

```
hash ──► seq ──► LCM for seq ──► extract the tx ──► verify hash ──► respond
      (this doc)  (ledger store)
```

The subsystem this document owns is the **hash → seq map**, plus the read-path rules that make the final fetch correct. Three properties of the key space shape the design:

- **Point lookups only.** There are no range or prefix queries over tx hashes, so order-preserving structures buy nothing — perfect-hash structures apply.
- **Hashes are uniform and immutable.** A transaction hash is never updated and corresponds to at most one applied transaction (the network's replay protection); the map is append-only, one batch of entries per ledger.
- **The full transaction is always fetched anyway.** The response needs the envelope/result/meta, so the read path always ends with the ledger store and can verify the full hash against the fetched transaction. The map therefore doesn't need to be exact — only *complete* (no false negatives; the one residual, a same-window 16-byte prefix collision, is fail-stop rather than a silent miss — §8.2); false positives are screened first by a fingerprint and finally by the fetch-and-verify step.

---

# Part 2: Architecture

## 3. The two tiers

An in-retention transaction is stored in exactly one place — one tier, one window, never duplicated — but a bare hash doesn't say *which*, so a lookup probes every home (§8.1) and at most one confirms (none, if the hash isn't there). The two homes:

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

The handoff is gap-free by write ordering: a chunk's hot DB is discarded only after the durable `.idx` covers the chunk (the streaming doc's discard stage gates on exactly this). Between a chunk's freeze and its coverage — normally one lifecycle tick, since freeze, rebuild, and discard chain within the tick — the chunk is served from its still-present hot DB.

There is **no dedicated transaction store at any layer**. The map resolves hashes to sequences; transaction bytes live in the ledger store (`ledgers` CF while hot, `.pack` files when cold).

## 4. Geometry

Two units organize the map; both are defined in the streaming doc and restated here because every structure below is named by them:

- **Chunk** — 10,000 ledgers (hardcoded). The unit of the hot DB and of the sorted runs.
- **Window** — `chunks_per_txhash_index` chunks (default 1000 = 10M ledgers). The unit of the cold index. Configurable, but pinned in the meta store on first start and immutable thereafter.

```
chunkID(seq)        = (seq - 2) / 10_000
chunkFirstLedger(c) = c * 10_000 + 2
chunkLastLedger(c)  = (c + 1) * 10_000 + 1
indexID(c)          = c / chunks_per_txhash_index       # takes a CHUNK id
chunksInIndex(w)    = [w*cpi, (w+1)*cpi - 1]            # cpi = chunks_per_txhash_index
```

With the default `chunks_per_txhash_index = 1000`: window 0 spans ledgers 2–10,000,001 (chunks 0–999), window N spans N×10M+2 – (N+1)×10M+1 (chunks N×1000 – (N+1)×1000−1). All ids zero-pad `%08d`.

---

# Part 3: Implementation Reference

## 5. Hot tier

### 5.1 Storage

The `txhash` CF is one column family of the per-chunk hot RocksDB (alongside `ledgers` and the events CFs — see the streaming doc's data model):

- **Key**: the full 32-byte transaction hash.
- **Value**: the 4-byte ledger sequence.

Full-key storage means the hot tier is *exact*: a lookup of a hash not in the chunk simply misses, with no fingerprint or verification subtleties. The CF carries its own tuning options (point-lookup-oriented: bloom filters, no ordering requirements) independent of its siblings.

### 5.2 Write path

The hot write path is the streaming doc's ingestion loop verbatim: each ledger commits as **one atomic, synced WriteBatch across all CFs** of the chunk's hot DB, and `putTxHashes` contributes one `(hash, seq)` entry per transaction in the LCM. A ledger's hashes are either all present or all absent; there is no separate tx-hash durability boundary to reason about.

### 5.3 Lifetime

The chunk's hot DB is created when ingestion enters the chunk and discarded whole once every cold artifact derived from the chunk is durable **and** the window index covers the chunk. The `txhash` CF is the reason for the *and*: the `.bin` (below) is never a serving tier, so without the coverage gate there would be a window where a frozen chunk's hashes had no queryable home. In steady state the freeze-to-coverage interval is the one tick in which the boundary's `ChunkBuild` and `IndexBuild` run; after catch-up or crashes it can span longer — the discard scan re-derives eligibility from durable keys, so the hot DB simply persists until coverage genuinely lands.

## 6. Cold artifacts

Two artifact kinds, both produced and cataloged under the streaming doc's one write protocol (mark `"freezing"` before any I/O → write → fsync file + dirents → flip `"frozen"`):

### 6.1 The per-chunk sorted run: `.bin`

`txhash/raw/{bucket:05d}/{chunk:08d}.bin`, meta-store key `chunk:{chunk:08d}:txhash`. Produced by `processChunk` in the same streaming pass that writes the chunk's `.pack` and events segment — per ledger, the tx-hash extractor collects entries; at the end of the pass they are **sorted in memory** (~3M entries ≈ 60 MB for a dense chunk — negligible) and written out.

**Format** (the streamhash merge format):

```
uint64 LE        entry count
entry × count    20 bytes each: [key: 16][seq: 4 LE]
```

- `key` is the **first 16 bytes of the transaction hash** — streamhash's routing-key width: it derives the perfect-hash slot from these 16 bytes alone, both when this run is built and when a lookup probes (§8.1). Two distinct in-retention hashes sharing these 16 bytes are a true duplicate key only when they land in the *same* window's build; across windows the shared prefix is harmless. Both cases, and their handling, are §8.2.
- Entries are sorted ascending by the **big-endian `uint64` prefix of `key`**.

The `.bin` is a *map-side-sorted run*, never a serving tier. Sorted runs are what make the rolling rebuild cheap: the index builder consumes them in a single streaming k-way merge, instead of the two passes (count, then add) it needs over unsorted input.

A `.bin` lives as long as its window needs it as rebuild input: every boundary re-merges **all** of the current window's runs, so they are retained for the whole life of the window and demoted en masse by the terminal build's commit batch (§7.3), then swept.

### 6.2 The per-window index: `.idx`

`txhash/index/{window:08d}/{lo:08d}-{hi:08d}.idx`, meta-store key `index:{window:08d}:{lo:08d}:{hi:08d}`. One streamhash minimal-perfect-hash file per **coverage**, built by streamhash's `SortedBuilder` over the k-way merge of `.bin[lo..hi]`, with the cold-txhash option set:

- **Payload: `payloadWidth` bytes** — the ledger seq stored as an offset from `MinLedger`, where `MinLedger = chunkFirstLedger(lo)` is derived from the build range. The width is sized to the window so the format never caps `chunks_per_txhash_index`: `payloadWidth = ceil(log2(chunks_per_txhash_index * 10_000) / 8)`, the bytes needed to hold the largest in-window offset (`chunks_per_txhash_index * 10_000 - 1`). At the default 1000 chunks (10M ledgers) this is **3 bytes** — a 24-bit offset spans 16.77M ledgers — and a window of 1678+ chunks (>16.77M ledgers) widens it to 4. (The format imposes no upper bound of its own; `chunks_per_txhash_index` is independently capped at `MaxChunksPerTxhashIndex` ≈ 429,496 — `floor(2³²/10_000)` — by the streaming doc's `validateConfig` so a window's ledger span always fits a uint32 seq, which is why the offset never needs more than 4 bytes.) Since `chunks_per_txhash_index` is immutable once stored, the width is fixed for every window's life; streamhash records it in the index file header (recovered when the file is opened), while `MinLedger` — which streamhash itself does not model — rides in the user-metadata slot. Both are read back at lookup time; no sidecar metadata.
- **Fingerprint: `fpWidth` bytes (default 1)** — a streamhash option screening foreign keys before fetch-and-verify. Since a hash lookup probes every in-retention window (§8.2), a wider fingerprint trades index size (+1 byte/tx) for fewer false-positive fetches across those windows. Fixed per build, like `payloadWidth`.

All-in, at the default 3-byte payload the index costs ≈4.2 bytes per transaction (MPHF structure + payload + fingerprint) — ≈12.5 GB for a dense full window, versus the ≈60 GB of `.bin` runs it consumes. A window past the 4-byte payload threshold adds one byte per transaction.

These formats match the measured pipeline in the bench harness (`bench-fullhistory`: `cold-ingest --types=txhash` + `build-txhash-index`), which is where the performance figures in Part 4 come from — adopting the formats unchanged is what makes those figures transfer to this design.

### 6.3 Keys, coverage, and the uniqueness invariant

An index key's **name carries the coverage; the value carries only lifecycle state** (`"freezing"` / `"frozen"` / `"pruning"`, with the same meanings as every artifact key in the system). The filename is derived from the key by a fixed bijection, so resolving a key to its file never reads the value or lists a directory — every file on disk, including a crashed attempt's partial, is reachable from its key alone.

- **Coverage is the whole identity** — there is no per-attempt counter. A retry of a crashed build re-marks the same key and rewrites the same file from scratch. The file readers hold is never a writer's target: a file is writable only under a key that has never been `"frozen"` in this run, and a scheduled build's coverage always differs from the window's frozen coverage — equality is precisely the case the build's skip check returns on.
- **`lo`** — the lowest chunk the perfect hash covers. Defaults to the window's first chunk; rises above that when `earliest_ledger` or the sliding retention floor cuts into the window at build time. `lo` rising is how a mid-window floor is encoded — there is no other floor representation in the index.
- **`hi`** — the highest chunk the perfect hash covers. For the *current* window (the one the network tip is in), `hi` advances by one chunk on each boundary. For a *finalized* window, `hi` is the window's last chunk.
- **Terminal-ness is derived, not stored**: the key whose `hi` equals its window's last chunk (`windowLastChunk` is computable forever from the window id and the immutable `chunks_per_txhash_index` pin). A window whose frozen key is terminal is finalized — never rebuilt again (only a retention-widening catch-up re-derives it, at its new, wider coverage). There is no `"finalized"` value and no marker: it would be a second copy of a derivable fact.

**The uniqueness invariant: at most one coverage per window is `"frozen"` at any moment — at all times, not just at quiescence.** The rebuild's commit is one atomic synced batch that promotes the new coverage and demotes its predecessor in the same write, so the frozen coverage changes hands atomically. Readers resolve "the window's index" as *the unique frozen key*: no tie-break, no value parsing. Everything else under the window's prefix is transient debris with an unambiguous disposition: `"freezing"` = a crashed attempt — re-marked and overwritten if its coverage is built again, otherwise swept; `"pruning"` = a superseded coverage — finish the unlink and drop the key.

So the `.idx` hashes exactly the transactions in chunks `[lo, hi]`. Chunks below `lo` are out of scope (floor); chunks above `hi` are not yet folded in — their lookups are served from their chunks' hot DBs until the next rebuild advances `hi`.

**Concrete example** (default `chunks_per_index = 1000`): while the tip is in chunk 5350, index 5 (chunks 5000–5999) is the current window. If the floor sits at chunk 5100, the store holds `index:00000005:00005100:00005349 = "frozen"` and the live file is `txhash/index/00000005/00005100-00005349.idx` — covering chunks 5100–5349, with chunk 5350 still streaming into its hot DB and chunks 5000–5099 below the floor. The next boundary puts `index:00000005:00005100:00005350 = "freezing"`, writes and fsyncs `00005100-00005350.idx`, then commits the batch {`[5100,5350]` → `"frozen"`, `[5100,5349]` → `"pruning"`}; the eager sweep unlinks the superseded file and deletes its key right after the commit.

## 7. The rolling rebuild

### 7.1 Why rebuild from scratch on every boundary

The current window's index is **re-derived from scratch on every chunk boundary** to absorb the chunk that just froze, growing until its window completes. Only the window the tip is in is ever rebuilt; a finalized window's index is static.

The rebuild is cheap relative to its cadence: a full-window build is ≈1 minute against a chunk boundary every ~14 hours at mainnet rates (Part 4). That headroom is what lets the index be rebuilt whole from sorted inputs every boundary, rather than updated incrementally:

- There is no incremental-update machinery and no partially-updated index state for a crash to expose. Every `.idx` on disk is a complete, deterministic function of its coverage.
- `lo` tracks the floor and `hi` tracks the tip automatically — no separate floor-driven rebuild is ever needed while the window is current.
- Catch-up and the steady-state tick share the build path identically (one scheduler, one set of postconditions — see §9), so there is no second regime to verify.
- A same-coverage rebuild writes byte-identical output, which collapses crash recovery to "re-mark and rewrite" with no salvage analysis.

### 7.2 The build protocol

`buildTxhashIndex(w, lo, hi)` runs the one write protocol with a batch-commit extension. The covered range is explicit: `lo` defaults to the window's first chunk, rising to `chunkID(effectiveRetentionFloor)` when the floor cuts in; `hi` is the highest frozen chunk in the window — the window's last chunk once it's complete, lower while it's still filling.

1. **Skip check**: if the window's unique frozen key already covers exactly `[lo, hi]`, return — there is nothing to write, and any leftover transient keys are the sweeps' job, not the builder's. (A frozen key covering the full window is terminal by definition, so the skip also covers re-scheduled builds of finalized windows, which must not demand `.bin` inputs the sweep has deleted.)
2. **Mark**: put `index:{w:08d}:{lo:08d}:{hi:08d}` = `"freezing"` — an idempotent overwrite when a crashed attempt (or a demoted coverage made desired again by a cross-restart regression) left the key behind. The build is **terminal** iff `hi` is the window's last chunk — a derived property, marked nowhere.
3. **Write**: k-way merge the sorted `.bin` files for chunks `[lo, hi]` into streamhash's `SortedBuilder`, writing `{lo:08d}-{hi:08d}.idx` — created or truncated wholesale; a writer only ever holds a file whose key is non-frozen, never one a reader can resolve. Fsync the file and its dir (and the dir's own dirent when this build created the window dir — the first build of a window).
4. **Commit**: one atomic synced batch — this coverage `"freezing"` → `"frozen"`; the window's predecessor frozen coverage (if any) → `"pruning"`; and iff this is the terminal build, every `chunk:{c}:txhash` key in the window → `"pruning"`. This batch is the *entire* finalization protocol — there is no separate cleanup step; the demoted keys become ordinary sweep work.

Precondition: every chunk in `[lo, hi]` has `chunk:{c}:txhash == "frozen"` (its `.bin` exists). The function fails loudly if violated — checked before any key is touched, which is also the backstop for a build whose input task failed in the executor (the streaming doc's done-channels broadcast completion, not success).

```go
func buildTxhashIndex(w WindowID, lo, hi ChunkID, cat Catalog) error {
	// Step 1 — skip check. Also covers re-scheduled builds of finalized
	// windows, whose .bin inputs the sweeps may already have removed.
	if fk := frozenCoverage(cat, w); fk != nil && fk.Lo == lo && fk.Hi == hi {
		return nil
	}
	// Precondition, checked loudly before any write.
	for c := lo; c <= hi; c++ {
		if cat.State(c, TxHashBin) != Frozen {
			return fmt.Errorf("window %d: chunk %d .bin not frozen", w, c)
		}
	}

	// Step 2 — mark the coverage. Re-marking a crashed attempt's key (or a
	// demoted coverage a cross-restart regression made desired again) is an
	// idempotent overwrite; the file is rewritten wholesale either way.
	terminal := hi == windowLastChunk(w) // derived; no marker anywhere
	key := indexKey(w, lo, hi)
	cat.Put(key, "freezing")

	// Step 3 — write the coverage's file from scratch (create-or-truncate:
	// a crashed attempt's partial is overwritten wholesale, never appended).
	f := createTruncate(indexFilePath(key))
	merge := newKWayMerge(binPaths(lo, hi))  // sorted runs → one streaming pass
	sb := streamhash.NewSortedBuilder(f, coldTxhashOptions(lo)) // §6.2 options
	for merge.Next() {
		sb.Add(merge.Entry())
	}
	sb.Finish()
	fsyncFile(f)
	fsyncDir(indexWindowDir(w)) // + fsyncParentDir on the window dir's first build

	// Step 4 — commit: ONE atomic synced batch, the entire finalization
	// protocol. Note: no file is unlinked here, ever — the batch only
	// DEMOTES keys, and deletion is exclusively the sweeps' job (§7.4):
	// eagerly by buildThenSweep right after this batch, in both regimes;
	// the tick's prune scan is the crash backstop.
	batch := cat.NewBatch()
	batch.Put(key, "frozen")
	if prev := frozenCoverage(cat, w); prev != nil {
		batch.Put(prev.Key, "pruning") // supersede the predecessor — a distinct
		//                                key: the skip check returned if the
		//                                frozen coverage equaled [lo, hi]
	}
	if terminal { // demote every input key in the window
		for c := windowFirstChunk(w); c <= hi; c++ {
			if cat.Has(chunkKey(c, TxHashBin)) {
				batch.Put(chunkKey(c, TxHashBin), "pruning")
			}
		}
	}
	batch.Commit()
	return nil
}
```

### 7.3 Finalization

A window finalizes when its terminal build's commit batch lands — the same write that freezes the full-coverage `.idx` demotes every `chunk:{c}:txhash` key in the window to `"pruning"`. Finalization is therefore never a separate step, for either caller: catch-up calls the same function for every window its range overlaps (a complete window's full desired range is a terminal build; the trailing window's producible range is non-terminal), and the boundary tick's window-end rebuild is terminal by arithmetic.

After finalization the `.idx` is static. If the floor later advances *within* the finalized window, the file becomes stale (`lo` references chunks pruning has since removed) — deliberately tolerated: index keys are swept only when their window falls *wholly* past the floor, and the read path handles the straddling case (§8.4). A window-straddling floor exists at most once at any moment — the window containing the effective retention floor.

### 7.4 Sweeps and disk bounds

The commit batch only demotes keys; all file deletion happens through the streaming doc's key-driven sweeps (unlink → `fsyncDir` → delete key — key absent ⟹ file gone). Two call sites:

- **Eagerly, inside every `IndexBuild`'s execution** (`buildThenSweep`, right after the commit batch, in both regimes): sweep the window's superseded coverage and, after a terminal build, its demoted `.bin` inputs. The sweep is **window-local** — it walks only this window's keys, so concurrent windows' sweeps touch disjoint keys and files.
- **The tick's prune scan** — the crash backstop, and the owner of retention pruning. A `"freezing"` index key it observes was *not* retried (builds run before the sweep in every regime), so its coverage is no longer desired: **delete file and key, never salvage**. The file might even be complete, but proving that buys nothing — a rebuild re-derives identical bytes — so one no-questions rule covers every crashed attempt.

The eager site is what bounds disk. Without it, a long backfill would accumulate every finalized window's demoted `.bin`s until the first tick (≈20 bytes per transaction across all of history); with it, transient `.bin` disk is bounded by the windows actually in flight — the floor is one dense window's worth (≈60 GB), irreducible because a window's build merges all of its runs at once.

**Provisioning note**: the old and new coverage files coexist from the start of a rebuild's write until the eager sweep's unlink, so the window dir transiently holds ~2× the index size (~25 GB at the end of a dense full window), and the window-end rebuild writes ~12.5 GB in ~1 minute (~200 MB/s burst) — trivial on instance NVMe, but worth provisioning for on throughput-capped volumes like EBS gp3.

### 7.5 Why rewriting coverage-named files in place is safe

The hazard: a reader holds the live `.idx` open while the next coverage is written into the same window directory. Four facts make the in-place rewrite safe anyway:

1. **The skip rule.** A build's target name equals the live file's name only when its coverage equals the frozen coverage — which is exactly the case the skip check returns on. So no scheduled build ever opens the file readers resolve.
2. **Stage ordering and sweep scope.** No sweep runs where a build could collide with it: the `"freezing"`-key sweep — the only sweep that can touch a name a future build may target — lives solely in the tick's prune scan, which follows the plan stage (and catch-up precedes the lifecycle goroutine entirely); the eager sweep inside `buildThenSweep` touches only `"pruning"` keys in its own window, strictly after that window's commit; and a plan holds at most one `IndexBuild` per window — so concurrent windows' sweeps and builds touch disjoint keys and files.
3. **Floor monotonicity and reader lifetime.** Desired coverage is monotone within a run, and reader file handles die with the process — so a name any reader has resolved is never rewritten while held.
4. **Determinism.** A same-coverage rebuild writes identical bytes regardless — the merge is a deterministic function of the coverage — leaving a partial file under a `"freezing"` key as the only hazardous state, and no reader resolves a non-frozen key.

A change to any of the four — the skip rule, the stage ordering or the eager sweep's window-locality and pruning-only scope, the floor's monotonicity, or reader lifetime — must re-prove this argument.

### 7.6 Crash matrix

| Crash point | Durable state left | Convergence |
|---|---|---|
| after step 2, or mid step 3 | predecessor coverage still `"frozen"` (readers unaffected); new key `"freezing"`, file absent/partial/complete | next build of the coverage re-marks and rewrites wholesale; if the coverage is no longer desired, the prune scan deletes file + key unread |
| after step 4, before the eager sweep | new coverage `"frozen"` and live; predecessor `"pruning"`; terminal: window's `.bin` keys `"pruning"` | the sweeps finish — eager on the next build, prune scan otherwise |
| mid-sweep | `"pruning"` key outlives the durable unlink | the sweep re-runs; key absent ⟹ file gone |

At no crash instant are two coverages frozen, or none (once the window has one), or a `"frozen"` `chunk:{c}:txhash` key whose `.bin` has been deleted — the commit batch only ever demotes keys, and files are touched exclusively by the sweeps, under non-frozen keys. This is what lets the catch-up resolver (§9) trust `"frozen"` blindly.

## 8. Query path

### 8.1 Routing

A hash names no ledger, so the reader cannot know which home holds it in advance — it **probes them all**, and the hash resolves in exactly one:

| Tier | Probe set | How |
|---|---|---|
| cold — one `.idx` per window | **every in-retention window** | MPHF + fingerprint + verify (§8.2) |
| hot — `txhash` CF per chunk | the chunks above any window's `hi` (live, or frozen awaiting coverage) | exact full-key get (§8.3) |

The hot tier is a few chunks at most — one window's tail, normally just the live chunk — so the probe set is `≈ (in-retention windows) + (a handful of chunks)`. How the reader learns current coverage and stays consistent across rebuilds is the query-routing design's concern; this document requires only that the homes' union covers the retention window — guaranteed by the discard gate (§5.3) and the uniqueness invariant (§6.3) — and that each ledger has exactly one home, so **at most one probe confirms** — the verify runs on every fingerprint hit but succeeds for at most one.

### 8.2 Cold lookup

The cold tier **probes every in-retention window's `.idx`** — a hash gives no window hint (the window is `chunkID(seq) / chunks_per_txhash_index`, and `seq` is exactly what the lookup is trying to find), so there is nothing to pre-select. Each window probe:

```
for each in-retention window (its unique "frozen" key → {lo}-{hi}.idx):
  → MPHF probe on the hash's 16-byte prefix
  → fingerprint check (fpWidth bytes)             — miss ⇒ skip this window
  → on a fingerprint hit:
       seq = MinLedger + payload (payloadWidth bytes)
       retention gate: seq ≥ floor?               — else skip this window
       fetch the LCM for seq, extract the tx
       verify the full 32-byte hash               — confirms, or rejects a false positive
respond on the confirmed hit; not-found if no window confirms
```

Because the hash belongs to at most one window, **at most one window confirms**; a not-found lookup — a non-existent or not-yet-ingested hash — confirms none and must rule out every in-retention window.

The final verification is **mandatory, not defensive**: a minimal perfect hash maps *any* probe key to some slot, so a hash that is not in the set resolves to an arbitrary entry — the fingerprint screens most foreign keys, and the fetch-and-verify rejects the remainder.

A **16-byte prefix collision between two distinct in-retention transactions** has two cases, and only one bounds completeness. The cold index keys on streamhash's 128-bit routing key (§6.1), so two hashes sharing their first 16 bytes are indistinguishable *to a single window's build*.

*Different windows* — the more likely of the two, since a shared prefix is far more apt to straddle two of history's windows than to fall inside one. Each transaction keys into its own window's `.idx`, so neither build sees a duplicate and both transactions resolve normally. The collision shows up only as a fingerprint false-positive when a lookup probes the *other* window: that window's MPHF maps the shared prefix to its own resident transaction, the fingerprint (also derived from those 16 bytes) matches, and the fetch-and-verify above rejects it because the full 32-byte hashes differ. This is exactly the foreign-key path the verify already exists for — one wasted ledger fetch, no wrong answer and no false negative.

*Same window* — the genuine residual. The two are a single key to that window's builder, so streamhash rejects the duplicate at build time (`ErrDuplicateKey`) and the window's build fails **loudly and deterministically** — never a silently dropped transaction (the false not-found a keep-one-payload index would produce), and never, thanks to the verify, a wrong transaction. This same-window case is the sole bound on completeness; the birthday bound over a dense window's ~3×10⁹ keys against 2¹²⁸ puts it at ~10⁻²⁰ per window — a cryptographic-scale probability accepted as negligible, comparable to the undetected storage bit-errors the design likewise does not defend against. The design carries no per-key full hash or collision list to drive it to zero, because at 10⁻²⁰ that machinery would cost far more than the risk it removes.

**Probe ordering, parallelism, early-stop, and the resulting latency and I/O are the query-routing design's concern** (§8.1), out of scope here.

### 8.3 Hot lookup

Chunks above `hi` are probed in their hot DBs' `txhash` CF — an exact full-key point get, so misses are genuine misses with no verification subtleties (the fetch-and-verify still runs, as the response needs the transaction anyway). In steady state this tier is the live chunk plus, briefly, the chunk inside the freeze-to-coverage interval; after catch-up or a crash it can be several chunks, shrinking as rebuilds advance `hi`.

### 8.4 Reads and concurrent pruning

The cold-tier lifecycle unlinks files concurrently with in-flight reads — sweeps remove superseded `.idx` coverages (§7.4), and retention pruning removes a window's `.idx` and its chunks' `.pack`s. What makes that safe to do **unilaterally** is the streaming doc's reader retention contract: a read for any seq below `effectiveRetentionFloor` is not-found regardless of what is still on disk. How a read stays correct across these transitions otherwise — tier dispatch, coverage re-resolution, a file that vanishes mid-read — is the **query-routing design's** concern (§8.1), out of scope here.

## 9. Catch-up and recovery interaction

The cold tier converges from any state through the streaming doc's postcondition-driven resolver; the tx-hash kind contributes the one **per-window rule** (every other kind is per-chunk):

For each window overlapping the catch-up range, compare the **stored** coverage — `{lo, hi}` from the name of the window's unique frozen index key — with the **desired** coverage `[max(window_start, chunkID(floor)), min(window_last_chunk, range_end)]`. The upper cap is what makes the rule uniform: for a complete window it is the window's last chunk, for the trailing window it is the range end, and no special trailing case exists.

- **Desired ⊆ stored** → schedule *nothing* for this window: no `.bin` production, no build. Three states land here: every steady-state restart; a floor that *rose* (the stale stored `lo` is the read path's problem, §8.4 — never a rebuild trigger); and a finalized window the range ends inside.
- **Desired exceeds stored** (`desired_lo < stored_lo`, or `desired_hi > stored_hi`, or no frozen key exists) → request `.bin` production for **every** chunk in the desired range — chunks whose `.bin` is already frozen self-skip inside `processChunk`; chunks the old `.idx` covered re-derive from their local `.pack` files with no bulk-backend download — and emit one `buildTxhashIndex(w, desired_lo, desired_hi)`, terminal iff `desired_hi` is the window's last chunk.

Two clauses are load-bearing:

- **The `stored_hi` clause** catches downtime that crosses a window boundary: a window that was *current* at shutdown carries a frozen key with `hi <` its last chunk, and classifying by `lo` alone would see a frozen key and strand chunks `(hi, last_chunk]` permanently.
- **`"frozen"` is blindly trustable**: a `"frozen"` `chunk:{c}:txhash` key implies its `.bin` exists, unconditionally — input demotions ride the same synced write that freezes the terminal coverage, and files are deleted only by sweeps under non-frozen keys (§7.6). The resolver classifies on frozen state only; transient keys are invisible to it and are the sweeps' job.

**Retention widening** re-derives a finalized window at its new, wider coverage: `.bin`s for previously-covered chunks come from local `.pack`s; fully-pruned chunks refetch from the bulk source; the rebuild is terminal at the wider `[lo', last_chunk]` and its commit batch demotes the old coverage. This runs at the next startup — extending the bottom of storage is exclusively catch-up's job, behind backend validation — never in a tick. One corner needs help from outside the resolver: a widening that re-froze (or left mid-write) a finalized window's `.bin` keys and was then abandoned by narrowing retention back — the resolver correctly schedules nothing (desired ⊆ stored), so the tick's prune scan demotes and sweeps those provably-redundant inputs (`"frozen"` and `"freezing"` alike) — the final `.idx` covers their chunks, and the resolver never re-materializes a covered window.

In the executor, an `IndexBuild` waits on the done-channels of the chunk builds inside its coverage and draws from the same worker pool — the `.bin`s are map-side-sorted runs, the build is the per-window reduce. Done-channels broadcast completion, not success; the build's loud precondition (§7.2) is the backstop.

---

# Part 4: Capacity & Performance

## 10. Storage footprint

Per dense chunk (~3M transactions) and dense window (default 1000 chunks, ~3×10⁹ transactions):

| Structure | Unit cost | Dense chunk | Dense window | Lifetime |
|---|---|---|---|---|
| hot `txhash` CF | 36 B/tx raw (32 key + 4 value), before RocksDB overhead | ~110 MB raw | — (per-chunk) | chunk ingestion → index coverage |
| `.bin` sorted run | 20 B/tx exactly | ~60 MB | ~60 GB | chunk freeze → window finalization |
| `.idx` | ≈4.2 B/tx (3-byte payload) | — (per-window) | ~12.5 GB | build → superseded next boundary, or retention |

Transient peaks: ~2× the index size in the window dir during each rebuild (~25 GB at window end); the `.bin` floor is one dense in-flight window (~60 GB), bounded by the eager sweep (§7.4). Steady-state durable cost of the cold tier is the `.idx` files alone: ≈4.2 bytes per transaction across all retained history (at the default window; +1 B/tx past the 4-byte payload threshold).

## 11. Performance

- **Ingest, hot**: one `(hash, seq)` put per transaction inside the existing per-ledger WriteBatch — no separate sync, no separate store.
- **Ingest, cold**: the in-memory sort of ~3M entries is negligible against the chunk's streaming pass; the `.bin` write is sequential.
- **Rebuild**: a full dense window merges ~60 GB of sorted runs into a ~12.5 GB `.idx` in ≈1 minute (~200 MB/s write burst) — measured by the bench harness (`bench-fullhistory`: `cold-ingest --types=txhash` + `build-txhash-index`). Mid-window rebuilds scale with `hi − lo`. Against a ~14-hour boundary cadence at mainnet rates, the rebuild is ~0.1% duty cycle.
- **Lookup, cold**: one MPHF probe per in-retention window — fingerprint screen, then fetch-and-verify on a hit. The hash is in at most one window, so at most one fetch confirms; fingerprint false positives (bounded by `fpWidth`, §6.2) are rejected by the full-hash verify. Probe ordering, parallelism, and the resulting latency/throughput are the query-routing design's concern (§8.1).
- **Lookup, hot**: one RocksDB point get in a bloom-filtered CF, then the same ledger fetch.

---

## Related documents

- [full-history-streaming-workflow.md](./full-history-streaming-workflow.md) — the daemon this subsystem lives in: geometry, the meta store and one write protocol, `processChunk`, the resolver and executor, the lifecycle tick (freeze → rebuild → discard → prune), and the correctness invariants (INV-1 … INV-4) with their audits.
- The reader / query-routing design — how readers obtain current coverage and dispatch between hot DBs and frozen files across transitions.
- [getevents-full-history-design.md](./getevents-full-history-design.md) — the sibling subsystem (events), same hot/cold architecture over the same chunk geometry.
- [packfile-library.md](./packfile-library.md) — the `.pack` format the read path's ledger fetch lands on.
- `bench-fullhistory` — the measurement harness behind every figure in Part 4.
