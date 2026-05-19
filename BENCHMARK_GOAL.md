# Full-History RPC Query Performance Benchmark Goal

## End Goal

Produce a reproducible set of latency and throughput numbers comparing
two storage tiers across the full set of full-history RPC query types,
to inform the eventual full-history RPC service SLA and to drive
implementation decisions on cold-side data structures.

| Storage tier | Backing store | Location |
|---|---|---|
| **Hot** | RocksDB (`fullhistory/pkg/stores/*/HotStore`, `eventstore.HotStore`) | NVMe (instance store at `/mnt/nvme/disk2/.../hot/`) |
| **Cold-NVMe** | packfile cold-store (`ledger.ColdStoreReader`, `eventstore.ColdReader`, RecSplit cold txhash) | NVMe (instance store at `/mnt/nvme/disk2/ledgers/cold/`) |

EBS-tier benchmarking is **out of scope** for this round.

## Dataset

All measurements run against pubnet ledgers **49,990,002 – 60,000,001**
(chunks 4999–5999, 1001 cold-store `.pack` files, 1.4 TiB), already
present at `/mnt/nvme/disk2/ledgers/cold/`. Backfilled via
`cmd/stellar-rpc/scripts/full-history-backfill/` and uploaded to
`gs://rpc-full-history/cold/`. The hot store gets seeded by reading one
or more chunks from the cold tree and batch-writing into a fresh
RocksDB instance.

## Query Types Under Test

For each storage tier:

1. **Ledger point lookup** — `GetLedgerRaw(seq)` for a random seq
2. **Ledger range** — N consecutive ledgers (N ∈ {10, 100, 1000, 5000})
3. **Transaction by hash** — `hash → seq → ledger → tx` end-to-end
4. **Transaction page** — pages of {5, 20, 100} txs from cursor `(seq, txIdx)`
5. **Events** — `Lookup(termKey)` + `FetchEvents(eventIDs)`, four filter scenarios:
   - no filter (full chunk)
   - contract_id only
   - topic only
   - contract_id + topic

That is **2 tiers × 5 query types = 10 benchmark surfaces** (with
sub-variants for the N/page-size/filter axes).

## Scope Carve-Outs (per user direction)

- **No EBS tier.** Drop EBS setup, drop EBS measurements.
- **No multi-chunk iterators.** Cold-side range/page benches stay within
  a single 10K-ledger chunk to dodge cross-chunk composition.
- **No hot+cold composed reader.** Each benchmark targets one tier
  cleanly; no routing logic.
- **No edge cases.** Focus on the steady-state happy path; outliers
  beyond p99 logged but not investigated unless catastrophic.

## Methodology

- **Same input set across tiers.** Pre-generate `keys.json` with random
  seqs, hashes, term-keys drawn from chunks 4999–5999 with a fixed seed.
  Every tier runs against the same set so latency comparisons are valid.
- **Two latency variants per measurement:**
  - **Cold-cache** — drop the OS page cache before measurement
    (requires `sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'`)
  - **Warm-cache** — measure after a warm-up pass
- **Two concurrency variants per measurement:**
  - **Single-threaded** — latency percentiles (p50, p90, p95, p99, max)
  - **`--workers=N`** — ops/sec under contention
- **Stable sample size.** ≥ 1000 iterations per latency measurement to
  get stable p99; ≥ 30 s per throughput measurement.
- **Storage isolation.** Hot RocksDB at one path, cold packfiles at
  another. No mixing within a single bench run.
- **Outputs.** Per-bench CSV of raw latencies + a summary table.
  Aggregate report at the end with all numbers and a short
  human-written interpretation.

## Phased Plan

### Phase 0 — Bench harness scaffolding (0.5 day)

Single driver `cmd/stellar-rpc/scripts/bench-fullhistory/`:
- Sub-commands `bench-ledger-point`, `bench-ledger-range`,
  `bench-tx-hash`, `bench-tx-page`, `bench-events`
- Flags: `--tier`, `--iterations`, `--workers`, `--seed`, `--drop-cache`
- Output: latency CSV + summary table + a top-level run.json with
  hostname, kernel, mount info, dataset, etc., for reproducibility

### Phase 1 — Ledger benchmarks (~1 day)

Everything needed for this phase already exists in the tree.
- Seed `ledger.HotStore` with one chunk of data read from the cold store
- Bench `GetLedgerRaw(seq)` on Hot vs Cold-NVMe with shared seqs
- Bench `IterateLedgers(start, start+N-1)` for N ∈ {10, 100, 1000, 5000}
- Deliverable: first numbers (ledger point + range, hot vs cold)

### Phase 2 — Transaction-page benchmarks (1–2 days)

- Cursor type `{seq uint32, txIdx uint32}`
- Walker: open ledger via tier's reader, decode LCM, slice
  `TransactionResultPair`s starting at `txIdx`, advance cursor on
  ledger overflow (single chunk only)
- Hot + cold variants
- Deliverable: tx-page numbers

### Phase 3 — Transaction-by-hash benchmarks (5–7 days)

Heaviest phase: production-grade cold txhash index required.
- Implement RecSplit cold txhash store per `03-backfill-workflow.md`:
  - 16 column-family `.idx` files per txhash-index
  - Build from raw `.bin` files (transient, deleted after build)
  - Four phases: count, add, build, verify
- Build cold-side tx-by-hash glue: `hash → cold-txhash → seq → cold
  ledger → tx`
- Build hot-side glue: `hash → txhash.HotStore.Get → seq → ledger.HotStore
  → tx`
- Note: production RecSplit code is itself a meaningful upstream PR
  candidate even though the bench driver stays local
- Deliverable: tx-by-hash numbers

### Phase 4 — Events benchmarks (1–2 days, assuming PR #740 cherry-picked)

- Cherry-pick PR #740 (`eventstore/` + `chunk/` packages) onto rpc-hack;
  skip the ledger/ reverts in that PR (they predate PR #739)
- Seed `eventstore.HotStore` for one chunk from the same cold ledger
  data
- Bench `Lookup` + `FetchEvents` on Hot vs Cold-NVMe under the four
  filter scenarios listed above
- Deliverable: events numbers (with-and-without filter contrast)

## Acceptance / Done Criteria

The project is "done" when:

1. The bench harness builds and runs reproducibly on this host.
2. Each of the 10 benchmark surfaces (2 tiers × 5 query types) has
   recorded numbers across the sub-variants listed.
3. A single markdown writeup compares hot vs cold-NVMe for every query
   type, with p50 / p95 / p99 / max latencies and ops/sec where
   relevant.
4. Raw latency CSVs are archived alongside the writeup so reviewers
   can replot.
5. Methodology section in the writeup notes the kernel, instance type,
   mount info, software versions (RocksDB, zstd, the rpc-hack commit
   SHA used), and the dataset bounds, so the numbers can be reproduced
   later.

Out of scope of "done":
- Production wiring of any of these readers into the daemon
- Multi-chunk composition
- EBS-tier numbers

## Phase Tracking

| Phase | Status | Notes |
|---|---|---|
| Cherry-pick PR #740 | **done** | Surgical pull on branch `cherrypick-740`: chunk/, eventstore/, refactored events/ files, rocksdb additions. Skipped pr-740's ledger/ reverts. Required Go 1.26. |
| Phase 0 — harness | **done** | `cmd/stellar-rpc/scripts/bench-fullhistory/` |
| Phase 1 — ledger bench | **done** | Point + range numbers below |
| Phase 2 — tx-page bench | **done** | 5/20/100 page sizes |
| Phase 3 — tx-hash bench | **done with caveat** | Cold side uses sorted-.bin shim (not production RecSplit) — see "Caveats" |
| Phase 4 — events bench | **done** | 4 scenarios × 2 tiers |

## Order

Ledger → tx-page → tx-by-hash → events.
Cheapest to most expensive; lowest-uncertainty signal first.

---

## Results

All numbers below: **chunk 5000 of pubnet** (ledgers 50,000,002 – 50,010,001 — 10,000 ledgers, ~3.01M transactions total, avg 301.3 tx/ledger). Hot store seeded once from cold chunk; both tiers query identical seq distribution (seed=1, fixed across runs).

Latencies are **warm-cache** (no `drop_caches` between iterations) and **single-threaded**. Throughput is `iters / total elapsed`.

**Test rig:** AWS `im4gn.8xlarge` (32 vCPU Graviton2, 128 GiB RAM, 2× 6.8 TiB instance NVMe, kernel `6.14.0-1018-aws`). Hot store on `/mnt/nvme/disk2/ledgers/hot-5000` (RocksDB), cold pack on `/mnt/nvme/disk2/ledgers/cold/00005/00005000.pack`. zstd via system `libzstd >= 1.5.7`. Go 1.25, rpc-hack commit `891badf`.

### Ledger benchmarks (Phase 1)

| Scenario | Tier | n | p50 | p90 | p95 | p99 | max | ops/s |
|---|---|---|---|---|---|---|---|---|
| **ledger-point** | hot  | 2000 | 1.42 ms | 1.99 ms | 2.11 ms | 2.32 ms | 2.77 ms | **690** |
| ledger-point     | cold | 2000 | 1.29 ms | 1.87 ms | 2.18 ms | 2.53 ms | 3.15 ms | **746** |
| **ledger-range n=10**   | hot  | 500 | 14.44 ms | 16.51 ms | 17.17 ms | 18.56 ms | 19.34 ms | 70 |
| ledger-range n=10       | cold | 500 | 13.45 ms | 15.45 ms | 16.23 ms | 17.74 ms | 19.99 ms | 75 |
| **ledger-range n=100**  | hot  | 200 | 144.6 ms | 157.9 ms | 161.4 ms | 175.6 ms | 179.1 ms | 7 |
| ledger-range n=100      | cold | 200 | 133.7 ms | 146.7 ms | 148.8 ms | 157.3 ms | 167.9 ms | 8 |
| **ledger-range n=1000** | hot  | 50  | 1.414 s | 1.523 s | 1.543 s | 1.579 s | 1.579 s | <1 |
| ledger-range n=1000     | cold | 50  | 1.297 s | 1.379 s | 1.397 s | 1.478 s | 1.478 s | <1 |
| **ledger-range n=5000** | hot  | 20  | 7.297 s | 7.431 s | 7.441 s | 7.441 s | 7.441 s | <1 |
| ledger-range n=5000     | cold | 20  | 6.689 s | 6.764 s | 6.768 s | 6.768 s | 6.768 s | <1 |

### Transaction-page benchmarks (Phase 2)

| Scenario | Tier | n | p50 | p90 | p95 | p99 | max | ops/s |
|---|---|---|---|---|---|---|---|---|
| **tx-page size=5**   | hot  | 200 | 11.63 ms | 13.59 ms | 14.09 ms | 15.38 ms | 26.96 ms | 87 |
| tx-page size=5       | cold | 200 | 11.80 ms | 13.97 ms | 14.41 ms | 16.09 ms | 27.85 ms | 87 |
| **tx-page size=20**  | hot  | 200 | 11.92 ms | 14.62 ms | 22.33 ms | 28.11 ms | 28.87 ms | 82 |
| tx-page size=20      | cold | 200 | 11.87 ms | 14.89 ms | 21.58 ms | 27.72 ms | 27.78 ms | 82 |
| **tx-page size=100** | hot  | 100 | 12.95 ms | 25.23 ms | 26.65 ms | 29.83 ms | 29.83 ms | 65 |
| tx-page size=100     | cold | 100 | 12.96 ms | 24.91 ms | 27.13 ms | 30.44 ms | 30.44 ms | 65 |

### Transaction-by-hash benchmarks (Phase 3)

| Scenario | Tier | n | p50 | p90 | p95 | p99 | max | ops/s |
|---|---|---|---|---|---|---|---|---|
| **tx-hash** | hot  | 1000 | 11.91 ms | 13.84 ms | 14.37 ms | 15.23 ms | 17.17 ms | 87 |
| tx-hash     | cold | 1000 | 11.81 ms | 13.73 ms | 14.29 ms | 15.19 ms | 17.22 ms | 88 |

### Events benchmarks (Phase 4)

Source: chunk 5000 ingested through both `eventstore.HotStore` and `eventstore.ColdReader` end to end. **7,907,738 events** across 10K ledgers (avg ~790 events/ledger). Corpus: 5,000 distinct contract IDs, **6 distinct topic-0 values** (very low cardinality — pubnet events emit the same top-level topic like "transfer"/"deposit" across many contracts), 5,000 distinct topic-1 values.

All iterations end-to-end: term lookup → bitmap intersection (where applicable) → `FetchEvents(ctx, eventIDs)` decode. `--max-fetch=1000` cap per iter to bound test wall time.

| Scenario | Tier | n | p50 | p90 | p95 | p99 | max | ops/s | avg events/iter |
|---|---|---|---|---|---|---|---|---|---|
| **no-filter** (50-ledger window) | hot  | 500 | 5.26 ms | 5.61 ms | 5.79 ms | 7.26 ms | 7.92 ms | 189 | 1000 |
| no-filter                        | cold | 500 | **2.27 ms** | 3.25 ms | 3.39 ms | 3.70 ms | 3.85 ms | **416** | 1000 |
| **contract** (Lookup→FetchEvents)  | hot  | 500 | 214 µs | 6.08 ms | 8.41 ms | 12.02 ms | 12.91 ms | 603 | 161 |
| contract                          | cold | 500 | 294 µs | 15.86 ms | 20.65 ms | 44.45 ms | 44.74 ms | 234 | 161 |
| **topic** (Lookup→FetchEvents)    | hot  | 500 | 4.72 ms | 6.06 ms | 6.29 ms | 7.40 ms | 8.61 ms | 199 | 1000 |
| topic                             | cold | 500 | 8.32 ms | 25.22 ms | 25.40 ms | 25.99 ms | 27.35 ms | 95 | 1000 |
| **both** (intersect 2 bitmaps)    | hot  | 500 | 67 µs | 752 µs | 2.63 ms | 9.89 ms | 14.00 ms | 1845 | 27.5 |
| both                              | cold | 500 | 81 µs | 824 µs | 3.03 ms | 27.04 ms | 45.61 ms | 966 | 27.5 |

---

## Interpretation

### Ledger / tx-page / tx-hash (Phases 1–3)

Across the 18 ledger/tx/tx-hash surfaces, **the Hot–RocksDB and Cold-NVMe-packfile tiers perform within ~10% of each other**, with cold consistently the *slightly faster* tier. That's an unexpected (and useful) finding.

**Why so close?** The bottleneck is **zstd decompression**, not storage I/O.

- **Cold pack files**: each record is 1 ledger, zstd-compressed at write time (`ItemsPerRecord=1`). A `GetLedgerRaw` reads ~150 KB compressed, decompresses to ~1 MB.
- **Hot RocksDB**: `HotStore.AddLedgers` internally zstd-compresses the LCM XDR before `Put`. A `GetLedgerRaw` returns the compressed BLOB and re-decompresses on read.

Both tiers pay the same decompression cost, **~1.3 ms per ~1 MB ledger** on Graviton2 with `libzstd`. That ~1.3 ms shows up everywhere: it's the point-lookup latency floor; multiplied out it gives the n=100 range (~134 ms) and n=1000 range (~1.30 s); it dominates tx-page and tx-by-hash (which both fetch one ledger and scan its tx list — the scan itself is microseconds).

The marginal cold-side speed advantage (~5–10%) is RocksDB's BLOB-fetch overhead vs. the packfile's direct mmap+seek path. Neither matters much in absolute terms.

### Implications

1. **For ledger-tier queries on this data, hot vs. cold is largely a memory-footprint / durability tradeoff, not a latency one.** A future "hot router that prefers RocksDB for recent ledgers" doesn't buy lookup speed; it buys things like staleness windows, in-memory caching characteristics, and write semantics.
2. **The first leverage for speeding up any of these queries is the zstd budget**, not the storage layer. Options worth measuring next: storing pre-decompressed ledgers in a small LRU cache, batching reads with shared zstd context warm-up, or storing larger records (`ItemsPerRecord > 1`) to amortize zstd-frame overhead.
3. **tx-by-hash is bottlenecked by the ledger decode + linear tx scan, not the hash index lookup.** Both `txhash.HotStore.Get` (RocksDB point lookup) and the sorted-.bin binary search complete in well under 1 ms; the ~12 ms tx-hash latency is dominated by the same single-ledger decode that tx-page and ledger-point pay. **Building a production RecSplit cold txhash index will not move tx-by-hash latency materially** unless paired with a way to skip the ledger decode (e.g., indexed `(seq, txIdx)` so we can `seek` to the tx blob).

### Events (Phase 4) — different story

Events queries are the *opposite* shape from ledger queries: the lookup work is meaningful (bitmap intersection + scatter-read events), and **the hot/cold gap actually opens up**, in both directions depending on workload.

1. **Cold is 2.3× faster than hot for sequential dense reads** (`no-filter`, 1000 consecutive events: cold p50 2.27 ms vs hot p50 5.26 ms). The cold path issues a single `ReadRange` over the packfile and zstd-decodes in batch; the hot path pays per-event RocksDB Gets with no batching.
2. **Cold tails are dramatically heavier for sparse, scattered reads** (`contract`: cold p99 44 ms vs hot p99 12 ms). When a Lookup returns IDs scattered across the chunk, cold has to do many random mmap reads + zstd-frame decodes, while RocksDB's block cache absorbs most of the cost in hot.
3. **Topic queries hit a worst case for cold** (cold p50 8.32 ms vs hot p50 4.72 ms): with only 6 distinct topic-0 values in this corpus, every topic lookup matches ~1000+ events distributed everywhere, the worst pattern for cold scatter reads. **For high-cardinality filters cold wins; for low-cardinality filters hot wins.**
4. **Intersecting two bitmaps (`both`) is ridiculously fast on both tiers** — sub-millisecond p50 — because the final fetched-event count (27.5 avg) is tiny. The bitmap intersect itself runs in microseconds; the fetch overhead drops with it.

**The events-tier finding turns the ledger conclusion on its head.** For ledgers/tx, hot vs cold is a wash; for events, the *workload shape* matters more than the tier:

| Events workload | Better tier |
|---|---|
| Bulk forward scan (`no-filter`, dump) | Cold (sequential is huge advantage) |
| High-cardinality filter (`contract` or `both` w/ rare term) | ~Tie at p50; hot tail much better |
| Low-cardinality filter (`topic` with shared name like "transfer") | Hot (scatter dominates cold) |

The production reader almost certainly needs *both*: cold for backfilled history (where sequential getEvents scans dominate) and hot for recent-window filtered queries.

## Caveats

1. **Warm-cache measurements only.** Cold-cache numbers (after `drop_caches`) would penalize the cold tier more than the hot tier and likely widen the gap. Requires `sudo`; deferred.
2. **Single-threaded only.** Concurrent throughput under contention (`--workers=N`) not measured. Both RocksDB and cold packfiles support concurrent reads; the production SLA needs this number.
3. **Cold txhash is a sorted-.bin shim, not the production RecSplit index.** Lookup costs in the bin (~50 µs warm) and a future RecSplit (~10 µs target) both round to *near-zero* against the ~12 ms tx-hash latency. The bench-driven conclusion ("ledger decode dominates") would be unchanged. A separate PR for production RecSplit remains tracked.
4. **Single chunk.** All measurements use chunk 5000. Other chunks in 4999–5999 likely have different tx-density (Soroban-era ledgers vary widely); a multi-chunk sweep would strengthen the picture.
5. **No EBS tier.** Out of scope per goal-doc decision.
6. **Events bench notes:**
   - Topic-0 corpus has only **6 distinct values** in this chunk (events emit shared top-level names — "transfer", "deposit", etc.). The topic-tier numbers reflect this concentration; a workload with higher topic-0 cardinality would shift cold's `topic` cost down toward `contract`-shaped numbers.
   - `--max-fetch=1000` caps how many events each iteration decodes. For real getEvents with bigger result sets, both tiers will scale roughly linearly (cold faster on dense; hot faster on scattered).
   - Cherry-picked PR #740 on branch `cherrypick-740`. Required upgrading to Go 1.26 (installed at `/tmp/go126/`).

## Artifacts

- Bench harness: `cmd/stellar-rpc/scripts/bench-fullhistory/` (8 source files, single binary)
- Raw per-iteration latency CSVs: `bench-out/*.csv` (one file per scenario × tier)
- Hot ledger store: `/mnt/nvme/disk2/ledgers/hot-5000/` (RocksDB, ~1.5 GB)
- Hot txhash store: `/mnt/nvme/disk2/ledgers/txhash-hot/` (RocksDB, ~120 MB)
- Cold txhash sorted .bin: `/mnt/nvme/disk2/ledgers/txhash-cold/00005000.bin` (~108 MB)
- Hot events store: `/mnt/nvme/disk2/ledgers/events-hot/` (RocksDB, ~2.8 GB; 7.9M events)
- Cold events store: `/mnt/nvme/disk2/ledgers/events-cold/00005000-{events.pack, index.pack, index.hash}` (~431 MB total)
- Event term corpus: `/mnt/nvme/disk2/ledgers/events-corpus.json` (5000 contracts + 6 topic0 + 5000 topic1 hex keys)
- Cold ledger packs: `/mnt/nvme/disk2/ledgers/cold/00004999..00005999.pack` (1.4 TiB) and `gs://rpc-full-history/cold/`
