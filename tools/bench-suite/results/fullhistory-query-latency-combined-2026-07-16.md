# Full-history query POC — combined latency results (2026-07-16)

Two complementary measurement regimes for the full-history query POC, merged:

| Source branch | Regime | Tiers | Ledgers |
|---|---|---|---|
| `poc/fullhistory-query` | **Synthetic apply-load profiles** (sac / token / soroswap) | hot + cold | large: 10–17 MB decompressed LCM, thousands of tx + Soroban meta |
| `poc/fullhistory-query-localrun` | **Live pubnet, hot-DB tip querying** | hot only | real pubnet tip, ~1.3–1.7 MB LCM |

- Synthetic source: [`fullhistory-latency-2026-07-16/README.md`](./fullhistory-latency-2026-07-16/README.md) · [`latency.csv`](./fullhistory-latency-2026-07-16/latency.csv)
- Live-pubnet source: [`local-run/BENCHMARKS.md`](../../../cmd/stellar-rpc/internal/fullhistory/local-run/BENCHMARKS.md)

The two were run on **different hardware** — see per-section environment rows. Compare
*shapes and ratios* across them, not raw absolute numbers.

---

# Part A — Synthetic ledger profiles (hot + cold tier)

Branch `poc/fullhistory-query`. Real profile ledgers replayed through the daemon.

**Environment:** AWS `c6id.8xlarge` — Intel Xeon Platinum 8375C, 32 vCPU, 64 GiB, local NVMe.
Harness: `TestServeE2E_ProfileLatency` (hot) / `TestServeE2E_ColdQueryLatency` (cold) /
`BenchmarkHotGetLedgerRaw` (direct read). Window: 20 steady-state ledgers (10202–10221)
after a 200-ledger warmup skip. Query pass: 500 reps/endpoint, `getLedgers` at `limit=1`,
45 s per-endpoint wall budget. 0 errors throughout.

## A.0 Ledger characteristics (steady-state, per ledger)

| profile | decompressed LCM | on-disk (zstd) | compression |
|---|--:|--:|--:|
| sac | ~17.3 MB | ~2.1 MB | ~8.2× |
| token | ~16.5 MB | ~1.7 MB | ~9.5× |
| soroswap | ~10.0 MB | ~0.5 MB | ~19× |

## A.1 Ingest → durable commit, per ledger (ms)

| profile | p50 | p90 | p99 |
|---|--:|--:|--:|
| sac | 129.3 | 138.9 | 145.3 |
| token | 129.9 | 133.6 | 144.7 |
| soroswap | 76.7 | 82.6 | 85.4 |

## A.2 Query latency — HOT tier (ms)

`getLedgers` at `limit=1` (budget-capped `count`); getTransaction/getEvents 500 reps.

| profile | endpoint | count | p50 | p90 | p99 | max |
|---|---|--:|--:|--:|--:|--:|
| sac | getLedgers | 80 | 567.4 | 581.8 | 607.6 | 607.6 |
| sac | getTransaction | 500 | 18.1 | 19.2 | 25.4 | 28.9 |
| sac | getEvents | 500 | 7.8 | 8.0 | 12.0 | 12.6 |
| token | getLedgers | 82 | 550.8 | 557.7 | 570.6 | 570.6 |
| token | getTransaction | 500 | 18.2 | 19.5 | 24.8 | 26.8 |
| token | getEvents | 500 | 7.5 | 7.9 | 10.0 | 13.6 |
| soroswap | getLedgers | 127 | 355.8 | 363.8 | 370.6 | 375.3 |
| soroswap | getTransaction | 500 | 11.6 | 12.5 | 17.6 | 19.8 |
| soroswap | getEvents | 500 | 4.4 | 5.5 | 8.0 | 8.6 |

## A.3 Query latency — COLD tier (ms)

Served from frozen cold artifacts (page-cache-warm). Run 1 covered sac only; run 2
(same day/box/method, after a methodology review) added token/soroswap by re-freezing
their catalogs and re-measured sac as a reproduction check.

**Run 1 — sac, hot vs cold:**

| endpoint | tier | count | p50 | p90 | p99 | max |
|---|---|--:|--:|--:|--:|--:|
| getLedgers | hot | 80 | 567.4 | 581.8 | 607.6 | 607.6 |
| getLedgers | **cold** | 71 | 641.3 | 664.4 | 685.4 | 685.4 |
| getTransaction | hot | 500 | 18.1 | 19.2 | 25.4 | 28.9 |
| getTransaction | **cold** | 500 | 17.7 | 30.0 | 38.7 | 45.7 |
| getEvents | hot | 500 | 7.8 | 8.0 | 12.0 | 12.6 |
| getEvents | **cold** | 500 | 6.2 | 8.4 | 9.5 | 12.7 |

**Run 2 — cold tier, all profiles:**

| profile | endpoint | count | p50 | p90 | p99 | max |
|---|---|--:|--:|--:|--:|--:|
| sac | getLedgers | 73 | 617.2 | 653.1 | 671.0 | 671.0 |
| sac | getTransaction | 500 | 18.3 | 31.7 | 40.3 | 43.3 |
| sac | getEvents | 500 | 8.8 | 10.5 | 14.4 | 14.9 |
| token | getLedgers | 74 | 604.9 | 638.9 | 664.0 | 664.0 |
| token | getTransaction | 500 | 16.0 | 25.4 | 34.0 | 41.5 |
| token | getEvents | 500 | 3.4 | 4.2 | 4.9 | 5.4 |
| soroswap | getLedgers | 109 | 415.4 | 433.9 | 449.5 | 451.6 |
| soroswap | getTransaction | 500 | 11.7 | 18.9 | 25.4 | 26.5 |
| soroswap | getEvents | 500 | 18.9 | 22.8 | 24.8 | 27.3 |

**Cold ≈ hot — for getLedgers, getTransaction, and dense-match getEvents.** getEvents
cold is even slightly faster at p50 on dense-match profiles; getTransaction has a
heavier cold tail (cold txhash MPHF lookup + reading the large ledger from the pack).
The exception is **selective multi-term getEvents on cold** — see A.3b.

## A.3b getEvents OR-union index-term sweep (run 2; p50/p90/p99 ms)

A *term* = one indexed constraint (a contract ID, or a topic value at a position).
Each row OR-unions N real harvested terms, 500 reps, limit 10.

| profile | tier·scope | t=1 | t=4 | t=8 | t=15 |
|---|---|--:|--:|--:|--:|
| sac | hot·window | 8.4 / 9.7 / 11.4 | 8.4 / 9.0 / 10.8 | 8.5 / 9.1 / 11.1 | 6.8 / 7.8 / 11.4 |
| sac | cold·window | 8.4 / 10.4 / 13.4 | 8.6 / 10.7 / 12.8 | 6.6 / 9.2 / 11.9 | 8.7 / 10.5 / 14.1 |
| token | hot·window | 7.6 / 7.9 / 11.9 | 6.2 / 7.7 / 8.6 | 7.5 / 7.8 / 10.2 | 7.7 / 7.9 / 12.4 |
| token | cold·window | 3.4 / 4.2 / 4.9 | 5.1 / 6.1 / 7.1 | 8.1 / 13.2 / 15.4 | 16.9 / 19.6 / 22.0 |
| soroswap | hot·window | 4.2 / 4.7 / 5.9 | 4.3 / 6.3 / 7.8 | 6.1 / 6.8 / 7.7 | 6.1 / 6.7 / 7.3 |
| soroswap | cold·window | 9.5 / 12.8 / 16.4 | 52.7 / 60.0 / 67.0 | 98.5 / 115.7 / 129.6 | 137.2 / 150.2 / 158.3 |

- **Hot is flat everywhere** (~4–9 ms, 1→15 terms) — term count is nearly free on hot.
- **Cold is flat when terms match densely** (sac: one match-all contract → ~8 ms for any N).
- **Cold scales with term count when terms are selective:** token ≈1 ms/term; soroswap
  (most selective vocabulary) ≈12 ms/term → 15 terms ≈ 137 ms.
- **Cause:** the cold reader is opened *per request* (POC, no LRU) and sparse matches
  trigger the POC underfill re-query loop in `events_handler.scanChunk`. Known POC costs;
  productionization paths are an LRU for cold readers and a streaming pager.

## A.4 Direct in-process read (no serve path; ms)

`store.Get` = RocksDB point read (compressed value); `GetLedgerRaw` adds the zstd decode.

| profile | `store.Get` (compressed) | hot `GetLedgerRaw` | cold `GetLedgerRaw` |
|---|--:|--:|--:|
| sac | 0.63 | 8.2 | 7.9 |
| token | 0.62 | 7.4 | 7.3 |
| soroswap | 0.16 | 4.6 | 3.7 |

---

# Part B — Live pubnet, hot-DB tip querying

Branch `poc/fullhistory-query-localrun`. Local `stellar-rpc full-history` daemon
ingesting live **pubnet** and serving over JSON-RPC on `localhost:8000`.

**Environment:** 16 vCPU, 58 GB RAM, Debian 13; stellar-core 27.1.0 (protocol 27);
data dir on **tmpfs** (RAM-backed); loopback (~0 RTT); load tool `hey` 0.1.5.
Retention 2 chunks (20,000 ledgers), `earliest_ledger = "now"`. Tip ≈ 63,499,700;
queries 500–1000 ledgers behind tip. All HTTP 200, zero errors.

> tmpfs data dir ⇒ best-case storage latency; a disk/network-volume deployment
> would be slower on cold reads.

## B.1 Single-request latency (warm, sequential, n=40; ms)

End-to-end `curl` `time_total` over loopback:

| method | resp size | min | p50 | p90 | p99 | max | mean |
|---|--:|--:|--:|--:|--:|--:|--:|
| `getLedgers` (limit 1) | 1.7 MB | 33.0 | 35.0 | 38.1 | 38.9 | 39.1 | 35.4 |
| `getTransactions` (limit 5) | 17.5 KB | 7.9 | 8.5 | 9.8 | 16.5 | 16.7 | 9.2 |
| `getTransaction` (by hash) | 2.9 KB | 2.4 | 2.5 | 4.0 | 4.1 | 10.4 | 2.9 |
| `getEvents` (limit 10) | 4.9 KB | 1.7 | 1.7 | 1.8 | 2.2 | 4.2 | 1.8 |

Server-side handler latency (from `/metrics`, excludes network/transfer):

| method | avg | p95 ≤ | p99 ≤ |
|---|--:|--:|--:|
| `getEvents` | 1.68 | 5 | 5 |
| `getTransaction` | 2.89 | 5 | 10 |
| `getLedgers` | 4.61 | 10 | 25 |
| `getTransactions` | 10.51 | 25 | 25 |

`getLedgers` end-to-end (35 ms) is dominated by shipping the ~1.7 MB `LedgerCloseMeta`,
not by the ~4.6 ms server handler.

## B.2 Concurrency sweep (`hey`, loopback, keep-alive)

### getTransaction (~2.9 KB)
| conc | req/s | p50 | p90 | p99 | max |
|--:|--:|--:|--:|--:|--:|
| 1 | 323 | 2.5 | 4.7 | 9.3 | 11 |
| 8 | 1,324 | 4.2 | 11.3 | 16.6 | 19 |
| 16 | 1,637 | 8.1 | 17.5 | 22.8 | 33 |
| 32 | 1,650 | 19.1 | 29.4 | 37.9 | 51 |
| 64 | 1,700 | 36.5 | 51.2 | 64.5 | 101 |

### getEvents (limit 10, ~4.9 KB)
| conc | req/s | p50 | p90 | p99 | max |
|--:|--:|--:|--:|--:|--:|
| 1 | 460 | 1.9 | 2.5 | 7.1 | 14 |
| 8 | 1,905 | 2.9 | 8.3 | 13.7 | 16 |
| 16 | 2,207 | 5.3 | 13.4 | 19.9 | 24 |
| 32 | 2,213 | 13.4 | 22.9 | 29.8 | 38 |
| 64 | 2,256 | 27.5 | 38.6 | 55.8 | 74 |

### getTransactions (limit 5, ~29 KB)
| conc | req/s | p50 | p90 | p99 | max |
|--:|--:|--:|--:|--:|--:|
| 1 | 121 | 7.4 | 11.9 | 16.4 | 20 |
| 8 | 519 | 15.4 | 21.3 | 25.9 | 34 |
| 16 | 532 | 28.3 | 42.3 | 55.8 | 75 |
| 32 | 538 | 57.2 | 81.3 | 98.3 | 134 |

### getLedgers (limit 1, ~1.3 MB) — heavy payload
| conc | req/s | p50 | p90 | p99 | max |
|--:|--:|--:|--:|--:|--:|
| 1 | 36 | 27.0 | 30.4 | 35.4 | 44 |
| 4 | 83 | 47.5 | 58.3 | 68.0 | 72 |
| 8 | 80 | 95.6 | 118.7 | 158.5 | 165 |
| 16 | 80 | 193.8 | 244.0 | 311.4 | 372 |

Throughput ceilings (16-core box): getEvents ~2,250 req/s · getTransaction ~1,700 ·
getTransactions ~535 · getLedgers ~80 req/s. Saturates at concurrency ≈ 8–16, then
degrades gracefully (linear latency growth, textbook worker-pool queueing, no cliff).

---

# Combined analysis

The two regimes agree on the mechanism and bracket the range:

1. **`getLedgers` is payload-bound everywhere.** Live pubnet (~1.7 MB LCM) → ~35 ms
   end-to-end; synthetic (~17 MB LCM) → ~567 ms. ~10× payload ⇒ ~15× latency, while
   the storage read stays ~5–8 ms in both. Confirmed independently by the synthetic
   direct-read numbers (§A.4: 8 ms read vs 567 ms served = ~1.4 % read, ~98.6 % base64 +
   JSON + HTTP). The read path is *not* the cost; the size of the shipped blob is.

2. **The light endpoints stay cheap even on heavy ledgers.** getTransaction/getEvents:
   ~1.8–2.9 ms on pubnet tip (§B.1) → ~8–18 ms on 17 MB synthetic ledgers (§A.2). They
   scale with the target ledger's size but never approach getLedgers' cost.

3. **Cold ≈ hot — with one exception** (§A.3, §A.3b): the frozen `.pack` + MPHF/bitmap
   cold tier serves getLedgers, getTransaction, and dense-match getEvents at essentially
   hot-tier latency (cold getTransaction has a heavier tail from the txhash MPHF lookup).
   The exception is **selective multi-term getEvents on cold**, which scales with term
   count (token ≈1 ms/term; soroswap ≈12 ms/term → 15 terms ≈ 137 ms) while hot stays
   flat — caused by the POC's per-request cold-reader open + underfill re-query loop
   (fixable with an LRU + streaming pager). Cold reads here were page-cache-warm.

4. **Ingest dominates freshly-committed ledgers** (§A.1): 77–130 ms src→commit on
   synthetic profiles, vs the ~2–18 ms query legs — so end-to-end "how fast is a brand
   new ledger queryable" is gated by ingest, not query, for everything except getLedgers.

## Where to spend on getLedgers (size-bound, not read-bound)

1. Cap default/max page size (`[serve].default_ledgers_limit` / `max_ledgers_limit`).
2. Transport compression (gzip/zstd `Content-Encoding`) — base64'd XDR is highly compressible.
3. Stream the base64 encoder instead of materializing the full string per ledger.
4. Lighter projection (header-only / opt-out of full meta).
5. ~~Storage-read tuning~~ — the read is ~1 %; not worth it.

## Caveats (both regimes)

- **Different hardware** (32 vCPU c6id vs 16 vCPU Debian) and different data-dir media
  (NVMe vs tmpfs) — do not compare absolute ms across Part A and Part B, only shapes/ratios.
- Percentiles are nearest-rank. Synthetic `getLedgers` p99 is over a budget-capped sample
  (71–127), less robust than the 500-rep getTransaction/getEvents p99s.
- Live-pubnet numbers: client + server shared 16 cores (client stole CPU) and loopback
  RTT ≈ 0 — add real network latency to every p50 and expect higher ceilings on a
  dedicated client.
- Synthetic cold reads are OS-page-cache-warm; an uncached first-touch adds disk latency.
