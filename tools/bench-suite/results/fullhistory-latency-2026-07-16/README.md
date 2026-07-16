# Full-history ingest + query latency — 2026-07-16

Latency of the `full-history` daemon (branch `poc/fullhistory-query`) on the three
synthetic apply-load profiles (**sac / token / soroswap**), measured on **real**
profile ledgers — both the **hot** serving tier (RocksDB) and the **cold** tier
(frozen `.pack` + MPHF/bitmap index artifacts).

- **Machine:** AWS `c6id.8xlarge` — Intel Xeon Platinum 8375C, 32 vCPU, 64 GiB, local NVMe.
- **Harness:** `TestServeE2E_ProfileLatency` (hot) and `TestServeE2E_ColdQueryLatency` (cold)
  in `cmd/stellar-rpc/internal/fullhistory`; direct read via `BenchmarkHotGetLedgerRaw`
  in `.../storage/stores/ledger`. Driver: [`../../run-e2e-latency-suite.sh`](../../run-e2e-latency-suite.sh).
- **Window:** 20 steady-state ledgers (chunk 1, ledgers 10202–10221) after a
  200-ledger warmup skip (the chunk head is contract-deploy warmup, far lighter
  than steady state). Query pass: 500 reps/endpoint, `getLedgers` at `limit=1`,
  per-endpoint 45 s wall budget.
- Raw data: [`latency.csv`](./latency.csv). Report: [`report.html`](./report.html).

## Ledger characteristics (steady-state, per ledger)

These synthetic ledgers are large — thousands of tx + Soroban meta each.

| profile | decompressed LCM | on-disk (zstd) | compression |
|---|--:|--:|--:|
| sac | ~17.3 MB | ~2.1 MB | ~8.2× |
| token | ~16.5 MB | ~1.7 MB | ~9.5× |
| soroswap | ~10.0 MB | ~0.5 MB | ~19× |

## Ingest → query (source → durable commit), per ledger

| profile | p50 | p90 | p99 |
|---|--:|--:|--:|
| sac | 129.3 ms | 138.9 ms | 145.3 ms |
| token | 129.9 ms | 133.6 ms | 144.7 ms |
| soroswap | 76.7 ms | 82.6 ms | 85.4 ms |

## Query latency — HOT tier (ms)

`getLedgers` at `limit=1`; getTransaction/getEvents 500 reps. 0 errors throughout.
`getLedgers` is budget-capped (`count` = reps that fit the 45 s budget).

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

## Query latency — COLD tier (sac; ms)

Served from frozen cold artifacts. Only sac had a servable daemon catalog on the
box (token/soroswap have cold artifacts but bench-ingest discarded their catalogs).

| endpoint | tier | count | p50 | p90 | p99 | max |
|---|---|--:|--:|--:|--:|--:|
| getLedgers | hot | 80 | 567.4 | 581.8 | 607.6 | 607.6 |
| getLedgers | **cold** | 71 | 641.3 | 664.4 | 685.4 | 685.4 |
| getTransaction | hot | 500 | 18.1 | 19.2 | 25.4 | 28.9 |
| getTransaction | **cold** | 500 | 17.7 | 30.0 | 38.7 | 45.7 |
| getEvents | hot | 500 | 7.8 | 8.0 | 12.0 | 12.6 |
| getEvents | **cold** | 500 | 6.2 | 8.4 | 9.5 | 12.7 |

**Cold serves ≈ as fast as hot.** getEvents cold is even slightly faster at p50;
getTransaction has a heavier cold tail (cold txhash MPHF lookup + reading the large
ledger from the cold pack). (Cold reads here are page-cache-warm.)

## Direct in-process read (no serve path)

Isolates the storage read from base64/JSON/HTTP. `store.Get` is the RocksDB point
read (compressed value); `GetLedgerRaw` adds the zstd decode.

| profile | `store.Get` (compressed) | hot `GetLedgerRaw` (Get + decode) | cold `GetLedgerRaw` (pack + decode) |
|---|--:|--:|--:|
| sac | 0.63 ms | 8.2 ms | 7.9 ms |
| token | 0.62 ms | 7.4 ms | 7.3 ms |
| soroswap | 0.16 ms | 4.6 ms | 3.7 ms |

## End-to-end (synthesized: ingest + query, same percentile)

| profile | endpoint | e2e p50 | e2e p99 |
|---|---|--:|--:|
| sac | getLedgers | ~696.7 ms | ~752.9 ms |
| sac | getTransaction | ~147.4 ms | ~170.7 ms |
| sac | getEvents | ~137.1 ms | ~157.3 ms |
| token | getLedgers | ~680.7 ms | ~715.3 ms |
| token | getTransaction | ~148.1 ms | ~169.5 ms |
| token | getEvents | ~137.4 ms | ~154.7 ms |
| soroswap | getLedgers | ~432.5 ms | ~456.0 ms |
| soroswap | getTransaction | ~88.3 ms | ~103.0 ms |
| soroswap | getEvents | ~81.1 ms | ~93.4 ms |

(Summing per-stage percentiles is a conservative upper bound; commit→queryable is
treated as negligible — the design's visibility window is ~1–2 ms.)

## Key findings

1. **Ingest dominates the targeted endpoints.** getTransaction/getEvents add only
   ~4–18 ms on top of the ~77–130 ms ingest leg.
2. **getLedgers is the outlier — and it's payload-bound, not read-bound.** A direct
   hot read (incl. zstd decode) is ~8 ms, but getLedgers over HTTP is ~567 ms:
   **~1.4 % is the read, ~98.6 % is base64 + JSON + HTTP shipping the 10–17 MB
   ledger.** Decompression (~6–8 ms) is cheap; the size of what's shipped is the cost.
   `limit=1` vs `limit=5` scales ~5× linearly, confirming size-bound.
3. **Cold ≈ hot.** For this POC the cold tier serves at essentially hot-tier latency
   across all three endpoints (getTransaction has a somewhat heavier cold tail).
4. **Reference — toy baseline** (1-tx/1-event synthetic ledger): full src→query
   ~4.5 ms, ingest ~3.0 ms, visible ~1.5 ms. ~40× lighter than real steady-state
   ingest — do not read it as a production number.

### Where to spend on getLedgers (size-bound, not read-bound)

1. Cap the default/max page size (`[serve].default_ledgers_limit` / `max_ledgers_limit`).
2. Transport compression (gzip/zstd `Content-Encoding`) — base64'd XDR is highly compressible.
3. Stream the base64 encoder instead of materializing the full string per ledger.
4. Lighter projection (header-only / opt-out of full meta) — the only way to avoid shipping the blob.
5. ~~Storage-read tuning~~ — the read is ~1 %; not worth it.

## Caveats

- Percentiles are nearest-rank. `getLedgers` p99 is over the budget-capped sample
  count (71–127), less robust than the 500-rep getTransaction/getEvents p99s.
- Cold reads are OS-page-cache-warm (pack touched during harvest); an uncached
  first-touch adds disk latency.
- The per-ledger ingest→query *visibility* (commit→query) span is not reported for
  the real profiles: under heavy ingestion the single sequential poller can't stay
  at the commit frontier, so that span measures poller lag, not true visibility.
  `ingest` (in-process timestamps) and the query-latency pass are the reliable metrics.
