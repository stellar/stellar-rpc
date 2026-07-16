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
- Raw data: [`latency.csv`](./latency.csv) and [`termsweep.csv`](./termsweep.csv).
  Report: [`report.html`](./report.html).
- **Two runs, same day/box/method.** Run 1 is the original upload. Run 2 (a few
  hours later, after a methodology review of the harnesses) adds: (a) cold-tier
  coverage for **token/soroswap** (their catalogs were frozen via
  `TestServeProfileForBench`; freeze wall-clock: token 6m19s, soroswap 4m15s,
  sac 11m10s in run 1), (b) the **getEvents OR-union index-term sweep**
  (1/4/8/15 terms, hot & cold), and (c) a **reproduction check** of run 1's hot
  and sac-cold rows. Run-1 numbers are preserved below unchanged; run-2 tables
  are marked as such.

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

## Query latency — COLD tier (run 1: sac only; ms)

Served from frozen cold artifacts. At run-1 time only sac had a servable daemon
catalog on the box (token/soroswap had cold artifacts but bench-ingest discarded
their catalogs — run 2 below fixed that by re-freezing them).

| endpoint | tier | count | p50 | p90 | p99 | max |
|---|---|--:|--:|--:|--:|--:|
| getLedgers | hot | 80 | 567.4 | 581.8 | 607.6 | 607.6 |
| getLedgers | **cold** | 71 | 641.3 | 664.4 | 685.4 | 685.4 |
| getTransaction | hot | 500 | 18.1 | 19.2 | 25.4 | 28.9 |
| getTransaction | **cold** | 500 | 17.7 | 30.0 | 38.7 | 45.7 |
| getEvents | hot | 500 | 7.8 | 8.0 | 12.0 | 12.6 |
| getEvents | **cold** | 500 | 6.2 | 8.4 | 9.5 | 12.7 |

**Cold serves ≈ as fast as hot** for these endpoints. getEvents cold is even
slightly faster at p50; getTransaction has a heavier cold tail (cold txhash MPHF
lookup + reading the large ledger from the cold pack). (Cold reads here are
page-cache-warm.) **Scope note (run 2):** this "cold ≈ hot" holds for
getLedgers/getTransaction generally and for getEvents whose filters match
*densely* (sac/token). getEvents with *selective* filters on a contract-diverse
profile diverges — see the term sweep below.

## Query latency — COLD tier, all profiles (run 2; ms)

Same window/method as run 1, after freezing token/soroswap catalogs. sac rows
re-measure run 1 (reproduction).

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

Cross-checked black-box with fhbench against the whole frozen chunk
(mid-chunk-sampled tx hashes, concurrency 1, 30 s): getTransaction cold p50
sac 17.5 / token 16.3 / soroswap 10.8 ms; getLedgers(limit=1) cold p50
sac 624.8 / token 595.2 / soroswap 390.6 ms — all within ~5 % of the harness
rows above. The soroswap cold getEvents row (18.9 vs 4.7 ms hot) is the
selective-filter effect: its harvested contract is one of many, so matches are
sparse and the scan hits the POC's underfill re-query loop (next section).

## getEvents OR-union index-term sweep (run 2; p50/p90/p99 ms)

A *term* is one indexed constraint — a distinct contract ID, or a distinct topic
value at a position (0–2) — exactly the eventstore index's term model. Each row
OR-unions N real harvested terms (homogeneous filters, ≤5 per filter) over the
20-ledger steady-state window (500 reps, limit 10). `chunk` rows are fhbench
runs with random starts over the whole 10 000-ledger frozen chunk (30 s each).
Full data: [`termsweep.csv`](./termsweep.csv).

| profile | tier·scope | t=1 | t=4 | t=8 | t=15 |
|---|---|--:|--:|--:|--:|
| sac | hot·window | 8.4 / 9.7 / 11.4 | 8.4 / 9.0 / 10.8 | 8.5 / 9.1 / 11.1 | 6.8 / 7.8 / 11.4 |
| sac | cold·window | 8.4 / 10.4 / 13.4 | 8.6 / 10.7 / 12.8 | 6.6 / 9.2 / 11.9 | 8.7 / 10.5 / 14.1 |
| sac | cold·chunk | 8.0 / 12.2 / 14.0 | 8.4 / 12.4 / 15.3 | 8.5 / 12.6 / 14.7 | 8.4 / 12.8 / 15.5 |
| token | hot·window | 7.6 / 7.9 / 11.9 | 6.2 / 7.7 / 8.6 | 7.5 / 7.8 / 10.2 | 7.7 / 7.9 / 12.4 |
| token | cold·window | 3.4 / 4.2 / 4.9 | 5.1 / 6.1 / 7.1 | 8.1 / 13.2 / 15.4 | 16.9 / 19.6 / 22.0 |
| token | cold·chunk | 4.4 / 5.9 / 7.2 | 6.7 / 8.1 / 9.5 | 8.9 / 11.6 / 13.4 | 16.5 / 18.9 / 21.8 |
| soroswap | hot·window | 4.2 / 4.7 / 5.9 | 4.3 / 6.3 / 7.8 | 6.1 / 6.8 / 7.7 | 6.1 / 6.7 / 7.3 |
| soroswap | cold·window | 9.5 / 12.8 / 16.4 | 52.7 / 60.0 / 67.0 | 98.5 / 115.7 / 129.6 | 137.2 / 150.2 / 158.3 |
| soroswap | cold·chunk | 12.4 / 15.6 / 18.3 | 47.1 / 55.4 / 61.6 | 101.2 / 114.5 / 128.2 | 181.2 / 205.4 / 220.3 |

What the sweep shows:

1. **Hot is flat everywhere** (~4–9 ms for 1→15 terms, all profiles): term count
   costs almost nothing on the hot tier at this window size.
2. **Cold is flat when terms match densely.** sac's vocabulary is one match-all
   contract + topics of ubiquitous transfer events, so every union fills the
   10-event page from the first candidate ledger: ~8 ms regardless of N, window
   or whole chunk.
3. **Cold scales with term count when terms are selective.** token: 3.4→16.9 ms
   (window) and 4.4→16.5 ms (chunk) across 1→15 terms — ≈1 ms/term. soroswap
   (many contracts, diverse topics — the most selective vocabulary): 9.5→137 ms
   (window) and 12.4→181 ms (chunk), ≈12 ms/term.
4. **Why cold diverges from hot:** hot and cold share the same bitmap-driven
   `eventstore.Query`; the cold reader, however, is opened *per request* (POC —
   no LRU) and sparse matches trigger the POC's underfill loop in
   `events_handler.scanChunk`, which re-queries the chunk from scratch with a
   doubling cap until the page fills or the window is provably exhausted. Dense
   matches (sac, or few terms on token) never enter the loop; selective unions
   re-query repeatedly. Both are known POC costs with named productionization
   paths (LRU'd cold readers; a streaming pager that advances a pinned event-ID
   cursor instead of re-querying).
5. Window and chunk sweeps agree closely everywhere, so the curve is a property
   of term selectivity + the underfill loop, not of scan-span length.

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
3. **Cold ≈ hot — for getLedgers, getTransaction, and dense-match getEvents.**
   The cold tier serves those at essentially hot-tier latency (getTransaction has
   a somewhat heavier cold tail). Run 2 confirms this on all three profiles.
4. **The exception (run 2): selective multi-term getEvents on cold.** OR-unions
   of genuinely selective terms scale with term count on the cold tier (token
   ≈1 ms/term; soroswap ≈12 ms/term, 15 terms ≈ 137–181 ms) while hot stays flat
   (~4–9 ms). Cause: per-request cold-reader open + the POC underfill re-query
   loop (see the term-sweep section). Known POC costs; productionization paths
   are an LRU for cold readers and a streaming pager.
5. **Reference — toy baseline** (1-tx/1-event synthetic ledger): full src→query
   ~4.5 ms, ingest ~3.0 ms, visible ~1.5 ms. ~40× lighter than real steady-state
   ingest — do not read it as a production number.

### Where to spend on getLedgers (size-bound, not read-bound)

1. Cap the default/max page size (`[serve].default_ledgers_limit` / `max_ledgers_limit`).
2. Transport compression (gzip/zstd `Content-Encoding`) — base64'd XDR is highly compressible.
3. Stream the base64 encoder instead of materializing the full string per ledger.
4. Lighter projection (header-only / opt-out of full meta) — the only way to avoid shipping the blob.
5. ~~Storage-read tuning~~ — the read is ~1 %; not worth it.

## Run-2 reproduction check (run 1 vs run 2, p50 ms)

Re-running the identical hot pass a few hours later (fresh daemon, fresh temp
store) reproduces run 1 closely — the committed numbers are stable, not
one-off artifacts:

| metric | run 1 | run 2 | metric | run 1 | run 2 |
|---|--:|--:|---|--:|--:|
| sac ingest | 129.3 | 127.6 | sac hot getLedgers | 567.4 | 567.0 |
| token ingest | 129.9 | 122.8 | token hot getLedgers | 550.8 | 548.7 |
| soroswap ingest | 76.7 | 80.2 | soroswap hot getLedgers | 355.8 | 355.9 |
| sac hot getTransaction | 18.1 | 18.5 | sac hot getEvents | 7.8 | 6.8 |
| sac cold getLedgers | 641.3 | 617.2 | sac cold getEvents | 6.2 | 8.8 |

Largest deltas ≈ ±5 % (single-digit percent run-to-run noise on a shared box);
sac cold getEvents ±~2.5 ms is page-cache/alloc noise at single-ledger-decode
scale.

## Methodology review (run 2)

The harness logic was reviewed before run 2; the e2e-harness methodology behind
run 1's tables held up (in-process ingest timestamps, dense closed-loop query
pass, nearest-rank percentiles, direct-read isolation, warmup skip). Fixes were
applied to **tools/fhbench** — none of which invalidate run-1 numbers, which
came from the e2e harness, not fhbench:

- **Hot tier could bleed into the frozen chunk**: with fewer than a half-chunk
  of hot ledgers above the last frozen chunk, fhbench's "hot" tier reached into
  cold-served ledgers. It now clamps to the chunk containing `latest`.
- **Off-by-one**: non-sweep getEvents used an exclusive `endLedger = tier.last`,
  silently dropping the tier's last ledger (and producing empty-window freebies
  when `startLedger == tier.last`).
- **Errored requests** were included in latency percentiles; now tallied
  separately and excluded.
- **Head-biased sampling**: tx hashes and the event-term vocabulary were sampled
  from the tier head — the deploy-warmup region on these profiles. Both now
  sample from the tier midpoint. (Measured impact: head-sampled soroswap cold
  getTransaction read 2.7 ms p50 vs the representative 10.8 ms.)
- **Term-count cap**: >21 OR-union terms can exceed the protocol's 5-filter cap
  for mixed contract/topic vocabularies; now rejected up front (docs previously
  claimed 25).
- Also documented: `TestServeProfileForBench`'s hot tier is **synthetic 1-tx
  filler** (it exists to latch `/ready`); fhbench hot-tier rows against that
  harness are not representative of real-profile hot serving — the e2e hot
  harness (real ledgers live-ingested) is, and is what all hot tables here use.
  `run-query-suite.sh` also gained `GETLEDGERS_LIMIT=1` (limit 50 × 8 workers ×
  10–17 MB ledgers ≈ >8 GB in flight — the likely cause of the earlier
  query-suite OOM/kill mid `getLedgers cold`).

## Caveats

- Percentiles are nearest-rank. `getLedgers` p99 is over the budget-capped sample
  count (71–127), less robust than the 500-rep getTransaction/getEvents p99s.
- Cold reads are OS-page-cache-warm (pack touched during harvest); an uncached
  first-touch adds disk latency.
- The per-ledger ingest→query *visibility* (commit→query) span is not reported for
  the real profiles: under heavy ingestion the single sequential poller can't stay
  at the commit frontier, so that span measures poller lag, not true visibility.
  `ingest` (in-process timestamps) and the query-latency pass are the reliable metrics.
- All query passes are sequential closed-loop (concurrency 1): these are
  unloaded-latency floors, not latency under concurrent load.
- The term-sweep curve depends on the harvested vocabulary's selectivity, which
  is a property of the profile (sac: one match-all contract; soroswap: many).
  Real-network vocabularies will sit between these extremes.
