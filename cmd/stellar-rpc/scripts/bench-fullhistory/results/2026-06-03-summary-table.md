# stellar-rpc full-history bench — 2026-06-03 streamlined summary

A condensed, self-contained view of the 2026-06-03 cross-machine run. Full
report with concurrency sweeps, ingest stages, and per-machine raw cells:
[`2026-06-03-cross-machine.md`](./2026-06-03-cross-machine.md). All numbers are
recomputed from `gs://rpc-full-history/benchmarks/2026-06-03/`.

## Glossary — what every term means

**Machines (rows):** AWS EC2 instances. `c6id` = Intel Ice Lake x86 (compute-
optimized, ~2 GB RAM/vCPU); `im4gn` = AWS Graviton2 ARM (memory-optimized,
~4 GB RAM/vCPU). The size suffix scales vCPUs and RAM:

| Instance | vCPUs | RAM |
|---|---|---|
| c6id.2xlarge | 8 | 15 GB |
| c6id.4xlarge | 16 | 31 GB |
| c6id.8xlarge | 32 | 62 GB |
| im4gn.4xlarge | 16 | 62 GB |

**Tier:**
- **cold** = read from on-disk packfiles, OS page cache evicted + a fresh file
  opened on every query (worst case, "data not in memory").
- **hot** = read from the live RocksDB store with a warm cache (best case,
  "recently-served data").

**Read workloads (columns in Tables 1–2):**

| Name | What it does | Fixed param |
|---|---|---|
| **ledgers** | Read a run of consecutive ledgers | `n=20` ledgers per read |
| **tx-page** | Fetch one page of transactions | `page=20` txns per page |
| **tx-hash** | `getTransaction(hash)` full round-trip (lookup → fetch → decode → re-serialize) | hits only |
| **events** | Event-filter query (random mix of contract/topic filters) | — |

**Ingest workloads (Tables 3–4):** writing data into the stores, not reading it.

| Name | What it does |
|---|---|
| **hot-ingest** | Single-stream synchronous ingest into the live hot store (each ledger's ledgers/txhash/events written + WAL-fsynced before the next) |
| **cold-ingest** | Bulk ingest of packfile chunks into cold storage (parallel chunk workers) |
| **build-txhash-index** | Phase-2 of the cold tx-hash index: k-way merge of per-chunk hash streams + MPHF (minimal-perfect-hash) construction |

**Variables:**
- **n** = ledgers read per `ledgers` query (here always 20).
- **page** = transactions per `tx-page` query (here always 20).
- **c** (query-concurrency) = queries in flight at once. `c=1` = one at a time
  (pure latency); higher `c` = load test.
- **p50 / p90 / p99** = latency percentiles in milliseconds. p50 = median
  (typical request); p99 = near-worst-case tail (1 in 100 requests is slower).
- **ops/s** = throughput (successful queries per second) at that concurrency.
- **ledgers/s** = ingest throughput (ledgers written per second), single stream.
- **keys/s** = tx-hash index build rate (hashes indexed per second).
- **stage** = one phase of the ingest pipeline (e.g. `extract` = pull the
  xdr-views out of the raw ledger; `write` = persist to RocksDB + WAL;
  `term-index` = build the event term→ledger bitmaps; `append` = write to the
  cold packfile). Stage timings are per item (per ledger, or per event-batch).

## Table 1 — Typical latency (p50 ms, single query, `c=1`)

Cleanest "how fast is one request" view. Lower = faster. Each cell is
`cold / hot`.

| Machine (vCPU / arch) | ledgers n=20 | tx-page p=20 | tx-hash | events |
|---|---|---|---|---|
| c6id.2xlarge (8, x86) | 14.3 / 13.6 | 12.3 / 10.3 | 12.2 / 11.5 | 15.8 / 5.5 |
| c6id.4xlarge (16, x86) | 15.2 / 12.9 | 11.9 / 10.4 | 12.2 / 11.0 | 15.5 / 5.3 |
| c6id.8xlarge (32, x86) | 14.8 / 13.2 | 11.5 / 9.8 | 11.7 / 10.6 | 16.0 / 5.2 |
| im4gn.4xlarge (16, ARM) | 27.5 / 24.8 | 20.3 / 18.5 | 21.7 / 20.1 | 20.0 / 9.1 |

*For point reads, cold ≈ hot (~1.1×) — decode cost dominates, a warm-NVMe file
open is cheap. Only **events** shows a big cold/hot gap (cold ~3× slower). The
ARM box is ~1.7–1.9× slower per request than same-vCPU x86.*

## Table 2 — Peak throughput (ops/s, best across c=1→16)

How many queries/sec each box sustains under load. Higher = better. Each cell is
`cold / hot`.

| Machine (vCPU / arch) | ledgers n=20 | tx-page p=20 | tx-hash | events |
|---|---|---|---|---|
| c6id.2xlarge (8, x86) | 250 / 363 | 289 / 302 | 263 / 285 | 118 / 430 |
| c6id.4xlarge (16, x86) | 501 / 702 | 509 / 520 | 479 / 508 | 239 / 828 |
| c6id.8xlarge (32, x86) | 775 / 913 | 717 / 745 | 689 / 700 | 504 / 1453 |
| im4gn.4xlarge (16, ARM) | 483 / 587 | 459 / 493 | 506 / 471 | 327 / 738 |

*Throughput scales with vCPU count (8xl ≈ 2× the 4xl). Hot **events** scales
best (pure in-memory bitmap intersect → 1,453 ops/s on the 32-vCPU box).*

## Table 3 — Ingest throughput

How fast each box writes data in. Higher = better.

| Machine (vCPU / arch) | hot-ingest (ledgers/s) | cold-ingest (ledgers/s, est.) | build-txhash-index (keys/s) |
|---|---|---|---|
| c6id.2xlarge (8, x86) | 74 | ~560 | 24.8 M |
| c6id.4xlarge (16, x86) | 79 | ~1,110 | 37.1 M |
| c6id.8xlarge (32, x86) | 80 | ~1,450 | 38.3 M |
| im4gn.4xlarge (16, ARM) | 49 | ~1,080 | 38.9 M |

*hot-ingest is single-stream and **WAL-fsync-bound**, so it barely scales with
vCPUs (~80 ledgers/s ceiling on x86); the ARM box is ~1.6× slower on the
fsync + encode path. **cold-ingest** is batched (no per-ledger fsync) and runs
chunks in parallel, so it is ~7–18× faster than hot and scales with
`--chunk-workers` — that's why the 4-worker c6id.2xlarge (~560) trails the
8-worker boxes (~1,100–1,450). build-txhash-index is CPU-bound and scales with
cores up to ~38 M keys/s (the 8-vCPU box is the outlier at ~25 M). Note: im4gn
ingested 140 chunks (1.4 M ledgers; 380 M index keys, 1.6 GB) vs 16 chunks
(160 K ledgers; 46 M keys, 199 MB) on the c6id boxes — per-item **rates** are
comparable, absolute totals are not.*

> The cold-ingest rate is an **estimate**: the harness records the summed
> per-chunk wall time, not the true end-to-end wall, so this is
> `sum(chunk_wall) ÷ chunk-workers` (i.e. it assumes the chunk workers stay
> fully busy — an upper bound). hot-ingest and build-txhash-index are measured
> directly.

## Table 4 — Ingest per-stage cost (p50 ms per item)

Where the ingest time goes, broken out by pipeline stage. Lower = faster. Ledger
and `extract`/`write` stages are per ledger; event stages are per event-batch.

| Machine (vCPU / arch) | hot: ledger write | hot: tx extract | hot: event extract | hot: event write | cold: event extract | cold: event term-index | cold: event append |
|---|---|---|---|---|---|---|---|
| c6id.2xlarge (8, x86) | 2.59 | 0.47 | 1.39 | 7.24 | 2.68 | 0.76 | 0.12 |
| c6id.4xlarge (16, x86) | 2.47 | 0.46 | 1.37 | 6.63 | 2.67 | 0.82 | 0.12 |
| c6id.8xlarge (32, x86) | 2.47 | 0.46 | 1.37 | 6.51 | 1.75 | 0.70 | 0.10 |
| im4gn.4xlarge (16, ARM) | 4.63 | 0.71 | 2.26 | 10.24 | 2.40 | 0.83 | 0.15 |

*Hot **event write** (RocksDB put + WAL) is the single most expensive stage
(~6.5–10 ms/batch) and dominates hot-ingest cost. xdr-view **extract** is cheap
(~0.5 ms/ledger for tx-hash, ~1.4 ms for events). Graviton2 is ~1.5–1.8× slower
on the CPU-bound extract/write stages.*

> ⚠️ These `ops/s` figures are **only comparable within this 6/03 run**. The
> metric was computed differently in the 2026-05-21 run, so do not compare
> throughput across the two reports — single-query p50 latency is the only valid
> cross-run comparison.
