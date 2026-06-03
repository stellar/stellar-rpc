# stellar-rpc full-history bench — 2026-06-03 streamlined summary

A condensed, self-contained view of the 2026-06-03 cross-machine run. Full
report with concurrency sweeps, ingest stages, and per-machine raw cells:
[`2026-06-03-cross-machine.md`](./2026-06-03-cross-machine.md). All numbers are
recomputed from `gs://rpc-full-history/benchmarks/2026-06-03/`.

> ## ⚠️ Harness corrected (PR #750) — only c6id.8xlarge re-run
>
> The original query benches were invalid (tx-page measured a tx *count*, not a
> page; xdr-views was disabled everywhere). The harness is fixed and
> **c6id.8xlarge has been re-run**; the other three machines are **🟥 STALE —
> pending re-run**. The c6id.8xlarge rows below show the corrected **roundtrip**
> path for like-for-like comparison with the stale rows; with **xdr-views** (the
> realistic server path) tx-page/tx-hash are a further 4–9× faster — see
> [`2026-06-03-cross-machine.md` §2](./2026-06-03-cross-machine.md#2-c6id8xlarge--corrected-fixed-harness).

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
| 🟥 c6id.2xlarge (8, x86) | 14.3 / 13.6 | 12.3 / 10.3¹ | 12.2 / 11.5 | 15.8 / 5.5 |
| 🟥 c6id.4xlarge (16, x86) | 15.2 / 12.9 | 11.9 / 10.4¹ | 12.2 / 11.0 | 15.5 / 5.3 |
| ✅ c6id.8xlarge (32, x86) | 14.8 / 13.2 | **13.2 / 11.1** | **11.9 / 10.6** | **15.4 / 6.1**² |
| 🟥 im4gn.4xlarge (16, ARM) | 27.5 / 24.8 | 20.3 / 18.5¹ | 21.7 / 20.1 | 20.0 / 9.1 |

🟥 = old harness (stale). ✅ c6id.8xlarge = fixed harness, **roundtrip** path.
¹ stale tx-page = count-only (not a real page). ² events = worst-case K=15.
**With xdr-views the c6id.8xlarge tx-page = 2.99 / 1.52 and tx-hash = 2.18 / 1.19**
(4–9× faster) — see the main report §2.1.

*For point reads on the roundtrip path, cold ≈ hot (~1.1×) — decode cost
dominates, a warm-NVMe file open is cheap. Only **events** shows a big cold/hot
gap (cold ~2.5× slower). The xdr-views path collapses tx-page/tx-hash to 1–3 ms.*

## Table 2 — Peak throughput (ops/s, best across c=1→16)

How many queries/sec each box sustains under load. Higher = better. Each cell is
`cold / hot`.

| Machine (vCPU / arch) | ledgers n=20 | tx-page p=20 | tx-hash | events |
|---|---|---|---|---|
| 🟥 c6id.2xlarge (8, x86) | 250 / 363 | 289 / 302¹ | 263 / 285 | 118 / 430 |
| 🟥 c6id.4xlarge (16, x86) | 501 / 702 | 509 / 520¹ | 479 / 508 | 239 / 828 |
| ✅ c6id.8xlarge (32, x86) | 775 / 913 | **621 / 637** | **680 / 706** | **500 / 1081**² |
| 🟥 im4gn.4xlarge (16, ARM) | 483 / 587 | 459 / 493¹ | 506 / 471 | 327 / 738 |

🟥 stale (old harness); ✅ c6id.8xlarge = fixed harness, **roundtrip** peaks.
¹ stale tx-page = count-only. ² events K=15.
**With xdr-views the c6id.8xlarge peaks jump to tx-page 3,456 / 4,830,
tx-hash 4,170 / 7,253, events 512 / 1,843** (5–10× the roundtrip ceiling).

*Throughput scales with vCPU count. The fixed-harness c6id.8xlarge shows the
real story: the **xdr-views** path sustains 4.8k–7.3k ops/s on tx-page/tx-hash —
the roundtrip path (and the stale rows) are decode-bound and cap far lower.*

## Table 3 — Ingest throughput

How fast each box writes data in. Higher = better.

| Machine (vCPU / arch) | hot-ingest (ledgers/s) | cold-ingest (ledgers/s) | build-txhash-index (keys/s) |
|---|---|---|---|
| 🟥 c6id.2xlarge (8, x86) | 74 | ~560 (est.) | 24.8 M |
| 🟥 c6id.4xlarge (16, x86) | 79 | ~1,110 (est.) | 37.1 M |
| ✅ c6id.8xlarge (32, x86) | **52 parsed / 112 view** | **1,431** | **42.2 M** |
| 🟥 im4gn.4xlarge (16, ARM) | 49 | ~1,080 (est.) | 38.9 M |

*hot-ingest is **WAL-fsync-bound**. The stale rows ran it single-stream
(serial), ~80 ledgers/s on x86. The fixed c6id.8xlarge ran it **`--parallel`,
both modes**: **views = 112 ledgers/s vs parsed = 52** — views ~2.1× faster by
skipping the 8.4 ms per-ledger `UnmarshalBinary`. The ARM box is ~1.6× slower on
the fsync + encode path. **cold-ingest** is batched (no per-ledger fsync) and runs
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
| 🟥 c6id.2xlarge (8, x86) | 2.59 | 0.47 | 1.39 | 7.24 | 2.68 | 0.76 | 0.12 |
| 🟥 c6id.4xlarge (16, x86) | 2.47 | 0.46 | 1.37 | 6.63 | 2.67 | 0.82 | 0.12 |
| ✅ c6id.8xlarge (32, x86) | 2.54 | 0.48 | 1.41 | 6.46 | 2.07 | 0.73 | 0.12 |
| 🟥 im4gn.4xlarge (16, ARM) | 4.63 | 0.71 | 2.26 | 10.24 | 2.40 | 0.83 | 0.15 |

✅ c6id.8xlarge = fixed run, **view** (xdr-views) mode. In **parsed** mode the
driver additionally pays ~8.4 ms/ledger `lcm_decode` (UnmarshalBinary) that view
mode skips — the dominant per-ledger difference (full breakdown: main report §2.3).

*Hot **event write** (RocksDB put + WAL) is the single most expensive stage
(~6.5–10 ms/batch) and dominates hot-ingest cost. xdr-view **extract** is cheap
(~0.5 ms/ledger for tx-hash, ~1.4 ms for events). Graviton2 is ~1.5–1.8× slower
on the CPU-bound extract/write stages.*

> ⚠️ These `ops/s` figures are **only comparable within this 6/03 run**. The
> metric was computed differently in the 2026-05-21 run, so do not compare
> throughput across the two reports — single-query p50 latency is the only valid
> cross-run comparison.
