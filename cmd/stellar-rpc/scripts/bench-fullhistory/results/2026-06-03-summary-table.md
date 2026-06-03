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

**Workloads (columns):**

| Name | What it does | Fixed param |
|---|---|---|
| **ledgers** | Read a run of consecutive ledgers | `n=20` ledgers per read |
| **tx-page** | Fetch one page of transactions | `page=20` txns per page |
| **tx-hash** | `getTransaction(hash)` full round-trip (lookup → fetch → decode → re-serialize) | hits only |
| **events** | Event-filter query (random mix of contract/topic filters) | — |

**Variables:**
- **n** = ledgers read per `ledgers` query (here always 20).
- **page** = transactions per `tx-page` query (here always 20).
- **c** (query-concurrency) = queries in flight at once. `c=1` = one at a time
  (pure latency); higher `c` = load test.
- **p50 / p90 / p99** = latency percentiles in milliseconds. p50 = median
  (typical request); p99 = near-worst-case tail (1 in 100 requests is slower).
- **ops/s** = throughput (successful queries per second) at that concurrency.

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

> ⚠️ These `ops/s` figures are **only comparable within this 6/03 run**. The
> metric was computed differently in the 2026-05-21 run, so do not compare
> throughput across the two reports — single-query p50 latency is the only valid
> cross-run comparison.
