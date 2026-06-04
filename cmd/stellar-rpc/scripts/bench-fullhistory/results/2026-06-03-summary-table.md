# stellar-rpc full-history bench — 2026-06-03 streamlined summary

Condensed view of the 2026-06-03 cross-machine run on the **corrected harness**
(PR #750, commit `b712b861`; all four machines re-run). Full report with the
concurrency sweeps and ingest stage detail:
[`2026-06-03-cross-machine.md`](./2026-06-03-cross-machine.md). Numbers recomputed
from `gs://rpc-full-history/benchmarks/2026-06-03/`.

## Glossary

**Machines (rows):** `c6id` = Intel Ice Lake x86; `im4gn` = AWS Graviton2 ARM.

| Instance | vCPUs | RAM |
|---|---|---|
| c6id.2xlarge | 8 | 15 GB |
| c6id.4xlarge | 16 | 31 GB |
| c6id.8xlarge | 32 | 62 GB |
| im4gn.4xlarge | 16 | 62 GB |

- **Tier — cold:** on-disk packfiles, page cache evicted + fresh open per query.
  **hot:** live RocksDB store with a warm cache. (Presented as separate tables.)
- **Decode path — roundtrip:** full XDR decode + field re-serialization (slow
  path). **xdr-views:** zero-copy slicing of the raw LCM (the realistic server
  path). All query workloads *except ledgers* benefit from xdr-views.
- **Workloads — ledgers** (`n=20` consecutive), **tx-page** (`page=20`, full
  response page), **tx-hash** (`getTransaction(hash)`, hits), **events**
  (worst-case **15 filters**).
- **p50 / p99** = median / 99th-pct latency (ms). **c** = concurrent in-flight
  queries. **ops/s** = throughput at that concurrency.

## Table 1 — Query latency, **xdr-views** path (p50 / p99 ms @ c=1)

The realistic server path. Lower = faster.

**Cold tier**
| Machine | tx-page | tx-hash | events (K=15) | ledgers |
|---|---|---|---|---|
| c6id.2xlarge | 3.11 / 7.10 | 2.23 / 4.45 | 14.48 / 45.68 | 14.88 / 25.70 |
| c6id.4xlarge | 3.09 / 6.87 | 2.22 / 4.49 | 14.87 / 50.17 | 14.61 / 26.72 |
| c6id.8xlarge | 2.99 / 6.36 | 2.18 / 4.22 | 14.38 / 45.96 | 15.11 / 25.28 |
| im4gn.4xlarge | 4.35 / 9.51 | 4.12 / 8.10 | 18.67 / 64.72 | 27.55 / 50.02 |

**Hot tier**
| Machine | tx-page | tx-hash | events (K=15) | ledgers |
|---|---|---|---|---|
| c6id.2xlarge | 1.59 / 5.00 | 1.18 / 2.55 | 4.74 / 9.42 | 13.53 / 18.40 |
| c6id.4xlarge | 1.60 / 4.69 | 1.23 / 2.78 | 4.83 / 8.95 | 13.26 / 19.04 |
| c6id.8xlarge | 1.52 / 5.02 | 1.19 / 2.71 | 4.44 / 7.66 | 13.29 / 21.24 |
| im4gn.4xlarge | 2.57 / 8.65 | 2.08 / 3.91 | 7.04 / 8.75 | 25.68 / 32.97 |

*ledgers has no xdr-views variant (raw bytes, no decode); its number is the same
in Table 2.*

## Table 2 — Query latency, **roundtrip** path / no views (p50 / p99 ms @ c=1)

The slow `UnmarshalBinary` + `ParseTransaction` path, for contrast.

**Cold tier**
| Machine | tx-page | tx-hash | events (K=15) |
|---|---|---|---|
| c6id.2xlarge | 13.82 / 31.31 | 12.35 / 21.36 | 15.94 / 49.81 |
| c6id.4xlarge | 13.59 / 32.40 | 12.44 / 21.54 | 16.46 / 48.99 |
| c6id.8xlarge | 13.22 / 29.84 | 11.86 / 20.31 | 15.44 / 48.52 |
| im4gn.4xlarge | 23.86 / 54.35 | 22.13 / 37.84 | 21.40 / 69.13 |

**Hot tier**
| Machine | tx-page | tx-hash | events (K=15) |
|---|---|---|---|
| c6id.2xlarge | 11.99 / 27.70 | 11.38 / 17.58 | 6.62 / 14.69 |
| c6id.4xlarge | 12.00 / 25.47 | 11.24 / 18.09 | 6.63 / 14.72 |
| c6id.8xlarge | 11.10 / 24.63 | 10.55 / 16.93 | 6.05 / 13.75 |
| im4gn.4xlarge | 21.24 / 44.59 | 19.92 / 30.58 | 10.69 / 16.55 |

*xdr-views cuts tx-page/tx-hash p50 by **4–9×**; events only ~1.1× cold / ~1.4×
hot (the query is index/IO-bound, not decode-bound).*

## Table 3 — Peak query throughput (ops/s @ c=16, xdr-views)

Higher = better.

**Cold tier**
| Machine | tx-page | tx-hash | events | ledgers |
|---|---|---|---|---|
| c6id.2xlarge | 1,501 | 1,329 | 127 | 255 |
| c6id.4xlarge | 2,925 | 2,439 | 251 | 483 |
| c6id.8xlarge | 3,456 | 4,170 | 512 | 783 |
| im4gn.4xlarge | 2,662 | 3,186 | 412 | 456 |

**Hot tier**
| Machine | tx-page | tx-hash | events | ledgers |
|---|---|---|---|---|
| c6id.2xlarge | 2,042 | 3,054 | 523 | 360 |
| c6id.4xlarge | 3,884 | 5,749 | 1,055 | 695 |
| c6id.8xlarge | 4,830 | 7,253 | 1,843 | 902 |
| im4gn.4xlarge | 3,899 | 5,688 | 901 | 510 |

## Table 4 — Ingest throughput

`--parallel` ingest. Hot is single-stream WAL-fsync-bound; cold is batched +
chunk-parallel (`--chunk-workers=8`). Higher = better.

| Machine | hot ingest (xdr-views) | hot ingest (parsed) | cold ingest (est.) | build-txhash-index |
|---|---|---|---|---|
| c6id.2xlarge | 94 ledgers/s | 42 ledgers/s | ~568 ledgers/s | 25.9 M keys/s |
| c6id.4xlarge | 101 ledgers/s | 47 ledgers/s | ~1,107 ledgers/s | 36.9 M keys/s |
| c6id.8xlarge | 112 ledgers/s | 52 ledgers/s | ~1,630 ledgers/s | 38.5 M keys/s |
| im4gn.4xlarge | 68 ledgers/s | 29 ledgers/s | ~1,013 ledgers/s | 40.1 M keys/s |

*xdr-views ingest is ~2.1–2.4× faster than parsed (it skips the ~8–16 ms/ledger
`lcm_decode`). cold-ingest rate is an upper-bound estimate
(`160k ÷ (sum(chunk_wall) ÷ chunk-workers)`).*

> ⚠️ `ops/s` and `ledgers/s` are comparable **only within this run** — the
> 2026-05-21 report computed throughput differently. Only single-in-flight p50
> latency compares across reports.
