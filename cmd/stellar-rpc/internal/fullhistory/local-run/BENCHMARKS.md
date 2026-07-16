# Full-history query POC — latency & concurrency benchmarks

Measured against a local `stellar-rpc full-history` daemon (this branch,
`poc/fullhistory-query-localrun`) ingesting live **pubnet** and serving
`getLedgers` / `getTransactions` / `getTransaction` / `getEvents` over JSON-RPC
on `localhost:8000`, per [`README.md`](./README.md).

## Environment

| | |
|---|---|
| Host | 16 vCPU, 58 GB RAM, Debian 13 (trixie) |
| stellar-core | 27.1.0 (protocol 27) |
| Data dir | tmpfs (RAM-backed) — see note |
| Network | loopback (client + server on the same box, ~0 RTT) |
| Load tool | [`hey`](https://github.com/rakyll/hey) 0.1.5; single-request timings via `curl -w` |
| Retention | 2 chunks (20,000 ledgers), `earliest_ledger = "now"` |
| Ledger window at test | tip ≈ 63,499,700; queries aimed 500–1000 ledgers behind tip |

> The data dir was placed on a RAM-backed tmpfs (the host's physical disk was
> too small for pubnet's ~15 GB live bucket state). Numbers are therefore a
> best case for storage latency; a spinning-disk / network-volume deployment
> would be slower on cold reads.

All runs returned HTTP 200 with valid JSON-RPC results (zero errors).

---

## 1. Single-request latency (warm, sequential, n=40)

End-to-end `curl` `time_total` over loopback:

| Method | resp size | min | p50 | p90 | p99 | max | mean |
|---|--:|--:|--:|--:|--:|--:|--:|
| `getLedgers` (limit 1) | 1.7 MB | 33.0 | 35.0 | 38.1 | 38.9 | 39.1 | 35.4 ms |
| `getTransactions` (limit 5) | 17.5 KB | 7.9 | 8.5 | 9.8 | 16.5 | 16.7 | 9.2 ms |
| `getTransaction` (by hash) | 2.9 KB | 2.4 | 2.5 | 4.0 | 4.1 | 10.4 | 2.9 ms |
| `getEvents` (limit 10) | 4.9 KB | 1.7 | 1.7 | 1.8 | 2.2 | 4.2 | 1.8 ms |

### Server-side handler latency (from `/metrics`, excludes network/transfer)

| Method | avg | p95 ≤ | p99 ≤ |
|---|--:|--:|--:|
| `getEvents` | 1.68 ms | 5 ms | 5 ms |
| `getTransaction` | 2.89 ms | 5 ms | 10 ms |
| `getLedgers` | 4.61 ms | 10 ms | 25 ms |
| `getTransactions` | 10.51 ms | 25 ms | 25 ms |

`getLedgers` end-to-end (35 ms) is dominated by shipping the ~1.7 MB
`LedgerCloseMeta`, not by the ~4.6 ms server handler.

---

## 2. Concurrency sweep (`hey`, loopback, keep-alive)

### getTransaction (by hash, ~2.9 KB)

| conc | req/s | p50 | p90 | p99 | max |
|--:|--:|--:|--:|--:|--:|
| 1 | 323 | 2.5 | 4.7 | 9.3 | 11 ms |
| 8 | 1,324 | 4.2 | 11.3 | 16.6 | 19 ms |
| 16 | 1,637 | 8.1 | 17.5 | 22.8 | 33 ms |
| 32 | 1,650 | 19.1 | 29.4 | 37.9 | 51 ms |
| 64 | 1,700 | 36.5 | 51.2 | 64.5 | 101 ms |

### getEvents (limit 10, ~4.9 KB)

| conc | req/s | p50 | p90 | p99 | max |
|--:|--:|--:|--:|--:|--:|
| 1 | 460 | 1.9 | 2.5 | 7.1 | 14 ms |
| 8 | 1,905 | 2.9 | 8.3 | 13.7 | 16 ms |
| 16 | 2,207 | 5.3 | 13.4 | 19.9 | 24 ms |
| 32 | 2,213 | 13.4 | 22.9 | 29.8 | 38 ms |
| 64 | 2,256 | 27.5 | 38.6 | 55.8 | 74 ms |

### getTransactions (limit 5, ~29 KB)

| conc | req/s | p50 | p90 | p99 | max |
|--:|--:|--:|--:|--:|--:|
| 1 | 121 | 7.4 | 11.9 | 16.4 | 20 ms |
| 8 | 519 | 15.4 | 21.3 | 25.9 | 34 ms |
| 16 | 532 | 28.3 | 42.3 | 55.8 | 75 ms |
| 32 | 538 | 57.2 | 81.3 | 98.3 | 134 ms |

### getLedgers (limit 1, ~1.3 MB) — heavy payload

| conc | req/s | p50 | p90 | p99 | max |
|--:|--:|--:|--:|--:|--:|
| 1 | 36 | 27.0 | 30.4 | 35.4 | 44 ms |
| 4 | 83 | 47.5 | 58.3 | 68.0 | 72 ms |
| 8 | 80 | 95.6 | 118.7 | 158.5 | 165 ms |
| 16 | 80 | 193.8 | 244.0 | 311.4 | 372 ms |

At c=16 `getLedgers` moves ~**98 MB/s** of XDR (80 req/s × 1.29 MB).

---

## 3. Analysis

- **Throughput ceilings on this 16-core box:** `getEvents` ~2,250 req/s ·
  `getTransaction` ~1,700 · `getTransactions` ~535 · `getLedgers` ~80 req/s.
- **Saturates at concurrency ≈ 8–16, then degrades gracefully.** Past the knee,
  req/s stays flat and latency grows *linearly* with concurrency — textbook
  queueing at a worker pool the size of the core count. Matches Little's law:
  e.g. getTransaction c=64 → 64 / 1,700 ≈ 37 ms (measured avg 36.7);
  getLedgers c=16 → 16 / 80 = 200 ms (measured 194). No errors, no cliff.
- **Response size dominates cost.** Ranked by req/s it is exactly inverse to
  payload size (~5 KB → 2,250/s; 1.3 MB → 80/s). For `getLedgers` /
  `getTransactions`, latency is the price of serializing/transferring XDR —
  tune `pagination.limit` accordingly.

### Caveats

- Client and server shared the same 16 cores, so the client stole CPU — a
  dedicated client (or larger host) would raise the ceilings.
- Loopback RTT ≈ 0; add real client↔server network latency to every p50.
- Data dir on tmpfs — real disk/network storage would be slower on cold reads.
