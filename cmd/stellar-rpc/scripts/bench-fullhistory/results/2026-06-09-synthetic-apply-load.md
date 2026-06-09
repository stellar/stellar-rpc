# stellar-rpc full-history bench — synthetic apply-load datasets (2026-06-09)

Addresses **[#762](https://github.com/stellar/stellar-rpc/issues/762)** — a
*controllable* synthetic dataset whose transaction profile we set deliberately,
so we can characterize ingest/query behavior under specific load shapes instead
of only whatever pubnet happened to produce.

Three synthetic datasets were generated with stellar-core `apply-load`
(`apply-load-gen.sh`) and run through the full `bench-fullhistory` read + ingest
suite. Datasets, configs, per-iter/per-sweep CSVs, and the machine-readable
`RESULTS.md` live at
`gs://rpc-full-history/synthetic-ledgers/2026-06-04-apply-load-20k/`; every
number here is recomputed from those CSVs.

> **Relation to #762.** The issue proposed a `--source=synthetic` bench source
> backed by `ingest/loadtest.ApplyLoad`. The pinned `go-stellar-sdk` doesn't yet
> include `ingest/loadtest`, so this uses the equivalent **`--source=lcm`** path
> (read the framed `LedgerCloseMeta` `apply-load` streams) — same acceptance
> criteria (generate N-chunk datasets → `cold-ingest`/`hot-ingest` →
> `cold-*`/`hot-*` query benches), no dependency bump. Swapping in
> `loadtest.ApplyLoad` later is a drop-in producer change.

**Interactive explorer:** [`2026-06-09-synthetic-vs-pubnet-explorer.html`](./2026-06-09-synthetic-vs-pubnet-explorer.html)
— a self-contained (offline) HTML with all sweep data embedded for the three
synthetic datasets **plus the pubnet baseline**; toggle tier / decode-path /
percentiles / dataset / concurrency, sort, and overlay series. Also at
`gs://rpc-full-history/synthetic-ledgers/2026-06-04-apply-load-20k/explorer.html`.

## Setup

- **machine:** AWS c6id.8xlarge — 32 vCPU (Intel Ice Lake), 61 GB RAM, local NVMe instance store
- **core:** `stellar-core 26.1.1-3289.51ecab1e7.noble~buildtests` (commit `51ecab1e7`, ledger protocol 27)
- **gen:** `APPLY_LOAD_MODE=benchmark`, `BATCH_SAC=1`, `CLUSTERS=8`, pubnet passphrase, tx-hash fixup applied at ingest
- **block model:** 600 ms (per-ledger tx count = TPS × 0.6)
- **bench:** concurrency sweep `1,4,8,16`; iters — ledgers 60, txpage 200, txhash 1000, events 500; both decode paths (roundtrip + xdr-views)

### Datasets (single machine; all reads run on the c6id.8xlarge above)

| profile | model tx | target | tx/ledger | ledgers | chunks | txs | cold size |
|---|---|---|---|---|---|---|---|
| **sac** | SAC transfer | 10,000 TPS | 6,000 | 10,000 | 1 | 59.87 M | 28 GB |
| **token** (oz) | custom_token | 9,000 TPS | 5,400 | 10,000 | 1 | 53.85 M | 23 GB |
| **soroswap** | AMM swap | 2,500 TPS | 1,500 | 20,000 | 2 | 29.92 M | 16 GB |

A full 10k-ledger chunk of 10k-TPS SAC needs ~96–128 GB RAM (in-memory soroban
state grows ~8.5 MB/ledger); on the 61 GB box sac/token were generated at 10k
ledgers (1 chunk) and soroswap — far lighter at 1,500 tx/ledger — at 20k (2
chunks). See `SYNTHETIC-LEDGERS.md` for the per-RAM sizing table.

**`pubnet`** = real pubnet chunk 5860 on the same c6id.8xlarge (corrected harness,
`results/2026-06-03-cross-machine.md`) — the non-synthetic baseline, for contrast.

## Table 1 — Query latency, p50 / p99 @ c=1 (ms). `cold / hot`

**xdr-views** path (the realistic server path):

| workload | pubnet (baseline) | sac | token | soroswap |
|---|---|---|---|---|
| tx-page | 3.0 / 1.5 | 26.2 / 21.2 | 25.6 / 21.3 | 14.1 / 11.5 |
| tx-hash | 2.2 / 1.2 | 16.8 / 14.3 | 17.6 / 14.0 | 9.1 / 7.1 |
| events | 14.4 / 4.4 | 229 / 14.0 | 235 / 13.6 | 210 / 32.5 |
| ledgers (n=20) | 15.1 / 13.3 | 101 / 91 | 92 / 84 | 53 / 51 |

**roundtrip** path (production `UnmarshalBinary` + `ParseTransaction`):

| workload | pubnet (baseline) | sac | token | soroswap |
|---|---|---|---|---|
| tx-page | 13.2 / 11.1 | 129 / 121 | 135 / 128 | 89 / 84 |
| tx-hash | 11.9 / 10.6 | 117 / 120 | 132 / 114 | 82 / 81 |

## Table 2 — Peak query throughput, ops/s (best across c=1→16, xdr-views). `cold / hot`

| workload | pubnet (baseline) | sac | token | soroswap |
|---|---|---|---|---|
| tx-page | 3,456 / 4,830 | 411 / 491 | 412 / 498 | 732 / 888 |
| tx-hash | 4,170 / 7,253 | 575 / 734 | 610 / 789 | 992 / 1,277 |
| events | 512 / 1,843 | 120 / 740 | 112 / 1,140 | 38 / 286 |

events note: `custom_token` emits **3-topic** events (others 4-topic), so token's
corpus is built with `EVENTS_TOPIC_COUNT=3`; it still reaches the full K=15
universe. Events latency falls sharply with K (token cold 250 ms @ K=1 → 47 ms @
K=15) as more filters select fewer events.

## Table 3 — Ingest throughput (cold-ingest from pack, `--parallel --xdr-views`)

| profile | total wall | ledgers/s (e2e) | per-ledger p50 / p99 | txhash items/s | events items/s | build-txhash-index |
|---|---|---|---|---|---|---|
| sac | 8m28s | 20 | 24 / 54 ms | 701 k | 142 k | 59.9 M keys @ 23.3 M/s |
| token | 4m22s | 38 | 18 / 33 ms | 707 k | 291 k | 53.8 M keys @ 11.8 M/s |
| soroswap | 3m06s | 108 | 14 / 24 ms | 371 k | 506 k | 29.9 M keys @ 17.1 M/s |

cold-ingest end-to-end is **events-stage-bound** (term-index + cold append);
per-ledger cost scales with events/ledger. Per-ledger cold-ingest stays **under
~55 ms through p99** even on 6k-tx ledgers (rare max-tail spikes to ~0.4–1 s on
packfile flush).

## How these compare to production (pubnet) chunks

Same machine + harness, vs the pubnet chunk-5860 run
(`results/2026-06-03-cross-machine.md`):

- **Per-query latency is ~5–9× higher** and **throughput ~5–8× lower** than the
  pubnet chunk. Cause is **per-ledger density**: every query touches a whole
  `LedgerCloseMeta`, and a 1.5k–6k-tx synthetic LCM is ~50–300× larger than a
  sparse pubnet ledger. The clean gradient *within* the synthetic set —
  soroswap (1.5k) ~2× faster than sac/token (6k) on the identical code path —
  confirms density, not "synthetic-ness", is the driver.
- **Ingest is item-bound, not ledger-bound:** synthetic ledgers/s is ~15–80×
  lower, but per-item rates (keys/s, items/s) match pubnet's order. Same work
  per tx, packed into fewer, fatter ledgers.
- **Qualitative findings match pubnet** across all four workloads: xdr-views is
  4–9× faster than roundtrip; hot ≈ cold for point reads but hot wins big on
  events; throughput scales with concurrency. All benches ran with **0 errors**
  and tx-hash **miss-rate 0**.

These datasets are a **density stress test** — they exercise sustained 2.5k–10k
TPS regimes pubnet rarely produces, so absolute latencies are higher than
typical pubnet serving. They are not a substitute for the pubnet baseline; they
characterize the read/ingest path under deliberate high-TPS load shapes.

## Reproducing

See [`SYNTHETIC-LEDGERS.md`](../SYNTHETIC-LEDGERS.md). End to end:

```sh
CORE_BIN=/usr/bin/stellar-core OUT_ROOT=/mnt/nvme/synth \
PROFILES="sac token soroswap" NUM_LEDGERS=<size-to-RAM> \
GCS_DEST=gs://rpc-full-history/synthetic-ledgers/<run> \
  ./synthetic-run.sh
```

Generation is reproducible from `(config + profile)`; exact transactions are
**not** byte-reproducible (apply-load seeds its RNG from wall-clock and exposes
no seed), so the generated cold packs are the canonical pinned artifact — kept
in GCS at the path above.
