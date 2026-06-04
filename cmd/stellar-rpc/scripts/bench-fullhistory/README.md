# bench-fullhistory

Benchmark harness for the Stellar full-history reader/ingest path. It
measures two things:

- **Read / query** performance — serving ledgers, transaction pages,
  `getTransaction(hash)`, and event queries from the **hot** tier
  (RocksDB) and the **cold** tier (immutable packfiles).
- **Ingest** performance — writing those tiers from a ledger source
  (a local cold packfile or a GCS-backed `BufferedStorageBackend`).

It is a developer benchmark, not a production tool: it prints latency
percentiles + throughput to stdout and writes per-stage aggregation
CSVs.

## Running

```sh
bench-fullhistory <sub-command> [flags]
```

Run `bench-fullhistory <sub-command> -h` for the exact flags of any
command; this README covers what each does and the flags you'll usually
reach for.

> **Storage matters.** The cold read benches evict the packfile from the
> OS page cache between iterations to measure cold-fault latency, and the
> ingest benches do a lot of sequential I/O. Point `--cold-dir` /
> `--cold-out-dir` at a real block device (e.g. local NVMe). A tmpfs /
> ramdisk defeats the cold-cache methodology and produces meaningless
> read numbers.

## Command families

| workload | cold tier (packfile) | hot tier (RocksDB) |
|---|---|---|
| ledger reads | `cold-ledgers` | `hot-ledgers` |
| tx-page reads | `cold-txpage` | `hot-txpage` |
| `getTransaction(hash)` | `cold-txhash` | `hot-txhash` |
| `eventstore.Query` | `cold-events` | `hot-events` |
| ingest | `cold-ingest` | `hot-ingest` |
| cold txhash index (phase 2) | `build-txhash-index` | — |

## Read / query benches

Read benches are split per tier (`cold-X` / `hot-X`) on purpose, because
each tier's measurement methodology is baked into the loop and the two
shapes can't share one body without misrepresenting what each tier costs:

- **cold**: per iteration, pick a chunk, **evict its packfile(s) from the
  OS page cache**, open a *fresh* `ColdReader`, do one operation, close.
  No warmup — every iteration pays the cold-fault cost.
- **hot**: open **one shared** `HotStore` handle for the whole run and do
  an N-iteration RocksDB block-cache **warmup** before the timed iters,
  matching the long-lived server process.

**Modeling concurrent load — the `--query-concurrency` flag.** Each read bench
models a server serving some number of queries **in parallel**:
`--query-concurrency=N` runs N goroutines that each issue queries back-to-back, so
N requests are in flight at any instant. It is a *closed-loop* load model
at concurrency N (a worker issues its next query the moment the previous
one returns — there is no inter-request think time or fixed arrival rate),
which measures latency + throughput under sustained concurrency N.

`--query-concurrency` is a **sweep**: pass a comma-list (e.g. `--query-concurrency=1,4,8,16`)
and the bench runs each concurrency level in turn, printing one row per
level + a saturation line and writing one summary CSV row per level (plus
per-iter detail rows tagged with the level).

Cold benches accept optional `--chunk-lo` / `--chunk-hi` to constrain the
chunk range (default: auto-discover from `--cold-dir`). One caveat: when
`--query-concurrency` exceeds the number of available chunks, cold workers begin
evicting each other's just-faulted pages, so that regime measures
warm-cache contention rather than pure cold-fault latency — keep
chunks ≫ workers (or use a single worker) for clean cold-fault numbers.

| command | what it measures |
|---|---|
| `cold-ledgers` / `hot-ledgers` | reading `--n` consecutive raw ledgers from a random in-chunk position |
| `cold-txpage` / `hot-txpage` | fetching a page of N transactions from a random cursor |
| `cold-txhash` / `hot-txhash` | `getTransaction(hash)` end-to-end (lookup → fetch → scan → materialize). `--xdr-views` toggles the scan/materialize between the zero-copy view path and the `UnmarshalBinary` + parse round-trip. `cold-txhash` evicts the streamhash MPHF from the page cache at startup so the run begins cold; `--evict-mphf` additionally evicts + re-opens the MPHF per iter (single-worker only) to measure cold-fault latency on every lookup, reported in a new `mphf_open_ns` column |
| `cold-events` / `hot-events` | `eventstore.Query`. A reproducible corpus is auto-generated per chunk (one-shot scan → highest-volume contracts + topic terms → round-robin K-filter partition per iter; see `corpus.go`). Reproducible from `(chunk, seed)` |

Example:

```sh
bench-fullhistory cold-events \
  --cold-events-dir=/path/to/events/cold \
  --query-concurrency=1,4,8,16 --out=bench-out
```

## Ingest benches

Ingest is **unified across data types**: one `hot-ingest` and one
`cold-ingest`, each selecting any subset of `{ledgers, txhash, events}`
via `--types=`. The driver streams each ledger's raw bytes from a
`ledgerbackend.LedgerStream` (via `RawLedgers`) and fans them out to every
enabled type's writer, so one pass produces all selected artifacts.

Shared flags:

| flag | meaning |
|---|---|
| `--types=ledgers,txhash,events` | which data types to ingest (any subset; required) |
| `--source=pack\|bsb\|lcm` | `pack` reads a local cold packfile; `bsb` reads from a GCS `BufferedStorageBackend`; `lcm` reads a framed `LedgerCloseMeta` file from stellar-core `apply-load` (see [Synthetic ledgers](#synthetic-ledgers-via-apply-load)) |
| `--cold-dir=DIR` | source cold-store dir (required for `--source=pack`) |
| `--bucket-path=...` | GCS `destination_bucket_path` (for `--source=bsb`); ADC credentials required |
| `--lcm-file=FILE` | apply-load `meta.xdr` (required for `--source=lcm`) |
| `--lcm-checkpoint=N` | skip leading ledgers with seq ≤ N (apply-load setup ledgers; for `--source=lcm`) |
| `--lcm-fix-tx-hashes` | repair apply-load's tx-hash/envelope mismatch so the roundtrip reader can consume the meta (default `true`; `--source=lcm`) |
| `--lcm-allow-partial` | allow a short final chunk when the run was sized below 10k ledgers (default `true`; `--source=lcm`) |
| `--bsb-buffer-size`, `--bsb-num-workers` | BSB prefetch tuning |
| `--chunk=N` | first chunk ID to ingest (required) |
| `--xdr-views` | extract via zero-copy XDR views instead of `UnmarshalBinary` + struct walk |
| `--parallel` | run the per-type ingesters concurrently within each ledger |
| `--out=DIR` | CSV output dir |
| `--cpuprofile`, `--memprofile` | write Go pprof profiles |

Output: one per-stage aggregation CSV per data type plus one driver CSV,
columns `stage,n,n_items,total_ns,p50_ns,p90_ns,p99_ns,max_ns`; a
percentile summary is printed to stdout.

### `hot-ingest`

Writes the hot (RocksDB) tier. **Single chunk per run** (`--chunk=N`);
output goes under `--hot-dir` (per-type subdirs are created). Because hot
writes fsync per ledger, `--parallel` is its only intra-run concurrency
axis.

```sh
bench-fullhistory hot-ingest --types=ledgers,txhash,events \
  --source=pack --cold-dir=/path/to/ledgers/cold \
  --chunk=5860 --hot-dir=/path/to/out/hot --xdr-views --out=bench-out
```

### `cold-ingest`

Writes the cold (packfile) tier and supports **multi-chunk** runs:

| flag | meaning |
|---|---|
| `--num-chunks=M` | ingest M consecutive chunks starting at `--chunk` |
| `--chunk-workers=W` | process up to W chunks concurrently (clamped to `--num-chunks`) |
| `--cold-out-dir=DIR` | output root (per-type subdirs `ledgers/`, `events/`, `txhash/` are created; must be empty / differ from `--cold-dir`) |
| `--ledgers-packfile-concurrency`, `--ledgers-bytes-per-sync` | per-packfile tuning for the ledger writer |
| `--events-packfile-concurrency`, `--events-bytes-per-sync` | per-packfile tuning for the events writer |

`--chunk-workers` is the throughput lever — each worker processes one
chunk with its own source backend (a per-chunk pack reader, or a per-chunk
BSB session). For `--source=bsb`, `--bsb-buffer-size` / `--bsb-num-workers`
are **per chunk worker**, so total GCS download concurrency and buffer
memory scale with `--chunk-workers`; size them accordingly.

`--types=txhash` writes only phase-1 `.bin` files; run `build-txhash-index`
to produce the queryable `.idx`.

```sh
bench-fullhistory cold-ingest --types=ledgers,txhash,events \
  --source=pack --cold-dir=/path/to/ledgers/cold \
  --chunk=5860 --num-chunks=16 --chunk-workers=8 --xdr-views \
  --cold-out-dir=/path/to/out/cold --out=bench-out
```

### `build-txhash-index`

Phase 2 of the cold txhash index build: k-way merges the per-chunk `.bin`
files produced by `cold-ingest --types=txhash` (in `--in-dir`) into a
single `streamhash` sorted index (`MinLedger` auto-derived from the
smallest chunk ID present and embedded as user metadata).

```sh
# phase 1
bench-fullhistory cold-ingest --types=txhash --source=pack \
  --cold-dir=/path/to/ledgers/cold --chunk=5860 --num-chunks=16 \
  --cold-out-dir=/path/to/out/cold --out=bench-out
# phase 2
bench-fullhistory build-txhash-index --in-dir=/path/to/out/cold/txhash
```

## Synthetic ledgers via `apply-load`

When you don't have (or don't want) real pubnet chunks, you can generate
**fully synthetic, density-controlled** packfiles with stellar-core's
`apply-load` command. `apply-load-gen.sh` drives the whole pipeline:

```
apply-load  →  meta.xdr (framed LedgerCloseMeta)  →  cold-ingest --source=lcm  →  packfiles  →  build-txhash-index
```

```sh
# A small SAC run is enough to exercise the read benches: TPS is set by
# per-ledger DENSITY, not ledger count, so a few hundred ledgers already hit
# the profile's target throughput.
CORE_BIN=/path/to/stellar-core PROFILE=sac NUM_LEDGERS=300 \
  ./apply-load-gen.sh
```

**Workload profiles** (`PROFILE=`) map to apply-load's model transactions and
target throughputs (TPS = txs-per-ledger ÷ ledger-close-time; defaults assume
`CLOSE_TIME_S=1`):

| `PROFILE` | model tx (`APPLY_LOAD_MODEL_TX`) | target |
|---|---|---|
| `sac` | `sac` (Stellar Asset Contract transfer) | ~10k SAC TPS |
| `token` (`oz`) | `custom_token` (OpenZeppelin-style token) | ~9k OZ TPS |
| `soroswap` | `soroswap` (AMM swap, real mainnet wasm) | ~2.5k TPS |

Key env knobs: `NUM_LEDGERS` (total ledgers to generate; **prefer this for a
quick run** — the final chunk may be partial), `CHUNKS` (10k-ledger chunks to
fill, default 16; ignored when `NUM_LEDGERS` is set), `CLOSE_TIME_S`,
`TXS_PER_LEDGER` (override the derived density), `TYPES`, `CHUNK_WORKERS`,
`OUT_ROOT`, `KEEP_META`, `BENCH_BIN`.

**Requirements & caveats:**

- Needs a stellar-core built with **`BUILD_TESTS`** (the CI build tagged
  `…~buildtests`) — `apply-load` + `ARTIFICIALLY_GENERATE_LOAD_FOR_TESTING`
  are test-only.
- **Cost scales with density, not just count.** apply-load close time grows with
  txs/ledger and accumulated state: `sac` (1 fat batched tx/ledger) runs at
  ~0.1 s/ledger, but `token`/`soroswap` apply ~9k txs/ledger at ~9 s/ledger and
  rising. A full 10k-ledger chunk of dense Soroban load is **hours to days** —
  so size dense profiles with a small `NUM_LEDGERS` (a few hundred), which still
  meets the TPS target.
- **apply-load tx-hash fixup (automatic).** `apply-load`'s streamed meta records
  the same transactions in the tx-set and in `TxProcessing`, but the stored
  result hash does **not** equal the envelope's real hash, so the go-stellar-sdk
  ingest `LedgerTransactionReader` (which pairs envelope↔result by hash) rejects
  it with *"unknown tx hash in LedgerCloseMeta"* — breaking the roundtrip
  tx-page / tx-hash benches. `cold-ingest --source=lcm` repairs this by default
  (`--lcm-fix-tx-hashes`): it pairs each result to its envelope via the
  fee-charged account and stamps the correct hash. See `lcm_fixup.go`.
- The `lcm` source assigns ledger sequences **positionally** per chunk (chunk 1
  → seqs 10002…20001, etc.), skipping apply-load setup ledgers
  (`--lcm-checkpoint`). The final chunk may be **partial** when the run was
  sized below a full chunk (`--lcm-allow-partial`, on by default); the read
  benches clamp their cursors to each chunk's actual ledger range.
- **`cold-events` works for `sac` and `soroswap`, not `token`.** The corpus
  builder needs enough unique *terms* (contract anchors + topic values) to fill
  the K-bucket sweep (≥ max K, default 15) — it does **not** require 3 distinct
  contracts. `sac` (one SAC contract whose `transfer` events vary `from`/`to`
  over thousands of accounts) and `soroswap` (router + pair contracts) both
  reach 15 terms from a single/few contracts. `token` (`custom_token`) emits
  events that are not 4-topic, so it yields no usable terms — use `sac` or
  `soroswap` for event benches. `cold-ledgers`/`cold-txpage`/`cold-txhash` work
  for all profiles.

## Interpreting ingest output

- **`total wall`** — end-to-end wall time. For multi-chunk cold runs it is
  followed by `sum(chunk_wall)/total`, the **effective concurrency**: how
  many chunks overlapped on average (≈ `--chunk-workers` when fully
  parallel, lower when I/O- or GC-bound).
- **`<type>.<stage>`** lines — per-stage percentiles (extract, write,
  term_index, cold_append, …).
- **`<type> wall=… / in-pipeline=…`** — end-to-end vs extract+write
  throughput for that data type.

## Layout

- `main.go` — sub-command dispatch + shared stats/CSV helpers.
- `bench_{hot,cold}_{ledgers,txpage,txhash,events}.go` — read benches.
- `bench_concurrent_runner.go`, `bench_grid.go` — the `--query-concurrency` sweep scaffolding.
- `bench_{hot,cold}_ingest.go` — ingest drivers.
- `ingest_{ledgers,txhash,events}.go` — per-type ingesters + collectors.
- `ingester.go`, `ledger.go`, `extract_{views,parsed}.go`, `sources.go` — ingest plumbing (`sources.go` has the `pack`/`bsb`/`lcm` ledger sources).
- `apply-load-gen.sh` — synthetic-ledger driver: stellar-core `apply-load` → `meta.xdr` → packfiles.
- `bench_build_txhash_index.go`, `streamhash_merge.go` — phase-2 index build.
- `corpus.go`, `cache*.go`, `tx_hash_helpers.go`, `metrics_helpers.go` — shared helpers.
