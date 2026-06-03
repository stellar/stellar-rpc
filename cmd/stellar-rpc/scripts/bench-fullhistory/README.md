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
| `--source=pack\|bsb` | `pack` reads a local cold packfile; `bsb` reads from a GCS `BufferedStorageBackend` |
| `--cold-dir=DIR` | source cold-store dir (required for `--source=pack`) |
| `--bucket-path=...` | GCS `destination_bucket_path` (for `--source=bsb`); ADC credentials required |
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
- `ingester.go`, `ledger.go`, `extract_{views,parsed}.go`, `sources.go` — ingest plumbing.
- `bench_build_txhash_index.go`, `streamhash_merge.go` — phase-2 index build.
- `corpus.go`, `cache*.go`, `tx_hash_helpers.go`, `metrics_helpers.go` — shared helpers.
