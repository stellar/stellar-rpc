# bench

`bench` measures the ingestion and the read handlers of the rpcv2 daemon. It
is a subcommand of the `stellar-rpc` binary in rpcv2 mode only. Select the mode
with `--enable-rpcv2` or `STELLAR_RPC_ENABLE_RPCV2=true`. Release builds cannot
run rpcv2, so build with `RPCV2=true`.

| Command | Does |
|---|---|
| `bench ingest cold` | Runs the daemon backfill over a chunk range. Writes a cold dataset. |
| `bench ingest hot` | Runs the daemon live ingestion loop over a chunk range. Writes a hot dataset. |
| `bench query` | Calls the daemon read handlers on a dataset at fixed request rates. Measures latency. |

```
make build-stellar-rpc RPCV2=true
./stellar-rpc --enable-rpcv2 bench ingest cold --pack-dir PACKS \
    --start-chunk 6000 --num-chunks 2 --cold-out-dir /data/cold --out out/ingest
./stellar-rpc --enable-rpcv2 bench query --data-dir /data/cold \
    --types ledgers,txhash --target-rps 10,100 --out out/query
```

`--help` on each command lists all flags and their defaults.

All commands:

- Write the report and `run.json` to `--out` (default `bench-out`). `--out`
  must be missing or empty.
- Write a Go CPU profile with `--cpuprofile PATH` and an allocation profile
  with `--memprofile PATH`. `bench query` writes one CPU profile for each
  scenario. It adds `.<type>.<rps>rps` before the extension, for example
  `cpu.ledgers.10rps.pprof`. The profile covers the measured iterations only.
- Stop on SIGINT or SIGTERM and write a partial report. The log marks it
  PARTIAL.

## Dataset

A dataset is a directory with the layout of the daemon data directory.

| Command | Writes |
|---|---|
| `bench ingest cold --cold-out-dir DIR` | Frozen files (`ledgers`, `events`, `txhash`) and a catalog (`catalog/rocksdb`) |
| `bench ingest hot --hot-dir DIR` | Hot chunk databases (`hot`) and a catalog |

- `DIR` must be missing or empty. `--out` must be outside `DIR`.
- When the ingest succeeds, it records the first ledger of the range as the
  earliest ledger. It also creates the hot database of the live chunk, as
  daemon startup does. Thus `bench query` changes no catalog key.

`bench query --data-dir DIR` opens the dataset with the daemon startup code
(`rpcv2.OpenServing`).

- The **served range** starts at the first ledger of `--start-chunk` (default:
  the first chunk of the dataset). It ends at the last committed ledger.
  `--sample-ledgers N` keeps only the first `N` ledgers of the range.
- All chunks in the served range must be cold, or all must be hot.
  `settings.tier` records the tier.
- On a cold dataset, the ledger and events files of the served chunks and
  the tx-hash index files that the catalog lists as frozen must exist.

## `bench ingest` report

Each CSV file has the columns `stage`, `n`, `n_items`, `total_ns`, `p50_ns`,
`p90_ns`, `p99_ns` and `max_ns`. `n` is the sample count and `n_items` is the
sum of the items in the samples. The bench drops samples of 0 ns, except in
`pace_lag`. It does not write a row with no samples.

| File | Rows |
|---|---|
| `ledgers.csv`, `txhash.csv`, `events.csv` | Cold: one row for each freeze stage of the data type (`term_index`, `write`, `finalize`) |
| `hot.csv` | Hot: one row for each phase of one ledger (`extract`, `ledgers`, `txhash`, `events`, `commit`, `apply`) |
| `driver.csv` | Cold: `backfill_wall`, `index_rebuild`, `chunk_total`, `<type>_total`, `cold_extract`. Hot: `ingest_total` (sum of the phases of one ledger), `run_wall`, `pace_lag` |

The row labels in `csvsink.go` say what each `driver.csv` row times.

`bench ingest hot --close-interval D` gives ingestion one ledger each `D`.
`pace_lag` is the time from the due time of a ledger to its commit. With the
default `0`, ingest does not wait between ledgers and the run measures
catch-up speed.

## `bench query`

### Request path

Each request is one JSON-RPC call to a daemon read handler
(`rpcv2.ReadMethods`). The call goes through the daemon JSON-RPC bridge
(`jsonrpc.NewBridge`) in the same process, without HTTP. The bridge runs one
handler for each CPU at a time. Other calls wait for a slot.

`rpcv2.ReadMethods` builds the handlers without the per-method queue limits
and request-duration limits. The bench adds no global request limits. Thus a
request that the daemon rejects or stops can succeed in the bench.

The load generator has a constant arrival rate. It starts each request at its
due time and does not wait for the response. At most 512 requests run at a
time. When 512 requests run at the due time of an iteration, the generator
drops that iteration.

### Formulas

A **scenario** is one query type at one target rate. A run does each type in
`--types` order, and each rate in `--target-rps` order. `r` is the rate, `D`
is `--duration` (default 60 s), `W` is `--warmup` (default 0) and `t0` is the
scenario start. Iterations `0` to `W − 1` are warmup. The report does not
include them.

```
interval            = round(1 s / r)
planned             = round(r × D) = started + dropped
started             = succeeded + failed
due(i)              = t0 + i × interval              for i = 0 … W + planned − 1
start_delay(i)      = max(reach(i) − due(i), 0)
latency(i)          = call_end(i) − call_start(i)
handler_latency(i)  = handler_end(i) − handler_start(i)
latency_from_due(i) = response(i) − due(i)
schedule            = planned × interval
elapsed             = max(schedule, last_response − due(W))
overrun             = elapsed − schedule
achieved_rps        = started / schedule
completion_rps      = succeeded / elapsed
process_cpu         = cpu(end) − cpu(reach(W))
pq                  = s[ceil(q × n) − 1]             for q = 0.50, 0.90, 0.99
```

- `reach(i)` is the time the generator wakes for iteration `i`. A late start
  does not move the due times that follow it.
- `latency` is the main result. It contains the bridge request parse, the wait
  for a handler slot, the handler and the response write. The request encode
  is outside the timer. The bench does not decode a successful response.
- `handler_latency` contains the handler only, with its read view.
- `latency`, `handler_latency` and `latency_from_due` use succeeded requests
  only. `start_delay` uses all measured iterations, dropped iterations too.
- `last_response` is the last measured response, failed requests included.
- `cpu(t)` is the user plus system CPU time of the process. `end` is when the
  last request of the scenario returns.
- `pq` is the nearest-rank percentile of the `n` sorted samples
  `s[0] … s[n − 1]`. The log shows a warning for a `latency` row with fewer
  than 100 samples.

### Failures and cancel

- A failed measured request stops the run after its scenario is in the
  report. `run.json` has `status` `failed`.
- A failed warmup request does not stop the run.
- On SIGINT or SIGTERM, the report holds each scenario that reached a measured
  iteration. The canceled scenario holds the iterations before the cancel.
  `run.json` has `status` `failed`. A request that the cancel stops counts as
  failed only if it returns an error.

### `latency.csv`

One row for each scenario, metric and outcome that has samples.

| Column | Value |
|---|---|
| `query_type` | `ledgers`, `txpage`, `txhash` or `events` |
| `target_rps` | `r` |
| `metric` | `latency`, `handler_latency`, `latency_from_due` or `start_delay` |
| `outcome` | `all` for each type. For `txhash` also `found` and `not_found`. For `events` also the label of each filter set (see [Events pool](#events-pool)). `start_delay` has `all` only. |
| `count` | `n` |
| `items` | Sum of the items in the responses. `0` for `start_delay`. |
| `total_ns` | Sum of the samples |
| `p50_ns`, `p90_ns`, `p99_ns`, `max_ns` | Percentiles and maximum |

### `scenarios.csv`

One row for each scenario, in run order.

| Column | Value |
|---|---|
| `query_type`, `target_rps` | As in `latency.csv` |
| `planned`, `started`, `dropped`, `succeeded`, `failed` | Measured counts |
| `warmup_planned`, `warmup_dropped`, `warmup_failed` | Warmup counts |
| `achieved_rps`, `completion_rps` | Rates |
| `schedule_ns`, `elapsed_ns`, `overrun_ns` | Windows |
| `process_cpu_ns` | `process_cpu` |
| `page_cache_evict_ns` | Time of the eviction before the scenario. Empty when no eviction ran. |

### `run.json`

All commands write `run.json` at the start with `status` `running`, and again
at the end.

| Key | Value |
|---|---|
| `schemaVersion` | `2` |
| `command` | Command path, for example `stellar-rpc bench query` |
| `flags` | Each flag value, defaults included |
| `binary` | `version`, `commitHash`, `buildTimestamp`, `branch` |
| `hostname`, `gomaxprocs`, `numCpu` | Host and CPUs |
| `startedAt`, `finishedAt` | UTC, RFC 3339 |
| `peakRssBytes` | `VmHWM`. Absent without `/proc`. |
| `settings` | Values that `bench query` sets (below). Absent for `bench ingest`. |
| `setupNs` | `storeOpen`: time of the dataset open of `bench query`, in ns |
| `status` | `running`, `ok` or `failed` |
| `error` | Error of a failed run |

| `settings` key | Value |
|---|---|
| `tier` | `cold` or `hot` |
| `format` | `--format` |
| `handlerConcurrency` | Bridge handler slots (number of CPUs) |
| `cacheScenario` | Cache state when measurement starts (see [Cache controls](#cache-controls)) |
| `pageCacheEviction` | `off`, `requested` or `unsupported-on-this-platform` |
| `txhashPoolHashes` | Hashes in the txhash pool |
| `eventsPool` | Labels of the filter sets in the events pool, comma-joined |
| `fixedReadRange` | Types whose requests read the same ledgers each time |

### `bench.txt`

`bench query` writes `bench.txt` in Go benchmark format, for `benchstat`. It
writes the file only when every scenario succeeds. Use it only when `run.json`
has `status` `ok`: if the process stops or the last `run.json` write fails,
`status` stays `running`.

After the `goos` and `goarch` lines, the file has one line for each scenario,
in run order. For `txhash` and `events`, when two or more outcomes have
samples, one line for each outcome follows. The name of an outcome line ends
with `/outcome=` and the `latency.csv` outcome, for example
`/outcome=no_match`.

```
BenchmarkQuery/tier=cold/type=ledgers/rps=10	600	812345 ns/op	790123 p50-ns	2345678 p99-ns	301234 handler-p50-ns	901234 handler-p99-ns	2400000 p99-from-due-ns	10 items/op	0 dropped
```

| Field | Value |
|---|---|
| Name | `tier` is `settings.tier`. `type` and `rps` are `query_type` and `target_rps`. |
| Iterations | `succeeded`. On an outcome line, `count` of the outcome. When no request succeeded, `planned`. |
| `ns/op` | `total_ns / count` of `latency`, in whole nanoseconds |
| `p50-ns`, `p99-ns` | `latency` percentiles |
| `handler-p50-ns`, `handler-p99-ns` | `handler_latency` percentiles |
| `p99-from-due-ns` | `latency_from_due` p99 |
| `items/op` | `items / count` of `latency` |
| `dropped` | `dropped`. Not on an outcome line. When no request succeeded, the only field. |

The CPU profile of the scenario in the example is `cpu.ledgers.10rps.pprof`.

To compare two builds, run each build 6 times or more on the same host, with
the same dataset and flags. Then:

```
cat old-*/bench.txt > old.txt
cat new-*/bench.txt > new.txt
benchstat -filter '/outcome:""' old.txt new.txt
```

This filter keeps one line for each scenario. Examine `dropped` first. If
`dropped` changed, the two builds ran under different loads.

That line of `txhash` and `events` mixes request kinds: found and not-found
lookups, or the filter sets of the events pool. To compare one kind, use the
outcome lines:

```
benchstat -filter '/type:txhash -/outcome:""' old.txt new.txt
```

### Query types

| Type | Request | `items` |
|---|---|---|
| `ledgers` | `getLedgers`, `--ledgers-limit` ledgers (default 10, maximum 20) from a random start | Ledgers |
| `txpage` | `getTransactions`, `--txpage-limit` transactions (default 200, maximum 200) from a random start | Transactions |
| `txhash` | `getTransaction` for a hash from the txhash pool, or for a random hash | 1 if found, else 0 |
| `events` | `queryEvents`, `--events-limit` events (default 10, maximum 1000) from a random start to the end of the range, with a filter set from the events pool | Events |

- A `ledgers` request fails if it returns fewer ledgers than it asks for. A
  `txhash` request fails if the found status is not the expected one. An
  `events` request with the `no_match` filter set fails if it returns events.
- `txpage` and `events` scan at most 10,000 ledgers for each request. An empty
  `events` page is not an error. Examine `items`.
- `fixedReadRange` lists a type when the served range is too small for random
  starts. The percentiles of that type then measure a cached read.
- `--seed` (default 1) sets the random choices of the requests and of the pool
  builds. A run with the same seed reads the same ledgers.
- The pool builds are not timed.

#### Txhash pool

Before the first `txhash` scenario, the run picks random ledgers from the
served range. It takes the hash of one random transaction from each ledger,
up to `--txhash-pool-size` hashes (default 512). A ledger with no
transactions gives no hash. The run then calls `getTransaction` for the first
hash, and the run fails if the call does not return the transaction:

- With a wrong `--network-passphrase`, the call returns an error, because the
  handler cannot pair the transaction with its envelope.
- A not-found result means that the tx-hash index of the dataset does not
  resolve a hash from a served ledger.

`--not-found-fraction` (default 0.12) of the lookups use 32 random bytes.
These lookups have the outcome `not_found`.

#### Events pool

Before the first `events` scenario, the run reads up to 20,000 events as
unfiltered `queryEvents` pages from random start ledgers. Each page uses the
maximum limit (1000), not `--events-limit`. The run makes these filter sets
from the events. Each `events` request uses one set, and the label of the set
is the outcome of the request.

| Set | Filter |
|---|---|
| `unfiltered` | No filter |
| `frequent_contract` | The contract with the most sampled events |
| `frequent_pair` | The most frequent pair of contract and first topic |
| `rare_contract` | The contract with the fewest sampled events |
| `no_match` | The `frequent_contract` contract and a first topic that the sample shows only with other contracts |

The run adds `no_match` only when a `queryEvents` probe of the served range
finds no event for it. It probes at most 8 candidates. A set that the sample
cannot supply is not in the pool. If the sample has no contract ID, the pool
holds only `unfiltered`.

### Cache controls

| `cacheScenario` | Before measurement |
|---|---|
| `cold-start` | Eviction ran, no warmup |
| `warm-run` | Warmup ran, after the eviction if one ran |
| `existing-cache` | No eviction, no warmup |

- `--evict-page-cache` (default true) applies to a cold dataset on Linux only.
  Before each scenario, it requests `POSIX_FADV_DONTNEED` for the ledger and
  events files of the served chunks and for the frozen tx-hash index files.
  The request is advisory, so the kernel can keep some pages.
- `pageCacheEviction` is `off` when the flag is false or the dataset is hot,
  and `unsupported-on-this-platform` on a cold dataset off Linux. Then no
  eviction runs, and the log shows a warning for the unsupported case.
- A warmup after the eviction loads pages back into the page cache, and the
  log shows a warning. Use `--warmup 0` to start measurement after the
  eviction.
- Without eviction, a scenario starts with the pages that the dataset open,
  the pool builds and the earlier scenarios read.
- On a hot dataset, the RocksDB block cache of each hot chunk (512 MB) keeps
  the blocks that earlier reads loaded. Page-cache eviction does not clear
  this cache.
- Eviction runs once, before the scenario. During the scenario, a request can
  read pages that earlier requests loaded, and this share grows with the rate.
  `txhash` found lookups read only the ledgers of the pool (at most
  `--txhash-pool-size`) and their index entries, so they read the same pages
  again more often than the other types. Thus `cacheScenario` gives the cache
  state only when measurement starts. A `cold-start` row is not cold-cache latency,
  and rows at different rates differ in cache state as well as in load.
