# Storage Benchmarks

`stellar-rpc-v2 bench` has three subcommands:

| Command | Does |
|---|---|
| `bench ingest cold` | Runs the daemon backfill over a chunk range. Writes a cold dataset. |
| `bench ingest hot` | Runs the daemon live ingestion loop over a chunk range. Writes a hot dataset. |
| `bench query` | Sends read requests to the daemon handlers over a dataset. Measures latency. |

Each command writes its report and `run.json` to `--out`. `--out` must be
missing or empty.

## Dataset

A dataset is a directory with the layout of the daemon data directory.

- `bench ingest cold --cold-out-dir DIR` writes frozen artifacts (`ledgers`,
  `events`, `txhash`) and a catalog (`catalog/rocksdb`).
- `bench ingest hot --hot-dir DIR` writes hot chunk databases (`hot`) and a
  catalog.
- `DIR` must be missing or empty. `--out` must be outside `DIR`.
- At the end, ingest records the first ledger of the range as the earliest
  ledger. It also makes sure that the live chunk has a hot database, as
  daemon startup does.

`bench query --data-dir DIR` opens the dataset with the daemon startup code
(`rpcv2.OpenServing`). It changes no catalog key.

- The **served range** starts at the first ledger of `--start-chunk` (default:
  the first chunk of the dataset). It ends at the last committed ledger.
  `--sample-ledgers N` keeps only the first `N` ledgers of the range.
- The catalog gives the tier of each chunk in the range. All chunks must have
  the same tier. `settings.tier` records it.

## Request path

Each request is one JSON-RPC call to a daemon read handler
(`rpcv2.ReadMethods`). The call goes through the daemon JSON-RPC bridge
(`jsonrpc.NewBridge`) in the same process, without HTTP. The bridge runs one
handler for each CPU at a time; other calls wait for a slot.

`bench query` is an open-loop load generator with a constant arrival rate. It
starts each request at its due time. It does not wait for a response before
it starts the next request. At most 512 requests run at a time. If 512
requests run when the generator reaches an iteration, it drops the iteration.

## Formulas

A **scenario** is one query type at one target rate. A run does each type in
`--types` order, and each rate in `--target-rps` order. `r` is the rate, `D`
is `--duration`, `W` is `--warmup` and `t0` is the scenario start.
Iterations `0` to `W − 1` are warmup; `latency.csv` does not include them.

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

- A late start does not move the due times that follow it.
- `reach(i)` is the time the generator wakes for iteration `i`.
- `latency` is the main result. It contains the bridge request parse, the wait
  for a handler slot, the handler and the response write. The request encode
  is outside the timer. The bench does not decode a successful response.
- `handler_latency` contains the handler only, with its read view.
- `latency`, `handler_latency` and `latency_from_due` include succeeded
  requests only. `start_delay` includes all measured iterations, dropped
  iterations too.
- `last_response` is the last measured response, failed requests included.
- `cpu(t)` is the user plus system CPU time of the process. `end` is when the
  last request of the scenario returns.
- `pq` is the nearest-rank percentile of the `n` sorted samples `s[0] … s[n − 1]`.
  The log shows a warning for a `latency` row with fewer than 100 samples.

## Output

A failed measured request fails the run after its scenario is in the report.
A failed warmup request does not fail the run.

### `latency.csv`

One row for each scenario, metric and outcome that has samples.

| Column | Value |
|---|---|
| `query_type` | `ledgers`, `txpage`, `txhash` or `events` |
| `target_rps` | `r` |
| `metric` | `latency`, `handler_latency`, `latency_from_due` or `start_delay` |
| `outcome` | `all`; for `txhash` also `found` and `not_found` |
| `count` | `n` |
| `items` | Sum of the items in the responses; `0` for `start_delay` |
| `total_ns` | Sum of the samples |
| `p50_ns`, `p90_ns`, `p99_ns`, `max_ns` | Percentiles |

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
| `page_cache_evict_ns` | Time of the eviction before the scenario; empty if no file was evicted |

### `run.json`

All subcommands write it at the start with `status` `running`, and again at
the end.

| Key | Value |
|---|---|
| `schemaVersion` | `2` |
| `command` | Command path |
| `flags` | Each flag value, defaults included |
| `binary` | `version`, `commitHash`, `buildTimestamp`, `branch` |
| `hostname`, `gomaxprocs`, `numCpu` | Host and CPUs |
| `startedAt`, `finishedAt` | UTC, RFC 3339 |
| `peakRssBytes` | `VmHWM`; absent without `/proc` |
| `settings` | Values the run sets; see below |
| `setupNs` | Setup times in ns; `storeOpen` is the dataset open of `bench query` |
| `status` | `running`, `ok` or `failed` |
| `error` | Error of a failed run |

`bench query` settings:

| Key | Value |
|---|---|
| `tier` | `cold` or `hot` |
| `format` | `--format` |
| `warmup` | Warmup iterations after the tier default |
| `handlerConcurrency` | Bridge handler slots (number of CPUs) |
| `cacheScenario` | `warm-run` (warmup > 0), `cold-start` (eviction, no warmup) or `existing-cache` |
| `pageCacheEviction` | `off`, `requested` or `unsupported-on-this-platform` |
| `txhashPoolHashes`, `txhashPoolLedgers` | Hashes in the txhash pool, and the ledgers they came from |
| `eventsPool` | `derived` or `unfiltered` |
| `fixedReadRange` | Types whose requests read the same ledgers each time |

## Query types

| Type | Request | `items` |
|---|---|---|
| `ledgers` | `getLedgers`, `--ledgers-limit` ledgers from a random start | Ledgers |
| `txpage` | `getTransactions`, `--txpage-limit` transactions from a random start | Transactions |
| `txhash` | `getTransaction` for a hash from the txhash pool, or for a random hash | 1 if found, else 0 |
| `events` | `queryEvents`, `--events-limit` events from a random start to the end of the range, with a filter set from the events pool | Events |

- A `ledgers` request fails if it returns fewer ledgers than the limit (cut at
  the end of the range). A `txhash` request fails if the found status is not
  the expected one.
- `txpage` and `events` scan at most 10,000 ledgers for each request. An empty
  `events` page is not an error; examine `items`.
- `fixedReadRange` lists a type when the served range is too small for random
  starts. Its percentiles then measure a cached read.

**Txhash pool.** Before the first `txhash` scenario, the run takes one hash
from each of a set of random ledgers, up to `--txhash-pool-size`. It checks
one hash against `--network-passphrase` and `getTransaction`. A wrong
passphrase fails the build. `--not-found-fraction` of the lookups use 32
random bytes; these have the outcome `not_found`.

**Events pool.** Before the first `events` scenario, the run reads up to
20,000 stored events of the range. The pool holds the unfiltered read, one
filter for each of the two contracts with the most events, and one filter for
the most frequent contract and first topic. `eventsPool` is `unfiltered` when
the scan found no filter term.

The pool builds are not timed.

## Cache controls

- `--warmup` defaults to 0 on a cold dataset and to 20 on a hot dataset.
- `--evict-page-cache` (default true) applies to a cold dataset on Linux only.
  Before each scenario, it requests `POSIX_FADV_DONTNEED` for the frozen files
  of the served chunks and for the tx-hash index files. The kernel can keep
  some pages.
- Without eviction, a scenario starts with the pages that the dataset open, the
  pool builds and the earlier scenarios read.
