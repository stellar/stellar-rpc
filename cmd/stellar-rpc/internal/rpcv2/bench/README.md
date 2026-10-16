# Storage Benchmarks

This package has one benchmark command, `bench`, with two subcommands:

- `bench ingest cold` and `bench ingest hot` write a dataset and measure
  ingestion.
- `bench query` reads a dataset and measures read latency.

A dataset is a directory with the same layout as the data directory of the
daemon. A **cold** dataset holds frozen artifacts. A **hot** dataset holds hot
chunk databases. The tier names do not describe the state of the OS page cache.

This document defines the datasets, the terms and the formulas of
`bench query`. It also defines `run.json`, which all subcommands write.

## 1. Datasets

### 1.1 Layout

The dataset root is `--cold-out-dir` for `bench ingest cold` and `--hot-dir`
for `bench ingest hot`. `bench query` reads the same root through
`--data-dir`. The root holds:

- `catalog/rocksdb`: the catalog of the dataset. The catalog records each
  chunk, its state and the earliest ledger of the dataset, as the catalog of
  the daemon does.
- `ledgers`, `events` and `txhash`: the frozen artifacts of a cold dataset.
- `hot`: one database for each hot chunk.

### 1.2 Ingest

An ingest run keeps the catalog in the dataset. The first successful run
records the first ledger of its range as the earliest ledger of the dataset.

A later run into the same root adds chunks to the dataset. The run must start
at the chunk after the last committed ledger of the dataset. The run stops
with an error, before it writes a chunk, when:

- The dataset holds ledgers of the range (overlap).
- The range starts after the next chunk (gap). If the dataset ends inside a
  chunk, no chunk can follow it.
- The range starts below the earliest ledger.
- The dataset holds chunks of the other tier.
- The catalog holds chunks from a run that did not finish. Delete the dataset
  directory and ingest again.

A cold run backfills from the first chunk of the dataset, as the daemon
backfills from its retention floor. The run skips the frozen chunks and builds
the tx-hash index again over all chunks. Then it deletes the index files that
the new index replaces. Thus, the timings of a run that adds chunks are
different from the timings of a run into an empty root. `settings.append` and
`settings.datasetFirstChunk` in `run.json` record this.

At the end of a successful run, the ingest opens the **live chunk** as daemon
startup does: the chunk that holds the ledger after the last committed ledger.
If the run ends at a chunk boundary, the live chunk is new and empty. If the
run ends in a chunk, the live chunk is that chunk, and no run can add chunks
to the dataset. A run that adds chunks removes the empty live chunk first.

### 1.3 Query

`bench query` opens the dataset with the startup steps of the daemon: it
finds the last committed ledger, opens the live chunk and opens the read
registry. It does not write chunks into the dataset. The run stops with an
error when the dataset has no catalog, no earliest ledger or no live chunk.
Ingest such a dataset again with this version.

The **served range** starts at the first ledger of `--start-chunk`, or of the
dataset when you do not set the flag. It ends at the last committed ledger.
`--sample-ledgers` limits the range to that number of ledgers from its start.
The read handlers serve no ledger outside the served range.

The catalog gives the tier of each chunk in the served range. All chunks must
have the same tier, cold or hot. `settings.tier` records it.

The run opens the catalog and the hot databases read-write, as the daemon
does. Both take an exclusive lock, so one dataset serves one run at a time.

## 2. Method

### 2.1 Request path

Each request is one JSON-RPC call to the method handler of the daemon. The
handlers are the handlers that the daemon serves (`rpcv2.ReadMethods`). The
call goes through the JSON-RPC bridge of the daemon (`jsonrpc.NewBridge`) in
the same process. The call does not use HTTP or a network. The bridge runs
one handler for each CPU at a time. Other calls wait for a free slot, as on
the daemon. `settings.handlerConcurrency` records the number of slots.

The handlers apply the page caps of the default daemon configuration. A flag
value above a cap is an error.

### 2.2 Open-loop load generator

`bench query` is an **open-loop load generator**. An open-loop generator
starts each request at a time that a schedule sets. It does not wait for a
response before it starts the next request.

A **closed-loop** generator starts a request only after the previous response
arrives. When the store is slow, a closed-loop generator sends fewer
requests. The results then do not show the delay. This error is **coordinated
omission**.

Reference: B. Schroeder, A. Wierman, M. Harchol-Balter, "Open Versus Closed:
A Cautionary Tale", NSDI 2006.

The schedule has a **constant arrival rate**: the time between two due times
is always the same. The k6 load tool uses the same model in its
[`constant-arrival-rate`](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/)
executor.

| k6 term | `bench query` term |
|---|---|
| scenario | scenario |
| iteration | iteration |
| `rate` / `timeUnit` | `--target-rps` |
| `duration` | `--duration` |
| `maxVUs` | `maxConcurrent` (512) |
| `dropped_iterations` | `dropped` |

### 2.3 Bounded concurrency and dropped iterations

At most 512 requests (`maxConcurrent`) run at the same time. The generator
does this check when it reaches an iteration, not at the due time of the
iteration. If 512 requests run when the generator reaches an iteration, the
generator does not start it. The generator **drops** the iteration and counts
it. This prevents an unbounded queue in the client. The bridge slots of
section 2.1 are a different limit: a request that waits for a slot is not
dropped.

## 3. Terms

| Term | Definition |
|---|---|
| scenario | One query type at one target rate for one duration. A run does the scenarios of each type in `--types` order. For each type, it does one scenario for each rate, in `--target-rps` order. |
| iteration | One unit of work in a scenario, with one due time. The generator starts one request for it, or drops it. |
| due time | The time at which an iteration must start. |
| reach | The generator reaches an iteration when its wait for the due time of that iteration ends. |
| warmup iteration | An iteration before the measured iterations. The generator starts or drops it as it does a measured iteration, but does not record its timings. |
| measured iteration | An iteration after the warmup iterations. The report includes it. |
| planned | The number of measured iterations that the generator reached. |
| started | The number of measured iterations whose request the generator started. |
| dropped | The number of measured iterations that the generator did not start, because 512 requests ran. |
| succeeded | The number of started requests that returned no error. |
| failed | The number of started requests that returned an error. |
| response | The end of a started request. A failed request also has a response. |
| schedule | The due times of the measured iterations. In the formulas, `schedule` is the length of the schedule: `planned × interval`. |
| start delay | The time from the due time of an iteration to the time that the generator reaches it. |
| latency | The time of one call through the bridge. **This is the main result.** See section 5. |
| handler latency | The time of the method handler inside that call. |
| latency from due | The time from the due time of an iteration to its response. |
| elapsed | The larger of two values: the schedule length, and the time from the first measured due time to the last measured response. |
| overrun | The time from the end of the schedule to the last measured response. It is 0 if the last response came before the end of the schedule. |

## 4. Schedule

Symbols: `r` is the target rate in requests per second (`--target-rps`), `D`
is the duration (`--duration`), `W` is the number of warmup iterations
(`--warmup`) and `t0` is the clock time when the scenario starts.

```
interval = round(1 s / r)                       (nanoseconds)
planned  = round(r × D)                         (warmup not included)
due(i)   = t0 + i × interval                    for i = 0 … W + planned − 1
```

Iterations `0` to `W − 1` are warmup iterations. Each due time is absolute. A
late start does not move the due times that follow it.

Limits:

- `r` must be a positive finite number, 100,000 or less.
- `planned` must be 1 or more.
- `W + planned` must be 100,000,000 (`maxIterations`) or less.
- `(W + planned) × interval` must fit in a Go `time.Duration`.
- `--duration` must be more than 0. `--warmup` must be 0 or more.

`bench query` checks the flags before it changes `--out` or opens the
dataset. It checks the hot `--warmup` default after it opens the dataset.

If you cancel a run (SIGINT or SIGTERM), the generator starts no more
iterations. It waits until the started requests return. Each started request
receives the cancel through its context.

## 5. Request timings

For each measured iteration `i`:

```
start_delay(i)      = max(reach(i) − due(i), 0)
latency(i)          = call_end(i) − call_start(i)
handler_latency(i)  = handler_end(i) − handler_start(i)
latency_from_due(i) = response(i) − due(i)
```

- `latency` contains the request parse of the bridge, the wait for a handler
  slot, the handler and the write of the response to memory. The run encodes
  the request before the timer starts, and decodes the response after the
  timer stops.
- `handler_latency` contains the handler only. The handler also gets the read
  view of the request.
- `latency − handler_latency` is the cost of the bridge: the request parse,
  the wait for a slot and the response encoding. A large difference shows a
  queue for the slots, or large responses.
- `--format` sets the response format: `base64` (XDR) or `json`. The JSON
  format costs more in the handler.

| Distribution | Contains |
|---|---|
| `latency`, `handler_latency`, `latency_from_due` | Succeeded requests only. |
| `start_delay` | All measured iterations, dropped iterations included. |

Failed requests and dropped iterations are not in `latency`. Always read
`latency` together with the counts in `scenarios.csv`.

## 6. Counts, windows and rates

```
planned        = started + dropped
started        = succeeded + failed
schedule       = planned × interval
elapsed        = max(schedule, last_response − due(W))
overrun        = elapsed − schedule
achieved_rps   = started / schedule
completion_rps = succeeded / elapsed
process_cpu    = cpu(end) − cpu(reach(W))
```

- `last_response` is the last response of a measured iteration. A failed
  request counts.
- `cpu(t)` is the user plus system CPU time of the whole process at time `t`.
  The process includes the store, the handlers and the generator.

Signs of too much load: `dropped` is more than 0, `overrun` increases with the
target rate, or `latency` increases faster than `handler_latency`.

## 7. Percentiles

For one distribution, sort the `n` samples: `s[0] ≤ s[1] ≤ … ≤ s[n − 1]`.

```
pq    = s[ceil(q × n) − 1]                      for q = 0.50, 0.90, 0.99
max   = s[n − 1]
total = s[0] + s[1] + … + s[n − 1]
```

This is the nearest-rank method. Zero durations stay in the distribution.
When `n` is less than 100, `p99` is equal to `max`, and the log shows a
warning for the `latency` row.

## 8. Output files

`--out` must not exist, or it must be an empty directory. The command checks
the flags and that `--data-dir` holds a catalog before it changes `--out`.
When the checks pass, `bench query` writes `run.json`. It writes
`latency.csv` and `scenarios.csv` when at least one scenario has a result.

If a measured request fails, the run fails after the command adds its scenario
to the report. A failed warmup request does not fail the run. If the run fails
or you cancel it, the command writes the scenarios that have a result, and
the log marks the report as PARTIAL.

### 8.1 `latency.csv`

One row for each distribution of each scenario. A distribution with no
samples has no row.

| Column | Definition |
|---|---|
| `query_type` | `ledgers`, `txpage`, `txhash` or `events`. |
| `target_rps` | The target rate `r`. |
| `metric` | `latency`, `handler_latency`, `latency_from_due` or `start_delay`. See section 5. |
| `outcome` | `all`, or for `txhash` also `found` and `not_found`. `start_delay` is always `all`. |
| `count` | `n`, the number of samples. |
| `items` | The sum of the items in the responses. `0` for `start_delay`. |
| `total_ns` | The sum of the samples, in nanoseconds. |
| `p50_ns`, `p90_ns`, `p99_ns`, `max_ns` | See section 7. |

### 8.2 `scenarios.csv`

One row for each scenario, in run order.

| Column | Definition |
|---|---|
| `query_type`, `target_rps` | As in `latency.csv`. |
| `planned`, `started`, `dropped`, `succeeded`, `failed` | The counts of the measured iterations. See section 6. |
| `warmup_planned`, `warmup_dropped`, `warmup_failed` | The counts of the warmup iterations. |
| `achieved_rps`, `completion_rps` | See section 6. |
| `schedule_ns`, `elapsed_ns`, `overrun_ns` | The time windows, in nanoseconds. |
| `process_cpu_ns` | The CPU time of the process during the scenario. See section 6. |
| `page_cache_evict_ns` | The time of the eviction requests before the scenario. Empty when no file was advised. See section 10. |

### 8.3 `run.json`

The command writes `run.json` before the run starts, with `status` set to
`running`. It writes the file again when the run ends, with `status` set to
`ok` or `failed`. If `status` is `running` after the process stops, the
process stopped before its last write.

| Key | Definition |
|---|---|
| `schemaVersion` | The version of this format. The current version is 2. |
| `command` | The command path, for example `stellar-rpc-v2 bench query`. |
| `flags` | The value of each flag, default values included. |
| `binary` | The build: `version`, `commitHash`, `buildTimestamp` and `branch`. |
| `hostname`, `gomaxprocs`, `numCpu` | The host and the CPUs that the process can use. |
| `startedAt`, `finishedAt` | UTC start and end times (RFC 3339). |
| `peakRssBytes` | The peak resident set size (`VmHWM`). Absent on systems without `/proc`. |
| `settings` | Values that the run sets and that the flags do not show. |
| `setupNs` | One-time setup durations, in nanoseconds. `setupNs.storeOpen` is the dataset open of `bench query`. |
| `status` | `running`, `ok` or `failed`. |
| `error` | The error message of a failed run. |

`bench query` writes these `settings`:

| Key | Definition |
|---|---|
| `tier` | `cold` or `hot`. See section 1.3. |
| `format` | The response format. See section 5. |
| `warmup` | The warmup iterations of each scenario, after the tier default. See section 10. |
| `handlerConcurrency` | The number of bridge slots. See section 2.1. |
| `cacheScenario`, `pageCacheEviction` | See section 10. |
| `txhashPoolHashes`, `txhashPoolLedgers` | The size of the txhash pool and the number of ledgers that supplied it. See section 9.1. |
| `eventsPool` | `derived` or `unfiltered`. See section 9.2. |
| `fixedReadRange` | The types whose requests read the same ledgers each time. See section 9. |

## 9. Query types

| Type | Request | `items` |
|---|---|---|
| `ledgers` | `getLedgers` with `--ledgers-limit` (1 to 20), from a random start ledger. The request fails if the response has fewer ledgers than the smaller of `--ledgers-limit` and the ledgers from the start to the end of the served range. | Ledgers. |
| `txpage` | `getTransactions` with `--txpage-limit` (1 to 200), from a random start ledger. The handler scans up to 10,000 ledgers to fill the page, and fails on a missing ledger. | Transactions. |
| `txhash` | `getTransaction` for a hash from the txhash pool, or for a hash that is not in the dataset. The request fails if the status is not the expected one. | 1 if found, 0 if not. |
| `events` | `queryEvents` with `--events-limit` (1 to 1,000), from a random start ledger to the end of the served range, with a filter set from the events pool. An empty page is not an error. | Events. |

A `ledgers` limit that is equal to or more than the served range, or a served
range of one ledger, makes each request read the same ledgers. The log then
shows a warning, and `settings.fixedReadRange` lists those types.

`--seed` sets the random draws of each request. With the same seed, dataset
and flags, iteration `i` of a scenario makes the same request in each run.

An `events` request scans 10,000 ledgers or fewer. If the filter matches no
event in that window, the page is empty. Before you read the `events`
percentiles as filtered-read latency, examine `items`.

### 9.1 Txhash pool

The run builds the txhash pool before the first `txhash` scenario. The build
reads the dataset directly, not through the handlers, and is not timed.

- `--txhash-pool-size` sets the maximum number of hashes (default 512, 1 to
  1,000,000).
- The build takes one random hash from each of a set of random ledgers. Each
  chunk supplies its share. A pool with fewer hashes than the maximum gives a
  warning. A pool with no hashes is an error.
- The build fails if one pool hash does not pair with an envelope under
  `--network-passphrase`, or if the tx-hash index does not find it.

`--not-found-fraction` (default 0.12, 0 to 1) sets the fraction of lookups
for 32 random bytes. `latency.csv` shows these lookups with the outcome
`not_found`.

### 9.2 Events pool

The run scans up to 20,000 stored events in the served range. The pool holds
the unfiltered read, a filter for each of the two contracts with the most
events, and a filter for the most frequent pair of contract and first topic.
Each `events` request uses one set from the pool, at random.
`settings.eventsPool` is `derived` when the scan found a filter term, and
`unfiltered` otherwise.

## 10. Cache controls

`--warmup` defaults to 0 on a cold dataset and to 20 on a hot dataset.
`flags.warmup` in `run.json` shows the flag value; `settings.warmup` shows the
value that the run used.

`--evict-page-cache` (default true) applies to a cold dataset only. On Linux,
it requests OS page-cache eviction (`POSIX_FADV_DONTNEED`) of the frozen
files that the catalog records, before each scenario. The kernel can keep
some pages, so the run does not verify that the cache is cold.

The pool builds read the dataset before the first scenario of their type.
Without eviction, each scenario starts with the data that the open, the pool
builds and the earlier scenarios put in the caches.

`settings.cacheScenario` is `cold-start` (eviction requested on Linux, no
warmup), `warm-run` (warmup more than 0) or `existing-cache` (no warmup, no
eviction). `settings.pageCacheEviction` is `off`, `requested` or
`unsupported-on-this-platform`.

## 11. `bench query` flags

| Flag | Default | Definition |
|---|---|---|
| `--data-dir` | Required | The dataset root. See section 1. |
| `--start-chunk` | The first chunk of the dataset | The first chunk of the served range. |
| `--sample-ledgers` | `0` | The number of ledgers in the served range. 0 means all committed ledgers. |
| `--types` | `ledgers,txpage,txhash,events` | The query types, separated by commas. |
| `--target-rps` | `10` | The target rates, separated by commas. |
| `--duration` | `60s` | The measured duration of each scenario. |
| `--warmup` | `0` (cold), `20` (hot) | The warmup iterations of each scenario. |
| `--ledgers-limit` | `10` | See section 9. |
| `--txpage-limit` | `200` | See section 9. |
| `--events-limit` | `10` | See section 9. |
| `--txhash-pool-size` | `512` | See section 9.1. |
| `--not-found-fraction` | `0.12` | See section 9.1. |
| `--format` | `base64` | `base64` or `json`. See section 5. |
| `--evict-page-cache` | `true` | See section 10. |
| `--network-passphrase` | The public network passphrase | The passphrase of the dataset. `txpage` and `txhash` use it. |
| `--seed` | `1` | The seed of the random draws. |
| `--out` | `bench-out` | The directory of the output files. |
| `--cpuprofile`, `--memprofile` | Empty | Write a Go CPU or allocation profile to this path. |

## 12. Known limits

- The generator waits for each due time on a Go timer, which can end late on
  a busy host. The delay adds to `start_delay` and `latency_from_due`, not to
  `latency`.
- When the interval is shorter than the timer delay, the generator starts the
  overdue iterations together. The average rate stays correct, but the
  requests arrive in small bursts.
- The generator, the bridge and the store run in the same process and use the
  same CPUs. `process_cpu_ns`, `gomaxprocs` and `numCpu` show the CPU use.
