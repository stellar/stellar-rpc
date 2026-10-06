# Storage Benchmarks

This package has one benchmark command, `bench`, with two subcommands:

- `bench ingest` writes a storage dataset and measures ingestion.
- `bench query` reads that dataset and measures read latency.

`bench query` has one subcommand for each storage tier:

- `bench query cold` reads frozen artifacts.
- `bench query hot` reads a hot chunk database.

The tier names do not describe the state of the OS page cache.

This document defines the terms and the formulas of `bench query`. It also
defines `run.json`, which both subcommands write.

## 1. Method

### 1.1 Open-loop load generator

`bench query` is an **open-loop load generator**. An open-loop generator
starts each request at a time that a schedule sets. It does not wait for a
response before it starts the next request.

A **closed-loop** generator starts a request only after the previous response
arrives. When the store is slow, a closed-loop generator sends fewer
requests. The results then do not show the delay. This error is **coordinated
omission**.

Reference: B. Schroeder, A. Wierman, M. Harchol-Balter, "Open Versus Closed:
A Cautionary Tale", NSDI 2006.

### 1.2 Constant arrival rate

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

### 1.3 Bounded concurrency and dropped iterations

At most 512 requests (`maxConcurrent`) run at the same time. The generator
does this check when it reaches an iteration, not at the due time of the
iteration. If 512 requests run when the generator reaches an iteration, the
generator does not start it. The generator **drops** the iteration and counts
it. The general name for this method is load shedding. It prevents an
unbounded queue and an unbounded number of goroutines in the client.

## 2. Terms

| Term | Definition |
|---|---|
| scenario | One query type at one target rate for one duration. A run does one or more scenarios, one after the other. The run does the scenarios of each type in `--types` order. For each type, it does one scenario for each rate, in `--target-rps` order. |
| iteration | One unit of work in a scenario, with one due time. The generator starts one request for it, or drops it. |
| due time | The time at which an iteration must start. |
| reach | The generator reaches an iteration when its wait for the due time of that iteration ends. If you do not cancel the run, the generator reaches all iterations. |
| warmup iteration | An iteration before the measured iterations. The generator starts it at the same rate, but does not record its timings. |
| measured iteration | An iteration after the warmup iterations. The report includes it. |
| planned | The number of measured iterations that the generator reached. |
| started | The number of measured iterations whose request the generator started. |
| dropped | The number of measured iterations that the generator did not start, because 512 requests ran when the generator reached them. |
| succeeded | The number of started requests that returned no error. |
| failed | The number of started requests that returned an error. |
| response | The end of a started request. A failed request also has a response. |
| schedule | The due times of the measured iterations. In the formulas, `schedule` is the length of the schedule: `planned × interval`. |
| start delay | The time from the due time of an iteration to the time that the generator reaches it. |
| latency | The time from the start of the request body to its end. **This is the main result.** |
| latency from due | The time from the due time of an iteration to its response. |
| elapsed | The larger of two values: the schedule length, and the time from the first measured due time to the last measured response. |
| overrun | The time from the end of the schedule to the last measured response. It is 0 if the last response came before the end of the schedule. |

## 3. Schedule

Symbols:

- `r`: the target rate, in requests per second (`--target-rps`).
- `D`: the duration (`--duration`).
- `W`: the number of warmup iterations (`--warmup`).
- `t0`: the clock time when the scenario starts.

Formulas:

```
interval = round(1 s / r)                       (nanoseconds)
planned  = round(r × D)                         (warmup not included)
due(i)   = t0 + i × interval                    for i = 0 … W + planned − 1
```

- Iterations `0` to `W − 1` are warmup iterations.
- The measured iterations start at `i = W`.
- Each due time is absolute. A late start does not move the due times that
  follow it.

Limits:

- `r` must be a positive finite number.
- `--target-rps` accepts rates of 100,000 or less. The generator itself
  accepts rates of 1,000,000,000 or less, so that `interval` is 1 ns or more.
- `interval` must fit in a Go `time.Duration` (about 292 years).
- `planned` must be 1 or more.
- `W + planned` must be 100,000,000 (`maxIterations`) or less. The limit
  applies to the rounded value of `planned`.
- `(W + planned) × interval` must fit in a Go `time.Duration`.
- `--duration` must be more than 0.
- `--warmup` must be 0 or more.

`bench query` checks all flags before it changes `--out` or opens the
dataset. It checks these limits for each `--target-rps` rate with `--duration`
and `--warmup`. An error names the flags.

If you cancel a run (SIGINT or SIGTERM), the generator stops its wait at once
and starts no more iterations. It then waits until the started requests
return. Each started request receives the cancel signal through its context.
Thus, a request can stop early. A request counts as failed only when it
returns an error. `planned` is then the number of measured iterations that
the generator reached.

## 4. Request timings

For each measured iteration `i`:

```
start_delay(i)      = max(reach(i) − due(i), 0)
latency(i)          = body_end(i) − body_start(i)
latency_from_due(i) = response(i) − due(i)
```

- `reach(i)` is the clock time when the generator reaches iteration `i`.
- `body_start(i)` and `body_end(i)` are the clock times when the request body
  starts and ends. The request body also gets the read view.
- `response(i)` is the clock time when the request goroutine reads the clock,
  after the request body returns.
- `latency_from_due(i)` is almost equal to `start_delay(i)`, plus the time
  that the new goroutine waits for a CPU, plus `latency(i)`.

Which timings each distribution contains:

| Distribution | Contains |
|---|---|
| `latency` | Succeeded requests only. |
| `latency_from_due` | Succeeded requests only. |
| `start_delay` | All measured iterations, dropped iterations included. |

**Read `latency` as the result.** It is the time that one request takes, from
the start of the request body to its end. `latency_from_due` adds client-side
delay. Read it together with
`start_delay`. Failed requests and dropped iterations are not in `latency`.
Always read `latency` together with the counts in `scenarios.csv`.

## 5. Counts, windows and rates

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
  request counts. If no measured request has a response, `elapsed` is equal
  to `schedule`.
- `achieved_rps` is the rate at which requests started.
- `completion_rps` is the rate of successful responses over the full elapsed
  time.
- `cpu(t)` is the user plus system CPU time of the whole process at time `t`.
  `end` is the time after the last request of the scenario returns. The
  process includes the store and the generator, so `process_cpu` includes
  both.

Signs of too much load on the store or the client:

- `dropped` is more than 0.
- For one query type, `overrun` increases as the target rate increases.
- The `start_delay` percentiles increase.

## 6. Percentiles

For one distribution, sort the `n` samples in ascending order:
`s[0] ≤ s[1] ≤ … ≤ s[n − 1]`.

```
pq    = s[ceil(q × n) − 1]                      for q = 0.50, 0.90, 0.99
max   = s[n − 1]
total = s[0] + s[1] + … + s[n − 1]
count = n
```

- This is the nearest-rank method. For example, when `n` is 100, `p50` is
  `s[49]`.
- Zero durations stay in the distribution.
- When `n` is less than 100, `p99` is equal to `max`.
- The log shows a warning for each `latency` row that has fewer than 100
  samples.

## 7. Output files

`--out` must not exist, or it must be an empty directory. Otherwise, the
command stops before it changes `--out`. This prevents a mix of results from
two runs, and it keeps the `run.json` of a run that stopped (see section 7.3).
The command also checks the flags and the dataset directories (see section 8)
before it changes `--out`. If a check fails, the command creates nothing in
`--out`. When the checks pass, `bench query` writes `run.json` to `--out`. It
writes `latency.csv` and `scenarios.csv` when at least one scenario has a
result. It writes `bench.txt` only when the run ends with `status` set to `ok`
(see section 7.4).

If a measured request fails, the run fails after the command adds its scenario
to the report. A failed warmup request does not fail the run. `warmup_failed`
counts it. If the run fails or you cancel it, the command writes the CSVs of
the scenarios that have a result, and the log marks the report as PARTIAL. It
does not write `bench.txt`. A scenario that you cancel before its first
measured iteration has no rows.

When the run succeeds, the log shows one summary line for each scenario. If no
measured request succeeded, the line shows `latency=none`.

### 7.1 `latency.csv`

One row for each distribution of each scenario. A distribution with no
samples has no row.

| Column | Definition |
|---|---|
| `query_type` | The query type: `ledgers`, `txpage`, `txhash` or `events`. |
| `target_rps` | The target rate `r` of the scenario. |
| `metric` | `latency`, `latency_from_due` or `start_delay`. See section 4. |
| `outcome` | `all`, or for `txhash` also `found` and `not_found`. `start_delay` is always `all`. |
| `count` | `n`, the number of samples. |
| `items` | The sum of the items that the responses carried. `0` for `start_delay`. |
| `total_ns` | The sum of the samples, in nanoseconds. |
| `p50_ns`, `p90_ns`, `p99_ns` | Percentiles, in nanoseconds. See section 6. |
| `max_ns` | The largest sample, in nanoseconds. |

### 7.2 `scenarios.csv`

One row for each scenario, in run order.

| Column | Definition |
|---|---|
| `query_type` | The query type. |
| `target_rps` | The target rate `r`. |
| `planned`, `started`, `dropped`, `succeeded`, `failed` | The counts of the measured iterations. See section 5. |
| `warmup_planned` | The number of warmup iterations that the generator reached. |
| `warmup_dropped`, `warmup_failed` | The dropped and failed warmup iterations. |
| `achieved_rps` | `started / schedule`. |
| `completion_rps` | `succeeded / elapsed`. |
| `schedule_ns`, `elapsed_ns`, `overrun_ns` | The time windows, in nanoseconds. |
| `process_cpu_ns` | The process CPU time of the measured iterations, in nanoseconds. See section 5. |
| `page_cache_evict_ns` | The time of the eviction-request pass before the scenario, in nanoseconds. Empty when the pass advised no file: eviction is off, the platform is not Linux, or the pass found no file. See section 10. |

### 7.3 `run.json`

The command writes `run.json` before the run starts, with `status` set to
`running`. It writes the file again when the run ends, with `status` set to
`ok` or `failed`. Each write replaces the file in one step. If `status` is
`running` after the process stops, the process stopped before its last write.
If you cancel the run, `status` is `failed`, and `error` contains
`context canceled`.

| Key | Definition |
|---|---|
| `schemaVersion` | The version of this format. The current version is 2. |
| `command` | The command path, for example `stellar-rpc-v2 bench query cold`. |
| `flags` | The value of each flag, default values included. |
| `binary` | The build of the binary: `version`, `commitHash`, `buildTimestamp` and `branch`. |
| `hostname` | The host name. |
| `gomaxprocs` | The value of `GOMAXPROCS` for the process. |
| `numCpu` | The number of logical CPUs that the process can use. |
| `startedAt`, `finishedAt` | UTC start and end times (RFC 3339). `finishedAt` is absent while the run continues. |
| `peakRssBytes` | The peak resident set size of the process (`VmHWM`). Absent while `status` is `running`, and on systems without `/proc`. |
| `settings` | Values that the run sets and that the flags do not show. Absent while `status` is `running`, and when empty. |
| `setupNs` | One-time setup durations, in nanoseconds, for example the store open. Absent while `status` is `running`, and when empty. |
| `status` | `running`, `ok` or `failed`. |
| `error` | The error message of a failed run. Absent when the run succeeded. |

`bench query` writes these keys in `settings` and `setupNs`:

| Key | Definition |
|---|---|
| `settings.cacheScenario` | `cold-start`, `warm-run` or `existing-cache`. See section 10. |
| `settings.pageCacheEviction` | `off`, `requested` or `unsupported-on-this-platform`. See section 10. |
| `settings.txhashPoolHashes` | The number of hashes in the txhash pool. See section 9.1. Absent when `--types` does not include `txhash`, or when the pool build does not complete. |
| `settings.txhashPoolLedgers` | The number of ledgers that supplied a hash to the pool. Absent when `--types` does not include `txhash`, or when the pool build does not complete. |
| `settings.eventsPool` | `derived` or `unfiltered`. See section 9.2. Absent when `--types` does not include `events`, or when the pool build does not complete. |
| `settings.fixedReadRange` | The types whose span covers all ledgers of the dataset, for example `ledgers,txpage`. Absent when no span covers them. |
| `setupNs.storeOpen` | The time to open the dataset, before the first scenario. |

### 7.4 `bench.txt`

`bench.txt` contains the scenarios in the Go benchmark format, for
`benchstat`. The first two lines are `goos` and `goarch`. Then there is one
line for each scenario that has at least one planned iteration, in run order:

```
BenchmarkQuery/tier=cold/type=ledgers/rps=10  600  812345 ns/op  790123 p50-ns  2345678 p99-ns  2400000 p99-from-due-ns  20 items/op  0 dropped
```

The fields are tab-separated. The name has the parts `tier=<cold|hot>`,
`type=<query_type>` and `rps=<target_rps>`. The tier is the subcommand. For
`txhash`, one more line for each lookup outcome follows the scenario line. Its
name ends with `/outcome=found` or `/outcome=not_found`. These lines do not
have `dropped`.

A failed run has no `bench.txt`. A measured request that fails also fails the
run, so `bench.txt` has no failed count.

| Field | Definition |
|---|---|
| Iterations (second field) | `succeeded`, or `count` of the outcome. When no request succeeded, `planned`. |
| `ns/op` | The mean `latency`: `total_ns / count`, in nanoseconds. |
| `p50-ns`, `p99-ns` | The `latency` percentiles, in nanoseconds. See section 6. |
| `p99-from-due-ns` | The p99 of `latency_from_due`, in nanoseconds. |
| `items/op` | `items / count` of the `latency` row. |
| `dropped` | The dropped measured iterations. See section 1.3. |

When no request succeeded, the scenario line has only `dropped`.

To compare two builds:

1. Run each build at least 6 times. Use a different `--out` for each run.
   With fewer runs, `benchstat` does not show a 95% confidence interval.
   Both builds must use the same subcommand, dataset and flags, cache controls
   included. The name contains the tier. It does not contain the dataset or
   the other flags, so `benchstat` cannot see a difference in them.
2. Put the `bench.txt` files of each build into one file, for example
   `cat old-*/bench.txt > old.txt` and `cat new-*/bench.txt > new.txt`.
3. Run `benchstat old.txt new.txt`.
4. Read the `dropped` table first. If `dropped` of a scenario changed, its
   latency tables compare different loads. Only the requests that started
   have a latency. Thus, do not read a lower latency of that scenario as an
   improvement.
5. Then read the latency tables.

`status` set to `ok` means that the run completed. It does not mean that the
run held the target rate. Dropped iterations do not fail a run.

`benchstat` shows the `ns` units in seconds, for example `p99-sec`. To
compare the rates of one build, run `benchstat -col /rps new.txt`.

`benchstat` tests each metric of each scenario separately. It does not correct
for multiple comparisons. Thus, when there are many scenarios, expect some
differences with p < 0.05 by chance.

## 8. Datasets

The command checks these conditions before it changes `--out`:

- `--cold-dir` or `--hot-dir` does not exist or is not a directory.
- For `hot`, the chunk has no database directory under `--hot-dir`.

If one of these conditions occurs, the command creates nothing in `--out` or
in the dataset tree.

The run opens the dataset before the first scenario. The open fails in these
conditions:

- For `cold`, a chunk of the range has no ledger pack.
- For `cold`, `--types` includes `txhash`, and no tx-hash window index covers
  the range (see section 8.3).
- For `hot`, the chunk has no committed ledger, or its last committed ledger
  is below the first ledger of the chunk.
- A chunk has no servable ledger store.
- `--types` includes `events`, and a chunk has no servable events store.

### 8.1 Scratch catalog

Each `bench query` run creates a scratch catalog in a `bench-query-catalog-*`
directory under the dataset root (`--cold-dir` or `--hot-dir`). Use
`--catalog-dir` to put the catalog in a different directory. A read-only
`--cold-dir` needs this flag. `bench query hot` writes to `--hot-dir` (see
section 8.2).

The run removes the directory when it ends, also when it fails or you cancel
it. If the process stops without a cancel, for example with SIGKILL or a
crash, the directory stays. Remove it before you measure again.

### 8.2 Hot tier database

`bench query hot` opens the chunk database read-write, as the daemon opens a
resumed chunk. `--hot-dir` must be writable. The open replays and flushes the
write-ahead log, and it can start compaction. Thus, after a run, the state on
the disk is different from the state that ingest left. A second run on the same
`--hot-dir` measures the state that the first run left. The open takes an
exclusive lock, so the run cannot use a directory that a daemon uses.

### 8.3 Cold tx-hash index

`bench query cold` uses the tx-hash window index on disk that covers all
chunks of the range. If no index covers the range, or the range spans more
than one window index, the run fails when `--types` includes `txhash`. For
other types, the log shows a warning and the run continues.

## 9. Query types

All requests use the storage read paths through `query.ReadView`. A `txhash`
request also goes through `adapters.TransactionReader` (see the table). The
measurement does not include RPC handlers, response serialization or network
work.

The requests and the pools use only the ledger range of the dataset. For
`cold`, this range is all ledgers of the chunk range. For `hot`, it is the
committed ledgers of the chunk. `--sample-ledgers` limits the `hot` range to
that number of ledgers from the start of the chunk.

| Type | Request | `items` |
|---|---|---|
| `ledgers` | One read of `--ledgers-span` ledgers, from a random start ledger. This is the read of `getLedgers`. | Ledgers. |
| `txpage` | A read of `--txpage-span` ledgers that makes the transaction views, envelopes included, up to `--txpage-limit` transactions. This is the read of `getTransactions`. It does not make or serialize the full RPC response. | Transactions. |
| `txhash` | One `adapters.TransactionReader.GetTransaction` call. The reader looks up the hash in the tx-hash indexes, checks the match against its ledger and parses the transaction view. `latency` includes all of this work. The hash is a hash from the txhash pool or a not-found hash (see section 9.1). This is the read of `getTransaction`. | 1 if found, 0 if not found. |
| `events` | One page of `--events-limit` events or fewer, with a filter set from the events pool. The read range starts at a random ledger and ends at the last ledger of the dataset. This is the read of `getEvents`. | Events. |

`--ledgers-span` and `--txpage-span` must be 1 to 10,000. A `txpage` request
stops at the ledger that fills the page.

A read of one ledger (span 1) goes through `ReadView.WithLedger`, the point
read of the daemon. A read of more ledgers goes through
`ReadView.ScanLedgers`. Thus, span 1 measures a different read path from the
wider spans. A `ledgers` request that reads fewer ledgers than its range
fails.

A `--ledgers-span` or `--txpage-span` can be equal to or more than the number
of ledgers in the dataset. Then each request of that type reads the same
ledgers. The log then shows a warning, and `settings.fixedReadRange` lists
those types.

`--seed` sets the random draws of each request: the start ledger, the pool
hash, the not-found hash and the filter set. Each iteration gets its draws
from the seed, the query type, the target rate and the iteration number. Thus,
with the same seed, dataset and flags, iteration `i` of a scenario makes the
same request in each run.

An `events` request scans one page window of 10,000 ledgers or fewer. If the
filter matches no event in that window, the request returns an empty page. The
request succeeds with zero items. Before you read the `events` percentiles as
filtered-read latency, examine `items` in `latency.csv`.

### 9.1 Txhash pool

The txhash pool holds the dataset hashes that `txhash` looks up. The run
builds it before the first `txhash` scenario.

- `--txhash-pool-size` sets the maximum number of hashes. The default is 512.
  The value must be 1 to 1,000,000.
- The pool build reads random ledgers, and takes one random hash from each
  ledger that has transactions. It reads each ledger one time at most. Each
  chunk supplies its share of the pool. Each chunk has a budget of 16 random
  draws for each hash that the pool still needs, and 512 draws or more. A
  draw can pick a ledger that the build already read, or a ledger with no
  transactions.
- `--seed` sets the draws, so the same seed and dataset give the same pool.
- The pool can be smaller than the maximum. The log then shows a warning. A
  pool with no hashes is an error.
- The pool build looks up one pool hash through the tx-hash index. The pool
  build fails if the lookup does not find the hash. It also fails if the hash
  does not pair with an envelope under `--network-passphrase`.
- If you cancel the run, the pool build stops.

`settings.txhashPoolHashes` gives the pool size, and
`settings.txhashPoolLedgers` gives the number of ledgers that supplied a hash.

`--not-found-fraction` sets the fraction of `txhash` lookups that ask for a
hash that is not in the dataset. The default is 0.12. The value must be 0 to 1.
Such a hash is 32 random bytes. A not-found lookup probes every hot index and
then every cold window index. It reads a ledger only if a cold fingerprint
gives a false match. `latency.csv` shows these lookups with the outcome
`not_found`.

If the lookup does not find a pool hash, or finds a not-found hash, the
request fails.

### 9.2 Events pool

The events pool holds the filter sets that `events` uses. To make it, the run
scans up to 20,000 stored events in the ledger range of the dataset. The pool
holds the unfiltered read. It also holds one filter for each of the two
contracts with the most events. It also holds one filter for the most frequent
pair of contract and first topic. If the scan finds fewer contracts or no pair,
the pool holds fewer filter sets. Each `events` request uses one filter set
from the pool. The run picks the set at random, and each set has the same
probability. `settings.eventsPool` is `derived` when the scan found a filter
term, and `unfiltered` when the pool holds only the unfiltered read.

## 10. Cache controls

Both tiers accept `--warmup`. The default is 0 for `cold` and 20 for `hot`.

`bench query cold` also accepts `--evict-page-cache`. The default is true. On
Linux, it requests OS page-cache eviction of the dataset files before each
scenario. `bench query hot` does not request eviction.

The eviction request is best effort. The request uses `POSIX_FADV_DONTNEED`.
The kernel keeps dirty pages, pages under writeback and pages that a process
maps, also when the request succeeds. Thus, the run does not verify that the
cache is cold.

The run builds the pool of a type before the first scenario of that type. The
pool build reads the dataset, and these reads fill the caches. Each scenario
then requests the eviction and after it does the warmup. Thus, on Linux, the
request applies to the dataset pages that the pool build read.
Without eviction (`hot`, `--evict-page-cache=false`, or a platform other than
Linux), the caches keep their data between scenarios. Thus, each scenario
starts with the data that the dataset open, the pool builds and the earlier
scenarios put in the caches.

`settings.cacheScenario` records the requested controls:

- `cold-start`: the run requests eviction on Linux and does no warmup. This
  value records the request, not a verified cold cache.
- `warm-run`: the warmup is more than 0, with or without eviction.
- `existing-cache`: there is no warmup, and the run does not request
  eviction. This includes a run with `--evict-page-cache=true` on a platform
  other than Linux.

`settings.pageCacheEviction` is `off`, `requested` (Linux) or
`unsupported-on-this-platform`.

## 11. Flags

Flags of both subcommands:

| Flag | Default | Definition |
|---|---|---|
| `--types` | `ledgers,txpage,txhash,events` | The query types, separated by commas. Each type can occur only one time. See section 9. |
| `--target-rps` | `10` | The target rates, separated by commas. Each rate can occur only one time. See section 3. |
| `--duration` | `60s` | The measured duration of each scenario. |
| `--warmup` | `0` (`cold`), `20` (`hot`) | The number of warmup iterations of each scenario. See section 10. |
| `--ledgers-span` | `10` | The ledgers that one `ledgers` request reads. 1 to 10,000. |
| `--txpage-span` | `5` | The ledgers that one `txpage` request reads. 1 to 10,000. |
| `--txpage-limit` | `200` | The maximum number of transactions in one `txpage` request. 1 or more. |
| `--events-limit` | `10` | The maximum number of events in one `events` page. 1 or more. |
| `--txhash-pool-size` | `512` | The maximum number of hashes in the txhash pool. 1 to 1,000,000. |
| `--not-found-fraction` | `0.12` | The fraction of `txhash` lookups for a hash that is not in the dataset. 0 to 1. |
| `--network-passphrase` | The public network passphrase | The passphrase of the network of the dataset. `txpage` and `txhash` use it. It must not be empty. Only the txhash pool build checks the passphrase (see section 9.1). The run builds this pool before the first `txhash` scenario. A wrong passphrase also fails each `txpage` request that reads a ledger with transactions. |
| `--seed` | `1` | The seed of the random draws. See section 9. |
| `--out` | `bench-out` | The directory of the output files. See section 7. |
| `--cpuprofile` | Empty | Write a Go CPU profile to this path. |
| `--memprofile` | Empty | Write a Go allocation profile to this path. |

Flags of `bench query cold`:

| Flag | Default | Definition |
|---|---|---|
| `--cold-dir` | Required | The root of the frozen artifacts. |
| `--start-chunk` | Required | The first chunk of the range. |
| `--num-chunks` | `1` | The number of chunks in the range. 1 or more. The last chunk of the range must be less than the last valid chunk ID. |
| `--catalog-dir` | `--cold-dir` | The directory of the scratch catalog. See section 8.1. |
| `--evict-page-cache` | `true` | Request best-effort OS page-cache eviction of the dataset files before each scenario (Linux only). See section 10. |

Flags of `bench query hot`:

| Flag | Default | Definition |
|---|---|---|
| `--hot-dir` | Required | The root of the hot chunk databases. See section 8.2. |
| `--chunk` | Required | The chunk to read. It must not be more than the last valid chunk ID. |
| `--catalog-dir` | `--hot-dir` | The directory of the scratch catalog. See section 8.1. |
| `--sample-ledgers` | `0` | The number of ledgers from the start of the chunk that the requests and the pools use. 0 means all committed ledgers. See section 9. |

## 12. Known limits

### 12.1 Timer precision

The generator waits for each due time on a Go timer. A Go timer can end its
wait late. The delay increases when the host is busy.

- This delay adds to `start_delay` and `latency_from_due`.
- This delay does not change `latency`.

### 12.2 Bursts at high rates

When the interval is shorter than the timer delay, the generator starts all
overdue iterations together after a late wait. The average rate stays correct,
but the requests arrive in small bursts. For example, at 10,000 rps the
interval is 0.1 ms. A timer delay of 1 ms then gives a burst of about 10
requests.

### 12.3 Shared process

The generator and the store run in the same process. They use the same CPUs
and the same Go scheduler. `process_cpu_ns` shows the CPU time that the run
used. `gomaxprocs` and `numCpu` show the CPUs that the run could use.
