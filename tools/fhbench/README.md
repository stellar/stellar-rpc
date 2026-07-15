# fhbench — full-history query benchmark harness

`fhbench` is a small, dependency-free load generator and latency reporter for the
full-history query POC's JSON-RPC read server. It is a **black-box** benchmark:
it speaks only the public HTTP JSON-RPC API (`getLedgers`, `getTransactions`,
`getTransaction`, `getEvents`) plus scrapes `/metrics`, and imports nothing from
the daemon (just `net/http` + `encoding/json`).

The point of the harness is to compare the **hot** serving path (recently
ingested ledgers still in RocksDB, plus the live chunk) against the **cold**
serving path (an old sealed chunk served from cold artifacts), so latency can be
attributed to each tier.

## 1. Run the daemon with a `[serve]` endpoint

The read server is off by default. Enable it by giving `[serve].endpoint` a
`host:port` in the full-history daemon's TOML config:

```toml
[service]
default_data_dir = "/var/lib/fullhistory"

[retention]
earliest_ledger = "genesis"   # or a chunk-aligned ledger; "now" for tip-only
# retention_chunks = 0        # 0 = full history

[ingestion]
captive_core_config = "/etc/stellar/captive-core.cfg"
history_archive_urls = ["https://history.stellar.org/prd/core-live/core_live_001"]

[serve]
endpoint = "127.0.0.1:8000"   # "" (or omitted) disables serving
# Per-endpoint limits default to the v1 RPC caps; override only if needed:
# max_ledgers_limit = 200
# default_ledgers_limit = 50
# max_transactions_limit = 200
# default_transactions_limit = 50
# max_events_limit = 10000
# default_events_limit = 100
```

Start the daemon:

```sh
stellar-rpc full-history --config /etc/stellar/fullhistory.toml
```

The server then exposes, on `[serve].endpoint`:

| Path       | What |
| ---------- | --- |
| `/`        | JSON-RPC 2.0 over HTTP POST (the four query methods) |
| `/metrics` | Prometheus text exposition |
| `/health`  | 200 when the daemon is up |
| `/ready`   | 200 once ingestion has committed a ledger, else 503 |

Let the daemon ingest (and, for a meaningful cold tier, freeze at least one full
chunk) before benchmarking. Poll `/ready` until it returns 200.

## 2. Build and run fhbench

```sh
go build -o /tmp/fhbench ./tools/fhbench
# or run directly:
go run ./tools/fhbench --url http://127.0.0.1:8000 --endpoint all --tier both \
    --concurrency 8 --duration 60s --limit 50
```

### Flags

| Flag            | Default                  | Meaning |
| --------------- | ------------------------ | --- |
| `--url`         | `http://127.0.0.1:8000`  | Base URL of the JSON-RPC server |
| `--endpoint`    | `all`                    | `getLedgers` \| `getTransactions` \| `getTransaction` \| `getEvents` \| `all` |
| `--tier`        | `both`                   | `hot` \| `cold` \| `both` |
| `--concurrency` | `8`                      | Closed-loop workers per (endpoint, tier) |
| `--duration`    | `60s`                    | Load duration per (endpoint, tier) |
| `--limit`       | `50`                     | Page limit for range/events requests |
| `--chunk-size`  | `10000`                  | Ledgers per chunk — must match the daemon's geometry (see below) |
| `--sample-size` | `100`                    | Target tx-hash samples per tier for `getTransaction` |

### How it works

1. **Discovery.** fhbench sends one `getLedgers` probe to learn the served range.
   Because v1 `getLedgers` validates that `startLedger` is inside the served
   range *before* returning that range, there is no zero-knowledge "successful"
   probe — so the probe deliberately uses `startLedger=1` (always below the
   genesis-clamped floor of 2) and reads `oldest`/`latest` out of the resulting
   out-of-range error message. If a server does answer, it takes the range from
   the successful response instead.
2. **Tiering.**
   - **hot** = the last `chunk-size/2` ledgers before `latest`.
   - **cold** = the oldest full chunk at or after `oldest` — chunk *k* covers
     `[k*chunk-size+2 .. (k+1)*chunk-size+1]` (genesis-anchored, `FirstLedgerSeq=2`).
3. **Sampling.** For `getTransaction`, fhbench pages `getTransactions` within each
   tier to collect real tx hashes *before* timing starts.
4. **Load.** N workers per (endpoint, tier) issue randomized requests in a closed
   loop (random start ledger for range endpoints, random sampled hash for
   `getTransaction`, rotating event filters incl. no-filter) for `--duration`,
   recording each request's wall time and status.

`--chunk-size` is **not** derived from the server (fhbench is black-box). The
daemon's `geometry`/`chunk.LedgersPerChunk` is `10000`; override the flag only if
the daemon is built with a different value. It shapes only the tier partition, not
correctness.

### Sample report

```
endpoint         tier     count        RPS       p50       p90       p99       max   errors
--------------------------------------------------------------------------------------------
getLedgers       hot       48213    803.5     8.9ms    14.2ms    31.7ms    88.1ms        0
getLedgers       cold       9127    152.1    48.6ms    71.3ms   130.4ms   402.9ms        0
getTransaction   hot       61044   1017.4     6.1ms     9.8ms    22.5ms    61.0ms        0
getTransaction   cold      12880    214.7    34.2ms    58.9ms   119.6ms   350.2ms        0
...
```

Quantiles use the **nearest-rank** method (`rank = ceil(q*N)`, value at that
1-based rank) over the sorted per-tier latency slice — no dependencies.

## 3. Prometheus queries

Point Prometheus (or `curl http://127.0.0.1:8000/metrics`) at the server while a
run is in progress. All metric names below are verified against the daemon's
`observability.go` and the serve server.

### Ingestion progress (namespace `soroban_rpc`, subsystem `fullhistory_streaming`)

```promql
# Ledgers committed per second (ingestion throughput).
rate(soroban_rpc_fullhistory_streaming_last_committed_ledger[1m])

# Absolute progress and retention floor (the two ends of the served window).
soroban_rpc_fullhistory_streaming_last_committed_ledger
soroban_rpc_fullhistory_streaming_retention_floor_ledger

# Hot-chunk count on disk, chunk-boundary handoffs, and swept/discarded artifacts.
soroban_rpc_fullhistory_streaming_live_hot_chunks
rate(soroban_rpc_fullhistory_streaming_chunk_boundaries_total[5m])
rate(soroban_rpc_fullhistory_streaming_discarded_hot_chunks_total[5m])
rate(soroban_rpc_fullhistory_streaming_pruned_artifacts_total[5m])

# Per-phase wall-clock (label `phase` = backfill_pass|freeze|rebuild|discard|prune).
histogram_quantile(0.99,
  sum by (le, phase) (rate(soroban_rpc_fullhistory_streaming_phase_duration_seconds_bucket[5m])))
```

### Per-endpoint query latency (`fullhistory_rpc_request_duration_seconds`, labels `endpoint`, `status`)

This histogram (buckets spanning 100µs–10s) is the server-side counterpart to
fhbench's client-side numbers.

```promql
# p50 / p90 / p99 latency per endpoint.
histogram_quantile(0.50,
  sum by (le, endpoint) (rate(fullhistory_rpc_request_duration_seconds_bucket[1m])))
histogram_quantile(0.90,
  sum by (le, endpoint) (rate(fullhistory_rpc_request_duration_seconds_bucket[1m])))
histogram_quantile(0.99,
  sum by (le, endpoint) (rate(fullhistory_rpc_request_duration_seconds_bucket[1m])))

# Request rate and error rate per endpoint.
sum by (endpoint) (rate(fullhistory_rpc_request_duration_seconds_count[1m]))
sum by (endpoint) (rate(fullhistory_rpc_request_duration_seconds_count{status="error"}[1m]))
```

The `endpoint` label is the JSON-RPC method name (`getLedgers`,
`getTransactions`, `getTransaction`, `getEvents`); `status` is `ok` or `error`.
The server does not label by tier, so use fhbench's `--tier hot` / `--tier cold`
runs (or watch the retention floor vs. the range you query) to separate the two
serving paths.

## Known POC performance note

`getEvents` with filters that are **type-selective but index-unselective** (e.g.
a bare `type=contract` filter with no contract-ID or topic narrowing) re-decodes
the whole scan window on every page/re-query. This is a deliberate POC latency
cliff — expect `getEvents` cold-tier latency to dominate the report when running
with type-only filters. Narrow with contract IDs/topics to avoid it.
