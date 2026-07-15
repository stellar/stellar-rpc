# Stage 1 — latencytrack + per-ledger ingestion benchmarking

## Mission

- Build the exact-quantile latency layer (`latencytrack`) both benchmarking asks lean on (decision D8).
- Instrument v2 ingestion so a single ledger's read-from-source, write-to-disk, and end-to-end durations are recorded with p50/p75/p90/p99, avg, count, and max-ever.
- Expose a minimal admin HTTP listener on the v2 daemon (Prometheus `/metrics` + `/latency.json`) so benchmarking works before the JSON-RPC service exists.

## Read first

- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/00-DECISIONS.md` and `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md`.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/hotloop.go` — the ingestion loop to instrument.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/ingest/metrics.go` — existing `MetricSink`/`PrometheusSink` (hot per-ledger PHASE histograms; cold per-chunk totals).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/daemon.go` — `runDaemonWith` (where `buildSinks` constructs the Prometheus registry; where the admin listener plugs in).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/config/config.go` — TOML schema to extend with `[serving]`.

## What exists today (verified 2026-07-15)

- `ingest.MetricSink.HotPhase(phase, d, items, err)` records per-ledger phase wall-clocks (extract/ledgers/txhash/events/commit/apply — "phases sum to the per-ledger total"). No single e2e series, no read-from-source timing, no max, bucket-limited quantiles only.
- `ingest.MetricSink.ColdIngest / ColdChunkTotal / ColdExtract` record backfill per-chunk timings.
- The hot loop (`runIngestionLoop`) pulls `cfg.Stream.RawLedgers(...)` (read) then `hotService.Ingest(ctx, seq, view)` (write). Time blocked in the iterator between ledgers is the read cost; it is unmeasured today.
- The v2 daemon has NO HTTP listener of any kind.

## Deliverables

### 1. Package `cmd/stellar-rpc/internal/latencytrack`

- A concurrent duration recorder with exact-enough quantiles and exact count/sum/max:

```go
// Tracker records durations for one series. Concurrent-safe; Record is
// lock-free (atomic bucket counters), so it is cheap enough for per-ledger
// and per-request call sites.
type Tracker struct { /* atomic count, sumNanos, maxNanos + geometric bucket array */ }

func (t *Tracker) Record(d time.Duration)
func (t *Tracker) Snapshot() Stats

// Stats is one series' summary. Quantiles come from log-spaced buckets
// (~5% relative error is fine); Count/Avg/Max are exact.
type Stats struct {
    Count              uint64
    Avg, Max           time.Duration
    P50, P75, P90, P99 time.Duration
}

// Set is a named collection: one Tracker per series name, created on first
// use. SnapshotAll returns a stable-ordered map for the metrics endpoint.
type Set struct { /* sync.Map or mutex+map */ }
func (s *Set) Tracker(name string) *Tracker
func (s *Set) SnapshotAll() map[string]Stats
```

- Bucket layout: geometric from 10µs to ~10min (factor ≈1.12, ~120 buckets, fixed array). Quantile = interpolate within the bucket. Overflows clamp to the last bucket but still update exact `Max`.
- Series names are plain strings; this stage uses `ingest.read`, `ingest.write`, `ingest.e2e`, `backfill.chunk`; stage 6 adds `rpc.<method>`.
- Cheap unit test: known distribution in → quantiles within tolerance, max/count exact (this is one of the "cheap insurance" spots per D-record).

### 2. Ingestion instrumentation

- In `runIngestionLoop` (`/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/hotloop.go`):

```go
// Per ledger, measure three durations:
//   read  = time blocked waiting for the next raw ledger from the stream
//   write = hotService.Ingest wall-clock (decode + all CF writes + commit + applies)
//   e2e   = read + write   (what "time to ingest one ledger" means end to end)
readStart := time.Now()
for raw, verr := range cfg.Stream.RawLedgers(...) {
    arrived := time.Now()                  // read = arrived - readStart
    ... hotService.Ingest(ctx, seq, view) ...
    done := time.Now()                     // write = done - arrived; e2e = done - readStart
    // record into latencytrack + emit Prometheus counterparts
    readStart = time.Now()                 // next iteration's read clock starts here
}
```

- Wire the `*latencytrack.Set` into `ingestionLoopConfig` (new optional field; nil-safe like `Health`).
- Backfill: record each `ColdChunkTotal`-equivalent into `backfill.chunk` (hook wherever `ingest.MetricSink.ColdChunkTotal` is invoked, or pass the Set into the cold path alongside the sink). Per-ledger backfill cost is derived (chunk / 10,000) — do not fake a per-ledger series for the cold path.
- Prometheus counterparts (same registry, `fullhistory_ingest` subsystem): add `hot_ledger_read_duration_seconds` and `hot_ledger_e2e_duration_seconds` histograms — either as new `MetricSink` methods implemented by `PrometheusSink` or registered beside it in `buildSinks`; pick whichever keeps `MetricSink` coherent, and keep the nop sink working.

### 3. Minimal admin HTTP listener

- New `[serving]` TOML section (this stage introduces only one key):

```toml
[serving]
admin_endpoint = "0.0.0.0:8001"   # "" (default) = disabled
```

- When set, `runDaemonWith` starts ONE `http.Server` (outside the `supervise` restart loop, torn down on daemon ctx cancel) serving:
  - `GET /metrics` — `promhttp.HandlerFor` over the daemon's existing Prometheus registry.
  - `GET /latency.json` — JSON dump of `Set.SnapshotAll()` (durations in seconds, float).
- Keep it dependency-light (net/http + promhttp + chi if convenient; v1's `setupAdminServer` in `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/daemon/daemon.go` is the pattern).
- Config loading is strict TOML — update `config.ParseConfig`/`WithDefaults` and their tests so the new section round-trips.

## Non-goals

- No JSON-RPC anything (stage 6 owns the `metrics` method; it will read the same `Set`).
- No registry/View/router work.
- No changes to what `MetricSink` phases measure today.

## Acceptance

- `go build ./...` and `go vet ./...` clean; `go test ./cmd/stellar-rpc/internal/fullhistory/... ./cmd/stellar-rpc/internal/latencytrack/...` green.
- `latencytrack` unit test proves quantiles/max/count.
- An e2e-style run (existing `e2e_test.go` harness or a manual daemon run) shows non-zero `ingest.read`/`ingest.write`/`ingest.e2e` snapshots and `/latency.json` + `/metrics` responding when `admin_endpoint` is set.
- CHECKPOINT.md updated per the protocol in `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/01-STAGES.md`.
