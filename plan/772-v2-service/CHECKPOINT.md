# CHECKPOINT — #772 v2 Service Implementation

- Living handoff document. Read FIRST in every session; append your entry LAST (template at bottom).
- Protocol: `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/01-STAGES.md`. Decisions: `00-DECISIONS.md` (binding).

---

## Stage 0 — Stock-taking, decisions, plan (2026-07-15) — COMPLETE

- Session: planning session with Karthik (grill-me interview). No code written.
- What exists (verified against code, not just design docs):
  - v2 read primitives are ALL built and tested, unwired: `ledger.ColdReader`/`HotStore`, `eventstore.Reader` + `Query` engine (empty filters = match-all), `txhash.TxReader` (probe hot exact → cold fingerprinted → fetch-and-verify), `hotchunk.DB` facades, catalog scans, `HealthSignal` in `cmd/stellar-rpc/internal/fullhistory/health.go` (doc comment literally names it the #772 read-server seam).
  - `ServeReads` is a no-op `func(ctx) error` field on `StartConfig` (`cmd/stellar-rpc/internal/fullhistory/startup.go:131` call site).
  - v1's handler seam: `internal.HandlerParams` (`cmd/stellar-rpc/internal/jsonrpc.go:62`) — the four target methods consume only `db.LedgerReader`(+Tx), `db.TransactionReader`, `db.EventReader`.
  - Gotchas already known: `hotchunk.OpenReadOnly` is a LEDGERS-ONLY view (`Events()` panics) → registry must own warmed write handles; NETWORK_PASSPHRASE parsed only inside `newCaptiveCoreOpener` (daemon.go) → must be surfaced to serving; per-ledger ingest metrics exist only as PHASE histograms (no read-from-source, no e2e series, no max-ever); v1 `request_duration_seconds` summary lacks p75/max/avg surface.
- Verified-in-code signatures are quoted in each stage doc; explorer-agent reports behind them are session-local (not persisted) — the stage docs are the surviving source.
- Warnings for stage 1: none. Start with `stage-1-ingestion-metrics.md`.

---

## Stage 1 — latencytrack + ingestion benchmarking + admin HTTP (2026-07-15) — COMPLETE

- Files added:
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/latencytrack/latencytrack.go` (+ `latencytrack_test.go`) — the exact-quantile recorder.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/latency.go` — series-name constants + `chunkLatencySink`.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/admin.go` (+ `admin_test.go`) — `startAdminServer`.
- Files changed:
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/ingest/metrics.go` — `MetricSink.HotLedger` + `PrometheusSink` impl + 2 histograms (+ `ingest_test.go` testSink stub).
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/hotloop.go` — `ingestionLoopConfig.Latency` + per-ledger read/write/e2e timing (+ `hotloop_test.go` latency test).
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/daemon.go` — Set construction, sink wrap, admin listener start, `adminUp` test seam (+ `daemon_test.go` admin e2e test).
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/startup.go` — `StartConfig.latency` → loop wiring.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/config/config.go` — `ServingConfig` (+ `config_test.go` round-trip asserts).
- As-built exported API:

```go
// package latencytrack — all types concurrent-safe; nil *Set / nil *Tracker fully inert
func (t *Tracker) Record(d time.Duration)     // lock-free (atomics)
func (t *Tracker) Snapshot() Stats
type Stats struct {
    Count              uint64
    Avg, Max           time.Duration          // exact
    P50, P75, P90, P99 time.Duration          // bucket estimates, ~6% relative error
}
func (s Stats) MarshalJSON() ([]byte, error)  // {"count","avg","max","p50","p75","p90","p99"} seconds floats
type Set struct{ ... }                        // zero value ready
func (s *Set) Tracker(name string) *Tracker   // create-on-first-use; takes the Set lock — resolve once on hot paths
func (s *Set) SnapshotAll() map[string]Stats

// package ingest — ADDED to MetricSink (NopSink, PrometheusSink, ingest_test testSink all implement):
HotLedger(read, e2e time.Duration)            // loop-level per-ledger brackets; success only

// package config
type ServingConfig struct {
    AdminEndpoint string `toml:"admin_endpoint"` // "" (default) = no admin listener
}                                                 // Config.Serving, TOML section [serving]
```

- In-package (fullhistory) surface later stages build on:
  - Series names (`latency.go`): `latSeriesIngestRead/Write/E2E`, `latSeriesBackfillChunk` = `"ingest.read"`, `"ingest.write"`, `"ingest.e2e"`, `"backfill.chunk"`.
  - `startAdminServer(endpoint string, registry *prometheus.Registry, latency *latencytrack.Set, logger *supportlog.Entry) (string, func(), error)` — bound addr + stop func; serves `GET /metrics`, `GET /latency.json`.
  - THE `*latencytrack.Set` is created once in `runDaemonWith` beside the Prometheus registry, OUTSIDE `supervise` (survives restarts, like the health signal); reaches the loop via unexported `StartConfig.latency` → `ingestionLoopConfig.Latency` (nil-safe). Stage 6's `metrics` method must read this same Set.
  - `daemonOptions.adminUp func(addr string)` — test-only; learns a `":0"` port.
  - New Prometheus series (`{ns}_fullhistory_ingest_*`): `hot_ledger_read_duration_seconds`, `hot_ledger_e2e_duration_seconds` (write needs no new histogram — it is the sum of the existing hot phases).
  - Timing semantics: read = time blocked in the stream iterator (the FIRST read includes captive-core startup — a deliberate max-ever outlier), write = whole `hotService.Ingest`, e2e = read + write; the read clock resets at loop-body END so boundary handoff time lands in no series; failed pulls/ingests record nothing.
- Deviations from the stage doc + why:
  - 160 buckets, not "~120": the doc's own span (10µs → ~10min at factor 1.12) needs ~158; 160 tops out at ~11min. Factor stays 1.12.
  - `backfill.chunk` hooked by wrapping the daemon's sink (`chunkLatencySink` overrides `ColdChunkTotal`) — the doc's "hook wherever ColdChunkTotal is invoked" option; zero signature changes to the cold path.
  - Prometheus counterparts as a new `MetricSink` method (the doc's first-offered option) — keeps every emit behind the sink seam and the nop sink working.
  - Series names as unexported fullhistory constants (wire values exactly as the doc specifies) — two feeding sites, typo insurance.
  - `Stats` JSON field names chosen here (doc fixed only "seconds, float").
- Verification:
  - `go build ./...` exit 0; `go vet ./...` exit 0. One-time env note: `make build-libs` (Rust libpreflight/xdr2json) must have run in the checkout or the final binary link fails — pre-existing condition, not stage related.
  - `go test ./cmd/stellar-rpc/internal/fullhistory/... ./cmd/stellar-rpc/internal/latencytrack/... -count=1`: every package ok except ONE pre-existing flake — lifecycle's `TestLifecycleLoop_RunsTickPerNotifyThenStopsOnCtx` failed once with "prune op: context canceled" (cancel/prune race; lifecycle untouched by this stage); it passed `-count=5` and the whole lifecycle package passed `-count=1` on immediate rerun.
  - `latencytrack` tests pin quantiles on a known distribution (5% tolerance), exact count/avg/max, overflow clamp with exact max, nil-safety, JSON wire form.
  - E2e-style acceptance: `TestRunDaemon_AdminEndpointServesLatencyAndMetrics` drives the real `runDaemonWith` from a TOML with `[serving] admin_endpoint = "127.0.0.1:0"`, waits for 3 live commits, asserts non-zero `ingest.read`/`ingest.write`/`ingest.e2e` counts over HTTP `/latency.json` plus the new histograms in `/metrics`, then a clean ctx-cancel shutdown.
  - golangci-lint not run locally: the installed 2.11.3 is built with go1.25 and refuses the repo's go1.26 target (CI-only).
- Warnings / notes for stage 2:
  - None binding — the registry needs no latencytrack wiring; nothing in stage 1 constrains it.
  - For stage 6 later: per-endpoint series are free-form strings (`rpc.<method>` — no constants needed); reuse `Set.SnapshotAll` + `Stats.MarshalJSON` so the `metrics` JSON-RPC method and `/latency.json` stay wire-identical; resolve each method's Tracker once at handler assembly, not per request.

---

<!-- APPEND NEW ENTRIES BELOW. Template:

## Stage N — <title> (<date>) — COMPLETE | PARTIAL(resume: <exact next action>)

- Files added/changed: absolute paths.
- As-built exported API: exact Go signatures for everything later stages consume.
- Deviations from stage doc + why.
- Verification: build/vet/test commands run and their results; manual smokes.
- Warnings / notes for the next stage.
-->
