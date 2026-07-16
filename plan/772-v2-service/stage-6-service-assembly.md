# Stage 6 — Service assembly: config, gate, JSON-RPC, health, metrics endpoint

## Mission

- Turn the pieces into the running service: `[serving]` config superimposing v1 knobs, the JSON-RPC server with the backfill gate, v2 `interfaces.Daemon`, stubs, getLedgerEntries via captive core, the v2 getHealth, per-endpoint latency middleware, and the custom `metrics` method.
- After this stage `stellar-rpc full-history --config x.toml` is a queryable service (decisions D1, D3–D5, D7, D8).

## Read first

- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/00-DECISIONS.md`, `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md` (stages 1–5 as-built).
- v1 assembly to mirror: `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/jsonrpc.go` (NewJSONRPCHandler: method table, queue limiters, duration limiters, decorateHandlers, jhttp bridge, CORS, MaxBytes) and `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/daemon/daemon.go` (`createJSONRPCHandler`, `setupHTTPServers`, `setupAdminServer`, `newCaptiveCore` — the HTTP query-server params for getLedgerEntries).
- v1 config option names/defaults: `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/config/options.go`.
- v2 daemon/entry: `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/daemon.go` (runDaemonWith, supervise, newCaptiveCoreOpener — where NETWORK_PASSPHRASE is parsed), `startup.go` (ServeReads call), `health.go` (HealthSignal), `config/config.go`.
- getLedgerEntries chain: `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/methods/get_ledger_entries.go` + `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/ledgerentries/main.go` (needs `interfaces.FastCoreClient` = stellarcore client at the captive HTTP query port) + `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/daemon/interfaces/interfaces.go`.

## Deliverables

### 1. `[serving]` config (extends stage 1's section)

- Strict-TOML keys, defaults copied from v1 `options.go`:

```toml
[serving]
endpoint = "localhost:8000"            # JSON-RPC listen addr
admin_endpoint = ""                     # stage 1's; metrics/pprof
max_events_limit = 10000
default_events_limit = 100
max_transactions_limit = 200
default_transactions_limit = 50
max_ledgers_limit = 200
default_ledgers_limit = 50
max_request_execution_duration = "25s"  # global HTTP duration limit
request_backlog_global_queue_limit = 5000
max_healthy_ledger_latency = "30s"
ledger_reader_cache = 128               # cold ledger readers (chunks)
event_reader_cache = 32                 # cold event readers (chunks)

[ingestion]                             # additions for getLedgerEntries
captive_core_http_port = 11626          # 0 disables
captive_core_http_query_port = 11628    # required >0 for getLedgerEntries
captive_core_http_query_thread_pool_size = <NumCPU>
captive_core_http_query_snapshot_ledgers = 4
```

- Per-method queue limits + per-method durations: take v1 defaults IN CODE (no TOML keys yet — fewer knobs; add later if asked). Reaper grace T = max per-method duration + 5s (D-record) — feed it to the registry constructor from here.
- The superimposition trick: build an in-memory v1 `config.Config` populated from `[serving]` + v1 defaults and hand it to `internal.NewJSONRPCHandler` unchanged. Fields it reads: `HistoryRetentionWindow` (set from v2 retention: `retention_chunks * 10_000`, or full-history → derive from floor..latest at call time — check what getHealth does with it and pick the honest value), `MaxHealthyLedgerLatency`, the limit fields above, `NetworkPassphrase`, `FriendbotURL` (leave empty), all `RequestBacklog*` + `MaxGet*ExecutionDuration` defaults.

### 2. Gate + ServeReads wiring

```go
// serve.Gate: an atomic.Pointer[serve.State]. nil = backfilling.
// State{Registry *registry.Registry, ...} published by ServeReads (stage 3
// changed its signature to receive the registry); cleared (and registry
// closed) by run()'s teardown so a supervised restart starts gated again.
//
// The HTTP listener + jrpc handler are built ONCE in runDaemonWith (outside
// supervise) against adapters that dereference the Gate PER ADMISSION:
//   gate closed -> jsonrpc error {code: -32603 or custom; message:
//     "backfill in progress; query serving not started"} for everything
//     except getHealth and metrics.
// Simplest correct shape: the stage-4/5 adapters take the Gate (not a raw
// *Registry) and Admit() through it; a closed gate returns a sentinel the
// method-layer maps to the error above. Keep the error SHAPE consistent
// across endpoints.
```

- HTTP servers: mirror v1's `setupHTTPServers`/`Run` panic-group pattern; JSON-RPC on `[serving].endpoint`, admin (metrics + pprof, absorbing stage 1's listener) on `admin_endpoint`. Both live for the daemon's whole life, outside `supervise`.

### 3. v2 `interfaces.Daemon` + stubs

- Implement `interfaces.Daemon` in the serve package: `MetricsRegistry()` → the v2 registry from buildSinks; `MetricsNamespace()` → `interfaces.PrometheusNamespace` (keeps metric names v1-compatible); `FastCoreClient()` → stellarcore client pointed at `http://localhost:<captive_core_http_query_port>` (v1 `createHighperfStellarCoreClient` is the template); `CoreClient()` → stub erroring client (sendTransaction is stubbed anyway); `GetCore()` → the captive handle if the CoreOpener exposes it, else nil.
- getVersionInfo calls `daemon.GetCore().GetCoreVersion()` — nil-guard it (tiny patch in `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/methods/get_version_info.go` returning "unavailable" core version on nil; v1 unaffected since its core is never nil). 
- Stubs (D5): one shared `serve.Unsupported(method string) jrpc2.Handler` returning the clean unsupported error; registered for getFeeStats, sendTransaction, simulateTransaction. Two assembly options — copy v1's method table into a v2 `NewJSONRPCHandler` variant in the serve package, or parameterize the v1 one; pick whichever touches v1 least, CHECKPOINT the choice. `FeeStatWindows`/`PreflightGetter`/`DataStoreLedgerReader` params: nil/absent in v2 assembly (verify nil-safety on the paths that remain reachable).
- getEvents response shape (D10, BINDING — added mid-flight during stage 5): the v2 response must NOT contain the `inSuccessfulContractCall` key (deprecated; SDK says remove-in-v24; the stage-5 veneer does not compute it and hands the zero value through). `protocol.EventInfo` is SDK-owned with no `omitempty` on that field, and the shared v1 handler must stay untouched (D1) — so register a v2 getEvents handler whose response-building tail is forked: a local EventInfo-shaped struct WITHOUT the field (everything else identical, both XDR and JSON formats). Acceptance must assert the key is ABSENT from a served response.

### 4. Captive core query server (getLedgerEntries)

- v2's `newCaptiveCoreOpener` builds `ledgerbackend.CaptiveCoreConfig` — add the same `HTTPQueryServerParams` wiring v1's `newCaptiveCore` does from the new `[ingestion]` keys (and `HTTPPort` if trivially adjacent). getLedgerEntries then works exactly as v1: `methods.NewGetLedgerEntriesHandler(logger, fastClient, ledgerReader, decodeOpts)`.
- Gate note: entries come from live core, but hold getLedgerEntries behind the gate anyway (uniform "backfilling" behavior; core may not even be running until ingestion starts).

### 5. v2 getHealth + `metrics` endpoint + latency middleware

- getHealth (gate-exempt): small v2 handler in serve (do NOT reuse v1's) —
  - gate closed → success `protocol.GetHealthResponse{Status: "backfill in progress"}` (plus latest/oldest zero-values; if cheap, include the derived last-committed ledger from the catalog/progress helpers).
  - gate open → v1 semantics: `HealthSignal.LastCommitClose()` age vs `max_healthy_ledger_latency` → "healthy" or jrpc error, ledger range from the LedgerReader adapter, retention window value consistent with §1's choice.
- `metrics` (gate-exempt, custom method name `metrics`): returns the `latencytrack.Set` snapshot as JSON —

```go
// {"ingestion": {"ingest.read": {count, avg_s, max_s, p50_s, p75_s, p90_s, p99_s}, ...},
//  "rpc": {"getLedgers": {...}, ...}}
// Field naming: stable snake_case; durations as float seconds.
```

- Latency middleware: wrap every registered method handler (`rpc.<method>` tracker, recorded on return — success and error alike), stacked with the existing v1 decoration pipeline (queue limiter → duration limiter → latency track). Keep v1's `request_duration_seconds` summary intact.

## Non-goals

- No devbox config/runbook (stage 7). No SQLite removal. No preflight/simulateTransaction support.

## Acceptance

- `go build ./...`, `go vet ./...`, all tests green.
- Manual smoke against a local run (testnet-ish config or the e2e harness): during backfill, getLedgers returns the gated error while getHealth returns "backfill in progress" and `metrics` returns ingestion stats; after gate opens, all four core endpoints + getLatestLedger/getNetwork/getVersionInfo/getLedgerEntries answer; stubs return the unsupported error; `metrics` shows per-endpoint quantiles.
- getEvents responses contain NO `inSuccessfulContractCall` key (D10) — assert key ABSENCE on the raw JSON of a served response, not just a zero value.
- CHECKPOINT.md updated: assembly choice (copy vs parameterize NewJSONRPCHandler), retention-window value choice for getHealth, exact `metrics` response schema, any v1 patches made (get_version_info nil-guard, etc.).
