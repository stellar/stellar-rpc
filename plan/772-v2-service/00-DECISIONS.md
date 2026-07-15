# #772 v2 Service — Decision Record

- Scope: implement the query-routing design and turn the full-history v2 daemon into an actual JSON-RPC service (issue https://github.com/stellar/stellar-rpc/issues/772).
- This file records every decision resolved with Karthik on 2026-07-15. Every stage session MUST read this file before coding and MUST NOT re-litigate these decisions.
- Companion docs, read in this order:
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/01-STAGES.md` — stage sequencing, checkpoint protocol, exact prompts.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md` — living state; what prior sessions actually built.

## Design-doc ground truth

- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/design-docs/query-routing-design.md` — THE spec being implemented: Registry, immutable View, reaper with grace period T, admission (`latest` before View), chunk resolution (cold wins over hot), per-endpoint routing, cursor rules.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/design-docs/full-history-streaming-workflow.md` — write side: geometry, catalog states, one write protocol, lifecycle run, reader contract (R1: only `"ready"`/`"frozen"` visible; R2: below floor = not found), INV-1..INV-4.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/design-docs/gettransaction-full-history-design.md` — tx-hash tiers, `.idx` coverage semantics, probe-all-windows + verify chain.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/design-docs/getevents-full-history-design.md` — per-chunk event segments, bitmap index, hot/cold read paths.

## Resolved decisions

| # | Decision | Resolution |
|---|---|---|
| D1 | Service shape | Grow the existing `full-history` subcommand. Fill the `ServeReads` seam in the v2 daemon: one process = backfill + ingestion + JSON-RPC serving, all from the one v2 TOML. The v1 daemon (`stellar-rpc` root command) stays untouched. |
| D2 | Handler layer | ONE query router (registry/View/reaper per the spec) is the only brain. The existing v1 interfaces `db.LedgerReader`, `db.TransactionReader`, `db.EventReader` (in `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/db/`) are implemented as thin veneers over that router, so the v1 `methods` handlers and `internal.NewJSONRPCHandler` are reused verbatim. Patch a v1 handler only where an interface is hopelessly SQLite-shaped, and keep such patches minimal. |
| D3 | Endpoint set | Serve: getLedgers, getTransactions, getTransaction, getEvents (core four) + getHealth, getLatestLedger, getNetwork, getVersionInfo (cheap) + getLedgerEntries (via captive core HTTP query server, like v1). Plus one custom `metrics` endpoint (D8). |
| D4 | Serving gate | The HTTP listener starts at daemon startup, BEFORE backfill completes. Until the router is built and published (post-backfill `ServeReads`), every endpoint except getHealth and `metrics` returns a JSON-RPC error "backfill in progress". getHealth returns a success response whose status reports backfill progress (leaning on `HealthSignal` in `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/health.go`). The gate opens only when serving state is published. |
| D5 | Stub methods | getFeeStats, sendTransaction, simulateTransaction: registered but return a clean JSON-RPC "method not supported by the full-history service" error. No fake success payloads. |
| D6 | Below-floor fallback | REMOVED. Strict R2: a request whose leading edge is below the admitted floor gets an out-of-range error carrying the available range. No `rpcdatastore` fallback in the v2 query path; the ledger lake stays backfill-only. |
| D7 | Config | Superimpose the v1 service knobs onto the v2 TOML as a new `[serving]` section (endpoint, admin endpoint, per-method limits, execution durations, cache sizes), defaults copied from v1 (`/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/config/options.go`). Captive-core query-server knobs go under `[ingestion]`. Karthik fills `earliest_ledger` and the `[backfill.datastore]`/`[backfill.bsb]` values himself — devbox config ships with placeholders. |
| D8 | Benchmarking metrics | (a) Per-ledger ingestion: read-from-source, write-to-disk, and end-to-end durations with exact p50/p75/p90/p99 + max-ever + count + avg. (b) Per-endpoint query latency: p50/p75/p90/p99 + avg + max. (c) A new custom JSON-RPC method `metrics` surfaces all of it as JSON; it is gate-exempt (works during backfill). Existing plumbing (`ingest.MetricSink`/`PrometheusSink`, v1 `request_duration_seconds` summary) stays; the new `latencytrack` package adds the exact-quantile layer Prometheus can't provide in-process. |
| D9 | Session strategy | Work split into staged packages, one fresh Claude session per stage, sequential, same repo/directory. Handoff via `CHECKPOINT.md` (each session reads it first, appends its entry last). Commits happen at session end ONLY on Karthik's explicit go-ahead ("commit"): one commit per stage (code + CHECKPOINT.md entry together) on the current branch. Never push. |

## Cross-cutting implementation choices (settled, do not re-open)

- New packages live under `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/`:
  - `latencytrack/` — exact-quantile latency recorder (stage 1).
  - `fullhistory/registry/` — Registry, View, Reaper, cold-reader caches, resolve (stage 2).
  - `fullhistory/serve/` — the `db.*` adapter veneers, gate, health handler, jsonrpc assembly (stages 4–6).
- Hot DB ownership transfers to the registry (per the spec's "Changes to the streaming workflow"): the boundary handoff stops closing the just-filled chunk's `hotchunk.DB`; the registry keeps the handle serving reads until discard retires it through the reaper. Rationale: hot events can ONLY be served from the warmed write handle — `hotchunk.OpenReadOnly` is a ledgers-only view (`Events()` panics).
- Reaper grace period `T` = max per-method request duration limit + 5s margin, computed at assembly time from the serving config. Registry takes `T` as a constructor parameter with a sane default (30s) for tests.
- Cold reader caches: per-kind LRU, config keys `[serving] ledger_reader_cache` (default 128 chunks) and `event_reader_cache` (default 32 chunks). Cache eviction and unpublish both route closes through the reaper — never close a reader inline.
- Chunk-aligned retention floor (spec choice): `oldestLedger` advances in 10,000-ledger steps. Not ledger-precise; do not "fix" this.
- Ascending-only getEvents (v1 parity). `eventstore.QueryOptions.Descending` exists but the v1 handler/cursor contract is ascending; keep parity.
- Network passphrase: single source of truth is the captive-core TOML (`NETWORK_PASSPHRASE`), already parsed inside `newCaptiveCoreOpener` in `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/daemon.go`. Surface it to serving (needed by `txhash.NewTxReader`, getTransactions parsing, getEvents, getNetwork) instead of adding a second config key.
- Tests: unit tests take a back seat (Karthik's call). Every stage must compile (`go build ./...`), pass `go vet`, and keep existing tests green (`go test ./cmd/stellar-rpc/internal/fullhistory/...`). New tests only where they're cheap insurance for gnarly logic (registry publish/admission ordering, latencytrack quantiles, cursor stitching).
- Commit only at session end and only when Karthik says "commit" (D9). Never push.

## Known sharp edges every session must respect

- `latest` advances LAST: publish the watermark only after `hotService.Ingest` returns (commit + in-memory event index applies). Serving structures may run ahead of `latest`, never lag it.
- Admission order: load `latest` BEFORE loading the View (spec §Admission). Reversing it can advertise a `latestLedger` the View cannot serve.
- Cold wins over hot when both exist for a chunk (deterministic resolution).
- R1: never publish a resource into a View while its catalog key is `"freezing"`/`"pruning"`/`"transient"`.
- The supervised restart loop (`supervise` in daemon.go) re-runs `run()`: registry + gate must tear down cleanly on exit (close handles, gate closed) and rebuild on restart.
- RocksDB read-only opens of hot chunk DBs take no LOCK (safe alongside the write handle); only one write handle per DB may exist.
