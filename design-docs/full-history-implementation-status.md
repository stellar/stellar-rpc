# Full-History Streaming Daemon — Implementation Status

Traceability from the issue breakdown (`full-history-implementation-issues.md`, design revision
`c586667a`) to the code on this branch (`streaming-ingestion-daemon`, PR against `feature/full-history`).
All paths are under `cmd/stellar-rpc/internal/fullhistory/streaming/` unless noted.

**Legend:** ✅ implemented · 🟡 partial (deferred portion noted) · ⛔ out of scope (composed dependency, tracked elsewhere)

## Summary

- **19 of 20 issues fully implemented.** Issue 13's second half (retiring the v1 SQLite write path +
  CHANGELOG) is intentionally deferred to the **#772** cutover.
- Reconciled to design revision **`c586667a`**.
- Full `fullhistory` tree green on the non-short test suite (RocksDB cgo; the heavy E2E runs and passes
  under a long `-timeout`).
- Independently reviewed across concurrency / test-intent / design-faithfulness lenses — **no blockers,
  no majors**.

## Phase 1 — Foundations

| # | Issue | Status | Primary code | Tests |
|---|---|---|---|---|
| 1 | Geometry & layout primitives | ✅ | `window.go`, `keys.go` (+ `pkg/chunk`) | `window_test.go` |
| 2 | Catalog: key schema + one write protocol | ✅ | `catalog.go`, `keys.go`, `paths.go`, `protocol.go` | `catalog_test.go`, `protocol_test.go` |
| 3 | Config schema, validation & locking | ✅ | `config.go`, `validate.go`, `lock.go` | `config_test.go`, `validate_test.go` |

## Phase 2 — Storage primitives

| # | Issue | Status | Primary code | Tests |
|---|---|---|---|---|
| 4 | Per-chunk hot DB lifecycle | ✅ | `ingest.go` (`openHotTierForChunk`), `hooks.go` (+ `pkg/stores/hotchunk` — single multi-CF DB) | `ingest_test.go` |
| 5 | `processChunk` + `backfillSource` (was `catchupSource`) | ✅ | `process.go`, `artifacts.go`, `eligibility.go` | `process_test.go`, `backfill_test.go` |
| 6 | Cold tx-hash rolling-rebuild protocol | ✅ | `build.go` (`buildTxhashIndex`) (+ #728 `BuildColdIndex`) | `build_test.go`, `perf_test.go` |
| 7 | Key-driven sweeps | ✅ | `sweep.go` | `sweep_test.go` |

## Phase 3 — Orchestration

| # | Issue | Status | Primary code | Tests |
|---|---|---|---|---|
| 8 | Derived progress | ✅ | `progress.go` (`lastCommittedLedger`) | `progress_test.go` |
| 9 | Postcondition resolver + executor | ✅ | `resolve.go` (`resolve`), `execute.go` (`executePlan`, `runBackfill`) | `resolve_test.go`, `execute_test.go` |
| 10 | Hot-DB ingestion loop | ✅ | `ingest.go` (`runIngestionLoop`), `hotsource.go` | `ingest_test.go` |
| 11 | Lifecycle goroutine (tick) | ✅ | `lifecycle.go` (`runLifecycleTick`, `lifecycleLoop`), `eligibility.go` | `lifecycle_test.go`, `convergence_test.go` |

## Phase 4 — Top-level wiring

| # | Issue | Status | Primary code | Tests |
|---|---|---|---|---|
| 12 | Startup orchestration (`startStreaming`) | ✅ | `startup.go` | `startup_test.go` |
| 13 | Daemon/CLI wiring + retire v1 backfill | 🟡 | `daemon.go` + `cmd/stellar-rpc/main.go` wiring | `daemon_test.go` |

> **Issue 13 — what's done vs deferred.** The streaming daemon entrypoint **is** wired into `main.go`. The
> v1 SQLite backfill/ingestion **write path** (`cmd/stellar-rpc/internal/ingest/backfill.go`,
> `ingest.BackfillMeta`) and the CHANGELOG entry are intentionally **not** removed here — per the design
> they are coordinated with the **#772 cutover**, because removing the v1 *write* path before the reader
> cuts over would break the v1 *query* path.

## Phase 5 — Operability & correctness

| # | Issue | Status | Primary code | Tests |
|---|---|---|---|---|
| 14 | Retention: prune / widen / shorten | ✅ | `retention.go`, `lifecycle.go` (`effectiveRetentionFloor`) | `retention_test.go` |
| 15 | Surgical recovery + hot-volume-loss | ✅ | `recovery.go` (`PlanSurgicalRecovery` / `ApplySurgicalRecovery`) | `recovery_test.go` |
| 16 | `audit` command (INV-1…4) | ✅ | `audit.go` (`Catalog.Audit` / `RunAudit`, incl. optional `DeepDeriver` INV-1) | `audit_test.go` (incl. an injected deep byte-mismatch) |
| 17 | Observability: metrics + logging | ✅ | `observability.go` (`PrometheusMetrics`) | `observability_test.go` |

## Phase 6 — Validation & performance

| # | Issue | Status | Primary code | Tests |
|---|---|---|---|---|
| 18 | Crash-injection & convergence suite | ✅ | (tests) | `convergence_test.go` — every injected state converges to INV-1∧2∧3∧4 via `audit` |
| 19 | End-to-end integration | ✅ (in-process variant) | (tests) | `e2e_test.go` — first-start / freeze / prune / restart-resume re-derivation / multi-window lookup |
| 20 | Bench-harness alignment | ✅ | `PERF.md` | `perf_test.go` — `…ByteIdenticalToColdPath`, `…Bin/Idx_MatchesSpecFormat` |

## Composed dependencies (⛔ not implemented here — tracked separately)

These are reused, not reimplemented; this design specifies *when/how/with what crash-safety* they are driven.

| Capability | Tracked in | Relationship |
|---|---|---|
| Per-type store write primitives (LCM → hot CF / cold artifact) | #765 | composed by the hot-DB lifecycle + `processChunk` |
| Hot / cold tx-hash store + single-index build | #728 / #729 | Issue 6 layers coverage keys + rolling rebuild on top |
| **Tx-hash read (lookup by hash across hot + cold)** | **#794 (#728)** | the **read counterpart** to Issue 6's writes; format-compatible (Issue 20 asserts the `.idx` written here is byte-identical to #728's `BuildColdIndex`); wired behind read serving at the #772 cutover. No file overlap with this PR. |
| XDR view extractors | #764 | composed by `processChunk` / ingestion |
| Hot / cold ledger & event stores | #695/#739, #740/#756 | composed by the hot-DB lifecycle + `processChunk` |
| **Read-path dispatch / reader routing** | #770 (design), #772 (cutover), #774 (events) | the daemon's `ServeReads` is an injected no-op recorder; read dispatch + v1 retirement land at the cutover |

## Build / test notes

- Built against **RocksDB 10.9.1** (grocksdb 1.10.7).
- The full `cmd` binary requires the pre-existing `make build-libpreflight` (rust FFI) to link; the Go code
  all compiles.
- The non-short E2E is slow under `-race` + contention (per-ledger synced fsyncs); test time budgets are
  sized for the contended path.
