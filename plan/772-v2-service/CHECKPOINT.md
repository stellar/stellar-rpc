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

## Stage 2 — fullhistory/registry: View, Registry, Reaper, caches, resolve, BuildFromCatalog (2026-07-15) — COMPLETE

- Files added (new package, nothing else in the repo touched):
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/registry/view.go` — View, ColdChunk, IndexCoverage, resolve (cold wins), clone, accessors, `ErrUnavailable`.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/registry/registry.go` — Registry, Snapshot, Admit, publish (clone-mutate-store), all write-side hooks, `LedgerStoreHandle`, reader resolution, defaults.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/registry/reaper.go` — grace-period Reaper (one goroutine, FIFO queue; grace fixed ⇒ head-only wait).
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/registry/cache.go` — generic per-kind LRU (`readerCache[R]`); eviction/retirement never close inline, always via reaper.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/registry/build.go` — Options + BuildFromCatalog (startup scan → initial View).
  - Tests: `helpers_test.go`, `reaper_test.go`, `cache_test.go`, `registry_test.go`, `build_test.go` in the same directory.
- As-built exported API (everything stage 3+ consumes):

```go
// package registry — import ".../internal/fullhistory/registry"
var ErrUnavailable error                      // chunk has no serving store in this view

type View struct{ /* unexported: floor, hot, cold, indexes */ } // immutable once published
func (v *View) Floor() chunk.ID
func (v *View) FloorLedger() uint32           // floor.FirstLedger()
func (v *View) HotChunks() []chunk.ID         // ascending
func (v *View) HotDB(c chunk.ID) (*hotchunk.DB, bool)  // registry-owned; never Close it
func (v *View) Indexes() []IndexCoverage      // copy, ascending by Window; readers registry-owned

type ColdChunk struct{ Ledgers, Events bool }
type IndexCoverage struct {
    Window geometry.TxHashIndexID
    Lo, Hi chunk.ID                           // the .idx's ACTUAL coverage, not window bounds
    Idx    *txhash.ColdReader
}

type Snapshot struct { Latest uint32; View *View }
func (r *Registry) Admit() Snapshot           // loads latest FIRST, then View (order load-bearing)
func (r *Registry) Reaper() *Reaper           // for lifecycle-deferred unlink/key-delete (stage 3 prune)

// Write-side hooks (stage 3 call sites per its hook table):
func (r *Registry) PublishHot(c chunk.ID, db *hotchunk.DB)       // ownership transfers to registry
func (r *Registry) AdvanceLatest(seq uint32)
func (r *Registry) PublishFrozen(c chunk.ID, kinds ...geometry.Kind) // KindTxHash accepted+skipped
func (r *Registry) SwapTxIndex(cov geometry.TxHashIndexCoverage) error // opens new reader; retires old via reaper; REFUSES non-frozen cov
func (r *Registry) UnpublishHot(c chunk.ID, destroy func() error) // after grace: handle.Close() THEN destroy()
func (r *Registry) AdvanceFloor(floor chunk.ID)                   // drops+retires below-floor hot/cold/indexes/cached readers; regression = warn+ignore
func (r *Registry) Close()                                        // idempotent; closes EVERYTHING now (reaper drained run-now)

// Read faces (stages 4–5):
type LedgerStoreHandle interface {            // satisfied by *ledger.ColdReader AND *ledger.HotStore
    GetLedgerRaw(seq uint32) ([]byte, error)
    IterateLedgers(start, end uint32) iter.Seq2[ledger.Entry, error]
}
func (r *Registry) LedgerReaderFor(v *View, c chunk.ID) (LedgerStoreHandle, error)
func (r *Registry) EventReaderFor(v *View, c chunk.ID) (eventstore.Reader, error)

type Reaper struct{ /* one goroutine */ }
func NewReaper(grace time.Duration, logger *supportlog.Entry) *Reaper
func (p *Reaper) Schedule(destroy func() error) // non-blocking; nil ignored; post-Close runs inline
func (p *Reaper) Close()                        // stop goroutine + run all pending destroys NOW

type Options struct {
    Grace          time.Duration              // 0 ⇒ DefaultGrace
    LedgerCacheCap int                        // 0 ⇒ DefaultLedgerCacheCap
    EventCacheCap  int                        // 0 ⇒ DefaultEventCacheCap
    PreOpened      map[chunk.ID]*hotchunk.DB  // live-chunk handle(s) ingestion already holds
    Logger         *supportlog.Entry          // nil ⇒ cat.Logger()
}
func BuildFromCatalog(cat *catalog.Catalog, ret geometry.Retention, latest uint32, opts Options) (*Registry, error)

const (
    DefaultGrace          = 30 * time.Second
    DefaultLedgerCacheCap = 128
    DefaultEventCacheCap  = 32
)
```

- Semantics pinned by tests (the behavioral contract, beyond signatures):
  - Admission: latest-then-View load order (hammered concurrently); write side must keep publishing a chunk's home BEFORE advancing latest into it.
  - Publish: clone-mutate-store under one mutex; old Snapshots stay fully usable (immutability test).
  - resolve: cold wins over hot per kind; per-kind independence (ledgers can be cold while events still hot); no home ⇒ `ErrUnavailable`.
  - Floor: `BuildFromCatalog` floor = `ret.FloorAt(geometry.LastCompleteChunkAt(latest))` (same derivation as `retentionFloorLedger` in startup.go); chunk-aligned per D-record.
  - UnpublishHot's reaper unit: handle Close FIRST, then destroy — destroy's body must NOT touch the DB (it gets a closed handle).
  - Registry.Close: no grace, synchronous, idempotent; every RocksDB LOCK released when it returns (supervised-restart requirement).
- Deviations from the stage doc + why:
  - `LedgerStoreHandle` is only `{GetLedgerRaw, IterateLedgers}` — `LastSeq` differs between the two stores (`(uint32, error)` cold vs `(uint32, bool, error)` hot) so it cannot be shared; doc said "define the smallest interface that fits both".
  - Additions beyond the doc's list (all needed by stages 3–4, recorded here as the API grew): `Registry.Reaper()` (stage 3 prune defers unlink/key-delete "via reaper"), View accessors `Floor/FloorLedger/HotChunks/HotDB/Indexes` (stage 4 assembles tx-probe sets + bounds from the View), exported `Default*` constants, `Options.Logger`.
  - `SwapTxIndex` REFUSES (error + View unchanged) a non-frozen coverage instead of assert-log-and-proceed — R1 says a transient resource must never enter a View; the error also covers the open-failure case the doc's signature already implied.
  - `BuildFromCatalog` EXCLUDES below-floor entries entirely (ready hot keys, frozen chunk flags, coverages with `Hi < floor`) rather than only gating at query time — matches the steady-state invariant AdvanceFloor maintains and avoids opening handles for prune debris. A PreOpened handle that isn't a ready in-retention chunk is rejected with a warn and stays CALLER-owned; accepted handles become registry-owned.
  - `PublishFrozen` silently skips `KindTxHash` (stage 3 can pass exactly the kinds it froze; the .bin is an index-build input, never chunk-served). Unknown kinds warn.
  - resolve returns an internal `tier` struct, not a `Store` interface — ledger and event readers have different types; the two `*ReaderFor` funcs are the public faces (doc's own framing).
  - Cold-reader cache opens use `eventstore.ColdReaderOptions{Concurrency: 0}` (serial coalesced reads) — tuning deferred to stages 5–6.
  - Known benign window (documented in cache.go): a query on an older View can re-insert a retired chunk's cold reader after unpublish/floor-advance; it is bounded by the LRU cap and closed on eviction or Close. The doc's acquire path ("hit → return; miss → open, insert, return") is implemented as written.
- Verification:
  - `go build ./...` exit 0; `go vet ./...` exit 0 (final state, after all edits).
  - `go test ./cmd/stellar-rpc/internal/fullhistory/... ./cmd/stellar-rpc/internal/latencytrack/... -count=1`: every package ok, exit 0 — including the full e2e (241s) and lifecycle (105s; the stage-1 flake did not recur).
  - Registry package additionally passes `-race -count=1` (24 tests): admission-order hammer (4 readers vs publisher), clone isolation, cold-over-hot preference (real RocksDB hot DB + type-asserted tiers), floor advance (drops hot/cold/index + cache retire + grace-delayed closes observed via `stores.ErrStoreClosed`), reaper grace timing (destroy stamps its own run time ≥ T), FIFO order, Close-runs-pending-now, exactly-once across concurrent Schedule/Close, LRU eviction→retire routing, BuildFromCatalog over a real catalog + real cold pack + real hot DB + real (empty) `.idx` with R1 noise (freezing chunk, freezing coverage, ready-key-without-dir below floor) and read-through assertions on both tiers, PreOpened adoption (same pointer, ownership transfer on Close) and rejection (handle left open for caller), pristine catalog, ready-chunk-won't-open build failure.
  - golangci-lint still not runnable locally (2.11.3 built with go1.25 vs repo go1.26) — CI-only, pre-existing.
- Warnings / notes for stage 3:
  - Ordering the hooks rely on: `PublishHot(C+1)` must complete before C+1's first `AdvanceLatest` (the loop's natural sequence — do not reorder); `AdvanceLatest` only on Ingest's success path.
  - `Registry.Close()` requires publishers and query admission stopped FIRST: a publish that lands after Close leaks its resource into the dead View (comment on Close says this). Teardown order in run(): stop loops → registry.Close().
  - `UnpublishHot(c, destroy)`: destroy runs AFTER the handle is closed — put only transient-mark/rmdir/key-delete in it, never DB reads. It is also scheduled even if the chunk was not in the View (safe for replayed discards).
  - Prune: call `AdvanceFloor(floor)` first, demote in the run, then `reg.Reaper().Schedule(...)` the unlink+key-delete bodies (keep the fsync ordering inside the destroy step).
  - `BuildFromCatalog` opens write handles for EVERY ready in-retention hot chunk not in `PreOpened` — pass the live chunk's handle (and any other ingestion-held handle) via `Options.PreOpened` or the build's second write-open will fail on the RocksDB LOCK.
  - The registry (and its reaper goroutine) is per-run: build it inside run() after backfill, Close it on run() exit, rebuild on supervised restart. `latest` seeds from the caller-derived last-committed ledger (`lastCommittedLedger(cat)`).
  - Grace: assembly must eventually pass real `T` = max request duration + 5s margin (stage 6); until then DefaultGrace 30s applies.
  - `hotchunk.DB.Close()` is idempotent (verified in its doc/comment: "releases the shared store exactly once. Idempotent.") — but the registry is single-owner, so double-close should not arise.

---

## Stage 3 — write integration: registry hooks through hotloop/backfill/lifecycle/startup (2026-07-15) — COMPLETE

- Files changed (no new production files):
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/hotloop.go` — `hotPublisher` interface, `ingestionLoopConfig.Registry` (REQUIRED; loop errors on nil), boundary no longer closes, per-ledger `AdvanceLatest`, ownership comments rewritten (HANDOFF FENCE, LIVE-CHUNK EXCLUSION).
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/startup.go` — registry built in `run()` (see below), `StartConfig.ServeReads` signature, hook wiring into lifecycle config, `defer reg.Close()` teardown.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/daemon.go` — `daemonOptions.ServeReads` signature + no-op default.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/backfill/process.go` — `ProcessConfig.OnFrozen`, called after `FlipChunkFrozen` with exactly the kinds frozen.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/backfill/txindex.go` — `BuildConfig.OnIndexSwap` + `BuildConfig.DeferDestroy`; `buildTxhashIndex` publishes after `CommitTxHashIndex` (skip path publishes nothing); `buildThenSweep` defers sweep bodies when `DeferDestroy` set.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/lifecycle/lifecycle.go` — `Config.UnpublishHot/AdvanceFloor/DeferDestroy`; `AdvanceFloor(floor)` fires at the top of the prune stage (gate → unpublish → demote → destroy).
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/lifecycle/eligibility.go` — discard/prune op bodies built per hook presence (nil ⇒ exact pre-registry inline behavior).
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/catalog/catalog_sweep.go` — each sweep split into exported demote/destroy halves (see API); `DiscardHotChunk` recomposed as `PutHotTransient` + `DestroyHotChunk`; destroy bodies re-read the durable key and SKIP re-frozen/"ready"/absent keys.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/catalog/catalog_protocol.go` — `deleteHotKey` doc comment only.
  - Tests: `hotloop_test.go` (hookRecorder owns handles; fence test rewritten — see deviations), `e2e_test.go` (registry captured via ServeReads; View-vs-catalog assertions), `catalog/catalog_sweep_test.go` (+5 split/guard tests), `startup_test.go`/`daemon_test.go` (signature swaps), 3 lifecycle test files (nil hook args).
- As-built API (everything stage 4+ consumes):

```go
// startup.go / daemon.go — THE ServeReads signature decision (doc offered field-vs-signature):
StartConfig.ServeReads  func(ctx context.Context, reg *registry.Registry) error
daemonOptions.ServeReads  // same signature; nil ⇒ no-op
// run() order: backfill → open resume hot DB → BuildFromCatalog(cat, cfg.Retention, lastCommitted,
//   Options{PreOpened: {resumeChunk: hotDB}, Logger}) → OpenCore → boundary seed → lifecycle cfg
//   (hooks wired) → ServeReads(ctx, reg) → errgroup{ingestion, lifecycle} → g.Wait → deferred reg.Close()

// hotloop.go — the loop's registry face (satisfied by *registry.Registry):
type hotPublisher interface {
    PublishHot(c chunk.ID, db *hotchunk.DB) // ownership transfers with the call
    AdvanceLatest(seq uint32)
}
// ingestionLoopConfig.Registry hotPublisher — REQUIRED (loop returns error on nil)

// backfill — nil-safe hook fields (startup backfill leaves all nil):
ProcessConfig.OnFrozen     func(c chunk.ID, kinds ...geometry.Kind)          // after FlipChunkFrozen
BuildConfig.OnIndexSwap    func(cov geometry.TxHashIndexCoverage) error      // after CommitTxHashIndex, cov.State=frozen; error FAILS the build
BuildConfig.DeferDestroy   func(destroy func() error)                        // nil ⇒ sweep inline

// lifecycle — nil-safe hook fields (all nil ⇒ pre-registry inline destruction):
Config.UnpublishHot  func(c chunk.ID, destroy func() error)
Config.AdvanceFloor  func(floor chunk.ID)
Config.DeferDestroy  func(destroy func() error)

// catalog — sweeps split so demote can run NOW and destroy LATER (reaper):
func (c *Catalog) DemoteChunkArtifacts(refs []ArtifactRef) error       // frozen→pruning batch, no I/O
func (c *Catalog) DestroyChunkArtifacts(refs []ArtifactRef) error      // unlink→fsync→del keys; guarded
func (c *Catalog) DemoteTxHashIndexKey(cov geometry.TxHashIndexCoverage) error
func (c *Catalog) DestroyTxHashIndexKey(cov geometry.TxHashIndexCoverage) error
func (c *Catalog) DestroyHotChunk(chunkID chunk.ID) error              // rmdir→fsync→del key; guarded
// SweepChunkArtifacts / SweepTxHashIndexKey / DiscardHotChunk = demote + destroy (unchanged semantics)
// Guard: every Destroy* re-reads the durable key first — absent ⇒ no-op (idempotent re-run);
// re-"frozen" (or hot "ready") ⇒ warn + SKIP, never unlink a serving artifact.
```

- Hook table as implemented (all eight rows):
  - `openHotDBForChunk` boundary open → `PublishHot(next, nextDB)` right after the open, BEFORE `Boundary.Publish(closed)` and before next's first commit.
  - Resume-chunk open → adopted via `BuildFromCatalog` `PreOpened` (the startup-scan row covers it; no explicit `PublishHot`).
  - Ingest success → `AdvanceLatest(seq)` immediately after `hotService.Ingest` returns nil (before the gauges; failed ingest records nothing).
  - Boundary → NO close; the filled chunk's handle stays registry-owned and serving.
  - `processChunk` freeze commit → `OnFrozen(chunk, kinds...)` (nil during startup backfill).
  - `buildTxhashIndex` commit → `OnIndexSwap(cov)` with `State=StateFrozen`; predecessor/.bin sweep bodies deferred via `DeferDestroy` in lifecycle runs, inline in startup backfill.
  - Discard op (registry mode) → read hot state (absent ⇒ no-op) → `UnpublishHot(c, DestroyHotChunk)` → `PutHotTransient(c)`; reaper closes handle THEN destroys after grace.
  - Prune → `AdvanceFloor(floor)` first, then per-op demote-now + `DeferDestroy(destroy)`.
- Deviations from the stage doc + why:
  - ServeReads SIGNATURE changed (not a StartConfig registry field): the registry is per-run — built inside `run()` after backfill — so it cannot live in the once-built StartConfig.
  - `ingestionLoopConfig.Registry` is REQUIRED, not nil-safe: with no owner, boundary-opened handles would leak (production always has the registry by loop time). The doc's nil-safety requirement applies to backfill/lifecycle hooks only, which are nil-safe.
  - `TestRunIngestionLoop_HandoffFenceClosesBeforeNextKey` REWRITTEN as `TestRunIngestionLoop_HandoffFenceKeepsHandleRegistersNext`: the old test pinned close-before-publish, which this stage deliberately removed. New assertions: filled chunk's write handle still HELD at publish (read-write reopen fails on the RocksDB LOCK), next key ready, next handle registry-owned before the boundary publish. Loop tests use a `hookRecorder` (owns handles; `closeAll` stands in for `registry.Close`) — the "loop closes nothing" contract is uniform, no legacy dual mode.
  - Freeze does NOT read through the registry handle: `backfillSource`'s read-only hot open kept as-is (the doc's default; the HotProbe seam remains an option for later).
  - Destroy-side state guards (skip re-frozen/ready/absent) added beyond the doc: with destruction now deferred up to grace T, a guard converts any future re-freeze-while-pending bug from silent artifact loss into a warn+skip. Analysis says no current flow can re-freeze a demoted key before its destroy runs (floors and coverage bounds are monotone; equal-coverage freezing debris is rebuilt by stage 1 of the same tick before the stage-3 scan defers anything); the guard is defense-in-depth, with a theoretical read-then-unlink TOCTOU accepted as unreachable.
  - `OnIndexSwap` error FAILS the build (doc silent on error handling): a failed swap would leave the View serving the superseded coverage while the catalog holds the new one; failing → supervised restart → `BuildFromCatalog` republishes correctly.
  - Deferred sweeps re-list until the destroy fires: a next tick's scan re-sees "pruning"/"transient" keys and re-schedules the same idempotent destroy (bounded duplicate work, correct outcome). Prune/Discard METRICS count at op/schedule time, not physical-deletion time.
- Verification:
  - `go build ./...` exit 0; `go vet ./cmd/stellar-rpc/internal/fullhistory/... ./cmd/stellar-rpc/internal/latencytrack/...` exit 0 (final tree).
  - `go test ./cmd/stellar-rpc/internal/fullhistory/... ./cmd/stellar-rpc/internal/latencytrack/... -count=1`: ALL packages ok, exit 0 — fullhistory 248s (full e2e), backfill 74s, lifecycle 111s, catalog 25s (incl. 5 new split/guard tests), registry 7s; the stage-1 lifecycle flake did not recur.
  - E2e extended per acceptance: `ServeReads` captures the run's registry; after both discards settle, `require.Eventually` pins the admitted View to the catalog — `Latest == last delivered ledger`, `HotChunks == {live chunk}`, floor == genesis chunk — then frozen chunks 0/1 resolve BOTH cold readers via `LedgerReaderFor`/`EventReaderFor` (a chunk-0 ledger reads back through the cold tier), and `View.Indexes()` carries both terminal coverages. Post-shutdown steps (key/file deletion, LOCK release, restart resume, prune) unchanged and green — they prove `registry.Close()` drains deferred destroys at run() exit.
  - golangci-lint still not runnable locally (2.11.3 built with go1.25 vs repo go1.26) — CI-only, pre-existing.
- Warnings / notes for stage 4:
  - `ServeReads(ctx, reg)` hands you the PER-RUN registry: it dies (Close) when `run()` exits and a fresh one arrives on the next supervised restart — hold it only inside the serving goroutine's run scope, stop admitting on ctx cancel, and never Close it yourself.
  - `Registry.Admit()` is the only admission point; `latest` seeds from the derived last-committed at build and advances per commit thereafter. The View has NO cold-flag accessor — resolve chunks through `LedgerReaderFor`/`EventReaderFor` (they wrap the caches + `ErrUnavailable`).
  - A discarded chunk's hot handle may serve an admitted View for up to grace T after `UnpublishHot` — adapters must not cache resolutions across admissions (per-request Admit, as the spec says).
  - Grace is still `DefaultGrace` 30s (stage 6 passes real T = max request duration + 5s via `Options.Grace`); cache caps still defaults (stage 6: `[serving]` keys).
  - The e2e helper `runDaemonInBackground` now returns `(cancel, done, *atomic.Pointer[registry.Registry])` — reuse the cell for read-path e2e assertions.
  - `hotchunk.DB.Close()` idempotency verified again by the hookRecorder tests (double closeAll is safe), but the registry remains single-owner — do not Close registry-owned handles in adapters (View doc comments say the same).

---

## Stage 4 — fullhistory/serve: db.LedgerReader + db.TransactionReader over the registry (2026-07-15) — COMPLETE

- Files added (new package; NOTHING else in the repo touched — no methods/* edits were needed):
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/serve/ledger_reader.go` — package doc, `LedgerReader`, `admission` (the one-Snapshot read core), `ledgerReaderTx`, `decodeLedgerHeader`.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/serve/transaction_reader.go` — `TransactionReader`, probe-set assembly, `boundedIndex`, `routingLedgerSource`.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/serve/serve_test.go` — real-store fixture (catalog + cold packs + `.bin`→`.idx` + live hot chunk) + 6 tests.
- As-built exported API (what stage 6 assembles):

```go
// package serve — import ".../internal/fullhistory/serve"
func NewLedgerReader(reg *registry.Registry) *LedgerReader   // implements db.LedgerReader
func NewTransactionReader(reg *registry.Registry, networkPassphrase string) (*TransactionReader, error)
                                                             // implements db.TransactionReader; errors on nil reg / "" passphrase
```

- Behavior contract (pinned by tests):
  - `NewTx` = the spec's admission: ONE `registry.Admit()` snapshot; every `LedgerReaderTx` method runs against it; `Done()` = no-op nil. Point-read methods (`GetLedger`, `GetLedgerRange`, `GetLatestLedgerSequence`) admit per call.
  - Bounds: found=false ONLY outside the admitted `[View.FloorLedger(), latest]` (below floor = R2, above latest = never-ahead-of-watermark). INSIDE the bounds a store-level miss surfaces as an ERROR — the View invariant says every in-range ledger has a serving home, so a hole is data loss, not a not-found.
  - `GetLedgerRange` = `{floor, latest}` with close times from two point reads (cold rides the LRU; per stage doc, memoize per-View only if getHealth profiling ever shows it hot). Empty registry (`latest == 0` or floor ledger > latest) → `db.ErrEmptyDB` (v1's empty-DB shape); same for `GetLatestLedgerSequence`. The v1 handlers' own request.Validate against this range reproduces v1's out-of-range error shape verbatim — the adapter never fabricates that error itself.
  - `BatchGetLedgers` walks `[start, end]` chunk by chunk ascending, clamps silently to the admitted bounds exactly like v1's SQL BETWEEN (rejection is the handler's job), copies borrowed iterator bytes, derives `Header` via v1's partial decode (version → skip V1+ ext → header; copied byte-for-byte from `db/ledger.go`), errors with v1's exact "batch size must be greater than zero" on start > end, returns empty-non-nil on a fully clamped range, checks ctx per chunk.
  - `StreamAllLedgers` / `StreamLedgerRange` / `GetLedgerCountInRange`: honest "not supported by the full-history backend" errors — grep confirmed NO served endpoint reaches them (v1-ingest/migration helpers only).
  - `GetTransaction`: per-request admit → probe sets from the View (hot = every hot chunk's `Txhash()`, cold = every `Indexes()` `.idx`, both newest-first) → `txhash.NewTxReader` → direct `LedgerTransactionView` → `db.Transaction` mapping; found=false → `db.ErrNoTransaction`.
- THE TxReader soft-miss verification result (stage doc's mandatory check):
  - `TxReader.scan` DOES keep probing after a cold-candidate ledger-fetch failure, BUT records it as a soft error, and `GetTransaction` deliberately converts "all tiers missed + softErr != nil" into a `"txhash: lookup incomplete"` ERROR (the in-code comment cites #772: a soft failure means "not found" cannot be asserted). The ONLY sentinel the scan skips cleanly on is `stores.ErrNotFound` from the INDEX `Get`.
  - Therefore the floor gate lives on the indexes, not the ledger source: `boundedIndex` wraps every probe-set entry and maps a hit outside `[floorLedger, latest]` to `stores.ErrNotFound`. The stage doc's fallback ("wrap the LedgerSource to return the sentinel the scan skips on") is impossible — no such sentinel exists on that path; every ledger-source error is soft for cold candidates and turns the lookup into an InternalError instead of notFound. Matches the design doc's §getTransaction line "Candidates below the admitted floor are skipped". TxReader semantics untouched.
  - `routingLedgerSource` still re-checks bounds (returns `stores.ErrNotFound`) as defense in depth — also keeps `chunk.IDFromLedger` (which panics below ledger 2) unreachable on garbage seqs.
- Stored-bytes (compression) finding (stage doc's mandatory check): `GetLedgerRaw` returns DECOMPRESSED raw LedgerCloseMeta XDR on BOTH tiers — zstd is internal to the stores (cold: packfile `RecordDecoder`; hot: facade decode) — in fresh caller-owned buffers, so `.Lcm` needs no decode helper in the veneer. `IterateLedgers` yields BORROWED bytes on both tiers (valid only until the next step — the batch walk clones per ledger). Cold `IterateLedgers` REJECTS ranges not fully inside pack coverage (`stores.ErrOutOfRange`) → the walk clips to `[chunk.FirstLedger(), chunk.LastLedger()] ∩ [lo, hi]` per chunk; hot tolerates any range.
- Deviations from the stage doc + why:
  - `db.ParseTransaction` NOT reused: it needs a parsed `ingest.LedgerTransaction`, but `TxReader` returns the raw-bytes `ingest.LedgerTransactionView` whose fields map 1:1 onto `db.Transaction` (same outer result-pair hash → hex, raw Result/Meta/Envelope wire bytes, `DiagnosticEvents` verbatim-no-wrap on both paths — verified against `TransactionMeta.GetDiagnosticEvents` — identical V3 soroban contract-event gate, empty-not-nil slices on both). Bonus: the view path handles TransactionMeta V0 (genesis-era ledgers) which `ParseTransaction`'s `GetTransactionEvents` errors on.
  - Above-latest gating added beyond the doc's floor-only framing: a hot exact-index hit past the admitted `latest` (ingest commits run ahead of `AdvanceLatest` by design) reads as clean notFound, so a response can never carry a tx whose ledger exceeds its own `latestLedger`. Pinned by `TestServe_NeverAheadOfLatest`.
  - `NewTransactionReader` returns an error (nil registry / empty passphrase) unlike v1's constructor — the passphrase is config-derived data and stage 6 should fail assembly loudly, not per-request.
- Fee-bump inner-hash finding (v1→v2 behavioral difference, NOT fixable in the adapter): v1's SQLite indexed BOTH `Result.TransactionHash` (outer) and `Result.InnerHash()` for fee-bumps; the v2 write side (hot entries and `.bin`→`.idx`, stages ≤3) indexes ONLY the TxProcessing outer hash, and `LedgerTransactionViewByHash` matches outer hashes only. So v2 `getTransaction(innerHash)` returns txNotFound where v1 found the outer tx. The gettransaction design doc is silent on inner hashes; changing this means changing ingestion + index format — flag to Karthik if inner-hash lookups must keep working.
- Verification:
  - `go build ./...` exit 0; `go vet ./...` exit 0.
  - `go test ./cmd/stellar-rpc/internal/fullhistory/... ./cmd/stellar-rpc/internal/latencytrack/... -count=1`: ALL packages ok, exit 0 — fullhistory 241s (full e2e), lifecycle 106s (stage-1 flake did not recur), serve 52s under the parallel run (2.8s standalone), registry/backfill/catalog/stores all green.
  - Serve tests cover the acceptance scenarios end-to-end over REAL stores (catalog + cold ledger packs incl. one full 10k-ledger pack + `WriteColdBin`→`BuildColdIndex` `.idx` + live `hotchunk.DB` adopted via `PreOpened`): (a) `BatchGetLedgers` spans the cold→hot chunk boundary with byte-exact `.Lcm` and `Header` == `lcm.LedgerHeaderHistoryEntry()`; (b) `GetTransaction` resolves txs from the cold `.idx` AND the live hot chunk (hash/seq/closeTime/appOrder/envelope asserted); (c) below-floor after `AdvanceFloor`: ledger reads flip to found=false, the advertised range starts at the new floor, batch clamps, and the still-indexed pruned tx returns `db.ErrNoTransaction` (NOT "lookup incomplete") — the boundedIndex gate's whole point. Plus never-ahead-of-latest and empty-registry (`ErrEmptyDB`) coverage.
  - golangci-lint still not runnable locally (2.11.3 built with go1.25 vs repo go1.26) — CI-only, pre-existing.
- Warnings / notes for stage 5:
  - Reuse the serve plumbing: `admission` shows the per-request pattern (one `Admit()`, then per-chunk `*ReaderFor` — never cache resolutions across admissions); the events adapter should mirror it with `EventReaderFor` + the same `[floor, latest]` clamps (leading-edge reject stays the handler's/validator's job; trailing edge truncates at `latest`).
  - The event veneer's per-chunk scan windows must clip to chunk boundaries the same way `BatchGetLedgers` does — cold eventstore readers are per-chunk artifacts.
  - `db.ErrEmptyDB` is the pre-first-commit shape from range/latest methods; stage 6's backfill gate keeps most traffic away from that state anyway.
  - Fixture builders worth lifting from `serve_test.go`: `buildLCM` (tx-bearing V2 LCM with deterministic closeTime=seq) and the `.bin`→`.idx` assembly — stage 5 needs event-bearing LCMs; extend `buildLCM` rather than re-inventing (or promote shared bits into `fhtest` if the copy grows).
  - For stage 6: surface NETWORK_PASSPHRASE (parsed in `newCaptiveCoreOpener`) to `serve.NewTransactionReader`; build all adapters INSIDE the `ServeReads(ctx, reg)` scope (registry is per-run); `getTransaction`'s handler does a second `GetLedgerRange` admission after the tx read — fine, latest is monotone, but don't "optimize" the two admissions into one shared snapshot across handler boundaries.

---

<!-- APPEND NEW ENTRIES BELOW. Template:

## Stage N — <title> (<date>) — COMPLETE | PARTIAL(resume: <exact next action>)

- Files added/changed: absolute paths.
- As-built exported API: exact Go signatures for everything later stages consume.
- Deviations from stage doc + why.
- Verification: build/vet/test commands run and their results; manual smokes.
- Warnings / notes for the next stage.
-->
