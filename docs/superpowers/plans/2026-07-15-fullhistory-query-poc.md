# Full-History Query-Support POC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A runnable full-history daemon that serves `getLedgers`, `getTransaction`, `getTransactions`, and `getEvents` over JSON-RPC from both hot RocksDB chunks and sealed cold artifacts, instrumented for ingestion + per-endpoint latency benchmarking, per the query-routing design in PR #843 (`design-docs/query-routing-design.md` on the `docs/query-routing-design` branch).

**Architecture:** A new `serve` package holds a **Registry** that owns an atomic `latest` watermark and an immutable **View** (floor, hot chunk handles, cold artifact flags, tx-hash window index readers). Queries admit by loading `latest` then the View, and resolve each chunk to cold (preferred) or hot. The v1 JSON-RPC handlers for getLedgers/getTransactions/getTransaction are reused verbatim behind thin `db.LedgerReader`/`db.TransactionReader` adapters; getEvents gets a new lean handler over `eventstore.Query`. The server plugs into the existing `ServeReads` seam.

**Tech Stack:** Go, grocksdb (existing), creachadair/jrpc2 + jhttp (existing), prometheus (existing). **No new dependencies.**

## Global Constraints

- Branch: `poc/fullhistory-query` (off `feature/full-history`). Commit after every task; commit messages `fullhistory: <what> (query POC)`. **Never** add AI/Claude attribution or Co-authored-by lines.
- This is a POC. Deliberate simplifications are marked `// POC:` with the ceiling and the upgrade path (e.g. `// POC: no reaper — superseded .idx readers leak (bounded by coverage swaps); add grace-period reaper at productionization`).
- **POC scope cuts (locked, do not re-add):** no LRU reader caches (per-request cold readers), no reaper (see safety rules in Task 2), no datastore fallback (pass `nil`), chunk-aligned retention floor.
- macOS test invocation (RocksDB 10.9.1 lives at `~/.rocksdb-1091`; brew's 11.x is incompatible):
  ```
  CGO_CFLAGS="-I$HOME/.rocksdb-1091/include" CGO_LDFLAGS="-L$HOME/.rocksdb-1091/lib -L/opt/homebrew/lib -Wl,-rpath,$HOME/.rocksdb-1091/lib -Wl,-rpath,/opt/homebrew/lib" go test ./cmd/stellar-rpc/internal/fullhistory/... -short -count=1
  ```
  Gate success on the command's exit code directly (`go test ... ; echo $?` is wrong under pipes — do not pipe).
- Style: gofumpt (not gofmt). Prefer `iter.Seq2[T, error]` range-over-func iterators over callback+sentinel patterns. `errors.Is` against `stores.Err*` sentinels (`storage/stores/errors.go`), never RocksDB/packfile errors.
- Comment prose in fullhistory intentionally says "window" where it means the tx-index span; do not "fix" it.
- All catalog access via the `catalog.Catalog` API; never list directories.

## Key file:line anchors (all under `cmd/stellar-rpc/internal/`)

| What | Where |
| --- | --- |
| ServeReads seam (no-op default) | `fullhistory/daemon.go:53`, default at `:160-164`, called at `fullhistory/startup.go:131` |
| Per-ledger commit + LastCommitted gauge | `fullhistory/hotloop.go:151-157` |
| Chunk boundary (close → open next → Publish) | `fullhistory/hotloop.go:168-192` |
| Lifecycle tick | `fullhistory/lifecycle/lifecycle.go:106` (runLifecycle), Loop at `:227` |
| Startup last-committed | `fullhistory/progress.go:33` (`lastCommittedLedger`) |
| Health signal | `fullhistory/health.go:14` (`HealthSignal`), fed at `hotloop.go:163-165` |
| Prometheus registry | `fullhistory/daemon.go:179` |
| Config struct (strict TOML) | `fullhistory/config/config.go:30`, `ResolvePaths` `:215`, `NewLayoutFromPaths` `:273` |
| Passphrase peek from core TOML | `fullhistory/daemon.go:413-459` |
| Catalog scans | `catalog/catalog.go`: `ChunkArtifactKeys():96`, `ReadyHotChunkKeys():126`, `FrozenTxHashIndex(w):139`, `AllTxHashIndexKeys():131`, `EarliestLedger():193` |
| Layout paths | `fullhistory/geometry/paths.go`: `HotChunkPath:72`, `LedgerPackPath:79`, `EventsBucketDir:87`, `TxHashIndexFilePath:128` |
| hotchunk.DB facades | `storage/stores/hotchunk/hotchunk.go`: `Ledgers():218`, `Txhash():224`, `Events():233` (panics read-only), `MaxCommittedSeq():254`, `OpenReadOnly:133` (ledgers-only) |
| Ledger stores | hot `GetLedgerRaw` `storage/stores/ledger/hot_store.go:87`, `IterateLedgers:121` (borrowed bytes); cold `OpenColdReader` `cold_reader.go:61`, `GetLedgerRaw:110`, `IterateLedgers:140` (borrowed), `Close:178` |
| Events | `eventstore.Reader` iface `storage/stores/eventstore/reader.go:56` (hot+cold both implement); `Query(ctx, r, filters, opts)` `query.go:179`; `Filter` `query.go:48`; `QueryOptions` `query.go:147`; `EventIDRangeForLedgers` `query.go:119`; cold open `cold_reader.go:136` |
| Txhash | hot `Get(hash)` `storage/stores/txhash/hot_store.go:100`; cold window `.idx` `OpenColdReader` `cold_reader.go:33`, `Get:56` (candidate), `MinLedger/MaxLedger:72-76`; verify chain `TxReader` `read_assembly.go:30-50`, `HashIndex` iface `:20`, `LedgerSource` iface `:26` |
| v1 handlers | `methods/get_ledgers.go:29` (`NewGetLedgersHandler`), `get_transactions.go:289`, `get_transaction.go:160`, `get_events.go` (reference only) |
| db interfaces | `db/ledger.go:25` (`LedgerReader`), `:35` (`LedgerReaderTx`), `:67` (`LedgerMetadataChunk`); `db/transaction.go:51/29/27` (`TransactionReader`/`Transaction`/`ErrNoTransaction`); `db.ParseTransaction` in `db/transaction.go` |
| jrpc2 assembly template | `internal/jsonrpc.go:157-363` (bridge, method map, middleware) |
| E2E harness | `fullhistory/e2e_test.go`: `runDaemonInBackground:209`, `e2eConfigPath:183`, tx-bearing ledger builders `:49-50,253`, cold/hot read pattern `:360-431`; `fhtest/fhtest.go`: `ZeroTxLCMBytes:39` |

---

### Task 1: Full-facade read-only hot open (`hotchunk.OpenReadView`)

**Why:** After the boundary fence closes the write handle (`hotloop.go:173`), the completed chunk must stay servable (ledgers, events, txhash) until its cold artifacts freeze. `OpenReadOnly` is ledgers-only (`Events()` panics). Read-only opens take no RocksDB LOCK (`hotloop.go:111-116`), so a full read-only view can coexist with the freeze source's own `OpenReadyView`.

**Files:**
- Modify: `cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk/hotchunk.go`
- Test: `cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk/hotchunk_test.go` (extend)

**Interfaces:**
- Produces: `func OpenReadView(state geometry.HotState, path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error)` — enforces `state == geometry.HotReady` (mirror `OpenReadyView` at hotchunk.go:147), opens the store read-only with the **full CF union** (`ColumnFamilies()` at hotchunk.go:57), and composes **all three facades** including `eventstore.NewWithStore` (which runs its mandatory warmup) so `Events()` and `Txhash()` work. `Close()` closes the store as usual.

- [ ] **Step 1: Write the failing test.** In the existing hotchunk test file, add `TestOpenReadView_ServesAllFacades`: create a DB via `Open`, ingest 2-3 ledgers carrying at least one tx and one contract event (reuse this package's existing ingest-test fixtures — see the `IngestLedger` tests in the same file for how views are built), close it, reopen via `OpenReadView(geometry.HotReady, path, chunkID, logger)`, then assert: `Ledgers().GetLedgerRaw(seq)` returns bytes; `Txhash().Get(hash)` returns the seq; `Events().EventCount()` > 0 and `Events().LookupKeys(...)` finds the ingested event's term. Also assert a second `OpenReadView` while the first is open succeeds (no LOCK conflict).
- [ ] **Step 2: Run it, confirm it fails** with `OpenReadView` undefined (use the macOS CGO command from Global Constraints, `-run TestOpenReadView`).
- [ ] **Step 3: Implement.** Follow the structure of `openReady`/`compose` (hotchunk.go:152-210): read-only `rocksdb.Config{ReadOnly: true, MustExist-equivalent}` with full `ColumnFamilies()` + the per-CF options used by the write open, then compose all three facades. Update the doc comments that say read-only views are ledgers-only (hotchunk.go:10,38,118,144-147,194,229) to mention the new full-facade variant — do not weaken the freeze-source docs.
- [ ] **Step 4: Run the package tests** (`go test ./cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk/ -short -count=1` with CGO flags). Expected: PASS.
- [ ] **Step 5: Commit** `fullhistory: add hotchunk.OpenReadView full-facade read-only open (query POC)`.

---

### Task 2: `serve` package — Registry, View, hooks, resolution

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/serve/registry.go`
- Create: `cmd/stellar-rpc/internal/fullhistory/serve/registry_test.go`

**Interfaces:**
- Consumes: `catalog.Catalog` scan methods, `geometry.Layout`/`Retention`/`TxHashIndexLayout`, `hotchunk.DB`, `hotchunk.OpenReadView` (Task 1), `txhash.OpenColdReader`, `ledger.OpenColdReader`, `eventstore.OpenColdReader`, `stores.Err*`.
- Produces (used by Tasks 3-7):

```go
package serve

// View is one immutable snapshot of the serving map (design: PR #843).
type View struct {
    Floor    chunk.ID                    // chunk-aligned retention floor
    Earliest uint32                      // catalog EarliestLedger (genesis clamp)
    Hot      map[chunk.ID]*hotchunk.DB   // live chunk: shared write handle; closed ready chunks: OpenReadView handle
    Cold     map[chunk.ID]ColdFlags
    TxIdx    []TxIndexCoverage           // ascending by window
}

type ColdFlags struct{ Ledgers, Events bool }

type TxIndexCoverage struct {
    Lo, Hi chunk.ID
    Reader *txhash.ColdReader
}

type Registry struct { /* mu sync.Mutex; current atomic.Pointer[View]; latest atomic.Uint32; cat *catalog.Catalog; retention geometry.Retention; logger *supportlog.Entry */ }

func NewRegistry(cat *catalog.Catalog, retention geometry.Retention, logger *supportlog.Entry) *Registry

// Admit: the two atomic loads, latest FIRST (order is load-bearing, see design §Admission).
func (r *Registry) Admit() (latest uint32, v *View)

// Hooks (all safe on nil *Registry so the no-serve path needs no guards):
func (r *Registry) LedgerCommitted(seq uint32)                  // hotloop, after Ingest returns
func (r *Registry) HotOpened(c chunk.ID, db *hotchunk.DB)      // hotloop + startup: publish shared write handle
func (r *Registry) ChunkClosed(c chunk.ID)                     // hotloop boundary: reopen via OpenReadView, republish
func (r *Registry) TickCompleted()                             // lifecycle: rescan catalog, republish
func (r *Registry) BuildInitial(lastCommitted uint32) error    // startup, before ServeReads returns

// Resolution (design §Chunk resolution: cold wins):
type Kind int // KindLedgers, KindEvents
func (v *View) ResolveLedgers(c chunk.ID, layout geometry.Layout) (LedgerChunk, error) // ErrUnavailable if no home
func (v *View) ResolveEvents(c chunk.ID, layout geometry.Layout) (EventsChunk, error)

// LedgerChunk / EventsChunk are tiny per-chunk handles the caller must Close()
// (hot-backed ones have a no-op Close; cold-backed ones own the cold reader).
type LedgerChunk struct{ /* Get(seq) ([]byte, error); Iterate(start, end) iter.Seq2[ledger.Entry, error]; Close() */ }
type EventsChunk struct{ /* Reader() eventstore.Reader; Close() */ }

var ErrUnavailable = errors.New("no serving home for chunk")
```

**Semantics (implement exactly):**
- `publish(mutate func(*View))`: lock mu → clone current View (shallow-copy maps/slice) → mutate → `current.Store` → unlock. Design §Publishing View updates.
- `BuildInitial`: `latest.Store(lastCommitted)`, then scan: `ChunkArtifactKeys()` filtered `StateFrozen` → `Cold` flags; `AllTxHashIndexKeys()` filtered frozen (or `FrozenTxHashIndex` per window) → open `.idx` readers via `layout.TxHashIndexFilePath(cov)`; `ReadyHotChunkKeys()` → for every ready chunk **except** ones that will be served by the live write handle (the resume chunk is published separately via `HotOpened`), open `OpenReadView`; `EarliestLedger()` → `Earliest`; `Floor = retention.FloorAt(geometry.LastCompleteChunkAt(lastCommitted))`.
- `TickCompleted` (rescan): recompute Cold/TxIdx/Floor from a fresh catalog scan. Hot map handling: keep existing handles; for a hot chunk that vanished from the catalog (discarded), remove it from the View **then close the handle** (rocksdb `Store.Close` is lifecycle-guarded — an in-flight read on an old View gets `stores.ErrStoreClosed`, memory-safe). For a superseded `.idx` coverage, swap in the new reader but **never close the old one** — streamhash Close unmaps and a racing read would fault. `// POC: superseded .idx readers leak (bounded by coverage swaps per run); reaper at productionization.`
- `ChunkClosed(c)`: `OpenReadView` the chunk, publish it into `Hot[c]` replacing the (now-closed) write handle entry. If the open fails, log + remove the entry (freeze will cover it after the next tick).
- `HotOpened(c, db)`: publish `Hot[c] = db`.
- `LedgerCommitted(seq)`: `latest.Store(seq)` — write-side order note: called only after `hotService.Ingest` returned, matching design "latest advances last".
- `ResolveLedgers/ResolveEvents`: cold flag wins → open the per-request cold reader (`ledger.OpenColdReader(layout.LedgerPackPath(c))` / `eventstore.OpenColdReader(c, layout.EventsBucketDir(c), eventstore.ColdReaderOptions{})`); else hot handle facade with no-op Close; else `ErrUnavailable`. `// POC: no LRU — cold readers are opened per request and closed by the caller.`

- [ ] **Step 1: Write failing tests** in `registry_test.go`. Build a real temp catalog (see `fullhistory/e2e_test.go:504` `e2eReadCatalog` and lifecycle tests for fixture patterns; `fhtest.RetentionFor` at `fhtest/fhtest.go:25`). Cover: (a) `Admit` returns seeded latest + initial View with frozen chunk in `Cold` and ready chunk in `Hot`; (b) admission order — `LedgerCommitted` then `Admit` observes the new latest with the old View until a publish (assert View pointer identity across `LedgerCommitted`); (c) `ResolveLedgers` prefers cold when both flags exist; returns `ErrUnavailable` for an unknown chunk; (d) `TickCompleted` after a catalog freeze-flip moves the chunk hot→cold and a discarded hot chunk's handle is closed (subsequent read on the stale View returns `stores.ErrStoreClosed`, not a crash); (e) nil-Registry hooks don't panic.
- [ ] **Step 2: Run, confirm failure** (package doesn't exist yet).
- [ ] **Step 3: Implement `registry.go`** per the semantics block above. Keep it one file; target ≤400 lines.
- [ ] **Step 4: Run package tests + `go vet ./...`**. Expected: PASS.
- [ ] **Step 5: Commit** `fullhistory: serve registry/View with catalog-rescan publishing (query POC)`.

---

### Task 3: `db.LedgerReader` adapter

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/serve/ledger_reader.go`
- Create: `cmd/stellar-rpc/internal/fullhistory/serve/ledger_reader_test.go`

**Interfaces:**
- Consumes: `Registry.Admit`, `View.ResolveLedgers`, `geometry.Layout`.
- Produces: `func NewLedgerReader(reg *Registry, layout geometry.Layout) db.LedgerReader` (from `db/ledger.go:25`). Handlers reuse it verbatim: `methods.NewGetLedgersHandler(lr, maxLimit, defaultLimit, nil /* POC: no datastore fallback */, logger)` and `methods.NewGetTransactionsHandler(logger, lr, maxLimit, defaultLimit, passphrase)`.

**Method semantics:**
- `NewTx(ctx)` → a `readTx` that calls `Admit()` **once** and pins `(latest, view)` for its lifetime (this IS the design's admission; `Done()` is a no-op release). All four used methods hang off it.
- `GetLedgerRange(ctx)`: `first = max(view.Floor.FirstLedger(), view.Earliest)`, `last = latest`. Fill the `ledgerbucketwindow.LedgerRange` sequence+closeTime fields by fetching+decoding those two ledgers through resolution (`xdr.LedgerCloseMetaView` gives cheap header access). If `latest == 0` (nothing ingested) return the same not-found error shape v1 uses for an empty range.
- `readTx.GetLedger(ctx, seq)`: bounds-check against `[first, latest]` (below floor → `(zero, false, nil)` not-found, matching v1 semantics); resolve `chunk.IDFromLedger(seq)`, `Get`, unmarshal to `xdr.LedgerCloseMeta`, close the chunk handle.
- `readTx.BatchGetLedgers(ctx, start, end)`: clamp `end` to `latest`; **reject** (error, matching v1's out-of-range error from `get_ledgers.go`) if `start < first` — read how `getLedgers` surfaces range errors and match it. Iterate chunk-by-chunk ascending using `Iterate(start, end)` per chunk, **copying** each borrowed `Entry.Bytes`, building `db.LedgerMetadataChunk{Header, Lcm}` (header via `LedgerCloseMetaView`). Close each chunk handle before moving on.
- Top-level `GetLedger`/`GetLatestLedgerSequence`: implement via a one-shot internal tx (cheap; getTransaction's helper path uses `GetLedgerRange` on the reader itself — check `get_transaction.go:53` and implement what's called).
- `StreamAllLedgers`, `GetLedgerCountInRange`, `StreamLedgerRange`: `return errors.New("not supported by full-history POC read path")` — none of the four handlers call them. `// POC:` comment each.

- [ ] **Step 1: Write failing tests.** Fixture: a Registry over a temp catalog with one frozen (cold) chunk and one live hot chunk holding real `fhtest.ZeroTxLCMBytes` ledgers (write the cold pack with the same writer backfill uses — find the `WriteColdChunk`/pack-writer used by `backfill/process.go` and reuse it, or freeze via the backfill helpers the e2e test uses). Cover: `BatchGetLedgers` spanning the cold→hot seam returns the full contiguous run with correct headers; below-floor start errors; end clamps to latest; `GetLedgerRange` endpoints and close times; `GetLedger` miss below floor is not-found not error.
- [ ] **Step 2: Run, confirm failure.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Integration check:** also add one test that mounts `methods.NewGetLedgersHandler(NewLedgerReader(...), 200, 50, nil, logger)` via `jrpc2` direct handler invocation (see `methods/get_ledgers_test.go` for how v1 tests call handlers without HTTP) and asserts a real `protocol.GetLedgersResponse` across the seam. Run package tests. Expected: PASS.
- [ ] **Step 5: Commit** `fullhistory: db.LedgerReader adapter over serve views (query POC)`.

---

### Task 4: `db.TransactionReader` adapter

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/serve/tx_reader.go`
- Create: `cmd/stellar-rpc/internal/fullhistory/serve/tx_reader_test.go`

**Interfaces:**
- Consumes: `Registry.Admit`, `View.Hot` (txhash facades), `View.TxIdx`, `View.ResolveLedgers`, `txhash.NewTxReader`/`TxReader.GetTransaction` (`read_assembly.go:37/50`), `db.ParseTransaction`.
- Produces: `func NewTransactionReader(reg *Registry, layout geometry.Layout, passphrase string) db.TransactionReader` (`db/transaction.go:51`, one method). Handler reuse: `methods.NewGetTransactionHandler(logger, txr, ledgerReader)`.

**Semantics:** Per request: `Admit()` once; assemble `hot []txhash.HashIndex` from every `View.Hot` handle's `Txhash()` facade (**newest chunk first** — design probes hot first, definitive) and `cold []txhash.HashIndex` from `View.TxIdx` readers (**newest window first**); `LedgerSource` = adapter over `View.ResolveLedgers` that opens/closes per call and **skips candidates below the floor** by returning not-found for `seq < max(floor.FirstLedger(), earliest)` (design: R2 for by-hash lookups). Call `txhash.NewTxReader(hot, cold, src, passphrase).GetTransaction(hash)`; `found=false` → `db.ErrNoTransaction`; convert the returned `ingest.LedgerTransactionView` into `db.Transaction` — read `db.ParseTransaction` (`db/transaction.go`) and the `db.Transaction` struct (`:29`) and produce the same fields (result/envelope/meta XDR bytes, events, ledger info with close time).

- [ ] **Step 1: Write failing tests.** Fixture needs tx-bearing ledgers: reuse the e2e tx builders (`e2e_test.go:49-50, 253` — keypair-signed envelopes hashed with the network passphrase; put shared helpers in `serve/fixtures_test.go` if Task 3 didn't already). Cover: hot hit (tx in live hot chunk); cold hit through a window `.idx` (build one with the backfill txindex path or `fhtest.ReadColdBin` fixtures — see how `e2e_test.go:386` opens a real `.idx`); miss → `db.ErrNoTransaction`; a candidate ledger below floor is skipped (probe returns not-found rather than serving pruned data).
- [ ] **Step 2: Run, confirm failure.**
- [ ] **Step 3: Implement** (~150 lines).
- [ ] **Step 4: Run package tests.** Expected: PASS.
- [ ] **Step 5: Commit** `fullhistory: db.TransactionReader adapter with hot/cold probe chain (query POC)`.

---

### Task 5: getEvents v2 handler

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/serve/events_handler.go`
- Create: `cmd/stellar-rpc/internal/fullhistory/serve/events_handler_test.go`

**Interfaces:**
- Consumes: `Registry.Admit`, `View.ResolveEvents`, `eventstore.Query` (`query.go:179`), `eventstore.Filter` (`query.go:48`), `QueryOptions`/`EventIDRangeForLedgers` (`query.go:147/119`), protocol types `protocol.GetEventsRequest/Response/EventInfo/Cursor`, format helpers used by `methods/get_events.go:274-311` (xdr2json / base64).
- Produces: `func NewGetEventsHandler(logger *log.Entry, reg *Registry, layout geometry.Layout, maxLimit, defaultLimit uint) jrpc2.Handler` — request/response wire-compatible with v1 `getEvents`.

**Why not reuse v1:** `db.EventReader` is SQL-shaped (column-indexed topic filters, ScanFunction streaming); adapting it would reimplement a coordinator inside the adapter. New handler is smaller.

**Semantics:**
1. Validate request exactly as v1 does (read `methods/get_events.go:120-232` and mirror: limit bounds, start/cursor exclusivity, filter count limits, out-of-range errors carrying the available range).
2. Admit once per page. Establish the scan window: from request `startLedger` or cursor+1 (cursor semantics identical to v1: `protocol.Cursor{Ledger,Tx,Op,Event}`, resume exclusive), capped at `LedgerScanLimit = 10000` ledgers (same constant semantics as `get_events.go:24`) and clamped to `[floor, latest]`.
3. Translate protocol filters → `[]eventstore.Filter`: **first read `eventstore/query.go` filter-matching semantics and its tests** to map contract-ID sets and topic wildcards faithfully; if one protocol filter ORs N contract IDs, expand to N `eventstore.Filter`s. Event-type filtering: check whether `eventstore.Filter` carries type; if not, post-filter decoded payloads by type exactly as v1 does.
4. Per chunk overlapping the window (ascending): `ResolveEvents` → `eventstore.Query(ctx, chunk.Reader(), filters, QueryOptions{MaxEvents: remaining, Range: EventIDRangeForLedgers(offsets, lo, hi)})`; map each `events.Payload` to `protocol.EventInfo` (cursor id, ledgerSeq, close time, txHash, format encoding via the same helpers v1 uses). Close each chunk handle.
5. Page termination + response cursor/`latestLedger`/`oldestLedger` fields exactly as v1 (`get_events.go:219-260`).
6. `// POC:` note that descending order is supported only if `QueryOptions.Descending` composes trivially; otherwise reject descending with a clear error and note it (v1 getEvents is ascending-only — verify against `protocol.GetEventsRequest` before deciding).

- [ ] **Step 1: Write failing tests.** Fixture: hot + cold chunks with contract events (build on Task 2-4 fixtures; cold events artifacts via the same backfill freeze used in Task 3's fixture). Cover: filter match across the cold→hot seam in one page; pagination cursor resumes exclusively; limit truncation sets cursor; empty-match page still advances cursor; out-of-range start errors with available range; base64 and json formats.
- [ ] **Step 2: Run, confirm failure.**
- [ ] **Step 3: Implement** (~250 lines target).
- [ ] **Step 4: Run package tests.** Expected: PASS.
- [ ] **Step 5: Commit** `fullhistory: getEvents handler over eventstore.Query (query POC)`.

---

### Task 6: JSON-RPC server, config, metrics

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/serve/server.go`
- Create: `cmd/stellar-rpc/internal/fullhistory/serve/server_test.go`
- Modify: `cmd/stellar-rpc/internal/fullhistory/config/config.go` (+ its validate/test files)

**Interfaces:**
- Consumes: Tasks 3-5 constructors, `methods.NewGetLedgersHandler/NewGetTransactionsHandler/NewGetTransactionHandler`, `fullhistory.HealthSignal` (`health.go:14`), prometheus registry.
- Produces:

```go
// config (strict TOML — every key must be declared):
type ServeConfig struct {
    Endpoint         string `toml:"endpoint"`           // "" = serving disabled (default)
    MaxGetLedgersLimit ... // copy v1 defaults: getLedgers 200/50, getTransactions 200/50, getEvents 10000/100
}
// added to Config as `Serve ServeConfig `toml:"serve"``, with WithDefaults filling limits.

package serve
type ServerParams struct {
    Registry   *Registry
    Layout     geometry.Layout
    Passphrase string
    Health     HealthLike // interface{ Ready() bool; LastCommitClose() (time.Time, bool) } — or import the fullhistory type if no cycle; serve must NOT import the fullhistory root package (it imports serve). Define the tiny interface locally.
    Metrics    *prometheus.Registry
    Logger     *supportlog.Entry
    Cfg        config.ServeConfig
}
func StartServer(ctx context.Context, p ServerParams) (addr net.Addr, shutdown func(), err error)
```

**Semantics:**
- Mimic `internal/jsonrpc.go` minimally: `handler.Map{ "getLedgers": ..., "getTransactions": ..., "getTransaction": ..., "getEvents": ... }` → `jhttp.NewBridge` → `http.Server` on `cfg.Endpoint` (a `net.Listen` first so port `:0` works and `addr` is real). No CORS/queue-limiters. `// POC: no backlog/duration limiters — benchmark client controls load.`
- Latency middleware: wrap each method handler recording into `prometheus.HistogramVec{Name: "fullhistory_rpc_request_duration_seconds", Labels: endpoint, status}` registered on `p.Metrics` — this is THE benchmark instrument; use buckets spanning 100µs–10s.
- Mux: `/` → bridge; `/metrics` → `promhttp.HandlerFor(p.Metrics, ...)` (satisfies daemon.go:178 TODO); `/health` + `/ready` → from `p.Health` (200/503 JSON, same shape as the #839-era probes if any exist — keep trivial).
- `StartServer` returns promptly; server goroutine exits on ctx cancel via `http.Server.Shutdown`.

- [ ] **Step 1: Write failing tests**: config round-trip (TOML with `[serve]` parses; unknown key still fails strict; defaults fill); `StartServer` on `127.0.0.1:0` → real JSON-RPC `getLedgers` POST over HTTP against a small fixture registry returns a valid response; `/metrics` contains `fullhistory_rpc_request_duration_seconds` after one request; `/ready` flips with a fake HealthLike.
- [ ] **Step 2: Run, confirm failure.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run package + config tests.** Expected: PASS.
- [ ] **Step 5: Commit** `fullhistory: [serve] JSON-RPC server with per-endpoint latency metrics (query POC)`.

---

### Task 7: Daemon wiring + end-to-end test

**Files:**
- Modify: `cmd/stellar-rpc/internal/fullhistory/hotloop.go` (hooks), `startup.go` (BuildInitial + HotOpened for the resume DB), `lifecycle/lifecycle.go` (post-tick observer in `Config`), `daemon.go` (build Registry when `[serve].endpoint != ""`, wire real ServeReads, pass hooks, plumb passphrase from the peek at daemon.go:413-459 — error out if serving enabled and passphrase empty)
- Create: `cmd/stellar-rpc/internal/fullhistory/serve_e2e_test.go`

**Interfaces:**
- Consumes: everything above.
- Produces: hook seams —
  - `ingestionLoopConfig` gains `Serving servingHooks` where `type servingHooks interface { LedgerCommitted(uint32); HotOpened(chunk.ID, *hotchunk.DB); ChunkClosed(chunk.ID) }` defined in package `fullhistory`; `*serve.Registry` satisfies it; nil-safe nop default so every existing test compiles unchanged.
  - `lifecycle.Config` gains `TickObserver interface{ TickCompleted() }` (defined in `lifecycle`), called after each successful `runLifecycle`.
  - `daemonOptions` gains a test seam `serveAddr func(net.Addr)` invoked with the bound address.

**Hook call sites (exact):**
- `hotloop.go:157` (after `metrics.LastCommitted(seq)`): `cfg.Serving.LedgerCommitted(seq)`.
- `hotloop.go:173` (immediately **after** `hotDB.Close()`, before `openHotDBForChunk`): `cfg.Serving.ChunkClosed(closed)`. Do not reorder the fence.
- `hotloop.go:180` (after `hotDB = nextDB`): `cfg.Serving.HotOpened(next, nextDB)` — before `Boundary.Publish` so the View serves the new chunk before `latest` can enter it (design ordering rule).
- `startup.go`: after the resume hot DB opens and before `cfg.ServeReads(ctx)` at `:131`: `registry.HotOpened(resumeChunk, hotDB)` then `registry.BuildInitial(lastCommitted)`; `ServeReads` closure (built in daemon.go) calls `serve.StartServer` and returns.
- `lifecycle.go` end of successful tick: `cfg.TickObserver.TickCompleted()`.

- [ ] **Step 1: Write the failing e2e test** `TestServeE2E_QueryHotAndCold` modeled on `e2e_test.go:209` `runDaemonInBackground`: config with `[serve] endpoint="127.0.0.1:0"`, tx+event-bearing synthetic ledgers spanning ≥1 full chunk + a partial live chunk (so freeze produces cold artifacts while hot keeps ingesting; reuse `chunksPerTxhashIndex: 1` trick from e2e for a quick `.idx`). Then over real HTTP JSON-RPC: (a) `getLedgers` page spanning the cold→hot seam; (b) `getTransaction` for a hash in the cold chunk AND one in the live chunk; (c) `getTransactions` across the seam with cursor continuation; (d) `getEvents` filter matching events in both tiers; (e) negative: `getLedgers` below the floor errors with available range; unknown tx hash → NotFound. Poll `/ready` before querying. Keep the whole test `-short`-safe within the existing budget patterns (see e2e budget notes — generous `require.Eventually` budgets, no hardcoded tight deadlines).
- [ ] **Step 2: Run, confirm failure** (hooks/wiring absent).
- [ ] **Step 3: Implement the wiring.** Keep diffs to `hotloop.go`/`startup.go`/`lifecycle.go` minimal (hook lines only); the invariant comments there are load-bearing — extend, don't rewrite.
- [ ] **Step 4: Run the FULL fullhistory tree** `-short -count=1` with CGO flags (gate on exit code). Expected: all green including the new e2e.
- [ ] **Step 5: Commit** `fullhistory: wire serve registry into daemon + query e2e (query POC)`.

---

### Task 8: Benchmark harness + runbook

**Files:**
- Create: `tools/fhbench/main.go` (single-file CLI, module already covers it via root go.mod — verify; if tools/ isn't in the module, put it at `cmd/stellar-rpc/internal/fullhistory/fhbench/` as `package main` is not allowed under internal for external use — then prefer `tools/`)
- Create: `tools/fhbench/README.md`

**Interfaces:**
- Consumes: only the HTTP JSON-RPC API + `/metrics`. Plain `net/http` + `encoding/json`; no new deps.
- Produces: CLI:
  ```
  fhbench --url http://host:port --endpoint getLedgers|getTransaction|getTransactions|getEvents|all \
          --tier hot|cold|both --concurrency 8 --duration 60s --limit 50
  ```

**Semantics:**
- Discovery phase: one `getLedgers` (limit 1) to learn `oldestLedger`/`latestLedger`. Define tiers: **hot** = last `chunk-size/2` ledgers before latest; **cold** = the oldest full chunk ≥ oldest. Sample tx hashes per tier via `getTransactions` pages before timing starts.
- Load phase: N workers, each a closed loop issuing randomized requests within the tier (random start ledger for range endpoints, random sampled hash for getTransaction, rotating event filters incl. no-filter). Record per-request wall time + status.
- Report: per endpoint × tier — count, RPS, p50/p90/p99/max (sorted-slice quantiles, no deps), error count. Plain text table.
- README: how to run the daemon with `[serve]`, how to run fhbench, and the PromQL for ingestion numbers (`rate(fullhistory_streaming_last_committed_ledger[1m])` style — verify metric names from `observability.go`) + per-endpoint latency histograms.

- [ ] **Step 1: Write a failing smoke test** `tools/fhbench/main_test.go`: run the sampler + report path against an `httptest.Server` stubbing the RPC responses; assert quantile math on a known latency set.
- [ ] **Step 2: Run, confirm failure. Step 3: Implement. Step 4: Test + `go vet`. Expected: PASS.**
- [ ] **Step 5: Commit** `fullhistory: fhbench query benchmark harness (query POC)`.

---

### Task 9: Final review gate

- [ ] Opus-review findings from Tasks 1-8 all addressed (each task got an opus review before merge-forward).
- [ ] Full tree: `go build ./...`, `go vet ./...`, fullhistory tree `-short` green, `golangci-lint` clean on the diff (repo lint: build v2.11.3 with go1.26, `--new-from-rev origin/feature/full-history`, gofumpt).
- [ ] Fable (max-effort) end-to-end review of the cumulative diff vs this plan + design doc: admission ordering, fence preservation, borrowed-bytes copying, error taxonomy, POC markers present.
- [ ] Fix anything CONFIRMED; re-run tests; final commit.

## Self-review notes

- Spec coverage: registry/View/admission (T2), hot ownership + boundary continuity (T1/T2/T7), all four endpoints (T3-T5), R2 floor gating (T3 ranges, T4 by-hash skip, T5 clamp), server+metrics for benchmarking (T6), ingestion+query benchmark numbers (T8), reaper consciously replaced by close-guard/leak rules (T2, marked). Open design questions resolved for POC: datastore fallback dropped, chunk-aligned floor, sequential probe, no cache sizing.
- Known intentional gaps (all `// POC:` marked): no reaper grace period (rocksdb close-guard + leaked idx mmaps instead), no request duration limits, no CORS, descending getEvents per T5 note, unused LedgerReader methods stubbed.
