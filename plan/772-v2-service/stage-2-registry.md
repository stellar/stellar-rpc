# Stage 2 — Registry core: View, Reaper, caches, resolve

## Mission

- Implement the query-routing spec's coordination layer as a self-contained package: `cmd/stellar-rpc/internal/fullhistory/registry`.
- Everything in-memory + catalog-scan driven; NO wiring into the daemon yet (stage 3 does that). The package must be fully exercisable against a catalog built by tests/fixtures.

## Read first

- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/00-DECISIONS.md`, `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md`.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/design-docs/query-routing-design.md` — implement THIS, sections: Registry and View, Admission, Publishing View updates, The reaper, View update points, Open-handle management, Chunk resolution.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/catalog/catalog.go` — the scan/read API the startup build uses.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/geometry/` — `keys.go` (State/Kind/TxHashIndexCoverage), `paths.go` (Layout), `retention.go` (Retention.FloorAt), `txhash_index.go`.
- Store open/read APIs:
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk/hotchunk.go` — `DB`, `OpenReadyWrite`, accessors (`Ledgers()/Txhash()/Events()`), the read-only-view-is-ledgers-only caveat.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger/cold_reader.go` — `OpenColdReader(path)`.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore/cold_reader.go` — `OpenColdReader(chunkID, bucketDir, opts)`.
  - `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash/cold_reader.go` — `OpenColdReader(path)`, `MinLedger/MaxLedger`.

## Verified signatures you build on (do not re-derive)

```go
// catalog scans (all ascending, key-driven):
func (c *Catalog) ReadyHotChunkKeys() ([]chunk.ID, error)
func (c *Catalog) ChunkArtifactKeys() ([]ArtifactRef, error)          // {Chunk, Kind, State}
func (c *Catalog) AllTxHashIndexKeys() ([]geometry.TxHashIndexCoverage, error)
func (c *Catalog) FrozenTxHashIndex(w geometry.TxHashIndexID) (geometry.TxHashIndexCoverage, bool, error)
func (c *Catalog) EarliestLedger() (uint32, bool, error)

// geometry:
func (r Retention) FloorAt(frontier int64) chunk.ID   // frontier = last complete chunk (signed, -1 ok)
func (l Layout) LedgerPackPath(c chunk.ID) string
func (l Layout) EventsBucketDir(c chunk.ID) string
func (l Layout) TxHashIndexFilePath(cov geometry.TxHashIndexCoverage) string
```

## Deliverables — package `registry`

### View + Registry + admission

- Follow the spec's shapes (adapt names to Go conventions as needed):

```go
type View struct {
    floor   chunk.ID
    hot     map[chunk.ID]*hotchunk.DB              // registry-owned shared handles
    cold    map[chunk.ID]ColdChunk                 // flags only; readers come from caches
    indexes []IndexCoverage                        // one open txhash.ColdReader per in-retention window
}
type ColdChunk struct{ Ledgers, Events bool }
type IndexCoverage struct {
    Window  geometry.TxHashIndexID
    Lo, Hi  chunk.ID
    Idx     *txhash.ColdReader
}

type Registry struct {
    mu      sync.Mutex                 // serializes publishes
    current atomic.Pointer[View]
    latest  atomic.Uint32
    reaper  *Reaper
    // + ledger/eventstore cold-reader LRU caches, layout, logger
}

// Admission: latest FIRST, then View (spec §Admission — order is load-bearing).
type Snapshot struct { Latest uint32; View *View }
func (r *Registry) Admit() Snapshot
```

- Publish is the spec's clone-mutate-store under `mu`; removed resources go to the reaper.

### Write-side hook surface (stage 3 calls these; define + implement now)

```go
func (r *Registry) PublishHot(c chunk.ID, db *hotchunk.DB)        // after hot:chunk flips "ready"
func (r *Registry) AdvanceLatest(seq uint32)                      // after hotService.Ingest returns
func (r *Registry) PublishFrozen(c chunk.ID, kinds ...geometry.Kind) // after FlipChunkFrozen commit
func (r *Registry) SwapTxIndex(cov geometry.TxHashIndexCoverage) error // after CommitTxHashIndex; opens the new reader, retires the old via reaper
func (r *Registry) UnpublishHot(c chunk.ID, destroy func() error) // discard: drop from View, reaper closes handle then runs destroy after T
func (r *Registry) AdvanceFloor(floor chunk.ID)                   // prune: publish floor; drop below-floor hot/cold/indexes; retire their resources
func (r *Registry) Close()                                        // teardown on run() exit: close everything now (no grace; process is stopping)
```

- R1 enforcement: `PublishFrozen`/`SwapTxIndex` are only called post-commit by stage 3, but assert-log if a caller hands a transient state anyway.
- `AdvanceFloor` also retires cached cold readers for dropped chunks (route closes through the reaper).

### Reaper

```go
type Reaper struct { /* grace T, queue of {destroy func() error, notBefore time.Time}, one goroutine */ }
func NewReaper(grace time.Duration, logger *supportlog.Entry) *Reaper
func (p *Reaper) Schedule(destroy func() error)
func (p *Reaper) Close()   // drain-or-run-now on daemon shutdown; keep it simple
```

- Grace `T` comes from the Registry constructor (default 30s; assembly feeds the real value later).
- No persistent state: crash recovery is the existing lifecycle scans (spec §The reaper).

### Cold reader caches

- Two LRUs keyed by chunk.ID: `*ledger.ColdReader` (cap default 128) and `*eventstore.ColdReader` (cap default 32). Caps are constructor params.
- Acquire path: hit → return; miss → open via Layout path, insert, return. Eviction and retirement NEVER close inline — `reaper.Schedule(reader.Close)`.
- Expose what stage 4/5 need:

```go
func (r *Registry) LedgerReaderFor(v *View, c chunk.ID) (LedgerStoreHandle, error)  // resolve()+cache: cold wins, else hot, else ErrUnavailable
func (r *Registry) EventReaderFor(v *View, c chunk.ID) (eventstore.Reader, error)   // same; hot returns db.Events()
```

- `LedgerStoreHandle` = minimal per-chunk surface the adapters need (`GetLedgerRaw(seq)`, iterate range) satisfied by both `ledger.ColdReader` and `ledger.HotStore`; define the smallest interface that fits both (verify exact method sets in the two files before defining).

### resolve

```go
// spec §Chunk resolution: deterministic, cold wins when both exist.
func (v *View) resolve(c chunk.ID, k geometry.Kind) (tier, error)  // internal; the *ReaderFor funcs are the public face
var ErrUnavailable = errors.New(...)
```

### Startup build

```go
// BuildFromCatalog constructs the initial View before serving opens
// (spec table, last row): "ready" hot keys -> open write handles
// (hotchunk.OpenReadyWrite — warmed events, see D-record: read-only opens are
// ledgers-only), "frozen" chunk artifact keys -> cold flags, frozen coverages ->
// open txhash.ColdReaders, floor from Retention.FloorAt(last complete chunk).
// `latest` seeds from the derived last-committed ledger (caller passes it).
func BuildFromCatalog(cat *catalog.Catalog, ret geometry.Retention, latest uint32, opts Options) (*Registry, error)
```

- Only `"ready"`/`"frozen"` enter the View (R1). Transient keys are invisible.
- The live chunk's handle is special-cased in stage 3 (ingestion already holds it); accept a `map[chunk.ID]*hotchunk.DB` of pre-opened handles in `Options` so stage 3 can hand them in instead of double-opening.

## Non-goals

- No daemon wiring, no hotloop/lifecycle edits (stage 3).
- No `db.*` adapters (stages 4–5), no HTTP (stage 6).

## Acceptance

- `go build ./...`, `go vet ./...` clean; existing fullhistory tests green.
- Registry unit tests (cheap-insurance tier) cover: admission order under concurrent publish (latest-then-View), clone-publish isolation (old snapshot unaffected), resolve preference (cold over hot), floor advance drops + retires, reaper delays destruction until T, cache eviction routes through reaper, BuildFromCatalog over a `fhtest`-built catalog (see `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/fhtest/fhtest.go` and `e2e_test.go` for fixture patterns).
- CHECKPOINT.md updated: exact exported API as built, any deviations from this doc.
