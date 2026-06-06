# Full-history ingest core (`pkg/ingest`) — design

**Date:** 2026-06-05
**Branch:** `fh-stores-integration` (was `fh-ingest-core`)
**Status:** Approved design; in implementation

---

## AMENDMENT (2026-06-05) — two pivots found during execution

Execution surfaced facts that override decisions below. Read this first.

**1. Decode path is PARSED, not XDR-views.** Open PR #756 (commit `2739b51c`)
*deleted* `internal/events/ingest_view.go` / `LCMToPayloadsFromRaw` — the
zero-copy view extractor the "views only" decision relied on. The surviving
events API is parsed-only: `events.LCMToPayloads(passphrase string, lcm
xdr.LedgerCloseMeta) ([]events.Payload, error)`. So:
- `Ledger` carries `{Seq, Raw, LCM *xdr.LedgerCloseMeta}`; the driver decodes
  `raw → LCM` once per ledger when events or txhash are enabled, shared
  read-only (the bench's `newParsedLedger` / `needsLCM` path — the path the
  original plan said to drop; now it's the *only* path, views dropped instead).
- The events ingester takes a **network passphrase** (new required input on
  `Config`/driver) for `LCMToPayloads`.
- Events hot: `eventstore.OpenHotStore(dir, chunkID, logger)` +
  `IngestLedgerEvents(seq, payloads)`. Events cold: `eventstore.NewColdWriter`
  + per-payload `Append` + offsets + `Finish(offsets)` + `WriteColdIndex`.

**2. Base is `fh-stores-integration`, not `#756`.** The three store deps lived
on un-integrated branches (feature/full-history = ledger-cold + OLD events;
#756 = new events + NO ledger-cold; txhash-cold = rpc-hack only). Built a
throwaway integration branch `fh-stores-integration` = feature/full-history +
merge #756 + txhash-cold (streamhash import migrated `tamirms/streamhash` →
`github.com/stellar/streamhash`). All store deps + events test green on it.
Upstream redoes the integration when #756 merges to feature/full-history.

**3. txhash cold scope.** The cold txhash *write* path is bench logic: the cold
ingester accumulates entries and writes a sorted per-chunk `.bin`; a SEPARATE
streamhash-MPHF build turns `.bin` files into the queryable cold index that
`txhash/cold_reader.go` reads. This slice's `txhashCold.Finalize` produces the
`.bin` (faithful to the bench); the MPHF index-build step is a follow-up, so
the cold-txhash round-trip is not asserted end-to-end here (ledger + events
cold round-trips are).

Everything below is unchanged EXCEPT: wherever it says "views + parallel only" /
`newViewLedger` / drop-LCM, substitute the parsed path above.

---

## Context

`rpc-hack` is the kitchen-sink branch for full-history work, built on top of
`feature/full-history` (+~16.6k LOC). Reviewable slices are being peeled off it
one at a time onto `feature/full-history`, each as its own PR. Merged/open
slices so far:

| Slice | PR | Status | Paths |
|-------|-----|--------|-------|
| Ledger cold store reader/writer + hot store | #739, #755 | merged | `stores/ledger/` |
| packfile writer buffer pool | #754 | merged | `internal/packfile/` |
| Events index + eventstore core (no query engine) | #756 | open | `internal/events/`, `stores/eventstore/` (minus `query.go`) |

This spec covers the next slice: the **productionized ingest core** — the code
that streams ledgers and writes ledgers, transactions, and events into both the
hot and cold stores. On `rpc-hack` this logic lives as `package main` in
`cmd/stellar-rpc/scripts/bench-fullhistory/`, fused to benchmark scaffolding.
This slice lifts the production-relevant core into a real package and discards
the benchmarking code.

## Goal

A reusable ingest-core package usable for both **frontfill** (live network tip)
and **backfill** (historical ranges), feeding both **hot** and **cold** stores.

Out of scope for this slice (deferred to later slices):
- A `stellar-rpc` ingest subcommand / fill orchestration (range selection,
  resume, live-tip handoff).
- The eventstore query engine (`query.go` + postfilter) — separate slice on #756.

## Decisions (from brainstorming)

1. **Deliverable:** reusable ingest-core package only. No subcommand yet.
2. **Sequencing:** stack on #756; extract the cold-txhash store as a
   prerequisite slice first (see Topology). The txhash-cold-store extraction is
   **in scope for this effort** — done as its own branch/PR, but as the first
   implementation step here, not handed off separately.
3. **Measurement:** none. All benchmarking-only code (collectors, samples,
   driver metrics, CSV/percentile reporting, cpu/mem profiling, `flag` parsing)
   is refactored away, not ported. (This supersedes an earlier "observer hook"
   decision — with the bench harness out of scope, no metrics plumbing is
   needed in the core.)
4. **Modes:** single path only — zero-copy XDR views + parallel fan-out. The
   `parsed` decode mode and the `serial` fan-out mode are dropped.
5. **Structure:** one self-contained package, two drivers (hot + cold),
   ingesters unexported. (Approach A.)

## Architecture & package layout

New package `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/`, at the top of
the store dependency stack:

```
internal/fullhistory/pkg/ingest/
  ledger.go      Ledger value {Seq, Raw, LCM} + newViewLedger constructor
  ingester.go    Ingester (Ingest+Close) and ColdIngester (adds Finalize) interfaces
  source.go      LedgerStream sources: packStream (cold-packfile replay) + BSB (GCS); OpenChunkStream factory
  driver.go      RunHot (continuous single-stream fan-out) + RunCold (chunk-range, parallel chunk workers, Finalize)
  ledgers.go     ledgersHot / ledgersCold ingesters  -> stores/ledger
  events.go      eventsHot / eventsCold ingesters    -> stores/eventstore (zero-copy view path)
  txhash.go      txhashHot / txhashCold ingesters    -> stores/txhash
```

**Dependency direction (no cycles):**
`ingest` → `stores/{ledger,eventstore,txhash}` → `chunk`, `packfile`, `rocksdb`;
plus `internal/events` (index types) and `go-stellar-sdk/ingest/ledgerbackend`
(the `LedgerStream` interface). Nothing depends back on `ingest`.

**Consumers:** a future backfill/frontfill subcommand (later slice) imports this
package. The existing `scripts/bench-fullhistory` harness is **not** rewired in
this slice — its parsed/serial comparisons cannot route through the single-path
package, so it is left as-is (or pruned) and is explicitly out of scope.

## Public API

Small surface: a config, the source factory, and two driver functions.
Ingester implementations are unexported (consumers drive via the tier driver).

```go
// Config selects which data types to ingest. Decode=views, fan-out=parallel are fixed.
type Config struct {
    Ledgers, Txhash, Events bool   // at least one required
}

// Source selects the backend kind (pack replay or GCS BSB).
type Source string
const (
    SourcePack Source = "pack"
    SourceBSB  Source = "bsb"
)

// SourceOpts carries pack/BSB tuning (cold dir or bucket path + BSB buffer/workers/retry).
type SourceOpts struct {
    ColdDir    string        // required for SourcePack
    BucketPath string        // required for SourceBSB
    BufferSize uint          // BSB prefetch depth
    NumWorkers uint          // BSB download workers
    RetryLimit uint
    RetryWait  time.Duration
}

// OpenChunkStream returns an independent LedgerStream for one chunk
// (concurrent chunk workers each get their own — BSB's cursor can't be shared).
func OpenChunkStream(source Source, opts SourceOpts, chunkID chunk.ID) (ledgerbackend.LedgerStream, error)

// RunHot: continuous fan-out over a single stream into the hot stores.
func RunHot(ctx context.Context, logger *supportlog.Entry,
    stream ledgerbackend.LedgerStream, chunkID chunk.ID, hotDir string, cfg Config) error

// RunCold: orchestrates a chunk range, chunkWorkers chunks concurrently,
// each worker opening its own backend; Finalize lands each chunk's cold artifact.
func RunCold(ctx context.Context, logger *supportlog.Entry,
    src Source, opts SourceOpts, coldDir string,
    startChunk chunk.ID, numChunks, chunkWorkers int, cfg Config) error
```

Internally unchanged from `rpc-hack` minus metrics: the `Ingester` /
`ColdIngester` interfaces, the typed `[]Ingester` / `[]ColdIngester` slices
(preserving the compile-time hot/cold split), and per-ledger `errgroup` fan-out.
The bench's `buildHotDeps`/`hotDeps` struct existed only to inject collectors
into a testable loop; with metrics gone, the natural seam is the
`ledgerbackend.LedgerStream` interface itself (a fake stream feeds the driver),
so the drivers become plain exported functions.

## Data flow

**Hot (continuous / frontfill-shaped).** `RunHot` pulls
`stream.RawLedgers(BoundedRange(first,last))`; for each raw ledger it builds a
`Ledger` via the zero-copy view path (`newViewLedger` — `Raw` set, `LCM` nil),
then fans out to the enabled hot ingesters concurrently via `errgroup`. The
borrowed `raw` slice is valid only until the next yield, so `g.Wait()` gates the
next pull and each ingester copies what it retains. Hot ingesters have no
finalize — writes land as they go.

**Cold (chunk-batched / backfill-shaped).** `RunCold` spawns up to
`chunkWorkers` goroutines (`errgroup` + `SetLimit`) over
`[startChunk, startChunk+numChunks)`. Each worker (`runOneChunkCold`) opens its
**own** backend (a pack reader or an independent BSB session — BSB's sequential
cursor can't be shared), opens per-type cold ingesters, iterates the chunk's
ledgers fanning out per-ledger, then on the success path calls `Finalize` on
each `ColdIngester` to durably land that chunk's cold artifact (ledger `Commit`,
events `WriteColdIndex`, txhash sort + write `.bin`).

**Same seam, both fill directions.** Backfill = a bounded range over historical
chunks (BSB from GCS, or local pack replay). Frontfill = the same driver fed an
advancing range at the network tip. Fill *orchestration* (range selection,
resume, live-tip handoff) is a later slice; this package is the reusable engine.
`RunHot` as ported here is bounded/single-chunk (`BoundedRange`); continuous
live-tip ingest belongs to the deferred orchestration slice — confirmed in
scope of *that* later slice, not this one.

## Error handling & lifecycle

- **Fail-fast on ingest:** any `Ingest` error aborts the loop and propagates; in
  parallel fan-out the `errgroup` context cancels siblings.
- **Close always deferred, idempotent:** for cold ingesters, if `Finalize` never
  ran (failure path), the packfile writer's `Close` removes the partial file —
  a failed chunk leaves no half-written cold artifact. `Close` errors are joined
  via `errors.Join`.
- **Finalize explicit, never deferred, error-checked:** a `Finalize` error means
  the chunk's cold artifact did not durably land and must be re-ingested.
- **Context cancellation** (SIGINT/SIGTERM) honored at each loop step and
  between chunks.
- **Store-boundary sentinels** (`stores.ErrCorrupt` / `ErrOutOfRange` / …)
  propagate as-is; the ingest package adds no new error taxonomy.

## Testing

- **Driver tests** drive `RunHot` / `RunCold` against a **fake `LedgerStream`**
  (in-memory ledgers) into a `t.TempDir()` store root, asserting all enabled
  types are written and the cold path produces a finalized, re-readable
  artifact. The `LedgerStream` interface is the seam — no metrics injection.
- **Round-trip test:** ingest a small synthetic chunk → reopen each store's
  reader → assert ledgers / events / tx-hashes read back. The real regression
  guard.
- **Faithfulness:** this slice *refactors* (drops metrics), so equivalence to
  `rpc-hack` is asserted by the round-trip tests + `go build` / `go vet` on the
  new package — not by a byte-identical `git diff` (unlike #755).
- Existing store-level tests already cover the write primitives; ingest tests
  cover orchestration.

## Git topology & branch plan

The ingest core depends on both #756 (events) and a cold-txhash store that has
not been extracted yet. The stack:

```
origin/feature/full-history
  ├─ origin/events-eventstore-core        (#756, OPEN)
  └─ txhash-cold-store                    (NEW prereq slice; byte-identical extraction like #755)

fh-ingest-core   ← branch off events-eventstore-core, then merge in txhash-cold-store
                   (so it compiles against BOTH); PR base = events-eventstore-core,
                   GitHub retargets to feature/full-history as parents merge.
```

Two branches result from this work:

1. **`txhash-cold-store`** — off `feature/full-history`; byte-identical port of
   `stores/txhash/{cold_format,cold_format_test,cold_reader,cold_reader_test}.go`
   plus the `hot_store.go` deltas from `rpc-hack` (its own small PR, mirrors
   #755). Prerequisite; its own spec/PR.
2. **`fh-ingest-core`** — off `events-eventstore-core`, with `txhash-cold-store`
   merged in; contains the new `pkg/ingest/` package. **This spec.**

## Source-of-truth references on `rpc-hack`

Files to port (minus metrics) from
`cmd/stellar-rpc/scripts/bench-fullhistory/`:

- `ledger.go` → `ingest/ledger.go` (drop `parsed`-mode `newParsedLedger`)
- `ingester.go` → `ingest/ingester.go` (interfaces verbatim)
- `sources.go` → `ingest/source.go` (pack + BSB; export factory)
- `bench_hot_ingest.go` → `ingest/driver.go` `RunHot` (drop flags, collectors, profiling, reporting; keep the loop)
- `bench_cold_ingest.go` → `ingest/driver.go` `RunCold` (drop flags, collectors, profiling, reporting; keep chunk-worker orchestration + `runOneChunkCold`)
- `ingest_ledgers.go` → `ingest/ledgers.go` (drop collector arg + sample recording)
- `ingest_events.go` → `ingest/events.go` (drop collector arg; keep view path only)
- `ingest_txhash.go` → `ingest/txhash.go` (drop collector arg; keep view path only)
