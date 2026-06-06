# Full-history Ingest Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the full-history ingest pipeline from the `rpc-hack` bench harness into a reusable production package `cmd/stellar-rpc/internal/fullhistory/pkg/ingest`, dropping all benchmarking code and keeping a single path (zero-copy XDR views + parallel fan-out).

**Architecture:** One self-contained package with two driver functions (`RunHot` continuous-bounded; `RunCold` chunk-range with per-chunk `Finalize`), fed by a `ledgerbackend.LedgerStream` source (local cold packfile replay or GCS BufferedStorageBackend). Three data-type ingesters (ledgers, events, txhash) each with a hot and a cold implementation, satisfying `Ingester` / `ColdIngester` interfaces. The package sits atop the already-extracted `stores/{ledger,eventstore,txhash}`.

**Tech Stack:** Go, `github.com/stellar/go-stellar-sdk/ingest/ledgerbackend`, `golang.org/x/sync/errgroup`, RocksDB-backed hot stores + packfile-backed cold stores, `tamirms/streamhash` (txhash cold index).

**Source of truth:** the bench harness on branch `rpc-hack` at `cmd/stellar-rpc/scripts/bench-fullhistory/`. Throughout, retrieve a source body with:
`git show rpc-hack:cmd/stellar-rpc/scripts/bench-fullhistory/<file>`

**Branch topology:**
```
origin/feature/full-history
  ├─ origin/events-eventstore-core   (#756, OPEN)         ← fh-ingest-core is based here
  └─ txhash-cold-store               (Phase 0, NEW)       ← merged into fh-ingest-core in Phase 1
fh-ingest-core  (already created off events-eventstore-core; holds the design spec)
```

---

## Phase 0 — `txhash-cold-store` prerequisite slice

A standalone #755-style byte-identical extraction of the cold txhash store onto `feature/full-history`. It has **no external importer** on `feature/full-history`, and the new files import only `streamhash` + `pkg/stores` sentinels (both present), so the checkout compiles as-is.

### Task 0.1: Extract cold txhash store onto its own branch

**Files (all copied verbatim from `rpc-hack`):**
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/cold_format.go`
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/cold_format_test.go`
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/cold_reader.go`
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/cold_reader_test.go`
- Modify: `cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/hot_store.go` (rename `NewHotStore`→`OpenHotStore`, `Get`→`Lookup`, + concurrency docs)
- Modify: `cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/hot_store_test.go` (uses `Lookup`/`OpenHotStore`)

- [ ] **Step 1: Create the branch off `feature/full-history`**

```bash
cd /Users/simonchow/stellar-rpc
git fetch origin feature/full-history
git checkout -b txhash-cold-store origin/feature/full-history
```

- [ ] **Step 2: Pull the txhash store paths verbatim from `rpc-hack`**

```bash
git checkout rpc-hack -- cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/
git status --short
```
Expected: the 4 new files + 2 modified files staged.

- [ ] **Step 3: Verify the tree matches `rpc-hack` byte-for-byte (the #755 invariant)**

```bash
git diff --quiet rpc-hack -- cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/ && echo "IDENTICAL" || echo "DIVERGED"
```
Expected: `IDENTICAL`

- [ ] **Step 4: Build the package**

```bash
go build ./cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/
```
Expected: no output (success). If it fails on a missing symbol, the failing reference is the scope boundary — stop and report; do not pull unrelated files.

- [ ] **Step 5: Test the package**

```bash
go test ./cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/
```
Expected: `ok  ...stores/txhash`

- [ ] **Step 6: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/
git commit -m "txhash: add cold store (packfile + streamhash point lookup); rename hot Get->Lookup, NewHotStore->OpenHotStore"
```

- [ ] **Step 7: Push and (optionally) open the PR**

```bash
git push -u origin txhash-cold-store
gh pr create --repo stellar/stellar-rpc --base feature/full-history --head txhash-cold-store \
  --title "txhash: add cold store + hot Lookup/OpenHotStore rename" \
  --body "Part of the series extracting reviewable components from rpc-hack. Byte-identical to the corresponding paths on rpc-hack. Cold txhash store (cold_format/cold_reader) + the hot-store API rename (Get->Lookup, NewHotStore->OpenHotStore). Prerequisite for the ingest-core slice."
```

---

## Phase 1 — Base setup on `fh-ingest-core`

### Task 1.1: Merge the txhash prereq into `fh-ingest-core` and confirm all store deps compile

**Files:** none created; merge only.

- [ ] **Step 1: Switch to `fh-ingest-core` and merge the prereq**

```bash
git checkout fh-ingest-core
git merge --no-ff txhash-cold-store -m "merge txhash-cold-store prereq into fh-ingest-core"
```
Expected: clean merge (the txhash paths don't overlap with the #756 events/eventstore paths).

- [ ] **Step 2: Verify all three store dependencies build on this base**

```bash
go build ./cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger/ \
         ./cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore/ \
         ./cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash/ \
         ./cmd/stellar-rpc/internal/events/
```
Expected: no output (all succeed). This confirms the ingest package has every symbol it imports.

- [ ] **Step 3: Create the package directory + doc file**

Create `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/doc.go`:
```go
// Package ingest streams ledgers from a backend (local cold packfile replay
// or GCS BufferedStorageBackend) and writes ledgers, transactions, and events
// into the hot and cold full-history stores. RunHot drives a single bounded
// stream into the hot stores; RunCold orchestrates a chunk range into the cold
// stores, finalizing each chunk's artifact. Decode is always zero-copy XDR
// views; per-ledger fan-out across enabled data types is always parallel.
package ingest
```

- [ ] **Step 4: Commit the skeleton**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/doc.go
git commit -m "ingest: add package skeleton"
```

---

## Phase 2 — Port the ingest core (no metrics; views + parallel only)

Each port task: create the file from the named `rpc-hack` source with the listed deltas, then `go build` the package. The package will not fully build until the interfaces + value type exist (Tasks 2.1–2.2), so build-verification starts at Task 2.3.

### Task 2.1: `ledger.go` — the per-ledger value (view path only)

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ledger.go`
- Source: `git show rpc-hack:.../bench-fullhistory/ledger.go`

- [ ] **Step 1: Create `ledger.go`**

Port the `Ledger` struct and `newViewLedger` **only**. Drop `newParsedLedger` and the `LCM` field's parsed-mode role — the view path never sets `LCM`.

```go
package ingest

// Ledger is the per-iteration value handed to every ingester. The view
// path sets Raw (borrowed wire-format LedgerCloseMeta bytes, valid only
// until the next stream yield); ingesters copy what they retain.
type Ledger struct {
	Seq uint32
	Raw []byte
}

// newViewLedger builds a Ledger for the zero-copy XDR-view extraction path.
func newViewLedger(seq uint32, raw []byte) Ledger {
	return Ledger{Seq: seq, Raw: raw}
}
```

> Note: the bench's `Ledger` carried an `LCM *xdr.LedgerCloseMeta` for parsed mode. Confirm none of the view-path ingesters (Tasks 2.4–2.6) read `l.LCM`; they decode from `l.Raw` via XDR views. If a ported ingester references `l.LCM`, that line belongs to the dropped parsed branch — remove it.

- [ ] **Step 2: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ledger.go
git commit -m "ingest: add Ledger value (view path)"
```

### Task 2.2: `ingester.go` — the interfaces

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ingester.go`
- Source: `git show rpc-hack:.../bench-fullhistory/ingester.go`

- [ ] **Step 1: Create `ingester.go` verbatim from source**

The source `ingester.go` has no metrics coupling — port it as-is (only the package clause stays `package ingest`). Final content:

```go
package ingest

import (
	"context"
	"io"
)

// Ingester ingests one data type into one storage tier. Hot ingesters
// implement only this interface; cold ingesters implement ColdIngester.
// The driver holds one typed slice per tier — hot uses []Ingester, cold
// uses []ColdIngester — so there is no type assertion in the per-ledger loop.
//
// Concurrency: Ingest on a single instance is invoked serially across
// ledgers; distinct instances may be invoked concurrently within the same
// ledger (parallel fan-out). Each instance owns its state and reads the
// read-only Ledger value.
//
// Lifecycle: constructors open the underlying store/writer; Ingest is called
// once per ledger; Close is ALWAYS deferred, idempotent, and (for cold tiers
// whose Finalize never ran) removes any partial file.
type Ingester interface {
	Ingest(ctx context.Context, l Ledger) error
	io.Closer
}

// ColdIngester adds the cold-tier commit step. Finalize is called explicitly
// by the driver on the success path with an error check; never deferred. A
// Finalize error means the chunk's cold artifact did not durably land and must
// be re-ingested. Hot ingesters intentionally do NOT implement ColdIngester —
// enforced at compile time by the per-driver slice type.
type ColdIngester interface {
	Ingester
	Finalize(ctx context.Context) error
}
```

- [ ] **Step 2: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ingester.go
git commit -m "ingest: add Ingester / ColdIngester interfaces"
```

### Task 2.3: `source.go` — ledger stream sources + exported factory

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/source.go`
- Source: `git show rpc-hack:.../bench-fullhistory/sources.go`

- [ ] **Step 1: Create `source.go`**

Port `packStream` (with its `RawLedgers` method) verbatim. Replace the bench's `const (sourcePack = "pack"; sourceBSB = "bsb")` and the unexported `openChunkStream(source, coldDir, bucketPath string, opts BSBOpts, chunkID)` with the **exported** API from the spec. Keep `BSBOpts` but expose it as part of `SourceOpts`.

New exported surface (replaces the bench's flag-string plumbing):
```go
type Source string

const (
	SourcePack Source = "pack"
	SourceBSB  Source = "bsb"
)

// SourceOpts carries the pack/BSB tuning previously passed as separate args.
type SourceOpts struct {
	ColdDir    string        // required for SourcePack
	BucketPath string        // required for SourceBSB
	BufferSize uint          // BSB prefetch depth
	NumWorkers uint          // BSB download workers
	RetryLimit uint
	RetryWait  time.Duration
}

// OpenChunkStream returns an INDEPENDENT LedgerStream for one chunk. Concurrent
// chunk workers must each call this (BSB's sequential cursor cannot be shared).
func OpenChunkStream(source Source, opts SourceOpts, chunkID chunk.ID) (ledgerbackend.LedgerStream, error) {
	switch source {
	case SourcePack:
		if opts.ColdDir == "" {
			return nil, errors.New("SourceOpts.ColdDir is required for SourcePack")
		}
		path := packPath(opts.ColdDir, uint32(chunkID))
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("cold pack missing: %s: %w", path, err)
		}
		return &packStream{coldDir: opts.ColdDir, chunkID: chunkID}, nil
	case SourceBSB:
		if opts.BucketPath == "" {
			return nil, errors.New("SourceOpts.BucketPath is required for SourceBSB")
		}
		dsConfig := datastore.DataStoreConfig{
			Type:   "GCS",
			Params: map[string]string{"destination_bucket_path": opts.BucketPath},
		}
		cfg := ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: uint32(opts.BufferSize),
			NumWorkers: uint32(opts.NumWorkers),
			RetryLimit: uint32(opts.RetryLimit),
			RetryWait:  opts.RetryWait,
		}
		return ledgerbackend.NewBufferedStorageStream(cfg, dsConfig, nil), nil
	default:
		return nil, fmt.Errorf("unknown source %q; expected %q|%q", source, SourcePack, SourceBSB)
	}
}
```

> `packPath` is a small helper in the bench harness (`packPath(coldDir string, chunkID uint32) string`). Port it into `source.go` too — retrieve it with `git show rpc-hack:.../bench-fullhistory/ | grep -rn "func packPath"`; it is defined in `bench_cold_ledgers.go`. Keep the `var _ ledgerbackend.LedgerStream = (*packStream)(nil)` assertion.

- [ ] **Step 2: Build the package**

```bash
go build ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/
```
Expected: failure only on the not-yet-created ingesters/drivers referenced elsewhere — but `source.go`, `ledger.go`, `ingester.go` themselves must have no unresolved symbols. If the error mentions `packStream`, `OpenChunkStream`, `SourceOpts`, or `packPath`, fix here; errors about `RunHot`/`ledgersHot` are expected (later tasks).

- [ ] **Step 3: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/source.go
git commit -m "ingest: add LedgerStream sources (pack replay + BSB) and OpenChunkStream"
```

### Task 2.4: `ledgers.go` — ledger ingesters (drop collector)

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ledgers.go`
- Source: `git show rpc-hack:.../bench-fullhistory/ingest_ledgers.go`

- [ ] **Step 1: Create `ledgers.go`**

Port **only** the ingester types `LedgersHot` and `LedgersCold`, the `LedgersColdOpts` struct, and their methods. **Drop** everything above the first ingester (the `ledgerSample`, `LedgerCollector`, and all its methods — they are benchmarking-only). Apply these signature deltas (rename to unexported types, drop the collector arg, drop the sample appends):

```go
// before (bench): func NewLedgersHot(c *LedgerCollector, dir string, logger *supportlog.Entry) (*LedgersHot, error)
// after:          func newLedgersHot(dir string, logger *supportlog.Entry) (*ledgersHot, error)

// before: func NewLedgersCold(c *LedgerCollector, outRoot string, chunkID chunk.ID, opts LedgersColdOpts) (*LedgersCold, error)
// after:  func newLedgersCold(outRoot string, chunkID chunk.ID, opts LedgersColdOpts) (*ledgersCold, error)
```

In each `Ingest` method, remove the `t0 := time.Now()` and the `h.collector.samples = append(...)` / `c.collector.samples = append(...)` lines; keep the actual store write. Remove the `collector *LedgerCollector` struct field. In `LedgersCold.Finalize`, remove the `if len(c.collector.samples) == 0 { ... }` empty-chunk guard that depended on samples — replace with the equivalent state already tracked by the writer (check the body: if the only use of samples was metrics, drop the branch; if it guarded an empty-commit, gate on the writer's own "anything written" state instead). `LedgersColdOpts{Concurrency, BytesPerSync int}` is kept verbatim.

Resulting exported-to-package symbols: `ledgersHot`, `ledgersCold`, `LedgersColdOpts`, `newLedgersHot`, `newLedgersCold`.

- [ ] **Step 2: Build**

```bash
go build ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/
```
Expected: no errors about `ledgers.go` symbols (driver errors still expected). Confirm no `time`/collector imports remain unused.

- [ ] **Step 3: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ledgers.go
git commit -m "ingest: add ledger hot/cold ingesters (no metrics)"
```

### Task 2.5: `events.go` — events ingesters (view path only, drop collector)

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/events.go`
- Source: `git show rpc-hack:.../bench-fullhistory/ingest_events.go`

- [ ] **Step 1: Create `events.go`**

Drop the `eventSample` / `EventsCollector` block (benchmarking-only). Port `EventsHot` and `EventsCold` (+ `EventsColdOpts{Concurrency, BytesPerSync int}`) with deltas:

```go
// before: func NewEventsHot(c *EventsCollector, dir string, chunkID chunk.ID, logger *supportlog.Entry, xdrViews bool) (*EventsHot, error)
// after:  func newEventsHot(dir string, chunkID chunk.ID, logger *supportlog.Entry) (*eventsHot, error)

// before: func NewEventsCold(c *EventsCollector, outRoot string, chunkID chunk.ID, opts EventsColdOpts, xdrViews bool) (*EventsCold, error)
// after:  func newEventsCold(outRoot string, chunkID chunk.ID, opts EventsColdOpts) (*eventsCold, error)
```

Remove the `xdrViews bool` param **and** the parsed-mode branch inside each `Ingest` (the code path taken when `xdrViews == false` / `l.LCM != nil`); keep only the zero-copy view extraction from `l.Raw`. Remove the `collector *EventsCollector` field and every `e.collector.samples = append(...)` site. Watch the `EventsCold.Ingest` body: per the source comment near line 256, `offsets.Append` must still be called even though the sample append is removed — keep the `offsets.Append`, drop only the sample line.

Resulting symbols: `eventsHot`, `eventsCold`, `EventsColdOpts`, `newEventsHot`, `newEventsCold`.

- [ ] **Step 2: Build**

```bash
go build ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/
```
Expected: no `events.go` symbol errors.

- [ ] **Step 3: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/events.go
git commit -m "ingest: add events hot/cold ingesters (view path, no metrics)"
```

### Task 2.6: `txhash.go` — txhash ingesters (view path only, drop collector)

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/txhash.go`
- Source: `git show rpc-hack:.../bench-fullhistory/ingest_txhash.go`

- [ ] **Step 1: Create `txhash.go`**

Drop the `txhashSample` / `TxhashCollector` block. Port `TxhashHot` and `TxhashCold` with deltas:

```go
// before: func NewTxhashHot(c *TxhashCollector, dir string, logger *supportlog.Entry, xdrViews bool) (*TxhashHot, error)
// after:  func newTxhashHot(dir string, logger *supportlog.Entry) (*txhashHot, error)

// before: func NewTxhashCold(c *TxhashCollector, outRoot string, chunkID chunk.ID, xdrViews bool) (*TxhashCold, error)
// after:  func newTxhashCold(outRoot string, chunkID chunk.ID) (*txhashCold, error)
```

Remove the `xdrViews bool` param and the parsed branch; keep the view extraction. Remove the `collector` field and sample appends. `TxhashCold.Close` returns `nil` in the source (entries buffer in memory, flushed in `Finalize`) — keep that. The hot ingester must use the renamed store API from Phase 0: ensure it calls `txhash.OpenHotStore` (not `NewHotStore`); if the bench source still says `NewHotStore`, rename it.

Resulting symbols: `txhashHot`, `txhashCold`, `newTxhashHot`, `newTxhashCold`.

- [ ] **Step 2: Build**

```bash
go build ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/
```
Expected: no `txhash.go` symbol errors.

- [ ] **Step 3: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/txhash.go
git commit -m "ingest: add txhash hot/cold ingesters (view path, no metrics)"
```

### Task 2.7: `driver.go` — `RunHot`

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/driver.go`
- Source: `git show rpc-hack:.../bench-fullhistory/bench_hot_ingest.go` (the `runHot` loop, lines ~200–270)

- [ ] **Step 1: Create `driver.go` with `RunHot` and the shared `buildLedger` helper**

Port the **loop body** of the bench's `runHot`, discarding: flag parsing (`buildHotDeps`), the `hotDeps` struct, `driverMetrics`, cpu/mem profiling, `reportHot`, and all `dm.*` timing appends. Keep: the per-type ingester construction (now via `newLedgersHot`/`newEventsHot`/`newTxhashHot`), the `errgroup` parallel fan-out, the borrowed-`raw` contract (`g.Wait()` gates the next yield), the deferred idempotent `Close` chain with `errors.Join`. Decode is always views, so `buildLedger` becomes `newViewLedger(seq, raw)` directly.

Exported signature:
```go
func RunHot(ctx context.Context, logger *supportlog.Entry,
	stream ledgerbackend.LedgerStream, chunkID chunk.ID, hotDir string, cfg Config) (err error)
```

Add `Config` here (or in a tiny `config.go`):
```go
// Config selects which data types to ingest; at least one must be true.
// Decode is always XDR views; per-ledger fan-out is always parallel.
type Config struct {
	Ledgers, Txhash, Events bool
}

func (c Config) any() bool { return c.Ledgers || c.Txhash || c.Events }
```

`RunHot` skeleton (fill the ingester construction from the enabled `cfg` flags, mirroring the bench's per-type switch but without collectors):
```go
func RunHot(ctx context.Context, logger *supportlog.Entry,
	stream ledgerbackend.LedgerStream, chunkID chunk.ID, hotDir string, cfg Config) (err error) {
	if !cfg.any() {
		return errors.New("ingest: Config selects no data types")
	}
	var ings []Ingester
	if cfg.Ledgers {
		ing, ierr := newLedgersHot(filepath.Join(hotDir, "ledgers"), logger)
		if ierr != nil { return fmt.Errorf("open ledgers hot: %w", ierr) }
		ings = append(ings, ing)
	}
	if cfg.Txhash {
		ing, ierr := newTxhashHot(filepath.Join(hotDir, "txhash"), logger)
		if ierr != nil { closeAll(ings); return fmt.Errorf("open txhash hot: %w", ierr) }
		ings = append(ings, ing)
	}
	if cfg.Events {
		ing, ierr := newEventsHot(filepath.Join(hotDir, "events"), chunkID, logger)
		if ierr != nil { closeAll(ings); return fmt.Errorf("open events hot: %w", ierr) }
		ings = append(ings, ing)
	}
	for _, ing := range ings {
		ing := ing
		defer func() {
			if cerr := ing.Close(); cerr != nil {
				err = errors.Join(err, fmt.Errorf("close: %w", cerr))
			}
		}()
	}

	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	seq := first
	for raw, serr := range stream.RawLedgers(ctx, ledgerbackend.BoundedRange(first, last)) {
		if cerr := ctx.Err(); cerr != nil { return cerr }
		if serr != nil { return fmt.Errorf("RawLedgers(%d): %w", seq, serr) }
		l := newViewLedger(seq, raw)
		g, gctx := errgroup.WithContext(ctx)
		for _, ing := range ings {
			ing := ing
			g.Go(func() error { return ing.Ingest(gctx, l) })
		}
		if werr := g.Wait(); werr != nil { return werr }
		seq++
	}
	return nil
}

// closeAll closes already-opened ingesters on a constructor failure path.
func closeAll(ings []Ingester) { for _, ing := range ings { _ = ing.Close() } }
```

- [ ] **Step 2: Build**

```bash
go build ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/
```
Expected: only `RunCold` references missing now (added next task). If `RunCold` isn't referenced yet, this should fully build.

- [ ] **Step 3: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/driver.go
git commit -m "ingest: add RunHot driver + Config"
```

### Task 2.8: `RunCold` + `runOneChunkCold`

**Files:**
- Modify: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/driver.go`
- Source: `git show rpc-hack:.../bench-fullhistory/bench_cold_ingest.go` (`runCold` lines ~216–250, `runOneChunkCold` ~258–365)

- [ ] **Step 1: Add `RunCold` and `runOneChunkCold`**

Port the cold orchestration, discarding the `coldDeps` struct, `chunkResult` metrics fields, collector merges, and `reportCold`. Keep: the `errgroup` with `g.SetLimit(chunkWorkers)` over `[startChunk, startChunk+numChunks)`, the per-chunk `OpenChunkStream` (each worker its own backend), the per-type cold ingester construction (`newLedgersCold`/`newEventsCold`/`newTxhashCold`), the per-ledger fan-out, and the explicit `Finalize` loop on the success path (error-checked, not deferred) plus the deferred idempotent `Close`.

Exported signature:
```go
func RunCold(ctx context.Context, logger *supportlog.Entry,
	src Source, opts SourceOpts, coldDir string,
	startChunk chunk.ID, numChunks, chunkWorkers int, cfg Config) error
```

`RunCold` validates (`numChunks >= 1`, `chunkWorkers >= 1`, clamp `chunkWorkers` to `numChunks`, `cfg.any()`), then:
```go
g, gctx := errgroup.WithContext(ctx)
g.SetLimit(chunkWorkers)
for i := 0; i < numChunks; i++ {
	chunkID := startChunk + chunk.ID(uint32(i))
	g.Go(func() error {
		if rerr := runOneChunkCold(gctx, logger, src, opts, coldDir, chunkID, cfg); rerr != nil {
			return fmt.Errorf("chunk %d: %w", uint32(chunkID), rerr)
		}
		return nil
	})
}
return g.Wait()
```

`runOneChunkCold(ctx, logger, src, opts, coldDir, chunkID, cfg) error`:
1. `stream, err := OpenChunkStream(src, opts, chunkID)` (each worker its own).
2. Build enabled `[]ColdIngester` via `newLedgersCold(filepath.Join(coldDir,"ledgers"), chunkID, LedgersColdOpts{...})`, `newEventsCold(...)`, `newTxhashCold(...)`. For `Concurrency`/`BytesPerSync`, thread them through `Config` or default to `0` (the writer treats `0` as "serial/default" per the ledger `ColdWriterOptions` contract). **Decision for this slice:** default both to `0` (no new knobs); a later subcommand slice can expose them.
3. Defer idempotent `Close` on each (with `errors.Join`).
4. Iterate `stream.RawLedgers(ctx, BoundedRange(chunkID.FirstLedger(), chunkID.LastLedger()))`, building `newViewLedger` and fanning out via `errgroup` exactly like `RunHot`.
5. On success, loop the cold ingesters calling `Finalize(ctx)`, returning the first error.

- [ ] **Step 2: Build + vet**

```bash
go build ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/
go vet ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/
```
Expected: both clean.

- [ ] **Step 3: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/driver.go
git commit -m "ingest: add RunCold chunk-range driver with per-chunk Finalize"
```

---

## Phase 3 — Tests

### Task 3.1: Fake `LedgerStream` test helper + synthetic ledgers

**Files:**
- Create: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ingest_test.go`

The eventstore/txhash/ledger packages already have synthetic-LCM helpers on `rpc-hack`. Find a reusable one:
`git show rpc-hack:.../bench-fullhistory/ | grep -rln "func.*LedgerCloseMeta" cmd/stellar-rpc/internal/fullhistory/` — reuse the store packages' test fixtures if exported, else build minimal valid `LedgerCloseMeta` bytes here.

- [ ] **Step 1: Write the fake stream**

```go
package ingest

import (
	"context"
	"iter"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// fakeStream replays a fixed set of raw LedgerCloseMeta byte slices, keyed by
// sequence, as a ledgerbackend.LedgerStream — the driver's only input seam.
type fakeStream struct {
	raws map[uint32][]byte // seq -> wire-format LCM bytes
}

var _ ledgerbackend.LedgerStream = (*fakeStream)(nil)

func (f *fakeStream) RawLedgers(_ context.Context, r ledgerbackend.Range) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for seq := r.From(); seq <= r.To(); seq++ {
			if !yield(f.raws[seq], nil) {
				return
			}
		}
	}
}
```

> The synthetic `raws` must be real marshaled `LedgerCloseMeta` bytes (the ingesters decode via XDR views). Add a helper `marshalLCM(t, seq) []byte` that builds a minimal `xdr.LedgerCloseMeta` (V1) with the given ledger seq and zero transactions, then `MarshalBinary`. Retrieve a known-good fixture shape from `git show rpc-hack:cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore/hot_store_test.go` and adapt.

- [ ] **Step 2: Commit the helper**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ingest_test.go
git commit -m "ingest: add fake LedgerStream + synthetic LCM test helpers"
```

### Task 3.2: `RunHot` round-trip test

**Files:**
- Modify: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ingest_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestRunHot_RoundTrip(t *testing.T) {
	const first, last = uint32(1), uint32(8) // chunk 0's range; adjust to chunk.ID(...).FirstLedger()
	chunkID := chunk.ID(0) // pick a chunk whose [First,Last] covers the synthetic seqs
	raws := map[uint32][]byte{}
	for seq := chunkID.FirstLedger(); seq <= chunkID.LastLedger(); seq++ {
		raws[seq] = marshalLCM(t, seq)
	}
	stream := &fakeStream{raws: raws}
	hotDir := t.TempDir()
	logger := supportlog.New()

	err := RunHot(context.Background(), logger, stream, chunkID, hotDir, Config{Ledgers: true, Txhash: true, Events: true})
	require.NoError(t, err)

	// Reopen the ledger hot store and assert each seq reads back.
	ls, err := ledger.OpenHotStore(filepath.Join(hotDir, "ledgers"), logger)
	require.NoError(t, err)
	defer ls.Close()
	for seq := chunkID.FirstLedger(); seq <= chunkID.LastLedger(); seq++ {
		got, gerr := ls.GetLedgerRaw(seq)
		require.NoError(t, gerr, "ledger %d", seq)
		require.NotEmpty(t, got)
	}
}
```

> Adjust the reader calls to the actual hot-store reader API on this branch (`ledger.OpenHotStore` / `GetLedgerRaw` per #755). If the events/txhash hot readers are convenient, assert one read from each too; otherwise the ledger read + `RunHot` returning nil is the minimum guard.

- [ ] **Step 2: Run, expect FAIL (compile or assertion)**

```bash
go test ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ -run TestRunHot_RoundTrip -v
```
Expected: FAIL until `marshalLCM` and reader calls are correct.

- [ ] **Step 3: Fix `marshalLCM` / reader calls until green**

Iterate on the synthetic LCM shape until the ingesters accept it and reads return data.

- [ ] **Step 4: Run, expect PASS**

```bash
go test ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ -run TestRunHot_RoundTrip -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ingest_test.go
git commit -m "ingest: add RunHot round-trip test"
```

### Task 3.3: `RunCold` round-trip test (chunk → finalized artifact → reopen)

**Files:**
- Modify: `cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ingest_test.go`

- [ ] **Step 1: Write the failing test**

`RunCold` opens its own stream via `OpenChunkStream`, so the fake stream can't be injected directly. Drive cold through `SourcePack` against a temp cold dir seeded by a ledger `ColdWriter` (the cleanest in-package fixture), OR add an unexported seam `runColdWithStreamFactory(... factory func(chunk.ID)(LedgerStream,error) ...)` that `RunCold` delegates to, and inject `fakeStream` in the test. **Decision:** add the factory seam — it keeps the test hermetic (no packfile fixture needed).

```go
func TestRunCold_RoundTrip(t *testing.T) {
	chunkID := chunk.ID(0)
	raws := map[uint32][]byte{}
	for seq := chunkID.FirstLedger(); seq <= chunkID.LastLedger(); seq++ {
		raws[seq] = marshalLCM(t, seq)
	}
	factory := func(chunk.ID) (ledgerbackend.LedgerStream, error) { return &fakeStream{raws: raws}, nil }
	coldDir := t.TempDir()
	logger := supportlog.New()

	err := runColdWithStreamFactory(context.Background(), logger, factory, coldDir,
		chunkID, 1 /*numChunks*/, 1 /*chunkWorkers*/, Config{Ledgers: true})
	require.NoError(t, err)

	// Reopen the finalized cold ledger artifact for this chunk.
	cr, err := ledger.OpenColdReader(filepath.Join(coldDir, "ledgers", coldPackName(chunkID)))
	require.NoError(t, err)
	defer cr.Close()
	got, gerr := cr.GetLedgerRaw(chunkID.FirstLedger())
	require.NoError(t, gerr)
	require.NotEmpty(t, got)
}
```

> Refactor Task 2.8 so `RunCold` calls `runColdWithStreamFactory` with the production factory `func(id chunk.ID)(ledgerbackend.LedgerStream,error){ return OpenChunkStream(src, opts, id) }`. `coldPackName`/`packPath` give the chunk's artifact path — reuse the helper from `source.go`.

- [ ] **Step 2: Run, expect FAIL**

```bash
go test ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ -run TestRunCold_RoundTrip -v
```
Expected: FAIL until the factory seam + path helper are wired.

- [ ] **Step 3: Wire the factory seam in `driver.go`; iterate to green**

- [ ] **Step 4: Run, expect PASS**

```bash
go test ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ -run TestRunCold_RoundTrip -v
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/stellar-rpc/internal/fullhistory/pkg/ingest/
git commit -m "ingest: add RunCold round-trip test + stream-factory seam"
```

---

## Phase 4 — Verify & finalize

### Task 4.1: Full build/vet/test, lint, push, open PR

- [ ] **Step 1: Build, vet, and test the package + its deps**

```bash
go build ./cmd/stellar-rpc/internal/fullhistory/... ./cmd/stellar-rpc/internal/events/
go vet ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/
go test ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ -v
```
Expected: all green.

- [ ] **Step 2: Run the repo linter on the new package**

```bash
golangci-lint run ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/... 2>/dev/null || echo "(golangci-lint not installed — skip; CI will run it)"
```
Expected: no findings (or skipped).

- [ ] **Step 3: Confirm no benchmarking artifacts leaked into the package**

```bash
grep -rnE "Collector|Sample|driverMetrics|WriteCSV|pprof|flag\.|bench" cmd/stellar-rpc/internal/fullhistory/pkg/ingest/ && echo "LEAK — remove" || echo "CLEAN"
```
Expected: `CLEAN`

- [ ] **Step 4: Push and open the PR (base = #756 branch; retargets to feature/full-history as parents merge)**

```bash
git push -u origin fh-ingest-core
gh pr create --repo stellar/stellar-rpc --base events-eventstore-core --head fh-ingest-core \
  --title "ingest: productionized full-history ingest core (pkg/ingest)" \
  --body "$(cat <<'EOF'
Part of the series extracting reviewable components from rpc-hack into feature/full-history.

Lifts the ingest pipeline out of the scripts/bench-fullhistory harness into a reusable production package internal/fullhistory/pkg/ingest, dropping all benchmarking code (collectors, samples, driver metrics, CSV/percentile reporting, profiling, flag parsing, and the parsed/serial comparison modes). Single path: zero-copy XDR views + parallel fan-out.

- RunHot: bounded single-stream fan-out into the hot stores.
- RunCold: chunk-range orchestration into the cold stores, per-chunk Finalize.
- Sources: local cold-packfile replay + GCS BufferedStorageBackend, via OpenChunkStream.

Stacked on #756 (events-eventstore-core) and includes the txhash-cold-store prereq (merged). GitHub will retarget to feature/full-history once parents merge.

Design: docs/superpowers/specs/2026-06-05-fullhistory-ingest-core-design.md

## Test plan
- [x] go build ./cmd/stellar-rpc/internal/fullhistory/...
- [x] go test ./cmd/stellar-rpc/internal/fullhistory/pkg/ingest/
- [x] RunHot + RunCold round-trip tests (synthetic LCM -> reopen store -> read back)
EOF
)"
```

- [ ] **Step 5: Final task-list cleanup**

Mark the plan complete; note any follow-up slices (ingest subcommand / live frontfill orchestration; eventstore query engine) as separate future work.

---

## Self-review notes (author)

- **Spec coverage:** Phase 0 = txhash prereq (spec §Topology); Phase 1 = base/merge; Phase 2.1–2.8 = package layout & API (spec §Architecture, §Public API, §Data flow); error/lifecycle (spec §Error handling) is preserved in the ported `Close`/`Finalize`/`errgroup` logic; Phase 3 = tests (spec §Testing); Phase 4 = faithfulness via build+test (spec §Testing). All spec sections map to tasks.
- **Known port judgment calls flagged inline:** the `LedgersCold.Finalize` empty-chunk guard (2.4), the `EventsCold` `offsets.Append` retention (2.5), the `txhash` `OpenHotStore` rename (2.6), and the cold `Concurrency/BytesPerSync` default-to-0 decision (2.8). These require reading the source body — they are the only spots where a blind copy would be wrong.
- **Type consistency:** unexported ingester types `ledgersHot/ledgersCold/eventsHot/eventsCold/txhashHot/txhashCold`; constructors `new*`; exported `Config`, `Source`, `SourceOpts`, `OpenChunkStream`, `RunHot`, `RunCold`; opts structs `LedgersColdOpts`, `EventsColdOpts` kept verbatim. Consistent across tasks.
