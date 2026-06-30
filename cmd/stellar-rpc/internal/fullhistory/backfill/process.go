// Package backfill holds the per-chunk "build" primitives the daemon drives:
// materialize a chunk's cold artifacts and rebuild the rolling tx-hash index,
// each through the catalog's one-write protocol. See
// design-docs/full-history-streaming-workflow.md ("Backfill").
package backfill

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// ErrBackendCoverageTimeout is returned when the bulk backend's tip never reaches the chunk in time.
var ErrBackendCoverageTimeout = errors.New("backend never covered chunk within deadline")

// ErrHotVolumeLost is the case-4 fatal: a "ready" hot:chunk key whose DB is
// missing/unopenable — unrecoverable loss (the hot DB is the sole copy of the
// chunk's recently-ingested ledgers), never auto-healed. Detected LAZILY on the
// open that needs the DB. A sentinel so the daemon's top loop owns the fatal.
var ErrHotVolumeLost = errors.New("hot storage lost; run surgical recovery (case 4)")

// HotProbe answers backfillSource's hot branch: is the hot tier COMPLETE for this
// chunk (decision (a): maxCommittedSeq >= last ledger), and if so hand back a
// LedgerStream over its ledgers CF so the just-closed chunk freezes without a
// refetch. Injected: production wires NewRocksHotProbe, tests pass a fake.
type HotProbe interface {
	// OpenHotChunk borrows the chunk's hot DB for a freeze. ok==false / error under
	// a "ready" key (absent or unopenable dir) is case-4 loss.
	OpenHotChunk(chunkID chunk.ID) (HotChunk, bool, error)
}

// HotChunk is one chunk's opened hot tier: the single DB's completeness gate plus
// an LCM source over the ledgers CF.
type HotChunk interface {
	// MaxCommittedSeq is the single authoritative last-committed ledger (decision (a));
	// ok=false on an empty DB (so the chunk cannot be complete).
	MaxCommittedSeq() (seq uint32, ok bool, err error)
	// Source yields the chunk's LCMs from the ledgers CF as a LedgerStream the cold
	// writer (WriteColdChunk) drains.
	Source() ledgerbackend.LedgerStream
	// Close releases the shared hot DB.
	Close() error
}

// ProcessConfig is what processChunk/backfillSource need for a freeze pass.
type ProcessConfig struct {
	Catalog *catalog.Catalog
	Logger  *supportlog.Entry
	Sink    ingest.MetricSink

	// HotProbe opens the hot tier for backfillSource's hot branch. Nil (cold-only
	// backfill or a hot-less test) skips that branch — pack/backend sources only.
	HotProbe HotProbe

	// Backend is the bulk source for a chunk with no local copy (BSB now, captive
	// core later — see the Backend interface). It carries its own frontier Tip, so
	// the coverage wait needs no separate waiter. May be nil for frontfill-only;
	// backfillSource errors if a chunk then needs it.
	Backend Backend
}

func (cfg ProcessConfig) validate() error {
	if cfg.Catalog == nil {
		return errors.New("ProcessConfig.Catalog is nil")
	}
	if cfg.Logger == nil {
		return errors.New("ProcessConfig.Logger is nil")
	}
	return nil
}

// ingestConfigFor maps an artifact set to ingest.Config. It lives here, not on
// catalog.ArtifactSet, so catalog needn't import ingest (the #824 split invariant).
func ingestConfigFor(s catalog.ArtifactSet) ingest.Config {
	return ingest.Config{
		Ledgers: s.Has(geometry.KindLedgers),
		Txhash:  s.Has(geometry.KindTxHash),
		Events:  s.Has(geometry.KindEvents),
	}
}

// processChunk materializes the requested cold artifacts for ONE chunk via the
// one-write protocol (rule 1): a "frozen" kind self-skips; the rest are marked
// "freezing", written, fsynced, then flipped "frozen". It resolves the chunk's
// ledger source, then drives the source-blind ingest.WriteColdChunk over its raw
// ledger iterator.
func processChunk(ctx context.Context, chunkID chunk.ID, artifacts catalog.ArtifactSet, cfg ProcessConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cat := cfg.Catalog
	layout := cat.Layout()

	for _, kind := range artifacts.Kinds() {
		state, err := cat.State(chunkID, kind)
		if err != nil {
			return fmt.Errorf("read state chunk %s kind %s: %w", chunkID, kind, err)
		}
		if state == geometry.StateFrozen {
			artifacts = artifacts.Remove(kind)
		}
	}
	if artifacts.Empty() {
		return nil
	}
	kinds := artifacts.Kinds()

	// Choose the source before marking "freezing": a source error (a missing pack
	// or a coverage timeout) must not leave "freezing" debris for a chunk we then
	// refuse to produce. closeSource releases any opened hot DB after the pass.
	src, closeSource, err := backfillSource(ctx, chunkID, artifacts, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = closeSource() }()

	// The one-write protocol, straight-line (see catalog_protocol.go header). The
	// // one-write: labels keep the four steps greppable without a wrapper.

	// one-write:mark — every requested kind to "freezing" before any I/O.
	if merr := cat.MarkChunkFreezing(chunkID, kinds...); merr != nil {
		return fmt.Errorf("mark freezing chunk %s %s: %w", chunkID, artifacts, merr)
	}

	// one-write:create — materialize this chunk's cold artifacts from the resolved
	// source's raw ledger iterator. WriteColdChunk is source-blind.
	dirs := ingest.ColdDirs{
		Ledgers: layout.LedgersRoot(),
		Txhash:  layout.TxHashRawRoot(),
		Events:  layout.EventsRoot(),
	}
	raw := src.RawLedgers(ctx, ledgerbackend.BoundedRange(chunkID.FirstLedger(), chunkID.LastLedger()))
	if rerr := ingest.WriteColdChunk(
		ctx, cfg.Logger, chunkID, raw, dirs, cfg.Sink, ingestConfigFor(artifacts),
	); rerr != nil {
		return fmt.Errorf("cold ingest chunk %s %s: %w", chunkID, artifacts, rerr)
	}

	// one-write:barrier — fsync each file and its dirents before the keys flip.
	// BarrierNewFile always fsyncs the grandparent, so a bucket dir this freeze
	// created is made durable too.
	for _, kind := range kinds {
		for _, path := range layout.ArtifactPaths(chunkID, kind) {
			if berr := geometry.BarrierNewFile(path); berr != nil {
				return fmt.Errorf("fsync barrier %s: %w", path, berr)
			}
		}
	}

	// one-write:flip — every requested kind to "frozen" (the only state readers trust).
	if ferr := cat.FlipChunkFrozen(chunkID, kinds...); ferr != nil {
		return fmt.Errorf("flip frozen chunk %s %s: %w", chunkID, artifacts, ferr)
	}
	return nil
}

// backfillSource picks a chunk's ledger source (+ a closer for an opened hot DB;
// no-op otherwise), in preference order:
//  1. a ready, COMPLETE hot tier (decision (a): maxCommittedSeq >= last ledger) —
//     only if a HotProbe is wired; incomplete-but-present is staleness that falls
//     through (re-derivation recovers it), LOSS is fatal (ErrHotVolumeLost);
//  2. the frozen local .pack, unless ledgers is itself requested (circular);
//  3. the bulk backend, gated by a bounded waitForCoverage on its Tip.
func backfillSource(
	ctx context.Context, chunkID chunk.ID, artifacts catalog.ArtifactSet, cfg ProcessConfig,
) (ledgerbackend.LedgerStream, func() error, error) {
	noClose := func() error { return nil }
	cat := cfg.Catalog
	layout := cat.Layout()

	// (1) Hot branch: only when a HotProbe is wired and the hot key is "ready". A
	// "transient" key (mid-op or recovery-demoted) is not a read source.
	if cfg.HotProbe != nil {
		hotState, err := cat.HotState(chunkID)
		if err != nil {
			return nil, noClose, fmt.Errorf("read hot state chunk %s: %w", chunkID, err)
		}
		if hotState == geometry.HotReady {
			src, closer, used, herr := tryHotSource(chunkID, cfg)
			if herr != nil {
				return nil, noClose, herr // case-4 loss is fatal
			}
			if used {
				cfg.Logger.Debugf("backfillSource: chunk %s from complete hot tier", chunkID)
				return src, closer, nil
			}
			// Present but incomplete: legitimate staleness — fall through.
			cfg.Logger.Debugf("backfillSource: chunk %s hot tier present but incomplete; falling through", chunkID)
		}
	}

	// (2) Frozen local .pack, only when ledgers is not requested (producing ledgers
	// from the pack we'd write would be circular).
	ledgersState, err := cat.State(chunkID, geometry.KindLedgers)
	if err != nil {
		return nil, noClose, fmt.Errorf("read ledgers state chunk %s: %w", chunkID, err)
	}
	if ledgersState == geometry.StateFrozen && !artifacts.Has(geometry.KindLedgers) {
		packPath := layout.LedgerPackPath(chunkID)
		if _, serr := os.Stat(packPath); serr == nil {
			cfg.Logger.Debugf("backfillSource: chunk %s re-derived from frozen .pack", chunkID)
			return ledger.NewPackStream(packPath), noClose, nil
		}
		// frozen ⇒ file exists; a missing pack is a bug, not a re-download trigger.
		return nil, noClose, fmt.Errorf(
			"chunk %s ledgers is %q but pack file is missing at %s",
			chunkID, geometry.StateFrozen, packPath)
	}

	// (3) Bulk backend — the only source for a chunk with no local copy.
	if cfg.Backend == nil {
		return nil, noClose, fmt.Errorf(
			"chunk %s has no local copy and no bulk backend is configured", chunkID)
	}
	// The coverage wait is mandatory before reading the bulk backend: the freeze
	// must block until the backend's tip covers the chunk (design: backfillSource
	// always waits for coverage). cfg.Backend's own Tip drives it.
	if werr := waitForCoverage(
		ctx, cfg.Backend, chunkID.LastLedger(), defaultCoveragePollInterval, defaultCoverageTimeout,
	); werr != nil {
		return nil, noClose, werr
	}
	cfg.Logger.Debugf("backfillSource: chunk %s from bulk backend", chunkID)
	return cfg.Backend, noClose, nil
}

// tryHotSource handles the hot branch under a "ready" key: used=true when present
// AND complete; used=false when present-but-incomplete (staleness, caller falls
// through); err only for case-4 LOSS (ErrHotVolumeLost), detected lazily on the open.
func tryHotSource(chunkID chunk.ID, cfg ProcessConfig) (ledgerbackend.LedgerStream, func() error, bool, error) {
	hot, ok, err := cfg.HotProbe.OpenHotChunk(chunkID)
	if err != nil {
		// "ready" key but the DB cannot be opened — hot-volume loss.
		return nil, nil, false, fmt.Errorf("%w: chunk %s: %w", ErrHotVolumeLost, chunkID, err)
	}
	if !ok {
		// "ready" key but the dir is absent — hot-volume loss.
		return nil, nil, false, fmt.Errorf("%w: chunk %s: hot directory absent", ErrHotVolumeLost, chunkID)
	}
	maxSeq, present, merr := hot.MaxCommittedSeq()
	if merr != nil {
		_ = hot.Close()
		// A read error against an opened DB is loss, not staleness: the DB opened
		// but cannot answer its own progress.
		return nil, nil, false, fmt.Errorf("%w: chunk %s: max committed seq: %w", ErrHotVolumeLost, chunkID, merr)
	}
	// decision (a): complete iff the single DB's maxCommittedSeq reaches the chunk's
	// last ledger. An empty DB (present==false) cannot be complete.
	if present && maxSeq >= chunkID.LastLedger() {
		return hot.Source(), hot.Close, true, nil
	}
	_ = hot.Close()
	return nil, nil, false, nil
}
