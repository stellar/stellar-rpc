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

// ProcessConfig is what processChunk/backfillSource need for a freeze pass.
type ProcessConfig struct {
	Catalog *catalog.Catalog
	Logger  *supportlog.Entry
	Sink    ingest.MetricSink

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
	// refuse to produce.
	src, err := backfillSource(ctx, chunkID, artifacts, cfg)
	if err != nil {
		return err
	}

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

// backfillSource picks a chunk's ledger source as a bare ledgerbackend.LedgerStream:
//  1. the frozen local .pack, unless ledgers is itself requested (circular);
//  2. the bulk backend (cfg.Backend), gated by a bounded waitForCoverage on its Tip.
//
// The local pack needs no coverage wait (it is complete) and no close (its reader
// is opened and closed per RawLedgers call). The bulk backend is caller-owned (the
// daemon Closes it), so backfillSource returns no closer either.
func backfillSource(
	ctx context.Context, chunkID chunk.ID, artifacts catalog.ArtifactSet, cfg ProcessConfig,
) (ledgerbackend.LedgerStream, error) {
	cat := cfg.Catalog
	layout := cat.Layout()

	ledgersState, err := cat.State(chunkID, geometry.KindLedgers)
	if err != nil {
		return nil, fmt.Errorf("read ledgers state chunk %s: %w", chunkID, err)
	}
	if ledgersState == geometry.StateFrozen && !artifacts.Has(geometry.KindLedgers) {
		packPath := layout.LedgerPackPath(chunkID)
		if _, serr := os.Stat(packPath); serr == nil {
			cfg.Logger.Debugf("backfillSource: chunk %s re-derived from frozen .pack", chunkID)
			return ledger.NewPackStream(packPath), nil
		}
		// frozen ⇒ file exists; a missing pack is a bug, not a re-download trigger.
		return nil, fmt.Errorf(
			"chunk %s ledgers is %q but pack file is missing at %s",
			chunkID, geometry.StateFrozen, packPath)
	}

	if cfg.Backend == nil {
		return nil, fmt.Errorf(
			"chunk %s has no local copy and no bulk backend is configured", chunkID)
	}
	// The coverage wait is mandatory before reading the bulk backend: the freeze
	// must block until the backend's tip covers the chunk (design: backfillSource
	// always waits for coverage). cfg.Backend's own Tip drives it.
	if werr := waitForCoverage(
		ctx, cfg.Backend, chunkID.LastLedger(), defaultCoveragePollInterval, defaultCoverageTimeout,
	); werr != nil {
		return nil, werr
	}
	cfg.Logger.Debugf("backfillSource: chunk %s from bulk backend", chunkID)
	return cfg.Backend, nil
}
