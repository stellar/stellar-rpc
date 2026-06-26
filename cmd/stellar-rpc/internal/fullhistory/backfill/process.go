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
	"path/filepath"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ErrBackendCoverageTimeout is returned when the bulk backend's tip never reaches the chunk in time.
var ErrBackendCoverageTimeout = errors.New("backend never covered chunk within deadline")

// BackendWaiter blocks until the bulk backend's tip covers chunkLastLedger, or
// fails with ErrBackendCoverageTimeout.
type BackendWaiter interface {
	WaitForCoverage(ctx context.Context, chunkLastLedger uint32) error
}

// ProcessConfig is what processChunk/backfillSource need for a freeze pass.
type ProcessConfig struct {
	Catalog *catalog.Catalog
	Logger  *supportlog.Entry
	Sink    ingest.MetricSink

	// Backend is the bulk source for a chunk with no local copy (BSB by default).
	// May be nil for frontfill-only; backfillSource errors if a chunk then needs it.
	Backend ingest.ChunkSource

	// Required iff Backend is set.
	BackendWaiter BackendWaiter
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
// "freezing", written, fsynced, then flipped "frozen". It drives RunColdChunk and
// derives no writer of its own.
//
//nolint:cyclop // sequential one-write protocol; splitting the ordered steps fragments the invariant
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

	// Choose the source before marking "freezing": a source error must not leave
	// "freezing" debris for a chunk we then refuse to produce.
	source, closeSource, err := backfillSource(ctx, chunkID, artifacts, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = closeSource() }()

	if err := cat.MarkChunkFreezing(chunkID, kinds...); err != nil {
		return fmt.Errorf("mark freezing chunk %s %s: %w", chunkID, artifacts, err)
	}

	// Snapshot which bucket dirs exist before the write, so the barrier below
	// fsyncs the grandparent dirent only for dirs THIS freeze created. (Buckets
	// are created in arbitrary order, so a chunk-id heuristic can't tell.)
	bucketExisted := make(map[string]bool)
	for _, kind := range kinds {
		for _, path := range layout.ArtifactPaths(chunkID, kind) {
			dir := filepath.Dir(path)
			if _, seen := bucketExisted[dir]; seen {
				continue
			}
			_, statErr := os.Stat(dir)
			bucketExisted[dir] = statErr == nil
		}
	}

	dirs := ingest.ColdDirs{
		Ledgers: layout.LedgersRoot(),
		Txhash:  layout.TxHashRawRoot(),
		Events:  layout.EventsRoot(),
	}
	rerr := ingest.RunColdChunk(ctx, cfg.Logger, source, dirs, chunkID, cfg.Sink, ingestConfigFor(artifacts))
	if rerr != nil {
		return fmt.Errorf("cold ingest chunk %s %s: %w", chunkID, artifacts, rerr)
	}

	// Durability barrier before the keys flip: fsync each file + its dirents
	// (grandparent too for a bucket dir this freeze created).
	for _, kind := range kinds {
		for _, path := range layout.ArtifactPaths(chunkID, kind) {
			newParent := !bucketExisted[filepath.Dir(path)]
			if berr := geometry.BarrierNewFile(path, newParent); berr != nil {
				return fmt.Errorf("fsync barrier %s: %w", path, berr)
			}
		}
	}

	if ferr := cat.FlipChunkFrozen(chunkID, kinds...); ferr != nil {
		return fmt.Errorf("flip frozen chunk %s %s: %w", chunkID, artifacts, ferr)
	}
	return nil
}

// backfillSource picks a chunk's ledger source (and a closer, a no-op today):
//  1. the frozen local .pack, unless ledgers is itself requested (circular);
//  2. the bulk backend, gated by a bounded WaitForCoverage.
func backfillSource(
	ctx context.Context, chunkID chunk.ID, artifacts catalog.ArtifactSet, cfg ProcessConfig,
) (ingest.ChunkSource, func() error, error) {
	noClose := func() error { return nil }
	cat := cfg.Catalog
	layout := cat.Layout()

	ledgersState, err := cat.State(chunkID, geometry.KindLedgers)
	if err != nil {
		return nil, noClose, fmt.Errorf("read ledgers state chunk %s: %w", chunkID, err)
	}
	if ledgersState == geometry.StateFrozen && !artifacts.Has(geometry.KindLedgers) {
		if _, serr := os.Stat(layout.LedgerPackPath(chunkID)); serr == nil {
			cfg.Logger.Debugf("backfillSource: chunk %s re-derived from frozen .pack", chunkID)
			return ingest.NewPackSource(layout.LedgersRoot()), noClose, nil
		}
		// frozen ⇒ file exists; a missing pack is a bug, not a re-download trigger.
		return nil, noClose, fmt.Errorf(
			"chunk %s ledgers is %q but pack file is missing at %s",
			chunkID, geometry.StateFrozen, layout.LedgerPackPath(chunkID))
	}

	if cfg.Backend == nil {
		return nil, noClose, fmt.Errorf(
			"chunk %s has no local copy and no bulk backend is configured", chunkID)
	}
	// The coverage wait is mandatory before reading the bulk backend (design:
	// backfillSource always calls waitForBackendCoverage), so a missing waiter is
	// a config error, not a silently-skipped gate.
	if cfg.BackendWaiter == nil {
		return nil, noClose, fmt.Errorf(
			"chunk %s needs the bulk backend but no BackendWaiter is configured", chunkID)
	}
	if werr := cfg.BackendWaiter.WaitForCoverage(ctx, chunkID.LastLedger()); werr != nil {
		return nil, noClose, werr
	}
	cfg.Logger.Debugf("backfillSource: chunk %s from bulk backend", chunkID)
	return cfg.Backend, noClose, nil
}

// pollingBackendWaiter polls Tip until it reaches chunkLastLedger, ctx is
// canceled, or Timeout elapses.
type pollingBackendWaiter struct {
	Tip      func(ctx context.Context) (uint32, error)
	Interval time.Duration
	Timeout  time.Duration
}

// NewPollingBackendWaiter returns a BackendWaiter polling tip on interval up to
// timeout; a zero interval/timeout falls back to sane defaults.
func NewPollingBackendWaiter(
	tip func(ctx context.Context) (uint32, error), interval, timeout time.Duration,
) BackendWaiter {
	if interval <= 0 {
		interval = time.Second
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	return &pollingBackendWaiter{Tip: tip, Interval: interval, Timeout: timeout}
}

func (w *pollingBackendWaiter) WaitForCoverage(ctx context.Context, chunkLastLedger uint32) error {
	deadline := time.Now().Add(w.Timeout)
	for {
		// Bound the tip query by the overall deadline — the parent ctx may carry no
		// deadline, so a hung backend would otherwise outlast Timeout.
		tipCtx, cancel := context.WithDeadline(ctx, deadline)
		tip, err := w.Tip(tipCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("backend tip query: %w", err)
		}
		if tip >= chunkLastLedger {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: tip %d < needed %d after %s",
				ErrBackendCoverageTimeout, tip, chunkLastLedger, w.Timeout)
		}
		timer := time.NewTimer(w.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
