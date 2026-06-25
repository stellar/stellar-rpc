package streaming

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// ErrBackendCoverageTimeout is the bounded-wait fatal from backfillSource's bulk
// branch: the configured backend's tip never advanced to cover a
// genuinely-backend-only chunk within the deadline.
var ErrBackendCoverageTimeout = errors.New("streaming: backend never covered chunk within deadline")

// BackendWaiter bounds backfillSource's bulk branch: it blocks until the
// configured backend's tip covers chunkLastLedger, returning a wrapped
// ErrBackendCoverageTimeout if the tip never advances in time. A chunk WITH a
// local copy never reaches here, so a normal all-local restart never waits. An
// interface so the tip query is injectable (production wraps the LedgerBackend;
// tests pass an immediately- or never-covered fake).
type BackendWaiter interface {
	WaitForCoverage(ctx context.Context, chunkLastLedger uint32) error
}

// ProcessConfig is the dependency bundle processChunk/backfillSource read. It is
// the streaming spine's view of everything a freeze pass needs: the catalog
// (key state + path layout), the bulk backend source + its coverage waiter, and
// the metric sink/logger. Construction is the daemon's job; the primitives below
// never reach around it.
type ProcessConfig struct {
	Catalog *catalog.Catalog
	Logger  *supportlog.Entry
	Sink    ingest.MetricSink

	// Backend is the configured bulk LedgerBackend as a ChunkSource (BSB by
	// default — the pack/datastore ChunkSource from ingest). It is the only
	// source for a chunk with no local copy. May be nil in a frontfill
	// deployment that never backfills; backfillSource errors loudly if a chunk
	// actually reaches the bulk branch with no backend configured.
	Backend ingest.ChunkSource

	// BackendWaiter bounds the bulk branch's wait-for-coverage. Required iff
	// Backend is set; ignored otherwise.
	BackendWaiter BackendWaiter
}

func (cfg ProcessConfig) validate() error {
	if cfg.Catalog == nil {
		return errors.New("streaming: ProcessConfig.Catalog is nil")
	}
	if cfg.Logger == nil {
		return errors.New("streaming: ProcessConfig.Logger is nil")
	}
	return nil
}

// ingestConfigFor translates an artifact set into the merged ingest.Config
// (which selects data types by bool). Only the requested kinds are enabled, so
// RunColdChunk drives exactly the cold ingesters processChunk asked for. It
// lives in the streaming package, not on catalog.ArtifactSet, so catalog keeps
// its one-way dependency on geometry alone (the #824 split invariant).
func ingestConfigFor(s catalog.ArtifactSet) ingest.Config {
	return ingest.Config{
		Ledgers: s.Has(geometry.KindLedgers),
		Txhash:  s.Has(geometry.KindTxHash),
		Events:  s.Has(geometry.KindEvents),
	}
}

// processChunk materializes the requested cold artifact kinds (ledgers/.pack,
// events cold segment, txhash/.bin) for ONE chunk in a single streaming pass,
// applying the one-write protocol (rule 1):
//
//   - Per-kind idempotency: a kind already "frozen" self-skips; a
//     "freezing"/"pruning"/absent key re-materializes (the cold ingesters
//     overwrite at the canonical path, so re-materialization is itself idempotent).
//   - Mark-then-write: put each remaining kind "freezing" BEFORE any I/O, write
//     the files (RunColdChunk, from the source backfillSource chose), fsync each
//     file + its dirents (BarrierNewFile), then flip the keys to "frozen".
//
// It re-derives no extractor or writer — RunColdChunk is the same merged cold
// ingester set RunCold uses; processChunk only chooses the source and drives the
// protocol around the freeze.
func processChunk(ctx context.Context, chunkID chunk.ID, artifacts catalog.ArtifactSet, cfg ProcessConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cat := cfg.Catalog
	layout := cat.Layout()

	// rule 1 per-kind idempotency: frozen kinds self-skip.
	for _, kind := range artifacts.Kinds() {
		state, err := cat.State(chunkID, kind)
		if err != nil {
			return fmt.Errorf("streaming: read state chunk %s kind %s: %w", chunkID, kind, err)
		}
		if state == geometry.StateFrozen {
			artifacts = artifacts.Remove(kind)
		}
	}
	if artifacts.Empty() {
		return nil
	}

	// Choose the source BEFORE marking "freezing": backfillSource may fatal or
	// fall through, and we must not leave "freezing" debris for a chunk we then
	// refuse to produce. closeSource releases any opened hot stores after the pass.
	source, closeSource, err := backfillSource(ctx, chunkID, artifacts, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = closeSource() }()

	// Mark-then-write: every requested kind "freezing" BEFORE any I/O.
	if err := cat.MarkChunkFreezing(chunkID, artifacts.Kinds()...); err != nil {
		return fmt.Errorf("streaming: mark freezing chunk %s %s: %w", chunkID, artifacts, err)
	}

	// Record which per-kind bucket dirs exist BEFORE the cold pipeline creates
	// them, so the barrier below fsyncs the grandparent (root) dirent for exactly
	// the dirs THIS freeze created. A chunk-id heuristic can't tell: the executor
	// creates bucket dirs in arbitrary order and a backfill range can start
	// mid-bucket, so the bucket-first chunk may never run — either way the
	// grandparent fsync would be skipped, leaving a "frozen" file unreachable.
	bucketExisted := make(map[string]bool)
	for _, kind := range artifacts.Kinds() {
		for _, path := range layout.ArtifactPaths(chunkID, kind) {
			dir := filepath.Dir(path)
			if _, seen := bucketExisted[dir]; seen {
				continue
			}
			_, statErr := os.Stat(dir)
			bucketExisted[dir] = statErr == nil
		}
	}

	// One streaming pass through the merged cold pipeline; the cold ingesters
	// (re)create files at their canonical paths, overwriting any partial from a
	// crashed "freezing" attempt.
	dirs := ingest.ColdDirs{
		Ledgers: layout.LedgersRoot(),
		Txhash:  layout.TxHashRawRoot(),
		Events:  layout.EventsRoot(),
	}
	if rerr := ingest.RunColdChunk(ctx, cfg.Logger, source, dirs, chunkID, cfg.Sink, ingestConfigFor(artifacts)); rerr != nil {
		return fmt.Errorf("streaming: cold ingest chunk %s %s: %w", chunkID, artifacts, rerr)
	}

	// Durability barrier BEFORE the keys flip: the cold writers fsync file DATA
	// on Finalize, but the protocol also needs the dirents durable. BarrierNewFile
	// fsyncs each file + its parent dirent (+ grandparent when this freeze created
	// the bucket dir, per bucketExisted above) — the two-level barrier (paths.go).
	for _, kind := range artifacts.Kinds() {
		for _, path := range layout.ArtifactPaths(chunkID, kind) {
			newParent := !bucketExisted[filepath.Dir(path)]
			if berr := geometry.BarrierNewFile(path, newParent); berr != nil {
				return fmt.Errorf("streaming: fsync barrier %s: %w", path, berr)
			}
		}
	}

	// Flip every produced kind to "frozen" in one atomic synced batch.
	if ferr := cat.FlipChunkFrozen(chunkID, artifacts.Kinds()...); ferr != nil {
		return fmt.Errorf("streaming: flip frozen chunk %s %s: %w", chunkID, artifacts, ferr)
	}
	return nil
}

// backfillSource implements the cold source-preference order for one chunk. It
// returns the chosen ingest.ChunkSource, a closer (a no-op for the cold pack /
// bulk branches), and an error.
//
// Preference order:
//  1. The frozen local .pack via the ledger cold reader, when ledgers is NOT among
//     the requested outputs (re-derivation without a download).
//  2. The configured bulk backend, gated by a bounded WaitForCoverage.
func backfillSource(
	ctx context.Context, chunkID chunk.ID, artifacts catalog.ArtifactSet, cfg ProcessConfig,
) (ingest.ChunkSource, func() error, error) {
	noClose := func() error { return nil }
	cat := cfg.Catalog
	layout := cat.Layout()

	// (1) Frozen local .pack, only when ledgers is not requested (producing
	// ledgers from the pack we'd write would be circular).
	ledgersState, err := cat.State(chunkID, geometry.KindLedgers)
	if err != nil {
		return nil, noClose, fmt.Errorf("streaming: read ledgers state chunk %s: %w", chunkID, err)
	}
	if ledgersState == geometry.StateFrozen && !artifacts.Has(geometry.KindLedgers) {
		if _, serr := os.Stat(layout.LedgerPackPath(chunkID)); serr == nil {
			cfg.Logger.Debugf("backfillSource: chunk %s re-derived from frozen .pack", chunkID)
			// ingest.NewPackSource composes {coldDir}/{bucket}/{chunk}.pack, which
			// equals LedgerPackPath when coldDir is the ledgers root.
			return ingest.NewPackSource(layout.LedgersRoot()), noClose, nil
		}
		// A "frozen" ledgers key whose pack is gone violates the key invariant
		// (frozen ⇒ file exists); surface it rather than silently downloading.
		return nil, noClose, fmt.Errorf(
			"streaming: chunk %s ledgers is %q but pack file is missing at %s",
			chunkID, geometry.StateFrozen, layout.LedgerPackPath(chunkID))
	}

	// (2) Bulk backend — the only source for a chunk with no local copy.
	if cfg.Backend == nil {
		return nil, noClose, fmt.Errorf(
			"streaming: chunk %s has no local copy and no bulk backend is configured", chunkID)
	}
	if cfg.BackendWaiter != nil {
		if werr := cfg.BackendWaiter.WaitForCoverage(ctx, chunkID.LastLedger()); werr != nil {
			return nil, noClose, werr
		}
	}
	cfg.Logger.Debugf("backfillSource: chunk %s from bulk backend", chunkID)
	return cfg.Backend, noClose, nil
}

// pollingBackendWaiter is the default BackendWaiter: it polls Tip on Interval
// until Tip returns a value >= chunkLastLedger, the ctx is canceled, or Timeout
// elapses (ErrBackendCoverageTimeout). Tip is the bulk backend's current
// network/object-store tip ledger.
type pollingBackendWaiter struct {
	Tip      func(ctx context.Context) (uint32, error)
	Interval time.Duration
	Timeout  time.Duration
}

// NewPollingBackendWaiter returns a BackendWaiter that polls tip on interval up
// to timeout. A zero interval/timeout falls back to sane defaults.
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
		tip, err := w.Tip(ctx)
		if err != nil {
			return fmt.Errorf("streaming: backend tip query: %w", err)
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
