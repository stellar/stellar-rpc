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

// ErrHotVolumeLost is the case-4 fatal: a hot:chunk key is "ready" but its dir
// is missing or unopenable. The hot DB is the SOLE copy of a chunk's
// recently-ingested ledgers, so this is unrecoverable loss — never silently
// healed. Detected LAZILY, on the open that needs the DB (lastCommittedLedger's
// refinement open, openHotTierForChunk's "ready" branch, or backfillSource's hot
// branch), not by an eager all-ready-keys scan. Returned as a sentinel (not a
// process exit) so the daemon's top-level loop owns the fatal-and-surface
// decision and tests can assert it.
var ErrHotVolumeLost = errors.New("streaming: hot storage lost; run surgical recovery (case 4)")

// ErrBackendCoverageTimeout is the bounded-wait fatal from backfillSource's bulk
// branch: the configured backend's tip never advanced to cover a
// genuinely-backend-only chunk within the deadline.
var ErrBackendCoverageTimeout = errors.New("streaming: backend never covered chunk within deadline")

// HotProbe opens the per-chunk shared hot DB and answers backfillSource's hot
// branch: is the hot tier COMPLETE for this chunk (DECISION (a): the single DB's
// maxCommittedSeq >= the chunk's last ledger — every CF advances together in one
// atomic synced batch, so no min-of-three), and if so hand back a ChunkSource
// over the ledgers CF so the just-closed chunk freezes without a refetch.
//
// Injected so processChunk/backfillSource stay testable: production wires the
// real shared multi-CF RocksDB; tests pass a fake.
type HotProbe interface {
	// OpenHotChunk borrows the chunk's shared hot DB for a freeze pass (the daemon
	// owns the writer). Returns the handle, or — when the key said "ready" — an
	// error / ok==false (absent dir) the caller treats as case-4 loss.
	OpenHotChunk(chunkID chunk.ID) (HotChunk, bool, error)
}

// HotChunk is one chunk's opened hot tier: the single DB's completeness gate plus
// an LCM source over the ledgers CF.
type HotChunk interface {
	// MaxCommittedSeq is the single authoritative watermark (decision (a));
	// ok=false on an empty DB (so the chunk cannot be complete).
	MaxCommittedSeq() (seq uint32, ok bool, err error)
	// Source yields the chunk's LCMs from the ledgers CF as a ChunkSource the cold
	// pipeline (RunColdChunk) drains.
	Source() ingest.ChunkSource
	// Close releases the shared hot DB.
	Close() error
}

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
// (key state + path layout), the hot probe, the bulk backend source + its
// coverage waiter, and the metric sink/logger. Construction is the daemon's
// job; the primitives below never reach around it.
type ProcessConfig struct {
	Catalog *catalog.Catalog
	Logger  *supportlog.Entry
	Sink    ingest.MetricSink

	// HotProbe opens the per-chunk hot tier for the hot branch. Required.
	HotProbe HotProbe

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
	if cfg.HotProbe == nil {
		return errors.New("streaming: ProcessConfig.HotProbe is nil")
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
// The cold ingestion is the merged ingest.RunColdChunk over the per-type cold
// ingesters — processChunk does not re-derive any extractor or writer; it only
// chooses the LCM source (backfillSource) and drives the one write protocol
// around the freeze.
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

// backfillSource implements rule 2's source-preference order for one chunk. It
// returns the chosen ingest.ChunkSource, a closer (releasing any opened hot
// stores; a no-op for the pack/bulk branches), and an error. The hot branch
// fatals only on LOSS (ErrHotVolumeLost, detected lazily on the open); an
// incomplete-but-present hot DB is STALENESS that falls through to the next
// source, because re-derivation IS its recovery.
//
// Preference order:
//  1. A ready, COMPLETE hot tier read locally — completeness is DECISION (a):
//     the single shared DB's maxCommittedSeq >= chunkLastLedger.
//  2. The frozen local .pack via the ledger cold reader, when ledgers is NOT among
//     the requested outputs (re-derivation without a download).
//  3. The configured bulk backend, gated by a bounded WaitForCoverage.
func backfillSource(
	ctx context.Context, chunkID chunk.ID, artifacts catalog.ArtifactSet, cfg ProcessConfig,
) (ingest.ChunkSource, func() error, error) {
	noClose := func() error { return nil }
	cat := cfg.Catalog
	layout := cat.Layout()

	// (1) Hot branch: only consult it when the chunk is owned by ingestion
	// (hot key present) AND "ready". A "transient" key (mid creation/deletion or
	// recovery-demoted) is NOT a read source — it falls through like any other
	// non-ready state.
	hotState, err := cat.HotState(chunkID)
	if err != nil {
		return nil, noClose, fmt.Errorf("streaming: read hot state chunk %s: %w", chunkID, err)
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

	// (2) Frozen local .pack, only when ledgers is not requested (producing ledgers from
	// the pack we'd write would be circular). The ledger cold reader is the same
	// reader the merged pack ChunkSource opens.
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

	// (3) Bulk backend — the only source for a chunk with no local copy.
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

// tryHotSource handles backfillSource's hot branch under a "ready" key, returning
// (source, closer, used, err): used=true when the hot tier is present AND
// complete (single-watermark gate); used=false when present but incomplete
// (staleness — caller falls through); a non-nil err only for case-4 LOSS.
func tryHotSource(chunkID chunk.ID, cfg ProcessConfig) (ingest.ChunkSource, func() error, bool, error) {
	hot, ok, err := cfg.HotProbe.OpenHotChunk(chunkID)
	if err != nil {
		// "ready" key but the DB cannot be opened — hot-volume loss.
		return nil, nil, false, fmt.Errorf("%w: chunk %s: %w", ErrHotVolumeLost, chunkID, err)
	}
	if !ok {
		// "ready" key but the dir is absent — hot-volume loss.
		return nil, nil, false, fmt.Errorf("%w: chunk %s: hot directory absent", ErrHotVolumeLost, chunkID)
	}
	closer := hot.Close
	maxSeq, present, merr := hot.MaxCommittedSeq()
	if merr != nil {
		_ = hot.Close()
		// A read error against an opened DB is loss, not staleness: the
		// DB opened but cannot answer its own progress.
		return nil, nil, false, fmt.Errorf("%w: chunk %s: max committed seq: %w", ErrHotVolumeLost, chunkID, merr)
	}
	// DECISION (a): complete iff the single DB's maxCommittedSeq reaches the
	// chunk's last ledger. An empty DB (present==false) cannot be complete.
	if present && maxSeq >= chunkID.LastLedger() {
		return hot.Source(), closer, true, nil
	}
	_ = hot.Close()
	return nil, nil, false, nil
}

// ---------------------------------------------------------------------------
// pollingBackendWaiter — the default BackendWaiter: poll a tip function on a
// fixed backoff until it covers chunkLastLedger or the deadline expires.
// ---------------------------------------------------------------------------

// pollingBackendWaiter polls Tip on Interval until it returns a value >=
// chunkLastLedger, the ctx is canceled, or Timeout elapses (ErrBackendCoverage
// Timeout). Tip is the bulk backend's current network/object-store tip ledger.
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
