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
)

// ErrHotVolumeLost is the case-4 fatal: a hot:chunk key is "ready" but its
// directory is missing or unopenable. The hot DB is the SOLE copy of a chunk's
// recently-ingested ledgers, so this is unrecoverable loss — never silently
// healed. It is detected lazily, on the open that needs the DB (not an eager
// all-keys scan), and returned as a sentinel (not a process exit) so the
// daemon's top loop owns the fatal-and-surface decision and tests can assert it.
var ErrHotVolumeLost = errors.New("streaming: hot storage lost; run surgical recovery (case 4)")

// ErrBackendCoverageTimeout is the bounded-wait fatal from backfillSource's bulk
// branch: the configured backend's tip never advanced to cover a
// genuinely-backend-only chunk within the deadline.
var ErrBackendCoverageTimeout = errors.New("streaming: backend never covered chunk within deadline")

// HotProbe opens the per-chunk shared hot DB and answers backfillSource's two
// questions: (1) is the hot tier COMPLETE — the single DB's maxCommittedSeq >=
// the chunk's last ledger (decision (a); see pkg/stores/hotchunk) — and (2) if
// so, a ChunkSource streaming the chunk's LCMs from the ledgers CF, so a
// just-closed chunk freezes without a refetch. It is injected so
// processChunk/backfillSource stay testable without the live ingestion pipeline.
type HotProbe interface {
	// OpenHotChunk opens the chunk's shared hot DB read-only-ish (the daemon
	// owns the writer; this is a borrow for a freeze pass). It returns the
	// opened handle, or an error the caller treats as case-4 loss when the
	// catalog key said "ready". A nil error with ok==false means the dir is
	// absent (also loss when "ready").
	OpenHotChunk(chunkID chunk.ID) (HotChunk, bool, error)
}

// HotChunk is one chunk's opened hot tier: the single DB's completeness gate
// plus an LCM source over the ledgers CF. Close releases the shared DB.
type HotChunk interface {
	// MaxCommittedSeq returns the single authoritative watermark (decision (a)):
	// the highest ledger seq the shared DB has durably committed. ok=false if the
	// DB is empty, so the chunk cannot be complete.
	MaxCommittedSeq() (seq uint32, ok bool, err error)
	// Source yields the chunk's LCMs from the ledgers CF as a ChunkSource the
	// cold pipeline (RunColdChunk) can drain.
	Source() ingest.ChunkSource
	// Close releases the shared hot DB.
	Close() error
}

// BackendWaiter bounds backfillSource's bulk branch: it blocks until the
// configured backend's tip covers chunkLastLedger, returning
// ErrBackendCoverageTimeout if the tip never advances within the deadline. A
// chunk WITH a local copy never reaches here. It is an interface so the tip
// query is injectable (production wraps the LedgerBackend; tests pass a fake).
type BackendWaiter interface {
	WaitForCoverage(ctx context.Context, chunkLastLedger uint32) error
}

// ProcessConfig is the dependency bundle processChunk/backfillSource read:
// everything a freeze pass needs — the catalog, the hot probe, the bulk backend
// + its coverage waiter, and the metric sink/logger. The daemon constructs it.
type ProcessConfig struct {
	Catalog *Catalog
	Logger  *supportlog.Entry
	Sink    ingest.MetricSink

	// HotProbe opens the per-chunk hot tier for the hot branch. Required.
	HotProbe HotProbe

	// Backend is the configured bulk LedgerBackend as a ChunkSource (BSB by
	// default) — the only source for a chunk with no local copy. May be nil in a
	// frontfill-only deployment; backfillSource errors loudly if a chunk actually
	// reaches the bulk branch with no backend.
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

// processChunk materializes the requested cold artifact kinds (ledgers/.pack
// today; events/txhash later) for ONE chunk in a single streaming pass over its
// ledgers, applying the one-write protocol per kind (rule 1):
//
//   - Per-kind idempotency: a kind already "frozen" is dropped (self-skip); a
//     "freezing"/"pruning"/absent key re-materializes (idempotent — the cold
//     ingesters overwrite at the canonical path).
//   - Mark-then-write: every remaining kind goes "freezing" BEFORE any I/O, the
//     cold pipeline (RunColdChunk) writes the files, they + their dirents are
//     fsynced (barrierNewFile), and only then do the keys flip to "frozen".
//
// Cold ingestion is ingest.RunColdChunk over the same ingester set RunCold uses;
// processChunk only chooses the LCM source (backfillSource) and drives the
// protocol around the freeze.
func processChunk(ctx context.Context, chunkID chunk.ID, artifacts ArtifactSet, cfg ProcessConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cat := cfg.Catalog

	// rule 1 per-kind idempotency: frozen kinds self-skip.
	for _, kind := range artifacts.Kinds() {
		state, err := cat.State(chunkID, kind)
		if err != nil {
			return fmt.Errorf("streaming: read state chunk %s kind %s: %w", chunkID, kind, err)
		}
		if state == StateFrozen {
			artifacts = artifacts.Remove(kind)
		}
	}
	if artifacts.Empty() {
		return nil
	}

	// Choose the source BEFORE marking "freezing" — backfillSource may fatal or
	// fall through, and we must not leave "freezing" debris for a chunk we then
	// refuse to produce. The closer releases any opened hot store after the pass.
	source, closeSource, err := backfillSource(ctx, chunkID, artifacts, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = closeSource() }()

	// Mark-then-write: every requested kind "freezing" BEFORE any I/O.
	if err := cat.MarkChunkFreezing(chunkID, artifacts.Kinds()...); err != nil {
		return fmt.Errorf("streaming: mark freezing chunk %s %s: %w", chunkID, artifacts, err)
	}

	// Test-only observation at the mark-then-write instant: every requested kind
	// is "freezing" and no file written yet. No-op in production (hook nil); see
	// crashHooks.afterMarkFreezing.
	cat.hooks.fireAfterMarkFreezing()

	// Snapshot which artifact parent dirs are absent BEFORE the write creates
	// them, so the barrier below fsyncs the grandparent dirent for each dir this
	// freeze creates. Existence is the real signal: chunkID % ChunksPerBucket == 0
	// is not a reliable "first chunk in this bucket" — frontfill-at-tip or
	// out-of-order backfill/recovery can make any chunk the first materialized.
	createdParents := createdParentDirs(cat.layout, chunkID, artifacts.Kinds())

	// One streaming pass through the cold pipeline; the ingesters (re)create files
	// at their canonical paths, overwriting any partial from a crashed attempt.
	dirs := ingest.ColdDirs{
		Ledgers: cat.layout.LedgersRoot(),
	}
	rerr := ingest.RunColdChunk(ctx, cfg.Logger, source, dirs, chunkID, cfg.Sink, artifacts.ingestConfig())
	if rerr != nil {
		return fmt.Errorf("streaming: cold ingest chunk %s %s: %w", chunkID, artifacts, rerr)
	}

	// Durability barrier BEFORE flipping "frozen": the cold writers fsync file
	// DATA on Finalize, but the protocol also needs the dirents durable first.
	// barrierNewFile fsyncs file + parent (+ grandparent for a dir this freeze
	// created, so a freshly created bucket dir's dirent is itself durable).
	for _, kind := range artifacts.Kinds() {
		for _, path := range cat.layout.ArtifactPaths(chunkID, kind) {
			if berr := barrierNewFile(path, createdParents[filepath.Dir(path)]); berr != nil {
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

// createdParentDirs returns the artifact parent directories that do not yet
// exist — the dirs RunColdChunk's MkdirAll will create on this freeze. Each such
// dir's grandparent dirent must be fsynced (barrierNewFile's newParent) for the
// new dir to survive a crash. Existence is the signal because materialization
// order — not chunkID % ChunksPerBucket — decides which chunk is first in a
// bucket (frontfill-at-tip and out-of-order backfill/recovery break the
// arithmetic proxy).
func createdParentDirs(layout Layout, chunkID chunk.ID, kinds []Kind) map[string]bool {
	created := map[string]bool{}
	for _, kind := range kinds {
		for _, path := range layout.ArtifactPaths(chunkID, kind) {
			parent := filepath.Dir(path)
			if _, err := os.Stat(parent); os.IsNotExist(err) {
				created[parent] = true
			}
		}
	}
	return created
}

// backfillSource implements rule 2's source-preference order for one chunk,
// returning the chosen source, a closer (no-op for the pack/bulk branches), and
// an error. The hot branch fatals only on LOSS (a "ready" key whose dir is
// missing/unopenable — ErrHotVolumeLost, detected lazily here); an incomplete
// hot DB is STALENESS and falls through, because re-derivation IS its recovery.
//
// Preference order:
//  1. A ready, COMPLETE hot tier (decision (a): maxCommittedSeq >= chunkLastLedger).
//  2. The frozen local .pack, when ledgers is NOT a requested output (re-derive
//     without a download).
//  3. The configured bulk backend, gated by a bounded WaitForCoverage.
func backfillSource(
	ctx context.Context, chunkID chunk.ID, artifacts ArtifactSet, cfg ProcessConfig,
) (ingest.ChunkSource, func() error, error) {
	noClose := func() error { return nil }
	cat := cfg.Catalog

	// (1) Hot branch: only when the chunk is owned by ingestion and "ready". A
	// "transient" key (mid creation/deletion or recovery-demoted) is not a read
	// source — it falls through.
	hotState, err := cat.HotState(chunkID)
	if err != nil {
		return nil, noClose, fmt.Errorf("streaming: read hot state chunk %s: %w", chunkID, err)
	}
	if hotState == HotReady {
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

	// (2) Frozen local .pack, only when ledgers is not requested (deriving ledgers
	// from the pack we'd write would be circular).
	ledgersState, err := cat.State(chunkID, KindLedgers)
	if err != nil {
		return nil, noClose, fmt.Errorf("streaming: read ledgers state chunk %s: %w", chunkID, err)
	}
	if ledgersState == StateFrozen && !artifacts.Has(KindLedgers) {
		if _, serr := os.Stat(cat.layout.LedgerPackPath(chunkID)); serr == nil {
			cfg.Logger.Debugf("backfillSource: chunk %s re-derived from frozen .pack", chunkID)
			// ingest.NewPackSource composes {coldDir}/{bucket}/{chunk}.pack, which
			// equals LedgerPackPath when coldDir is the ledgers root.
			return ingest.NewPackSource(cat.layout.LedgersRoot()), noClose, nil
		}
		// A "frozen" ledgers key whose pack is gone violates the key invariant
		// (frozen ⇒ file exists); surface it rather than silently downloading.
		return nil, noClose, fmt.Errorf(
			"streaming: chunk %s ledgers is %q but pack file is missing at %s",
			chunkID, StateFrozen, cat.layout.LedgerPackPath(chunkID))
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

// tryHotSource handles backfillSource's hot branch under a "ready" key. It
// returns (source, closer, used, err): used=true with a source when the hot
// tier is present AND complete (single-watermark gate); used=false (source nil)
// when present but incomplete (staleness — caller falls through); a non-nil err
// only for case-4 LOSS (dir missing/unopenable under a "ready" key).
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

// pollingBackendWaiter — the default BackendWaiter: poll a tip function on a
// fixed interval until it covers chunkLastLedger or the deadline expires.

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
