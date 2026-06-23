package streaming

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ErrHotVolumeLost is the case-4 fatal: a hot:chunk key is "ready" but its
// directory is missing or unopenable. The hot DB is the SOLE copy of a chunk's
// recently-ingested ledgers, so this is unrecoverable loss — never silently
// healed. Loss is detected LAZILY, on the open that needs the DB (lastCommitted
// Ledger's one refinement open of the highest ready chunk before ingestion
// starts, openHotTierForChunk's "ready" branch, or backfillSource's hot branch),
// not by an eager all-ready-keys scan. It is returned as a sentinel (not a
// process exit) so the daemon's top-level loop owns the fatal-and-surface
// decision and tests can assert it.
var ErrHotVolumeLost = errors.New("streaming: hot storage lost; run surgical recovery (case 4)")

// ErrBackendCoverageTimeout is the bounded-wait fatal from backfillSource's bulk
// branch: the configured backend's tip never advanced to cover a
// genuinely-backend-only chunk within the deadline.
var ErrBackendCoverageTimeout = errors.New("streaming: backend never covered chunk within deadline")

// HotProbe opens the per-chunk shared hot DB for a chunk and answers the two
// questions backfillSource's hot branch asks: (1) is the hot tier COMPLETE for
// this chunk — DECISION (a): the single DB's maxCommittedSeq >= the chunk's
// last ledger — and (2) if so, hand back a ChunkSource that streams the chunk's
// LCMs from the ledgers CF so the just-closed chunk freezes without a refetch.
//
// It is injected so processChunk/backfillSource stay testable without the live
// ingestion pipeline: production wires the real shared multi-CF RocksDB; tests
// pass a fake. Under decision (a) the hot tier is ONE DB whose ledgers, events,
// and txhash CFs all advance together in one atomic synced WriteBatch per
// ledger, so completeness is a SINGLE watermark — no min-of-three.
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
	// MaxCommittedSeq returns the single authoritative watermark — the highest
	// ledger seq the shared DB has durably committed (every CF advances
	// together, decision (a)) — and ok=false if the DB is empty (no committed
	// seq, so the chunk cannot be complete).
	MaxCommittedSeq() (seq uint32, ok bool, err error)
	// Source yields the chunk's LCMs from the ledgers CF as a ChunkSource the
	// cold pipeline (RunColdChunk) can drain.
	Source() ingest.ChunkSource
	// Close releases the shared hot DB.
	Close() error
}

// BackendWaiter bounds backfillSource's bulk branch: it blocks until the
// configured backend's tip covers chunkLastLedger, polling on a backoff, and
// returns ErrBackendCoverageTimeout (wrapped) if the tip never advances within
// the deadline. A chunk WITH a local copy never reaches here, so this never
// gates a normal restart whose range is entirely local.
//
// It is an interface (not an inline poll) so the bulk source's tip query is
// injectable: production wraps the configured LedgerBackend's tip; tests pass a
// fake that is either immediately-covered or never-covered.
type BackendWaiter interface {
	WaitForCoverage(ctx context.Context, chunkLastLedger uint32) error
}

// ProcessConfig is the dependency bundle processChunk/backfillSource read. It is
// the streaming spine's view of everything a freeze pass needs: the catalog
// (key state + path layout), the hot probe, the bulk backend source + its
// coverage waiter, and the metric sink/logger. Construction is the daemon's
// job; the primitives below never reach around it.
type ProcessConfig struct {
	Catalog *Catalog
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

// processChunk materializes the requested cold artifact kinds (ledgers/.pack, events
// cold segment, txhash/.bin) for ONE chunk in a single streaming pass over its
// ledgers, applying the Phase A one-write protocol per kind (rule 1):
//
//   - Per-kind idempotency: a kind whose chunk key is already "frozen" is
//     dropped from the request (it self-skips); a "freezing"/"pruning"/absent
//     key triggers re-materialization, itself idempotent (the cold ingesters
//     overwrite at the canonical path).
//   - Mark-then-write: every remaining kind's key is put "freezing" BEFORE any
//     I/O, the cold pipeline (RunColdChunk) writes the files at their canonical
//     paths from the source backfillSource chose, the files + their dirents are
//     fsynced (barrierNewFile), and only then are the keys flipped to "frozen".
//
// The cold ingestion is the merged ingest.RunColdChunk over the same cold
// ingester set RunCold uses — processChunk does not re-derive any extractor or
// writer; it only chooses the LCM source (backfillSource) and drives the one
// write protocol around the freeze.
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

	// Choose the LCM source BEFORE marking "freezing": backfillSource may fatal
	// (case-4 loss) or fall through sources, and we must not leave "freezing"
	// debris for a chunk we then refuse to produce. The returned closer releases
	// any opened hot stores once the freeze pass finishes.
	source, closeSource, err := backfillSource(ctx, chunkID, artifacts, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = closeSource() }()

	// Mark-then-write: every requested kind "freezing" BEFORE any I/O.
	if err := cat.MarkChunkFreezing(chunkID, artifacts.Kinds()...); err != nil {
		return fmt.Errorf("streaming: mark freezing chunk %s %s: %w", chunkID, artifacts, err)
	}

	// Test-only observation point at the exact mark-then-write instant: every
	// requested kind is now "freezing" and no file has been written yet. A no-op
	// in production (hook nil); see crashHooks.afterMarkFreezing.
	cat.hooks.fireAfterMarkFreezing()

	// One streaming pass through the merged cold pipeline. The cold ingesters
	// (re)create files at their canonical paths — re-materialization overwrites
	// any partial from a crashed "freezing" attempt.
	dirs := ingest.ColdDirs{
		Ledgers: cat.layout.LedgersRoot(),
	}
	if rerr := ingest.RunColdChunk(ctx, cfg.Logger, source, dirs, chunkID, cfg.Sink, artifacts.ingestConfig()); rerr != nil {
		return fmt.Errorf("streaming: cold ingest chunk %s %s: %w", chunkID, artifacts, rerr)
	}

	// Durability barrier: fsync each file + its parent dirent (+ grandparent
	// when this chunk created a new bucket dir) BEFORE flipping to "frozen".
	// The cold writers fsync file DATA on Finalize, but the one-write protocol
	// also requires the directory entries be durable before the key flips —
	// barrierNewFile is the exact two-level barrier (paths.go).
	newBucket := uint32(chunkID)%chunk.ChunksPerBucket == 0
	for _, kind := range artifacts.Kinds() {
		for _, path := range cat.layout.ArtifactPaths(chunkID, kind) {
			if berr := barrierNewFile(path, newBucket); berr != nil {
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
// fatals only on LOSS (a "ready" key whose dir is missing/unopenable — ErrHot
// VolumeLost, detected lazily on this open); an incomplete-but-present hot DB is
// STALENESS and falls through to the next source, because re-derivation IS its
// recovery.
//
// Preference order:
//  1. A ready, COMPLETE hot tier read locally — completeness is DECISION (a):
//     the single shared DB's maxCommittedSeq >= chunkLastLedger.
//  2. The frozen local .pack via the ledger cold reader, when ledgers is NOT among
//     the requested outputs (re-derivation without a download).
//  3. The configured bulk backend, gated by a bounded WaitForCoverage.
func backfillSource(
	ctx context.Context, chunkID chunk.ID, artifacts ArtifactSet, cfg ProcessConfig,
) (ingest.ChunkSource, func() error, error) {
	noClose := func() error { return nil }
	cat := cfg.Catalog

	// (1) Hot branch: only consult it when the chunk is owned by ingestion
	// (hot key present) AND "ready". A "transient" key (mid creation/deletion or
	// recovery-demoted) is NOT a read source — it falls through like any other
	// non-ready state.
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

	// (2) Frozen local .pack, only when ledgers is not requested (producing ledgers from
	// the pack we'd write would be circular). The ledger cold reader is the same
	// reader the merged pack ChunkSource opens.
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
