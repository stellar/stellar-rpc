package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/lifecycle"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// run is the daemon's startup, in two steps: (1) BACKFILL to the tip, then
// (2) SERVE + INGEST — start captive core (injected), begin serving reads
// (injected), then run the live ingestion loop (which opens the resume chunk's hot
// DB itself) and the lifecycle loop as a joined errgroup pair (whichever returns
// first cancels the other; g.Wait surfaces the first error). Returns nil only on a
// clean shutdown (ctx canceled mid-run); any other return is a restartable error
// the supervisor warns on and retries with backoff (a first start with no
// reachable backend, a backfill/ingest/lifecycle failure, or a "ready" hot DB that
// won't open — none are auto-healed, all are re-attempted).
func run(ctx context.Context, cfg StartConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cfg = cfg.withDefaults()
	cat := cfg.Exec.Catalog
	logger := cfg.Exec.Logger

	// earliest_ledger must already be pinned by validateConfig: an absent pin reads
	// as 0 and mis-classifies a first start as a restart, so refuse it loudly.
	earliest, pinned, err := cat.EarliestLedger()
	if err != nil {
		return fmt.Errorf("startup read earliest ledger: %w", err)
	}
	if !pinned {
		return errors.New("startup requires config:earliest_ledger pinned " +
			"(validateConfig pins it before run; not done here)")
	}

	// Derived, never stored: highest durably-committed ledger (frozen cold artifacts
	// vs the highest ready hot DB's max committed seq), clamped by earliest-1. Passing
	// the logger refines with one read-only open of the highest ready hot DB before
	// ingestion opens a writer; a read-only open replays any synced WAL from an
	// ungraceful crash into memtables, so MaxCommittedSeq is correct.
	lastCommitted, err := lifecycle.LastCommittedLedger(cat, logger)
	if err != nil {
		return fmt.Errorf("startup derive last-committed: %w", err)
	}

	metrics := observability.MetricsOrNop(cfg.Exec.Metrics)
	metrics.LastCommitted(lastCommitted)
	metrics.RetentionFloor(lifecycle.EffectiveRetentionFloor(lastCommitted, cfg.RetentionChunks, earliest))
	logger.WithField("last_committed", lastCommitted).
		WithField("earliest", earliest).
		WithField("pinned", pinned).
		Info("startup — last-committed derived, beginning backfill")

	// Step 1: backfill to the tip.
	lastCommitted, err = backfillToTip(ctx, cfg, lastCommitted, earliest)
	if err != nil {
		return err
	}

	logger.WithField("last_committed", lastCommitted).
		WithField("resume_chunk", chunk.IDFromLedger(lastCommitted+1).String()).
		Info("backfill complete — opening resume hot tier and ingesting")

	// Step 2: serve + ingest. resumeLedger is one past the last-committed ledger —
	// the live chunk's next un-committed ledger. The ingestion loop opens that
	// chunk's hot DB itself (open and deferred close in one function, no cross-call
	// ownership gap) and consumes the stream from there.
	resumeLedger := lastCommitted + 1

	// The live ingestion stream. It owns the captive-core process (started on the
	// loop's first pull, torn down when the loop exits), so there is no eager
	// prepare and no closer to defer — the loop's ctx-scoped iteration is the
	// teardown. OpenCore only constructs, so a start failure surfaces as the loop's
	// first stream error for the daemon to classify (and restart).
	stream, err := cfg.Core.OpenCore(ctx)
	if err != nil {
		return fmt.Errorf("startup open ingestion stream: %w", err)
	}

	// The lifecycle goroutine runs one tick per boundary signal; ingestion Publishes
	// the just-completed chunk id into a latest-cell. It shares NO in-memory state
	// with ingestion — all derived from durable keys.
	boundary := lifecycle.NewBoundarySignal()

	// Seed the first tick with the last complete chunk at the resume point so it
	// fires at once. Skipped on a young network where no chunk is complete.
	if seed := geometry.LastCompleteChunkAt(lastCommitted); seed >= 0 {
		boundary.Publish(chunk.ID(seed)) //nolint:gosec // seed >= 0
	}

	// The lifecycle config draws on the SAME Exec wiring backfill uses, so the two
	// share one catalog/pool by construction.
	lifecycleCfg := lifecycle.Config{
		ExecConfig:      cfg.Exec,
		RetentionChunks: cfg.RetentionChunks,
	}.WithLifecycleDefaults()

	// Begin serving reads (injected) BEFORE launching the loops; it must return
	// promptly (launch, not block).
	if err := cfg.ServeReads(ctx); err != nil {
		return fmt.Errorf("startup serve reads: %w", err)
	}

	// Ingestion and the lifecycle run as a joined pair under errgroup.WithContext:
	// gctx cancels as soon as EITHER returns — and WithContext records the returning
	// goroutine's error BEFORE canceling, so g.Wait surfaces the real cause, not the
	// sibling's induced context-canceled. g.Wait joins both before run returns,
	// restoring the single-lifecycle-goroutine invariant across supervisor restarts.
	// supervise is the one clean-vs-restart decision point; a canceled parent ctx
	// classifies as clean.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := runIngestionLoop(gctx, ingestionLoopConfig{
			Stream:   stream,
			Resume:   resumeLedger,
			Catalog:  cat,
			Boundary: boundary,
			Logger:   logger,
			Metrics:  metrics,
			Sink:     cfg.Exec.Process.Sink,
		})
		if err == nil {
			// WithContext cancels gctx (unblocking the lifecycle sibling in g.Wait)
			// ONLY on a non-nil return. runIngestionLoop upholds that — every exit is
			// an error, including a clean stream end — but guard it so a future nil
			// return degrades to a supervised restart, never a silent g.Wait hang.
			return errors.New("ingestion loop returned nil unexpectedly")
		}
		return err
	})
	g.Go(func() error {
		return lifecycle.Loop(gctx, lifecycleCfg, cat, boundary)
	})
	return g.Wait()
}

// backfillToTip runs the backfill loop, returning lastCommitted as backfill makes
// progress. Each pass samples networkTip, anchors on max(tip, lastCommitted),
// computes [rangeStart, rangeEnd] with the mid-chunk exclusion, and breaks on an
// empty or non-advancing range.
func backfillToTip(ctx context.Context, cfg StartConfig, lastCommitted, earliest uint32) (uint32, error) {
	retentionChunks := cfg.RetentionChunks
	metrics := observability.MetricsOrNop(cfg.Exec.Metrics)
	logger := cfg.Exec.Logger

	// runBackfill is a test seam; nil ⇒ the real backfill.RunBackfill.
	runBackfill := cfg.runBackfill
	if runBackfill == nil {
		runBackfill = backfill.RunBackfill
	}

	backfilledThrough := int64(-1)
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		tip, err := networkTip(ctx, cfg.NetworkTip, cfg.TipBackoff, cfg.TipMaxAttempts)
		if err != nil {
			if lastCommitted < earliest {
				// First start, no reachable backend: error out — the daemon must never
				// serve incomplete history. Restartable: the property is enforced by
				// returning an error at all (each restart re-checks lastCommitted <
				// earliest), not by the exit shape, so a datastore mid-outage or a young
				// lake below genesis self-heals on a later restart.
				return 0, fmt.Errorf("network tip unavailable and no local history to serve: %w", err)
			}
			// Restart with local progress: serve what's below lastCommitted, skip backfill.
			tip = lastCommitted
		}

		// max() guards a lagging bulk tip: the tip alone could regress the floor below
		// pruning or drop a complete last-committed chunk.
		anchor := max(tip, lastCommitted)
		rangeStart := chunk.IDFromLedger(lifecycle.EffectiveRetentionFloor(anchor, retentionChunks, earliest))

		// Same anchor for rangeEnd: a complete last-committed chunk above a lagging tip
		// still folds in; chunks beyond the tip are durable and self-skip.
		rangeEndSigned := geometry.LastCompleteChunkAt(anchor)

		// Mid-chunk resume exclusion: a mid-chunk last-committed within one chunk of the tip
		// leaves the partial resume chunk to ingestion. Under the mid-chunk precondition
		// (guarded here) the last COMPLETE chunk is exactly one short of the live chunk,
		// so LastCompleteChunkAt names it directly — same vocabulary as rangeEndSigned above.
		if withinOneChunkOfTip(tip, lastCommitted) && lastCommittedMidChunk(lastCommitted) {
			rangeEndSigned = geometry.LastCompleteChunkAt(lastCommitted)
		}

		// Break on an empty or non-advancing range.
		if rangeEndSigned < int64(rangeStart) || rangeEndSigned <= backfilledThrough {
			break
		}
		rangeEnd := chunk.ID(rangeEndSigned) //nolint:gosec // > rangeStart >= 0

		logger.WithField("range_lo", rangeStart.String()).
			WithField("range_hi", rangeEnd.String()).
			WithField("tip", tip).
			WithField("last_committed", lastCommitted).
			Info("backfill pass starting")

		passStart := time.Now()
		if err := runBackfill(ctx, cfg.Exec, rangeStart, rangeEnd); err != nil {
			return 0, fmt.Errorf("startup backfill [%s,%s]: %w", rangeStart, rangeEnd, err)
		}
		passDuration := time.Since(passStart)

		// Advance the last-committed, never regressing (a lagging tip's rangeEnd can sit below it).
		lastCommitted = max(lastCommitted, rangeEnd.LastLedger())
		backfilledThrough = rangeEndSigned

		metrics.BackfillPass(passDuration)
		// Refresh the derived gauges as last-committed advances and the floor rises with it.
		metrics.LastCommitted(lastCommitted)
		metrics.RetentionFloor(lifecycle.EffectiveRetentionFloor(lastCommitted, retentionChunks, earliest))
		logger.WithField("range_lo", rangeStart.String()).
			WithField("range_hi", rangeEnd.String()).
			WithField("last_committed", lastCommitted).
			WithField("duration", passDuration.String()).
			Info("backfill pass complete")
	}
	return lastCommitted, nil
}

// withinOneChunkOfTip reports whether the last-committed sits within one chunk of the
// tip. Signed so a lagging tip below the resume point still reads true.
func withinOneChunkOfTip(tip, lastCommitted uint32) bool {
	return int64(tip)-int64(lastCommitted) < int64(chunk.LedgersPerChunk)
}

// lastCommittedMidChunk reports whether lastCommitted falls strictly inside a chunk.
// The genesis sentinel reads as a boundary, never mid-chunk.
func lastCommittedMidChunk(lastCommitted uint32) bool {
	c := geometry.ChunkIDOfLedger(lastCommitted)
	return lastCommitted != geometry.CompleteThrough(c)
}

// ---------------------------------------------------------------------------
// Injected external boundaries (so startup is testable with fakes).
// ---------------------------------------------------------------------------

// NetworkTipBackend samples the bulk backend's current network tip during backfill.
// It is consulted only during backfill; once ingestion runs, captive core is the tip.
type NetworkTipBackend interface {
	NetworkTip(ctx context.Context) (uint32, error)
}

// CoreOpener hands back the live ingestion stream the loop consumes. The stream
// OWNS its source's lifecycle (started on the first RawLedgers pull over the
// unbounded range from the loop's resume ledger, torn down when the loop exits),
// so there is no resume arg, no PrepareRange, and no closer for the caller to
// sequence. Production returns a captive-core stream; tests pass a fake
// LedgerStream.
type CoreOpener interface {
	OpenCore(ctx context.Context) (ledgerbackend.LedgerStream, error)
}

// StartConfig is run's resolved dependency bundle.
type StartConfig struct {
	// Exec drives backfill's RunBackfill; its Catalog/Logger are the shared ones.
	Exec backfill.ExecConfig

	// RetentionChunks bounds the sliding retention floor's width — the backfill
	// floor's width too (0 ⇒ the earliest-ledger floor only). run() assembles the
	// lifecycle.Config from Exec + this, so the lifecycle and backfill can never
	// diverge on the catalog/pool (the invariant is structural, not by comment).
	RetentionChunks uint32

	// NetworkTip samples the bulk backend's tip during backfill. Required.
	NetworkTip NetworkTipBackend

	// Core starts captive core and yields the ingestion getter. Required.
	Core CoreOpener

	// ServeReads begins serving reads; it must return promptly, not block. Required.
	ServeReads func(ctx context.Context) error

	// TipBackoff is networkTip's inter-attempt sleep; TipMaxAttempts bounds the
	// retries. Zero values fall back to defaults in withDefaults.
	TipBackoff     time.Duration
	TipMaxAttempts int

	// runBackfill is a test-only seam for one backfill pass; nil ⇒ backfill.RunBackfill.
	runBackfill func(ctx context.Context, exec backfill.ExecConfig, lo, hi chunk.ID) error
}

const (
	defaultTipBackoff     = time.Second
	defaultTipMaxAttempts = 5
)

// withDefaults fills the tip-backoff defaults and the embedded Exec defaults
// (Workers -> GOMAXPROCS). The lifecycle.Config is assembled from Exec +
// RetentionChunks in run().
func (cfg StartConfig) withDefaults() StartConfig {
	cfg.Exec = cfg.Exec.WithDefaults()
	if cfg.TipBackoff <= 0 {
		cfg.TipBackoff = defaultTipBackoff
	}
	if cfg.TipMaxAttempts <= 0 {
		cfg.TipMaxAttempts = defaultTipMaxAttempts
	}
	return cfg
}

func (cfg StartConfig) validate() error {
	if cfg.Exec.Catalog == nil {
		return errors.New("nil StartConfig.Exec.Catalog")
	}
	if cfg.Exec.Logger == nil {
		return errors.New("nil StartConfig.Exec.Logger")
	}
	if cfg.NetworkTip == nil {
		return errors.New("nil StartConfig.NetworkTip")
	}
	if cfg.Core == nil {
		return errors.New("nil StartConfig.Core")
	}
	if cfg.ServeReads == nil {
		return errors.New("nil StartConfig.ServeReads")
	}
	return nil
}

// networkTip samples backend.NetworkTip, retrying a transient error on a bounded
// constant backoff (maxAttempts total tries) and rejecting a sub-genesis tip as
// "not ready" so an unready backend never pins a garbage floor. Built on
// cenkalti/backoff — the same retry primitive as withRetries and waitForCoverage —
// so ctx cancellation aborts the wait and the sub-genesis case is backoff.Permanent.
func networkTip(
	ctx context.Context, backend NetworkTipBackend, interval time.Duration, maxAttempts int,
) (uint32, error) {
	if maxAttempts < 1 {
		maxAttempts = 1 // never underflow WithMaxRetries' uint64 into an unbounded loop
	}
	var (
		tip      uint32
		notReady bool
	)
	poll := func() error {
		t, err := backend.NetworkTip(ctx)
		if err != nil {
			return err // transient — retry
		}
		if t < chunk.FirstLedgerSeq {
			// Below genesis ⇒ backend empty/not-synced; permanent (it would keep returning 0).
			notReady = true
			return backoff.Permanent(fmt.Errorf("backend tip %d is below genesis %d — backend not ready",
				t, chunk.FirstLedgerSeq))
		}
		tip = t
		return nil
	}
	// Constant interval, count-bounded: maxAttempts tries == 1 initial + (maxAttempts-1) retries.
	bo := backoff.WithMaxRetries(backoff.NewConstantBackOff(interval), uint64(maxAttempts-1))
	switch err := backoff.Retry(poll, backoff.WithContext(bo, ctx)); {
	case err == nil:
		return tip, nil
	case notReady, ctx.Err() != nil:
		return 0, err // permanent (not ready) or ctx-canceled: surface as-is, not "exhausted"
	default:
		return 0, fmt.Errorf("network tip unavailable after %d attempts: %w", maxAttempts, err)
	}
}
