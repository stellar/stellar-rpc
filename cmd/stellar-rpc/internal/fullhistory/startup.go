package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/lifecycle"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// run is the daemon's startup, in two steps: (1) BACKFILL to the tip, then
// (2) SERVE + INGEST — open the resume chunk's hot DB (so a broken hot tier fails
// startup, not behind a crash-looping loop), start captive core (injected), begin
// serving reads (injected), then run the live ingestion loop (handed the open hot
// DB) and the lifecycle loop as a joined errgroup pair (whichever returns first
// cancels the other; g.Wait surfaces the first error). Never returns nil: a clean
// shutdown (ctx canceled mid-run) surfaces as a ctx-canceled error that supervise
// classifies via ctx.Err(); any other return is a restartable error the supervisor
// warns on and retries with backoff (a first start with no reachable backend, a
// backfill/ingest/lifecycle failure, or a "ready" hot DB that won't open — none are
// auto-healed, all are re-attempted).
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
	// vs the highest ready hot DB's max committed seq), clamped by earliest-1. The
	// derivation refines with one read-only open of the highest ready hot DB before
	// ingestion opens a writer; a read-only open replays any synced WAL from an
	// ungraceful crash into memtables, so MaxCommittedSeq is correct.
	lastCommitted, err := lastCommittedLedger(cat)
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
	// the live chunk's next un-committed ledger.
	resumeLedger := lastCommitted + 1

	// Open the resume chunk's hot DB BEFORE serving reads, so a broken hot tier (a
	// "ready" key whose DB won't open) fails startup instead of serving behind a
	// crash-looping ingestion loop. run() owns the close only until the loop takes
	// over: loopOwnsDB flips true at the errgroup launch, after which the loop's
	// deferred close owns it (and g.Wait joins before run returns, so there is no
	// window where neither owns it). Restarts re-enter run() from the top, so this
	// stays the single initial-open site; the loop still reopens at each boundary.
	hotDB, err := openHotDBForChunk(cat, chunk.IDFromLedger(resumeLedger), logger)
	if err != nil {
		return fmt.Errorf("startup open resume hot tier for ledger %d: %w", resumeLedger, err)
	}
	loopOwnsDB := false
	defer func() {
		if !loopOwnsDB {
			_ = hotDB.Close() // an error before the loop took ownership
		}
	}()

	// The live ingestion stream. It owns the captive-core process (started on the
	// loop's first pull, torn down when the loop exits), so there is no eager
	// prepare and no closer to defer — the loop's ctx-scoped iteration is the
	// teardown. OpenCore only constructs, so a start failure surfaces as the loop's
	// first stream error for the daemon to classify (and restart). (Eager core start
	// before serve would need a LedgerStream.Start hook the SDK deliberately omits.)
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
	// The loop's deferred close now owns hotDB; g.Wait joins it before run returns.
	loopOwnsDB = true
	g.Go(func() error {
		err := runIngestionLoop(gctx, ingestionLoopConfig{
			Stream:   stream,
			Resume:   resumeLedger,
			HotDB:    hotDB,
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
// progress. Each pass samples the network tip, anchors on max(tip, lastCommitted),
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

		// Time the whole pass, tip sample included: sampling the network tip (with its
		// retry-backoff sleeps) is part of the pass's work, so the timer starts before
		// it rather than only around runBackfill. Passes that break early (empty range)
		// record nothing — the metrics call is below the break.
		passStart := time.Now()
		// The backend owns its frontier (the lake for bsbSource, the archives for
		// captiveSource). A tip still unavailable after the bounded retry errors the
		// pass rather than proceeding — the daemon must never serve behind an unknown
		// frontier — and each supervised restart re-samples (a transient outage
		// self-heals).
		tip, err := sampleTipWithRetry(
			ctx, cfg.Exec.Process.Backend.Tip, defaultTipBackoff, defaultTipMaxAttempts)
		if err != nil {
			return 0, fmt.Errorf("sample network tip: %w", err)
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
	return lastCommitted != geometry.ChunkLastLedger(c)
}

// ---------------------------------------------------------------------------
// Injected external boundaries (so startup is testable with fakes).
// ---------------------------------------------------------------------------

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

	// Core starts captive core and yields the ingestion getter. Required.
	Core CoreOpener

	// ServeReads begins serving reads; it must return promptly, not block. Required.
	ServeReads func(ctx context.Context) error

	// runBackfill is a test-only seam for one backfill pass; nil ⇒ backfill.RunBackfill.
	runBackfill func(ctx context.Context, exec backfill.ExecConfig, lo, hi chunk.ID) error
}

// withDefaults fills the embedded Exec defaults (Workers -> GOMAXPROCS). The
// lifecycle.Config is assembled from Exec + RetentionChunks in run(); the tip
// retry policy is the sampleTipWithRetry defaults at its call sites, not here.
func (cfg StartConfig) withDefaults() StartConfig {
	cfg.Exec = cfg.Exec.WithDefaults()
	return cfg
}

func (cfg StartConfig) validate() error {
	if cfg.Exec.Catalog == nil {
		return errors.New("nil StartConfig.Exec.Catalog")
	}
	if cfg.Exec.Logger == nil {
		return errors.New("nil StartConfig.Exec.Logger")
	}
	if cfg.Exec.Process.Backend == nil {
		// The backend is the tip source every backfill pass samples (and the
		// freeze's bulk fetch); runDaemonWith guarantees one at startup.
		return errors.New("nil StartConfig.Exec.Process.Backend")
	}
	if cfg.Core == nil {
		return errors.New("nil StartConfig.Core")
	}
	if cfg.ServeReads == nil {
		return errors.New("nil StartConfig.ServeReads")
	}
	return nil
}
