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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/serving"
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
	metrics.RetentionFloor(retentionFloorLedger(cfg.Retention, lastCommitted))
	logger.WithField("last_committed", lastCommitted).
		WithField("earliest", earliest).
		WithField("pinned", pinned).
		Info("startup — last-committed derived, beginning backfill")

	// Step 1: backfill to the tip.
	lastCommitted, err = backfillToTip(ctx, cfg, lastCommitted)
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

	// The serving router holds the query watermark, advanced by the ingestion loop
	// below. It also owns the shared hot-database handles: the loop publishes them,
	// the freeze reads completed chunks through them (HotHandle), and the lifecycle
	// unpublishes them at discard (Router) for deferred deletion to close. It will
	// back the read server in later work; for now nothing reads it. Constructed per
	// run — no query survives a restart, so a fresh router each run is fine.
	router := serving.NewRouter(cat, cfg.Retention)
	cfg.Exec.Process.HotHandle = router.Handle

	// Before serving: destroy any resources a crashed run left demoted, then open
	// and publish the handles for ready hot chunks below the live one (completed
	// chunks a prior run had not yet discarded). The live chunk's handle is
	// published by the ingestion loop. Both complete before the loops start.
	liveChunk := chunk.IDFromLedger(resumeLedger)
	if err := lifecycle.StartupSweep(cat, liveChunk); err != nil {
		return fmt.Errorf("startup sweep: %w", err)
	}
	if err := router.BootstrapHandles(liveChunk, logger); err != nil {
		return fmt.Errorf("startup bootstrap handles: %w", err)
	}

	// The lifecycle config draws on the SAME Exec wiring backfill uses, so the two
	// share one catalog/pool by construction.
	lifecycleCfg := lifecycle.Config{
		ExecConfig: cfg.Exec,
		Retention:  cfg.Retention,
		Router:     router,
		Grace:      cfg.lifecycleGrace,
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
			Health:   cfg.health,
			Router:   router,
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
// progress. Each pass samples the backend tip, computes the target frontier
// (passTarget, which quarantines all tip logic), derives the floor from it, and
// breaks on an empty or non-advancing range.
func backfillToTip(ctx context.Context, cfg StartConfig, lastCommitted uint32) (uint32, error) {
	ret := cfg.Retention
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

		// Time the whole pass, tip sample included; passes that break early record
		// nothing (the metrics calls are below the break).
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

		// Break on a degenerate range: nothing-complete (-1) or non-advancing at the
		// first guard, an inverted range (earliest pinned above the target, e.g. a
		// first start with earliest_ledger="now") at the second.
		target := passTarget(tip, lastCommitted)
		if target <= backfilledThrough {
			break
		}
		rangeStart := ret.FloorAt(target)
		if int64(rangeStart) > target {
			break
		}
		rangeEnd := chunk.ID(target) //nolint:gosec // target > backfilledThrough >= -1 ⇒ target >= 0

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
		backfilledThrough = target

		metrics.BackfillPass(passDuration)
		metrics.LastCommitted(lastCommitted)
		metrics.RetentionFloor(rangeStart.FirstLedger()) // the floor this pass built to
		logger.WithField("range_lo", rangeStart.String()).
			WithField("range_hi", rangeEnd.String()).
			WithField("last_committed", lastCommitted).
			WithField("duration", passDuration.String()).
			Info("backfill pass complete")
	}
	return lastCommitted, nil
}

// passTarget is the highest complete chunk this backfill pass builds to: the last
// complete chunk at max(tip, lastCommitted), lowered by the mid-chunk exclusion so
// a mid-chunk last-committed within one chunk of the tip leaves its partial resume
// chunk to ingestion. Signed; -1 means no complete chunk exists. All tip logic is
// quarantined here, so the retention floor derives from the returned target alone.
func passTarget(tip, lastCommitted uint32) int64 {
	// max() guards a lagging bulk tip: the tip alone could drop a complete
	// last-committed chunk.
	target := geometry.LastCompleteChunkAt(max(tip, lastCommitted))
	if withinOneChunkOfTip(tip, lastCommitted) && lastCommittedMidChunk(lastCommitted) {
		target = geometry.LastCompleteChunkAt(lastCommitted)
	}
	return target
}

// retentionFloorLedger is the retention_floor_ledger gauge value for the highest
// committed ledger. FloorAt maps the -1 "nothing complete yet" frontier to the
// earliest chunk, so no young-store branch is needed here.
func retentionFloorLedger(ret geometry.Retention, committed uint32) uint32 {
	return ret.FloorAt(geometry.LastCompleteChunkAt(committed)).FirstLedger()
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

	// Retention is the floor policy, bound once at startup from the validated
	// earliest_ledger pin. run() assembles the lifecycle.Config from Exec + this,
	// so the lifecycle and backfill share one floor by construction.
	Retention geometry.Retention

	// Core starts captive core and yields the ingestion getter. Required.
	Core CoreOpener

	// ServeReads begins serving reads; it must return promptly, not block. Required.
	ServeReads func(ctx context.Context) error

	// runBackfill is a test-only seam for one backfill pass; nil ⇒ backfill.RunBackfill.
	runBackfill func(ctx context.Context, exec backfill.ExecConfig, lo, hi chunk.ID) error

	// lifecycleGrace overrides the deferred-deletion wait; 0 ⇒ lifecycle's
	// defaultGrace. Tests set it small so a run's end-of-run destroy does not park
	// for minutes.
	lifecycleGrace time.Duration

	// health is the readiness/health signal the ingestion loop feeds per commit;
	// #772's read server consumes it (as HealthSignal). nil ⇒ observe is a no-op.
	health *healthState
}

// withDefaults fills the embedded Exec defaults (Workers -> GOMAXPROCS). The
// lifecycle.Config is assembled from Exec + Retention in run(); the tip retry
// policy is the sampleTipWithRetry defaults at its call sites, not here.
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
