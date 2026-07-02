package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/lifecycle"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// run is the daemon's startup, in two steps: (1) BACKFILL to the
// tip, then (2) SERVE + INGEST — open the resume chunk's hot DB, start captive
// core (injected), launch the lifecycle goroutine on a doorbell, begin serving
// reads (injected), and run the live ingestion loop. Returns nil only on a clean
// shutdown (ctx canceled mid-run, or the ingestion loop's clean stop); any other
// return is a restartable error the supervisor warns on and retries with backoff
// (a first start with no reachable backend, a backfill/ingest failure, or a
// "ready" hot DB that won't open — none are auto-healed, all are re-attempted).
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
	metrics.LastCommitted(lastCommitted,
		lifecycle.EffectiveRetentionFloor(lastCommitted, cfg.RetentionChunks, earliest))
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

	// Step 2: serve + ingest. resumeLedger is one past the last-committed ledger — the live
	// chunk's next un-committed ledger; runIngestionLoop re-derives the exact resume
	// point from durable state, so a mid-chunk and a boundary last-committed ledger both resume right.
	resumeLedger := lastCommitted + 1
	resumeChunk := chunk.IDFromLedger(resumeLedger)

	hotDB, err := openHotDBForChunk(cat, resumeChunk, logger)
	if err != nil {
		return fmt.Errorf("startup open resume hot tier chunk %s: %w", resumeChunk, err)
	}

	// Start captive core from the resume ledger. On failure the resume hot DB is
	// already open; close it so a restart re-opens cleanly (the rocksdb LOCK must
	// be released).
	core, closeCore, err := cfg.Core.OpenCore(ctx, resumeLedger)
	if err != nil {
		_ = hotDB.Close()
		return fmt.Errorf("startup start captive core at ledger %d: %w", resumeLedger, err)
	}
	defer func() {
		if closeCore != nil {
			_ = closeCore()
		}
	}()

	// The lifecycle goroutine runs one tick per notification carrying the just-
	// completed chunk id. Buffered to LifecycleQueueDepth; ingestion sends at each
	// boundary. It shares NO in-memory state with ingestion — all derived from durable keys.
	lifecycleCh := make(chan chunk.ID, lifecycle.LifecycleQueueDepth)

	// Seed the first tick with the last complete chunk at the resume point so it
	// fires at once — clearing crash/downtime leftovers concurrently with serving.
	// Skipped on a young network where no chunk is complete (the first real boundary
	// triggers the first tick).
	if seed := geometry.LastCompleteChunkAt(lastCommitted); seed >= 0 {
		lifecycleCh <- chunk.ID(seed) //nolint:gosec // seed >= 0
	}

	// The lifecycle goroutine is tied to a PER-ITERATION child ctx (not the daemon
	// ctx) and is canceled + JOINED before run returns for ANY reason — restoring
	// the single-lifecycle-goroutine invariant across supervisor restarts (a
	// daemon-ctx-tied loop would survive a restartable return and run a tick
	// concurrently with the next iteration's lifecycle + ingestion: two backfill
	// passes truncating the same .pack/.idx). Loop checks ctx at every step, so
	// the join cannot block past the current step.
	lifecycleCtx, cancelLifecycle := context.WithCancel(ctx)
	// Assemble the lifecycle config from the SAME Exec wiring backfill uses, so
	// the two share one catalog/pool by construction. WithLifecycleDefaults fills
	// Fatalf when unset (Exec is already defaulted, so its re-default is a no-op).
	lifecycleCfg := lifecycle.Config{
		ExecConfig:      cfg.Exec,
		RetentionChunks: cfg.RetentionChunks,
		Fatalf:          cfg.Fatalf,
	}.WithLifecycleDefaults()
	var lifecycleWG sync.WaitGroup
	lifecycleWG.Go(func() {
		lifecycle.Loop(lifecycleCtx, lifecycleCfg, cat, lifecycleCh)
	})
	// The two return paths registered after this defer (the ingestion-loop return
	// and the ServeReads error path) have no live sender on lifecycleCh — ingestion
	// is a same-goroutine call whose inline notify has stopped, and the serve path
	// never starts it — so no send can race the cancel.
	defer func() {
		cancelLifecycle()
		lifecycleWG.Wait()
	}()

	// Begin serving reads (injected). It must return promptly (launch, not block).
	if err := cfg.ServeReads(ctx); err != nil {
		_ = hotDB.Close()
		return fmt.Errorf("startup serve reads: %w", err)
	}

	// The ingestion loop owns hotDB for the rest of its life (closes it on any exit,
	// reopens at each boundary). Returns the GetLedger/boundary error; the daemon top
	// level classifies a ctx-canceled return as a clean shutdown.
	return runIngestionLoop(ctx, core, hotDB, cat, lifecycleCh, logger, metrics, cfg.Exec.Process.Sink)
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
		// leaves the partial resume chunk to ingestion. Signed so genesis reads as a boundary.
		if withinOneChunkOfTip(tip, lastCommitted) && lastCommittedMidChunk(lastCommitted) {
			rangeEndSigned = lifecycle.ChunkIDOfLedger(lastCommitted) - 1 // one short of the live chunk
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
		metrics.LastCommitted(lastCommitted, lifecycle.EffectiveRetentionFloor(lastCommitted, retentionChunks, earliest))
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
	c := lifecycle.ChunkIDOfLedger(lastCommitted)
	return lastCommitted != lifecycle.CompleteThrough(c)
}

// ---------------------------------------------------------------------------
// Injected external boundaries (so startup is testable with fakes).
// ---------------------------------------------------------------------------

// NetworkTipBackend samples the bulk backend's current network tip during backfill.
// It is consulted only during backfill; once ingestion runs, captive core is the tip.
type NetworkTipBackend interface {
	NetworkTip(ctx context.Context) (uint32, error)
}

// CoreOpener prepares captive core at resumeLedger and hands back a LedgerGetter
// the ingestion loop polls plus a closer the caller defers. Production wraps
// captive core's PrepareRange + GetLedger; tests pass a fake getter.
type CoreOpener interface {
	OpenCore(ctx context.Context, resumeLedger uint32) (LedgerGetter, func() error, error)
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

	// Fatalf aborts the daemon on a lifecycle tick op failure; nil ⇒ the
	// lifecycle default (log.Fatalf). Tests override it.
	Fatalf func(format string, args ...any)

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
// (Workers -> GOMAXPROCS). The lifecycle.Config (including its Fatalf default) is
// assembled from Exec + RetentionChunks + Fatalf in run().
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
