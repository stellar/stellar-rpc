package streaming

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// startStreaming is the daemon's startup orchestration — the design's "Daemon
// flow -> Startup", in two steps:
//
//  1. CATCH UP via backfill. Bring on-disk coverage in line with the retention
//     window: each pass backfills up through the last complete chunk at the
//     network tip, re-passing while new chunks appear at the tip, with one
//     exclusion — a mid-chunk watermark within one chunk of the tip leaves the
//     partial resume chunk to ingestion (core replays its tail faster than a
//     bulk refetch, and a mid-chunk watermark can only have come from the live
//     hot DB, so the data is local by construction). runBackfill is the SAME
//     resolve + executePlan the lifecycle tick uses (Phase B); there is no
//     upfront producibility gate — each chunk's producibility is enforced
//     lazily during its build by the cold ingest.
//
//  2. SERVE + INGEST. Open the resume chunk's hot DB (Issue 10), start captive
//     core (injected), launch the lifecycle goroutine (Issue 11) on a doorbell,
//     start serving reads (injected), and run the ingestion loop (Issue 10).
//     The ingestion loop's first act is a doorbell ring, so the first lifecycle
//     tick doubles as startup convergence (finishing crash leftovers + pruning
//     downtime leftovers concurrently with early serving).
//
// EVERYTHING the daemon needs that startup cannot construct itself crosses an
// INJECTED interface (StartConfig.NetworkTip, .Core, .ServeReads), so this is
// unit-testable without captive core, a real bulk backend, or a real RPC
// server. validateConfig (the full TOML form) is Phase D; this accepts an
// already-resolved StartConfig and the pinned earliest_ledger is read from the
// catalog.
//
// It returns nil only on a clean shutdown (ctx canceled mid-run, or the
// ingestion loop's clean stop); any other return is restartable error the
// daemon's top-level loop surfaces (ErrFirstStartNoTip on a true first start
// with no reachable backend; a backfill/ingest failure; ErrHotVolumeLost).
func startStreaming(ctx context.Context, cfg StartConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	cfg = cfg.withDefaults()
	cat := cfg.Exec.Catalog
	logger := cfg.Exec.Logger

	// earliest_ledger is pinned by validateConfig BEFORE startStreaming runs (the
	// design's flow; the full TOML form is Phase D). It must be present here: the
	// loop's first-start predicate is `lastCommitted < earliest`, which only
	// classifies correctly when earliest is the real pinned floor (e.g. genesis
	// pins earliest=2, the watermark sentinel preGenesisLedger=1 sits below it).
	// An absent pin would read as 0 and mis-classify a genuine first start as a
	// degrade-and-serve restart, so refuse it loudly rather than silently.
	earliest, pinned, err := cat.EarliestLedger()
	if err != nil {
		return fmt.Errorf("streaming: startup read earliest ledger: %w", err)
	}
	if !pinned {
		return errors.New("streaming: startup requires config:earliest_ledger pinned " +
			"(validateConfig pins it before startStreaming; not done here)")
	}

	// Derived, never stored: the highest ledger durably committed (frozen cold
	// artifacts vs the highest ready hot DB's max committed seq, clamped by
	// earliest-1). With a probe it does ONE read of the highest ready hot DB and
	// detects hot-volume loss LAZILY on that open (ErrHotVolumeLost) before
	// ingestion ever opens a writer.
	lastCommitted, err := lastCommittedLedger(cat, cfg.Exec.Process.HotProbe)
	if err != nil {
		return fmt.Errorf("streaming: startup derive watermark: %w", err)
	}

	metrics := cfg.Exec.metrics()
	metrics.Watermark(lastCommitted, effectiveRetentionFloor(lastCommitted, cfg.Lifecycle.RetentionChunks, earliest))
	logger.WithField("last_committed", lastCommitted).
		WithField("earliest", earliest).
		WithField("pinned", pinned).
		Info("streaming: startup — watermark derived, beginning catch-up")

	// Step 1: catch up via backfill.
	lastCommitted, err = catchUp(ctx, cfg, lastCommitted, earliest)
	if err != nil {
		return err
	}

	logger.WithField("last_committed", lastCommitted).
		WithField("resume_chunk", chunk.IDFromLedger(lastCommitted+1).String()).
		Info("streaming: catch-up complete — opening resume hot tier and ingesting")

	// Step 2: serve + ingest. resumeLedger is one past the watermark — the live
	// chunk's next un-committed ledger (or the chunk's first ledger on an empty
	// resume DB; runIngestionLoop re-derives the exact resume point from durable
	// state, so a lastCommitted that lands mid-chunk and a lastCommitted on a
	// chunk boundary both resume correctly).
	resumeLedger := lastCommitted + 1
	resumeChunk := chunk.IDFromLedger(resumeLedger)

	hotDB, err := openHotTierForChunk(cat, resumeChunk, logger)
	if err != nil {
		return fmt.Errorf("streaming: startup open resume hot tier chunk %s: %w", resumeChunk, err)
	}

	// Start captive core from the resume ledger. On failure the resume hot DB is
	// already open; close it so a restart re-opens cleanly (the bracket is
	// idempotent, but the rocksdb LOCK must be released).
	core, closeCore, err := cfg.Core.OpenCore(ctx, resumeLedger)
	if err != nil {
		_ = hotDB.Close()
		return fmt.Errorf("streaming: startup start captive core at ledger %d: %w", resumeLedger, err)
	}
	defer func() {
		if closeCore != nil {
			_ = closeCore()
		}
	}()

	// The lifecycle goroutine runs one tick per notification, carrying the just-
	// completed chunk id. Buffered to lifecycleQueueDepth; the ingestion loop
	// sends at every chunk boundary. It shares NO in-memory state with ingestion —
	// it derives everything from durable keys.
	lifecycleCh := make(chan chunk.ID, lifecycleQueueDepth)

	// Seed the first tick with the last complete chunk at the resume point so its
	// run fires at once — clearing crash/downtime leftovers concurrently with
	// serving (the design's startup seed: lastCompleteChunkAt(resumeLedger - 1)).
	// Skipped on a young network where no chunk is complete (nothing to converge;
	// the first real boundary triggers the first tick).
	if seed := lastCompleteChunkAt(lastCommitted); seed >= 0 {
		lifecycleCh <- chunk.ID(seed) //nolint:gosec // seed >= 0
	}

	// The lifecycle goroutine is tied to a PER-ITERATION child ctx, not the
	// daemon-lifetime ctx, and is canceled + JOINED before startStreaming returns
	// for ANY reason. This restores the design's single-lifecycle-goroutine
	// invariant: startStreaming returns on a restartable error (a captive-core /
	// GetLedger hiccup, a boundary hot-DB open failure) and superviseStreaming
	// restarts it with the SAME live daemon ctx after a backoff — so if the
	// lifecycle were tied to the daemon ctx, the prior iteration's loop would never
	// be canceled and would leak (blocked forever on the old channel) or, worse,
	// run a tick CONCURRENTLY with the next iteration's lifecycle + ingestion (two
	// RunColdChunk passes truncating the same .pack/.idx; a stale tick's op error
	// firing Fatalf). runLifecycleTick checks ctx at every step and executePlan
	// returns on cancellation, so the join cannot block past the current step.
	lifecycleCtx, cancelLifecycle := context.WithCancel(ctx)
	var lifecycleWG sync.WaitGroup
	lifecycleWG.Go(func() {
		lifecycleLoop(lifecycleCtx, cfg.Lifecycle, cat, lifecycleCh)
	})
	// Cancel + join the lifecycle goroutine. This defer runs only on the two return
	// paths registered after it: the ingestion-loop return (ingestion is a
	// synchronous same-goroutine call whose inline notify is the sole writer to
	// lifecycleCh, so it has already stopped) and the ServeReads error path
	// (ingestion never started). Either way no send on lifecycleCh can race the
	// cancel. The earlier error paths (resume hot-DB open, OpenCore) return BEFORE
	// this defer is registered and before the goroutine starts — nothing to join.
	defer func() {
		cancelLifecycle()
		lifecycleWG.Wait()
	}()

	// Begin serving reads (injected). Serve-readiness is established by step 1
	// plus the resume chunk's hot DB just opened — crash debris and downtime
	// leftovers are reader-invisible, so the first tick clears them concurrently
	// with serving rather than ahead of it.
	if err := cfg.ServeReads(ctx); err != nil {
		_ = hotDB.Close()
		return fmt.Errorf("streaming: startup serve reads: %w", err)
	}

	// The ingestion loop owns hotDB for the rest of its life (it closes it on any
	// exit and reopens at each boundary). Returns the GetLedger/boundary error;
	// the daemon top level classifies a ctx-canceled return as a clean shutdown.
	return runIngestionLoop(ctx, core, hotDB, cat, lifecycleCh, allHotTypes, logger, metrics)
}

// catchUp runs the design's catch-up loop, mutating and returning lastCommitted
// as backfill makes progress. It samples networkTip each pass (degrading to
// lastCommitted on a transient backend error, FATAL via ErrFirstStartNoTip when
// there is no local history to serve either), anchors on max(tip, lastCommitted)
// to guard a lagging bulk tip, computes the [rangeStart, rangeEnd] window with
// the mid-chunk resume exclusion, and breaks on an empty/already-done range.
//
// backfilledThrough guards against infinite re-passes when the tip stops moving:
// a rangeEnd that does not advance past the previous pass breaks the loop.
func catchUp(ctx context.Context, cfg StartConfig, lastCommitted, earliest uint32) (uint32, error) {
	retentionChunks := cfg.Lifecycle.RetentionChunks
	metrics := cfg.Exec.metrics()
	logger := cfg.Exec.Logger

	backfilledThrough := int64(-1)
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		tip, err := networkTip(ctx, cfg.NetworkTip, cfg.TipBackoff, cfg.TipMaxAttempts)
		if err != nil {
			if lastCommitted < earliest {
				// True first start (no committed progress) with no reachable backend:
				// we can neither catch up nor serve local history. FATAL — never
				// start serving on empty/incomplete history. Returned as a sentinel
				// (not a process exit) so the daemon's top-level loop owns the
				// fatal-and-surface decision and the supervisor restarts; networkTip
				// retries on the next process start.
				return 0, fmt.Errorf("%w: %w", ErrFirstStartNoTip, err)
			}
			// Restart with local progress: the window below lastCommitted is
			// complete (catch-up-before-advance), so serve what is materialized and
			// skip catch-up this pass. A later pass with a reachable backend resumes
			// extending the bottom of storage.
			tip = lastCommitted
		}

		// max() guards a lagging bulk tip in BOTH uses below: anchored on the tip
		// alone, the floor would regress below where pruning advanced, and a
		// complete watermark chunk could fall outside the range. When the tip leads
		// (long downtime) it is the correct anchor.
		anchor := maxU32(tip, lastCommitted)
		rangeStart := chunk.IDFromLedger(effectiveRetentionFloor(anchor, retentionChunks, earliest))

		// rangeEnd anchored on the same max() so a complete watermark chunk above a
		// lagging bulk tip still folds into its window's index before serving. The
		// span beyond the bulk tip is only durable chunks (production self-skips) or
		// complete-in-hot-DB chunks (backfillSource's hot branch) — the bulk backend
		// is never asked for them.
		rangeEndSigned := lastCompleteChunkAt(anchor)

		// Mid-chunk resume exclusion: a mid-chunk watermark within one chunk of the
		// tip leaves the partial resume chunk to ingestion. watermarkMidChunk is
		// computed in the SIGNED domain so the genesis sentinel (lastCommitted =
		// earliest-1, chunk-aligned by construction) reads as a boundary, never
		// spuriously mid-chunk.
		if withinOneChunkOfTip(tip, lastCommitted) && watermarkMidChunk(lastCommitted) {
			// rangeEnd = chunkID(lastCommitted) - 1: stop one short of the live chunk.
			rangeEndSigned = chunkIDOfLedger(lastCommitted) - 1
		}

		// Lag/progress gauges each pass: the live tip-vs-watermark gap and where
		// catch-up has reached vs its target (the tip-anchored upper bound).
		metrics.IngestionLag(tip, lastCommitted)
		metrics.CatchupProgress(lastCommitted, anchor)

		// Break on an empty range (rangeEnd < rangeStart — a young network, or the
		// exclusion left nothing) or a non-advancing one (rangeEnd <=
		// backfilledThrough — the tip stopped moving).
		if rangeEndSigned < int64(rangeStart) || rangeEndSigned <= backfilledThrough {
			break
		}
		rangeEnd := chunk.ID(rangeEndSigned) //nolint:gosec // > rangeStart >= 0

		logger.WithField("range_lo", rangeStart.String()).
			WithField("range_hi", rangeEnd.String()).
			WithField("tip", tip).
			WithField("last_committed", lastCommitted).
			Info("streaming: catch-up pass starting")

		passStart := time.Now()
		if err := runBackfill(ctx, cfg.Exec, rangeStart, rangeEnd); err != nil {
			return 0, fmt.Errorf("streaming: startup backfill [%s,%s]: %w", rangeStart, rangeEnd, err)
		}
		passDuration := time.Since(passStart)

		// Advance the mutating watermark to the last ledger of the backfilled range
		// (never regress — a lagging tip's rangeEnd can sit below lastCommitted).
		lastCommitted = maxU32(lastCommitted, rangeEnd.LastLedger())
		backfilledThrough = rangeEndSigned

		metrics.CatchupPass(uint32(rangeStart), uint32(rangeEnd), passDuration)
		metrics.CatchupProgress(lastCommitted, anchor)
		logger.WithField("range_lo", rangeStart.String()).
			WithField("range_hi", rangeEnd.String()).
			WithField("last_committed", lastCommitted).
			WithField("duration", passDuration.String()).
			Info("streaming: catch-up pass complete")
	}
	return lastCommitted, nil
}

// withinOneChunkOfTip reports whether the watermark sits within one chunk of the
// tip. SIGNED so a lagging bulk tip BELOW the resume point (tip < lastCommitted)
// yields a negative difference < LedgersPerChunk and reads true — the watermark
// is then certainly the live (near-tip) chunk's, the exclusion's intent.
func withinOneChunkOfTip(tip, lastCommitted uint32) bool {
	return int64(tip)-int64(lastCommitted) < int64(chunk.LedgersPerChunk)
}

// watermarkMidChunk reports whether lastCommitted falls strictly inside a chunk
// (not on its last ledger). The genesis sentinel (preGenesisLedger) maps via
// chunkIDOfLedger to chunk -1 whose "last ledger" is preGenesisLedger, so the
// sentinel reads as a boundary — never spuriously mid-chunk.
func watermarkMidChunk(lastCommitted uint32) bool {
	c := chunkIDOfLedger(lastCommitted)
	return lastCommitted != completeThrough(c)
}

// maxU32 is the unsigned max the catch-up arithmetic uses (the built-in max
// works, but a named helper keeps the anchor/advance call sites self-documenting
// alongside the signed helpers above).
func maxU32(a, b uint32) uint32 { return max(a, b) }

// ErrFirstStartNoTip is the first-start FATAL: no committed local progress AND
// no reachable network tip, so the daemon can neither catch up nor serve a local
// history. Returned as a sentinel (not a process exit) so the daemon's top-level
// loop owns the fatal-and-surface decision and tests can assert it; the
// supervisor restarts and networkTip retries on the next process start.
var ErrFirstStartNoTip = errors.New("streaming: network tip unavailable and no local history to serve")

// ---------------------------------------------------------------------------
// Injected external boundaries. startStreaming touches NOTHING outside the
// process directly: the network tip, captive core, and the read server all
// cross an interface so startup is exercised end to end with fakes.
// ---------------------------------------------------------------------------

// NetworkTipBackend samples the configured bulk backend's current network tip
// (the highest ledger the backend can serve). Production wraps the daemon's
// LedgerBackend; tests pass a fake that is reachable / unreachable / unready.
// It is consulted only during catch-up; once ingestion runs, captive core is
// the tip.
type NetworkTipBackend interface {
	NetworkTip(ctx context.Context) (uint32, error)
}

// CoreOpener prepares captive core at resumeLedger and hands back a LedgerGetter
// the ingestion loop polls plus a closer the caller defers. Production wraps
// captive core's PrepareRange + GetLedger; tests pass a fake getter. The closer
// tears down the backend on daemon exit.
type CoreOpener interface {
	OpenCore(ctx context.Context, resumeLedger uint32) (LedgerGetter, func() error, error)
}

// StartConfig is startStreaming's resolved dependency bundle. It composes the
// scheduler/lifecycle configs (so catch-up and the lifecycle goroutine share one
// catalog, worker pool, and retention floor) and the three injected external
// boundaries, plus the networkTip backoff bounds. The full daemon Config
// (TOML-parsed paths, captive-core toml, …) is a superset assembled at the call
// site; only what startup reads lives here.
type StartConfig struct {
	// Exec drives catch-up's runBackfill (resolve + executePlan). Its Catalog and
	// Logger are the shared ones the whole startup reads.
	Exec ExecConfig

	// Lifecycle drives the lifecycle goroutine. Its embedded ExecConfig should be
	// the SAME wiring as Exec (one catalog, one pool); RetentionChunks is the
	// catch-up floor's width too.
	Lifecycle LifecycleConfig

	// NetworkTip samples the bulk backend's tip during catch-up. Required.
	NetworkTip NetworkTipBackend

	// Core starts captive core and yields the ingestion getter. Required.
	Core CoreOpener

	// ServeReads begins serving reads (the RPC server). It must return promptly
	// (it launches the server; it does not block until shutdown) — startup
	// proceeds to the blocking ingestion loop after it returns. Required.
	ServeReads func(ctx context.Context) error

	// TipBackoff is networkTip's inter-attempt sleep; TipMaxAttempts bounds the
	// retries against a transiently-unavailable backend before networkTip returns
	// an error (which catch-up then classifies first-start-fatal vs degrade). Zero
	// values fall back to defaults in withDefaults.
	TipBackoff     time.Duration
	TipMaxAttempts int
}

const (
	defaultTipBackoff     = time.Second
	defaultTipMaxAttempts = 5
)

// withDefaults fills the worker-pool / lifecycle / tip-backoff defaults. The
// embedded ExecConfig defaults (Workers -> GOMAXPROCS) and the LifecycleConfig
// Fatalf default are applied so a caller need not.
func (cfg StartConfig) withDefaults() StartConfig {
	cfg.Exec = cfg.Exec.WithDefaults()
	cfg.Lifecycle = cfg.Lifecycle.WithLifecycleDefaults()
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
		return errors.New("streaming: StartConfig.Exec.Catalog is nil")
	}
	if cfg.Exec.Logger == nil {
		return errors.New("streaming: StartConfig.Exec.Logger is nil")
	}
	if cfg.Exec.Process.HotProbe == nil {
		return errors.New("streaming: StartConfig.Exec.Process.HotProbe is nil (watermark derivation needs it)")
	}
	if cfg.NetworkTip == nil {
		return errors.New("streaming: StartConfig.NetworkTip is nil")
	}
	if cfg.Core == nil {
		return errors.New("streaming: StartConfig.Core is nil")
	}
	if cfg.ServeReads == nil {
		return errors.New("streaming: StartConfig.ServeReads is nil")
	}
	return nil
}

// networkTip samples backend.NetworkTip, hardened against the two ways the tip
// lies: it retries on a transient error with a fixed backoff (bounded by
// maxAttempts), and rejects a tip below genesis as "not ready" (an empty /
// not-yet-synced backend) so an unready tip never reaches the chunk arithmetic
// where it would pin a garbage floor. ctx cancellation aborts the wait
// immediately. The catch-up loop has a local substitute (lastCommitted) and
// degrades on the returned error EXCEPT on a true first start, where it fatals.
func networkTip(
	ctx context.Context, backend NetworkTipBackend, backoff time.Duration, maxAttempts int,
) (uint32, error) {
	var lastErr error
	for attempt := range maxAttempts {
		if attempt > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return 0, ctx.Err()
			case <-timer.C:
			}
		}
		tip, err := backend.NetworkTip(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		if tip < chunk.FirstLedgerSeq {
			// Genesis is the lowest valid tip; below it the backend is empty or not
			// yet synced. Treated as not-ready (an error catch-up classifies), NOT
			// retried — a synced-from-empty backend would just keep returning 0.
			return 0, fmt.Errorf("streaming: backend tip %d is below genesis %d — backend not ready",
				tip, chunk.FirstLedgerSeq)
		}
		return tip, nil
	}
	return 0, fmt.Errorf("streaming: network tip unavailable after %d attempts: %w", maxAttempts, lastErr)
}
