package backfill

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ExecConfig is the scheduler's dependency bundle, read by resolve, executePlan,
// and runBackfill. It composes the two primitive configs (ProcessConfig,
// BuildConfig) and adds the scheduler knobs; shared Catalog/Logger are projected
// down to each primitive.
type ExecConfig struct {
	Catalog *catalog.Catalog
	Logger  *supportlog.Entry

	// Metrics is the daemon's phase sink, distinct from Process.Sink (per-data-type
	// ingest). nil ⇒ NopMetrics via WithDefaults.
	Metrics observability.Metrics

	// Process and Build carry primitive-specific deps; their Catalog/Logger are
	// filled from the shared ones by the projection accessors.
	Process ProcessConfig
	Build   BuildConfig

	// Workers is the only concurrency knob: size of the single bounded pool every
	// task shares. Must be > 0 — a zero pool deadlocks executePlan. Defaults to
	// GOMAXPROCS via WithDefaults.
	Workers int

	// MaxRetries bounds per-task retries before a task aborts the whole plan. 0 =
	// try once, no retry.
	MaxRetries int

	// RetryBackoff is the base inter-retry delay: wait before retry N is
	// RetryBackoff << (N-1), capped at maxRetryBackoff. 0 ⇒ defaultRetryBackoff.
	RetryBackoff time.Duration

	// runChunk / runIndex are test-only seams; nil runs the real processChunk /
	// buildThenSweep.
	runChunk func(ctx context.Context, cb ChunkBuild, cfg ExecConfig) error
	runIndex func(ctx context.Context, b IndexBuild, cfg ExecConfig) error

	// retrySleep is a test-only seam for withRetries' backoff wait; nil ⇒ ctxSleep.
	retrySleep func(ctx context.Context, d time.Duration) error
}

const (
	// maxRetryBackoff caps the exponential growth so a high MaxRetries cannot wait
	// unboundedly.
	defaultRetryBackoff = 1 * time.Second
	maxRetryBackoff     = 30 * time.Second
)

// WithDefaults returns a copy of cfg with unset knobs filled in.
func (cfg ExecConfig) WithDefaults() ExecConfig {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.Metrics == nil {
		cfg.Metrics = observability.NopMetrics{}
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = defaultRetryBackoff
	}
	return cfg
}

// metrics returns the configured sink, or NopMetrics when unset, so a phase never
// nil-checks.
func (cfg ExecConfig) metrics() observability.Metrics { return observability.MetricsOrNop(cfg.Metrics) }

func (cfg ExecConfig) validate() error {
	if cfg.Catalog == nil {
		return errors.New("streaming: ExecConfig.Catalog is nil")
	}
	if cfg.Logger == nil {
		return errors.New("streaming: ExecConfig.Logger is nil")
	}
	if cfg.Workers <= 0 {
		// Loud, not silently corrected: a zero pool deadlocks executePlan.
		return fmt.Errorf("streaming: ExecConfig.Workers must be > 0 (got %d) — a zero pool deadlocks executePlan", cfg.Workers)
	}
	return nil
}

// processConfig projects the ExecConfig down to the ProcessConfig processChunk
// reads, filling the shared Catalog/Logger.
func (cfg ExecConfig) processConfig() ProcessConfig {
	p := cfg.Process
	p.Catalog = cfg.Catalog
	p.Logger = cfg.Logger
	return p
}

// buildConfig projects the ExecConfig down to the BuildConfig buildThenSweep reads.
func (cfg ExecConfig) buildConfig() BuildConfig {
	b := cfg.Build
	b.Catalog = cfg.Catalog
	b.Logger = cfg.Logger
	return b
}

// executePlan runs a Plan on one bounded worker pool, the same executor both
// callers use (runBackfill and the lifecycle tick). No task engine, no persisted
// state: resolve re-plans from durable keys every run, so there is nothing to
// resume.
//
// Two strata with one edge — an IndexBuild waits on the ChunkBuilds inside its
// coverage — expressed in the runtime:
//
//   - A ChunkBuild closes its done-channel only on success, after artifacts are
//     durable. On exhausted retries it leaves the channel open and returns the
//     error, cancelling gctx.
//   - An IndexBuild waits on its in-coverage chunks' done-channels FIRST (holding
//     no slot), THEN acquires a slot. Wait-before-acquire avoids deadlock: a
//     parked index build holds no slot, so chunk builds can always progress.
//   - A failed chunk never closes its channel, so a dependent index build unblocks
//     via <-gctx.Done() and bails rather than running on a missing input.
//
// A task exhausting its retries returns an error; errgroup cancels gctx and
// g.Wait returns the first error — the daemon aborts and a restart re-resolves.
func executePlan(ctx context.Context, plan Plan, cfg ExecConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	// One slot per worker — the single pool all task kinds share.
	slots := make(chan struct{}, cfg.Workers)

	// One done-channel per planned chunk build, created up front so an index build
	// can look up its dependencies before any goroutine runs.
	done := make(map[chunk.ID]chan struct{}, len(plan.ChunkBuilds))
	for _, cb := range plan.ChunkBuilds {
		done[cb.Chunk] = make(chan struct{})
	}

	runChunk := cfg.runChunk
	if runChunk == nil {
		procCfg := cfg.processConfig()
		runChunk = func(gctx context.Context, cb ChunkBuild, _ ExecConfig) error {
			return processChunk(gctx, cb.Chunk, cb.Artifacts, procCfg)
		}
	}
	runIndex := cfg.runIndex
	if runIndex == nil {
		buildCfg := cfg.buildConfig()
		runIndex = func(gctx context.Context, b IndexBuild, _ ExecConfig) error {
			return buildThenSweep(gctx, b, buildCfg)
		}
	}

	g, gctx := errgroup.WithContext(ctx)

	for _, cb := range plan.ChunkBuilds {
		g.Go(func() error {
			if err := acquireSlot(gctx, slots); err != nil {
				return err
			}
			defer releaseSlot(slots)
			if err := withRetries(gctx, cfg, func() error {
				return runChunk(gctx, cb, cfg)
			}); err != nil {
				// Leave done[cb.Chunk] open: dependents unblock via <-gctx.Done().
				return err
			}
			// Success: artifacts durable — unblock dependents to read the .bin.
			close(done[cb.Chunk])
			return nil
		})
	}

	for _, b := range plan.IndexBuilds {
		g.Go(func() error {
			// Wait on in-coverage chunk builds FIRST, holding no slot. Dependencies
			// are derived from the plan (every in-[Lo,Hi] chunk with a ChunkBuild),
			// not carried on the IndexBuild, so they can't drift.
			for c := b.Lo; ; c++ {
				if ch, ok := done[c]; ok {
					select {
					case <-ch:
					case <-gctx.Done():
						return gctx.Err()
					}
				}
				if c == b.Hi {
					break
				}
			}
			// Only now acquire a slot and run the build + eager sweep.
			if err := acquireSlot(gctx, slots); err != nil {
				return err
			}
			defer releaseSlot(slots)
			// Report rebuild throughput on completion (success or failure — a failed
			// rebuild's duration is signal).
			start := time.Now()
			err := withRetries(gctx, cfg, func() error {
				return runIndex(gctx, b, cfg)
			})
			cfg.metrics().Rebuild(int(b.Hi-b.Lo)+1, time.Since(start))
			return err
		})
	}

	return g.Wait()
}

// acquireSlot blocks until a worker slot is free or ctx is canceled.
func acquireSlot(ctx context.Context, slots chan struct{}) error {
	select {
	case slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseSlot frees a previously-acquired worker slot; never blocks.
func releaseSlot(slots chan struct{}) { <-slots }

// withRetries runs fn up to cfg.MaxRetries+1 times with exponential backoff,
// returning nil on first success and the last error once the budget is exhausted.
// A canceled ctx stops immediately so a task doesn't burn its budget against a
// gctx a sibling already aborted.
func withRetries(ctx context.Context, cfg ExecConfig, fn func() error) error {
	sleep := cfg.retrySleep
	if sleep == nil {
		sleep = ctxSleep
	}
	var err error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if attempt > 0 {
			// Wait before the retry (never before the first attempt).
			if serr := sleep(ctx, retryBackoffFor(attempt, cfg.RetryBackoff)); serr != nil {
				return serr
			}
		}
		if err = fn(); err == nil {
			return nil
		}
	}
	return err
}

// retryBackoffFor is the wait before retry attempt n (n >= 1): base << (n-1),
// capped at maxRetryBackoff.
func retryBackoffFor(attempt int, base time.Duration) time.Duration {
	if base <= 0 {
		base = defaultRetryBackoff
	}
	// Guard the shift against int64 overflow; past ~30 doublings it's at the cap.
	if attempt > 30 {
		return maxRetryBackoff
	}
	d := base << uint(attempt-1)
	if d <= 0 || d > maxRetryBackoff {
		return maxRetryBackoff
	}
	return d
}

// ctxSleep returns nil after d elapses, or ctx.Err() if canceled first.
func ctxSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// RunBackfill is backfill's entry point: resolve the missing work, then
// executePlan over the diff (the same executePlan the lifecycle tick uses).
//
// No upfront producibility gate: an unproducible chunk fatals from backfillSource
// when the executor reaches it. backfillSource's bounded WaitForCoverage handles a
// chunk above a lagging-but-advancing backend.
func RunBackfill(ctx context.Context, cfg ExecConfig, rangeStart, rangeEnd chunk.ID) error {
	cfg = cfg.WithDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	plan, err := resolve(cfg, rangeStart, rangeEnd)
	if err != nil {
		return fmt.Errorf("streaming: RunBackfill resolve [%s,%s]: %w", rangeStart, rangeEnd, err)
	}
	return executePlan(ctx, plan, cfg)
}
