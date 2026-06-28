package backfill

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ExecConfig is the scheduler's dependency bundle: the two primitive configs plus
// the scheduler knobs. Shared Catalog/Logger are projected down to each primitive.
type ExecConfig struct {
	Catalog *catalog.Catalog
	Logger  *supportlog.Entry

	// Metrics is the daemon's phase sink (distinct from Process.Sink); nil ⇒ NopMetrics.
	Metrics observability.Metrics

	Process ProcessConfig
	Build   BuildConfig

	// Workers sizes the single bounded pool; must be > 0 (a zero pool deadlocks). Defaults to GOMAXPROCS.
	Workers int

	// MaxRetries bounds per-task retries; 0 = try once.
	MaxRetries int

	// RetryBackoff is the base inter-retry delay (doubles, capped at maxRetryBackoff); 0 ⇒ default.
	RetryBackoff time.Duration

	// runChunk/runIndex are test-only seams; nil runs the real processChunk/buildThenSweep.
	runChunk func(ctx context.Context, cb ChunkBuild) error
	runIndex func(ctx context.Context, b IndexBuild) error
}

const (
	defaultRetryBackoff = 1 * time.Second
	maxRetryBackoff     = 30 * time.Second // caps the exponential growth
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

// metrics returns the configured sink, or NopMetrics when unset.
func (cfg ExecConfig) metrics() observability.Metrics { return observability.MetricsOrNop(cfg.Metrics) }

func (cfg ExecConfig) validate() error {
	if cfg.Catalog == nil {
		return errors.New("nil ExecConfig.Catalog")
	}
	if cfg.Logger == nil {
		return errors.New("nil ExecConfig.Logger")
	}
	if cfg.Workers <= 0 {
		return fmt.Errorf("invalid pool size: Workers must be > 0 (got %d) — a zero pool deadlocks executePlan", cfg.Workers)
	}
	return nil
}

// processConfig projects the shared Catalog/Logger into the ProcessConfig.
func (cfg ExecConfig) processConfig() ProcessConfig {
	p := cfg.Process
	p.Catalog = cfg.Catalog
	p.Logger = cfg.Logger
	return p
}

// buildConfig projects the shared Catalog/Logger/Metrics into the BuildConfig.
func (cfg ExecConfig) buildConfig() BuildConfig {
	b := cfg.Build
	b.Catalog = cfg.Catalog
	b.Logger = cfg.Logger
	b.Metrics = cfg.Metrics
	return b
}

// executePlan runs a Plan on one bounded worker pool. No persisted state: resolve
// re-plans from durable keys every run, so there is nothing to resume.
//
// The one dependency edge — an IndexBuild needs the ChunkBuilds inside its coverage:
// a ChunkBuild closes its done-channel only on success; an IndexBuild waits on its
// in-coverage channels FIRST (holding no slot), THEN acquires one. Wait-before-acquire
// avoids deadlock — a parked index build holds no slot, so chunk builds always
// progress; a failed chunk leaves its channel open, so dependents bail via gctx
// rather than run on a missing input. Any exhausted-retry task cancels gctx and
// g.Wait surfaces the first error; the daemon restarts and re-resolves.
func executePlan(ctx context.Context, plan Plan, cfg ExecConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	slots := make(chan struct{}, cfg.Workers) // one per worker, shared by all task kinds

	// One done-channel per chunk build, up front so index builds can look up
	// dependencies before any goroutine runs.
	done := make(map[chunk.ID]chan struct{}, len(plan.ChunkBuilds))
	for _, cb := range plan.ChunkBuilds {
		done[cb.Chunk] = make(chan struct{})
	}

	runChunk := cfg.runChunk
	if runChunk == nil {
		procCfg := cfg.processConfig()
		runChunk = func(gctx context.Context, cb ChunkBuild) error {
			return processChunk(gctx, cb.Chunk, cb.Artifacts, procCfg)
		}
	}
	runIndex := cfg.runIndex
	if runIndex == nil {
		buildCfg := cfg.buildConfig()
		runIndex = func(gctx context.Context, b IndexBuild) error {
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
				return runChunk(gctx, cb)
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
			// Wait on in-coverage chunk builds FIRST, holding no slot. Dependencies are
			// derived from the plan (in-[Lo,Hi] chunks with a ChunkBuild), so they can't drift.
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
			if err := acquireSlot(gctx, slots); err != nil {
				return err
			}
			defer releaseSlot(slots)
			// Report rebuild throughput on completion (failure duration is signal too).
			start := time.Now()
			err := withRetries(gctx, cfg, func() error {
				return runIndex(gctx, b)
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

// withRetries runs fn up to MaxRetries+1 times with exponential backoff between
// attempts, aborting the wait on ctx cancellation. Built on cenkalti/backoff, the
// same primitive waitForCoverage uses.
func withRetries(ctx context.Context, cfg ExecConfig, fn func() error) error {
	return backoff.Retry(fn, backoff.WithContext(cfg.retryBackOff(), ctx))
}

// retryBackOff is the per-task retry policy: count-bounded (MaxRetries) exponential
// backoff, no jitter (deterministic). A fresh instance per call — BackOff is stateful.
func (cfg ExecConfig) retryBackOff() backoff.BackOff {
	base := cfg.RetryBackoff
	if base <= 0 {
		base = defaultRetryBackoff
	}
	bo := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(base),
		backoff.WithMultiplier(2),
		backoff.WithRandomizationFactor(0),
		backoff.WithMaxInterval(maxRetryBackoff),
		backoff.WithMaxElapsedTime(0), // count-bounded by MaxRetries, not time-bounded
	)
	var maxRetries uint64
	if cfg.MaxRetries > 0 {
		maxRetries = uint64(cfg.MaxRetries) //nolint:gosec // guarded > 0
	}
	return backoff.WithMaxRetries(bo, maxRetries)
}

// RunBackfill resolves the missing work, then executePlans the diff. No upfront
// producibility gate: an unproducible chunk fatals from backfillSource when the
// executor reaches it (its bounded coverage wait handles a lagging-but-advancing backend).
func RunBackfill(ctx context.Context, cfg ExecConfig, rangeStart, rangeEnd chunk.ID) error {
	cfg = cfg.WithDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	plan, err := resolve(cfg, rangeStart, rangeEnd)
	if err != nil {
		return fmt.Errorf("resolve plan [%s,%s]: %w", rangeStart, rangeEnd, err)
	}
	start := time.Now()
	err = executePlan(ctx, plan, cfg)
	// One freeze stage = the work actually done this pass (vs. CatchupPass's range
	// width); reported even on failure (partial size/duration is signal).
	cfg.metrics().Freeze(len(plan.ChunkBuilds), len(plan.IndexBuilds), time.Since(start))
	return err
}
