package streaming

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"golang.org/x/sync/errgroup"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ExecConfig is the scheduler's dependency bundle — everything resolve,
// executePlan, and runBackfill read. It COMPOSES process.go's ProcessConfig
// (which drives processChunk + backfillSource) rather than redeclaring its
// fields, and adds the two scheduler knobs. The Catalog and Logger are shared,
// so they live here and are projected down to the primitive; the rest of the
// primitive config (HotProbe, Backend, …) is carried verbatim.
//
// This is the "one Config" the design's resolve/executePlan/runBackfill
// pseudocode reads `cfg.Catalog`, `cfg.Workers`, and `cfg.MaxRetries` from; the
// full daemon Config (retention, captive core, paths) is a superset assembled
// at startup and is out of this issue's scope.
type ExecConfig struct {
	Catalog *Catalog
	Logger  *supportlog.Entry

	// Metrics is the streaming control-plane sink (observability.go) shared by
	// backfill, the ingestion loop, and the lifecycle tick. nil ⇒ nopMetrics via
	// WithDefaults, so every phase reports unconditionally. It is the DAEMON's
	// phase sink, distinct from Process.Sink (the per-data-type ingest sink).
	Metrics Metrics

	// Process carries the primitive-specific dependencies. Its Catalog and
	// Logger fields are filled from the shared ones above by the projection
	// accessor, so a caller need not duplicate them.
	Process ProcessConfig

	// Workers is the ONLY concurrency knob: the size of the single bounded pool
	// every chunk build draws from. Must be > 0 — a zero pool deadlocks
	// executePlan (every task blocks acquiring a slot that never frees).
	// Defaults to GOMAXPROCS via WithDefaults.
	Workers int

	// MaxRetries bounds per-task retries before a task aborts the whole plan
	// (and, in production, the daemon). 0 means "try once, no retry".
	MaxRetries int

	// runChunk is a test-only seam: when nil (production) the executor runs the
	// real processChunk. Tests override it to drive the failure paths
	// deterministically without standing up the full ingestion pipeline. It
	// never appears in production wiring.
	runChunk func(ctx context.Context, cb ChunkBuild, cfg ExecConfig) error
}

// WithDefaults returns a copy of cfg with Workers defaulted to GOMAXPROCS when
// unset. Validation (Workers > 0, non-nil deps) is validate's job.
func (cfg ExecConfig) WithDefaults() ExecConfig {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.Metrics == nil {
		cfg.Metrics = nopMetrics{}
	}
	return cfg
}

// metrics returns the configured sink, or nopMetrics when unset — the read every
// phase uses so it never nil-checks (WithDefaults fills it for the daemon path,
// but a primitive called directly in a test may not have run WithDefaults).
func (cfg ExecConfig) metrics() Metrics { return metricsOrNop(cfg.Metrics) }

func (cfg ExecConfig) validate() error {
	if cfg.Catalog == nil {
		return errors.New("streaming: ExecConfig.Catalog is nil")
	}
	if cfg.Logger == nil {
		return errors.New("streaming: ExecConfig.Logger is nil")
	}
	if cfg.Workers <= 0 {
		// Loud, not silently corrected: a zero pool deadlocks executePlan, so the
		// caller's miswiring must surface rather than hang.
		return fmt.Errorf(
			"streaming: ExecConfig.Workers must be > 0 (got %d) — a zero pool deadlocks executePlan", cfg.Workers,
		)
	}
	return nil
}

// processConfig projects the ExecConfig down to the ProcessConfig processChunk
// reads, filling the shared Catalog/Logger so callers configure them once.
func (cfg ExecConfig) processConfig() ProcessConfig {
	p := cfg.Process
	p.Catalog = cfg.Catalog
	p.Logger = cfg.Logger
	return p
}

// executePlan runs a Plan on one bounded worker pool (cfg.Workers — the only
// resource knob). It is the SAME executor both callers use: runBackfill (catch-
// up) and the lifecycle tick. The structure is map without a job tracker —
// chunk builds are the maps — and there is deliberately no task engine and no
// persisted task state: resolve re-plans from durable keys on every run, so
// there is nothing to resume.
//
// Each ChunkBuild acquires a worker slot, runs (with retries), and on SUCCESS
// closes its done-channel AFTER its artifacts are durable (done-channels signal
// SUCCESS, not mere completion). A build that exhausts its retries LEAVES the
// channel open and RETURNS the error, which cancels gctx.
//
// At most Workers chunk builds execute at any instant. A task exhausting its
// retries returns an error, which errgroup propagates: gctx is canceled, every
// other task's slot-acquire/processChunk observes it, and g.Wait returns the
// first error — the daemon aborts and a restart re-resolves from durable keys.
func executePlan(ctx context.Context, plan Plan, cfg ExecConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	// One slot per worker — the single pool all chunk builds share.
	slots := make(chan struct{}, cfg.Workers)

	// One done-channel per planned chunk build, created up front.
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

	g, gctx := errgroup.WithContext(ctx)

	for _, cb := range plan.ChunkBuilds {
		g.Go(func() error {
			if err := acquireSlot(gctx, slots); err != nil {
				return err
			}
			defer releaseSlot(slots)
			if err := withRetries(gctx, cfg.MaxRetries, func() error {
				return runChunk(gctx, cb, cfg)
			}); err != nil {
				// SUCCESS semantics: leave done[cb.Chunk] OPEN and return the error.
				// errgroup cancels gctx and g.Wait returns the first error.
				return err
			}
			// Success: artifacts are durable.
			close(done[cb.Chunk])
			return nil
		})
	}

	return g.Wait()
}

// acquireSlot blocks until a worker slot is free or ctx is canceled. Pulling it
// out of the goroutine bodies keeps the cancel-vs-acquire select in one place.
func acquireSlot(ctx context.Context, slots chan struct{}) error {
	select {
	case slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseSlot frees a previously-acquired worker slot. It never blocks (the
// buffer always has room for a token this goroutine put there).
func releaseSlot(slots chan struct{}) { <-slots }

// withRetries runs fn up to maxRetries+1 times (one attempt plus maxRetries
// retries), returning nil on the first success and the last error after the
// budget is exhausted. A canceled ctx stops retrying immediately — once the
// errgroup cancels gctx (a sibling task aborted), there is no point burning
// this task's retry budget against a doomed context.
func withRetries(ctx context.Context, maxRetries int, fn func() error) error {
	var err error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if err = fn(); err == nil {
			return nil
		}
	}
	return err
}

// runBackfill is backfill's entry point: resolve the missing work, then
// executePlan over the resolver's diff. It is the SAME executePlan the lifecycle
// tick uses — one scheduler, two callers, sharing one set of postconditions.
//
// There is NO upfront producibility gate (item R2-5 / the design "folded the
// upfront gate into the per-chunk bounded wait"): a genuinely unproducible chunk
// — no local copy and no configured bulk backend — fatals from backfillSource
// itself when the executor reaches that chunk, on every retry. backfillSource's
// bounded WaitForCoverage handles a fall-through chunk above a lagging-but-
// advancing backend per chunk. The daemon therefore still fatals on an
// unproducible chunk; only the surface point moved from a pre-flight check to
// the per-chunk source selection (see the return note for the narrowing flag).
func runBackfill(ctx context.Context, cfg ExecConfig, rangeStart, rangeEnd chunk.ID) error {
	cfg = cfg.WithDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	plan, err := resolve(cfg, rangeStart, rangeEnd)
	if err != nil {
		return fmt.Errorf("streaming: runBackfill resolve [%s,%s]: %w", rangeStart, rangeEnd, err)
	}
	return executePlan(ctx, plan, cfg)
}
