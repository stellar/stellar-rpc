package streaming

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ExecConfig is the scheduler's dependency bundle — everything resolve,
// executePlan, and runBackfill read. It COMPOSES the two existing primitive
// configs (process.go's ProcessConfig drives processChunk + backfillSource;
// build.go's BuildConfig drives buildThenSweep) rather than redeclaring their
// fields, and adds the two scheduler knobs. The Catalog and Logger are shared,
// so they live here and are projected down to the primitives; the rest of each
// primitive config (HotProbe, Backend, BuildOpts, …) is carried verbatim.
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

	// Process and Build carry the primitive-specific dependencies. Their Catalog
	// and Logger fields are filled from the shared ones above by the projection
	// accessors, so a caller need not duplicate them.
	Process ProcessConfig
	Build   BuildConfig

	// Workers is the ONLY concurrency knob: the size of the single bounded pool
	// every task (chunk build or index build) draws from. Must be > 0 — a zero
	// pool deadlocks executePlan (every task blocks acquiring a slot that never
	// frees). Defaults to GOMAXPROCS via WithDefaults.
	Workers int

	// MaxRetries bounds per-task retries before a task aborts the whole plan
	// (and, in production, the daemon). 0 means "try once, no retry".
	MaxRetries int

	// runChunk / runIndex are test-only seams: when nil (production) the executor
	// runs the real processChunk / buildThenSweep. Tests override them to drive
	// the wait-ordering and failure paths deterministically without standing up
	// the full ingestion pipeline. They never appear in production wiring.
	runChunk func(ctx context.Context, cb ChunkBuild, cfg ExecConfig) error
	runIndex func(ctx context.Context, b IndexBuild, cfg ExecConfig) error
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
		return fmt.Errorf("streaming: ExecConfig.Workers must be > 0 (got %d) — a zero pool deadlocks executePlan", cfg.Workers)
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

// buildConfig projects the ExecConfig down to the BuildConfig buildThenSweep
// reads, filling the shared Catalog/Logger.
func (cfg ExecConfig) buildConfig() BuildConfig {
	b := cfg.Build
	b.Catalog = cfg.Catalog
	b.Logger = cfg.Logger
	return b
}

// executePlan runs a Plan on one bounded worker pool (cfg.Workers — the only
// resource knob). It is the SAME executor both callers use: runBackfill (catch-
// up) and the lifecycle tick. The structure is map/reduce without a job
// tracker — chunk builds are the maps, index builds are the per-group reduces —
// and there is deliberately no task engine and no persisted task state:
// resolve re-plans from durable keys on every run, so there is nothing to
// resume.
//
// The dependency graph is two strata with one edge type — an IndexBuild waits
// on the ChunkBuilds inside its coverage — expressed directly in the runtime:
//
//   - Each ChunkBuild closes a done-channel when it finishes. The close is in a
//     DEFER, so it fires whether the build succeeded OR exhausted its retries:
//     done-channels broadcast COMPLETION, not success.
//   - Each IndexBuild FIRST waits on the done-channels of the in-coverage
//     chunks that have a ChunkBuild in this plan (already-frozen inputs have no
//     channel and need no wait), THEN acquires a worker slot. Waiting before
//     acquiring is what avoids deadlock: a parked-on-its-dependency index build
//     holds no slot, so chunk builds always have slots to make progress. (The
//     reverse order — acquire then wait — could fill every slot with index
//     builds blocked on chunk builds that can never get a slot.)
//   - Because a failed chunk build still closes its channel, a dependent index
//     build can start; it then hits buildTxhashIndex's loud .bin precondition
//     (the input is not "frozen") and fails BEFORE writing any key, landing on
//     the same abort path as the original failure. That precondition is load-
//     bearing here.
//
// The "ready set" a DAG scheduler would maintain is simply the goroutines
// parked on the one semaphore; thousands of goroutines may exist (a few KB
// each), but at most Workers execute at any instant. A task exhausting its
// retries returns an error, which errgroup propagates: gctx is canceled, every
// other task's wait/slot-acquire/processChunk observes it, and g.Wait returns
// the first error — the daemon aborts and a restart re-resolves from durable
// keys.
func executePlan(ctx context.Context, plan Plan, cfg ExecConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	// One slot per worker — the single pool all task kinds share.
	slots := make(chan struct{}, cfg.Workers)

	// One done-channel per planned chunk build, created up front so an index
	// build can look up its in-coverage dependencies before any goroutine runs.
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
			// Completion broadcast — fires on success AND on exhausted retries, so
			// a dependent index build is never wedged waiting on a failed input.
			defer close(done[cb.Chunk])
			if err := acquireSlot(gctx, slots); err != nil {
				return err
			}
			defer releaseSlot(slots)
			return withRetries(gctx, cfg.MaxRetries, func() error {
				return runChunk(gctx, cb, cfg)
			})
		})
	}

	for _, b := range plan.IndexBuilds {
		g.Go(func() error {
			// Step 1 — wait on the in-coverage chunk builds FIRST, holding no slot.
			// Dependencies are DERIVED from the plan (every in-[Lo,Hi] chunk that
			// has a ChunkBuild), never carried on the IndexBuild, so they cannot
			// drift from what was actually scheduled.
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
			// Step 2 — only now acquire a slot (index builds draw from the same
			// pool) and run the build + eager sweep.
			if err := acquireSlot(gctx, slots); err != nil {
				return err
			}
			defer releaseSlot(slots)
			// Time the build and report its burst throughput — chunks folded into
			// one .idx over the wall-clock. Reported on completion (success OR
			// exhausted retries); a failed rebuild's duration is signal too.
			start := time.Now()
			err := withRetries(gctx, cfg.MaxRetries, func() error {
				return runIndex(gctx, b, cfg)
			})
			cfg.metrics().Rebuild(int(b.Hi-b.Lo)+1, time.Since(start))
			return err
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

// runBackfill is backfill's entry point: validate that the range is producible
// (a fall-through chunk needs a configured bulk source), then executePlan over
// the resolver's diff. It is the SAME executePlan the lifecycle tick uses — one
// scheduler, two callers, sharing one set of postconditions.
//
// validateRangeProducible fails BEFORE any work only if a fall-through chunk
// has NO configured source at all. It mirrors backfillSource's preference: a
// chunk needs the bulk backend only when it is not already durable (self-skips
// inside processChunk), not complete in a ready hot DB, and not re-derivable
// from a local .pack — so the check concerns only those fall-through chunks,
// NOT the whole range, and NOT backend-tip coverage (a fall-through chunk above
// a lagging-but-advancing backend is not-yet-producible, which backfillSource's
// bounded wait handles per chunk).
func runBackfill(ctx context.Context, cfg ExecConfig, rangeStart, rangeEnd chunk.ID) error {
	cfg = cfg.WithDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	if err := validateRangeProducible(cfg, rangeStart, rangeEnd); err != nil {
		return err
	}
	plan, err := resolve(cfg, rangeStart, rangeEnd)
	if err != nil {
		return fmt.Errorf("streaming: runBackfill resolve [%s,%s]: %w", rangeStart, rangeEnd, err)
	}
	return executePlan(ctx, plan, cfg)
}

// validateRangeProducible is runBackfill's pre-work gate. When a bulk Backend is
// configured every chunk has a source, so it passes immediately. When NO
// backend is configured it must prove every chunk the resolver would freeze can
// be produced locally — otherwise the backfill would abort mid-flight demanding
// chunks from a source that does not exist, on every retry.
//
// It mirrors backfillSource's source preference WITHOUT marking, writing, or
// holding the hot stores open (it is a pure pre-check): a planned ChunkBuild is
// locally producible iff
//
//	(a) its chunk's hot tier is "ready" AND complete (the MIN-of-three gate), or
//	(b) it does not request ledgers AND its frozen .pack exists on disk (re-derive).
//
// A chunk meeting neither is a genuine fall-through with no source — fatal.
// Chunks the resolver did not schedule (all kinds already frozen) need no
// source and are not examined.
func validateRangeProducible(cfg ExecConfig, rangeStart, rangeEnd chunk.ID) error {
	if cfg.Process.Backend != nil {
		return nil // every chunk has a source
	}
	plan, err := resolve(cfg, rangeStart, rangeEnd)
	if err != nil {
		return fmt.Errorf("streaming: validateRangeProducible resolve [%s,%s]: %w", rangeStart, rangeEnd, err)
	}
	for _, cb := range plan.ChunkBuilds {
		producible, perr := chunkLocallyProducible(cfg, cb)
		if perr != nil {
			return perr
		}
		if !producible {
			return fmt.Errorf(
				"streaming: chunk %s is required by the backfill range [%s,%s] but has no local copy "+
					"and no bulk backend is configured", cb.Chunk, rangeStart, rangeEnd)
		}
	}
	return nil
}

// chunkLocallyProducible answers validateRangeProducible's per-chunk question
// against the catalog and the filesystem, mirroring backfillSource's hot and
// pack branches but read-only. It opens the hot tier only to test completeness
// and always closes it.
func chunkLocallyProducible(cfg ExecConfig, cb ChunkBuild) (bool, error) {
	cat := cfg.Catalog

	// (a) Hot branch: a "ready" + complete hot tier produces any kind locally.
	hotState, err := cat.HotState(cb.Chunk)
	if err != nil {
		return false, fmt.Errorf("streaming: read hot state chunk %s: %w", cb.Chunk, err)
	}
	if hotState == HotReady && cfg.Process.HotProbe != nil {
		complete, herr := hotTierComplete(cfg.Process.HotProbe, cb.Chunk)
		if herr != nil {
			// A "ready" key whose stores can't be opened/queried is case-4 loss —
			// surface it here rather than letting the backfill discover it mid-write.
			return false, herr
		}
		if complete {
			return true, nil
		}
		// Present-but-incomplete falls through, exactly like backfillSource.
	}

	// (b) Pack branch: a frozen .pack re-derives every kind EXCEPT ledgers (deriving
	// ledgers from the pack we'd write is circular).
	if !cb.Artifacts.Has(KindLedgers) {
		ledgersState, lerr := cat.State(cb.Chunk, KindLedgers)
		if lerr != nil {
			return false, fmt.Errorf("streaming: read ledgers state chunk %s: %w", cb.Chunk, lerr)
		}
		if ledgersState == StateFrozen {
			if _, serr := os.Stat(cat.layout.LedgerPackPath(cb.Chunk)); serr == nil {
				return true, nil
			}
		}
	}

	return false, nil
}

// hotTierComplete opens the chunk's hot tier through the probe purely to read
// its single authoritative maxCommittedSeq (DECISION (a)), closes it, and
// reports whether it covers the chunk's last ledger. A "ready" key with an
// absent/unopenable dir is case-4 loss (ErrHotVolumeLost), matching
// backfillSource's hot branch.
func hotTierComplete(probe HotProbe, chunkID chunk.ID) (bool, error) {
	hot, ok, err := probe.OpenHotChunk(chunkID)
	if err != nil {
		return false, fmt.Errorf("%w: chunk %s: %w", ErrHotVolumeLost, chunkID, err)
	}
	if !ok {
		return false, fmt.Errorf("%w: chunk %s: hot directory absent", ErrHotVolumeLost, chunkID)
	}
	defer func() { _ = hot.Close() }()
	maxSeq, present, merr := hot.MaxCommittedSeq()
	if merr != nil {
		return false, fmt.Errorf("%w: chunk %s: max committed seq: %w", ErrHotVolumeLost, chunkID, merr)
	}
	return present && maxSeq >= chunkID.LastLedger(), nil
}
