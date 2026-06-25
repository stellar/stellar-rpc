package streaming

import (
	"context"
	"log"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// The lifecycle goroutine runs one tick per notification (the ingestion loop's
// startup seed, then one per chunk boundary carrying the just-completed chunk
// id), in three stages:
//
//  1. plan-and-execute — the SAME resolve + executePlan catch-up uses, over
//     [floor, lastChunk]. Here a just-closed chunk freezes (from its hot DB via
//     backfillSource's hot branch) and the current window's index folds it in.
//     lastChunk is "how far to go"; what to build/discard/prune is read from the
//     catalog.
//  2. discard scan — retire hot DBs the cold artifacts now fully serve (or that
//     fell past retention).
//  3. prune scan — sweep demoted and past-retention files, both key families.
//
// The retention floor plays two roles with OPPOSITE safe directions, kept
// separate (design "Lifecycle"):
//
//   - As a RETENTION boundary (prune scan, reader gate) erring low is harmless —
//     an extra chunk lingers, or a read lands on pruned data and returns
//     not-found via the reader's missing-file rule.
//   - As a PRODUCTION boundary erring low is DANGEROUS — planning a build below
//     existing storage demands chunks from a bulk source nobody validated it can
//     produce. So the plan range never starts below existing storage: start is
//     RAISED to lowestMaterializedChunk. Extending the bottom of storage is
//     catch-up's job; producibility is enforced lazily there, per chunk, by
//     buildTxhashIndex's .bin precondition (no pre-flight gate).
//
// The two goroutines share NO state: the tick is a pure function of the catalog.

// LifecycleConfig is the dependency bundle the lifecycle tick and loop read. It
// COMPOSES the scheduler's ExecConfig (resolve/executePlan share one set of
// postconditions and one worker pool with catch-up) and adds the retention knob
// plus an injectable fatal sink (a tick whose executePlan fails aborts the
// daemon, because startup is the recovery path).
type LifecycleConfig struct {
	ExecConfig

	// RetentionChunks bounds the sliding retention floor's width. 0 disables the
	// sliding floor (the fixed earliest-ledger floor alone applies).
	RetentionChunks uint32

	// Fatalf aborts the daemon on a tick op failure (the error policy). nil in a
	// caller's literal; WithLifecycleDefaults fills log.Fatalf. Tests override it.
	Fatalf func(format string, args ...any)
}

// WithLifecycleDefaults returns a copy with ExecConfig defaults applied and
// Fatalf defaulted to log.Fatalf when unset. The daemon calls this once at
// startup before launching the loop.
func (cfg LifecycleConfig) WithLifecycleDefaults() LifecycleConfig {
	cfg.ExecConfig = cfg.ExecConfig.WithDefaults()
	if cfg.Fatalf == nil {
		cfg.Fatalf = log.Fatalf
	}
	return cfg
}

// lastCompleteChunkAtID is geometry.LastCompleteChunkAt mapped to a chunk.ID for the
// resolver's rangeEnd, clamped at 0 (a negative result means no complete chunk
// exists; resolve's inverted-range guard then makes the plan empty when
// rangeEnd < rangeStart). The caller guards the negative case before using it.
func lastCompleteChunkAtID(ledger uint32) (chunk.ID, bool) {
	c := geometry.LastCompleteChunkAt(ledger)
	if c < 0 {
		return 0, false
	}
	return chunk.ID(c), true //nolint:gosec // c >= 0
}

// lowestMaterializedChunk is the lowest chunk holding ANY chunk:* artifact key
// or hot:chunk key — the bottom of existing storage. ok=false on an empty
// catalog (a first frontfill tick, where resolve's inverted-range guard makes
// the tick a no-op anyway). It is the production-boundary anchor: the tick's
// plan never starts below it.
func lowestMaterializedChunk(cat *catalog.Catalog) (chunk.ID, bool, error) {
	lowest := chunk.ID(0)
	found := false
	note := func(c chunk.ID) {
		if !found || c < lowest {
			lowest, found = c, true
		}
	}

	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return 0, false, err
	}
	for _, ref := range refs {
		note(ref.Chunk)
	}

	hot, err := cat.HotChunkKeys()
	if err != nil {
		return 0, false, err
	}
	for _, c := range hot {
		note(c)
	}
	return lowest, found, nil
}

// runLifecycleTick runs ONE tick over the three stages (in order) for the
// just-completed chunk lastChunk. through = lastChunk.LastLedger() is the single
// snapshot every stage shares, so a boundary committing mid-tick can't make one
// stage contradict another (the new chunk is simply next tick's work). The plan
// range is [floor, lastChunk] (start raised to existing storage); the
// discard/prune scans key off through.
//
// CLEAN-SHUTDOWN (binding): if executePlan errors AND ctx was cancelled, the tick
// returns WITHOUT calling Fatalf — cancellation is a shutdown request, never an
// op failure. Only a genuine failure (ctx still live) aborts via Fatalf.
func runLifecycleTick(ctx context.Context, cfg LifecycleConfig, cat *catalog.Catalog, lastChunk chunk.ID) {
	metrics := cfg.metrics()
	logger := cfg.Logger

	// through is the last ledger of the chunk ingestion handed over — the one
	// snapshot every stage shares.
	through := lastChunk.LastLedger()

	earliest, _, err := cat.EarliestLedger()
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		cfg.Fatalf("streaming: lifecycle tick: read earliest ledger: %v", err)
		return
	}
	floor := effectiveRetentionFloor(through, cfg.RetentionChunks, earliest)

	// Progress gauges, refreshed every tick from the snapshot: the derived
	// watermark (completeThrough) and the effective retention floor.
	metrics.Watermark(through, floor)
	if logger != nil {
		logger.WithField("through", through).
			WithField("floor", floor).
			Debug("streaming: lifecycle tick — derived snapshot")
	}

	// Plan range start = chunkID(floor), RAISED to lowestMaterializedChunk when
	// that is higher — the production-boundary rule (never plan below existing
	// storage; extending the bottom is catch-up's job).
	start := chunkIDOfLedger(floor)
	low, hasLow, err := lowestMaterializedChunk(cat)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		cfg.Fatalf("streaming: lifecycle tick: lowest materialized chunk: %v", err)
		return
	}
	if hasLow && int64(low) > start {
		start = int64(low)
	}

	// Stage 1 — plan-and-execute (freeze + index fold). One timed phase; the plan
	// sizes are the chunk/index build counts (0/0 with no producible range, still
	// reported so the empty-tick rate is visible).
	//
	// rangeEnd is lastChunk CLAMPED to the highest durably-complete chunk: the
	// production stage must never target the live or a not-yet-complete chunk
	// (whose hot DB ingestion holds open). In the running daemon lastChunk IS that
	// highest-complete chunk, so the clamp is a no-op; it only bites on the
	// seed/young-network/recovery edges. No complete chunk ⇒ empty range, so
	// production is skipped while the discard and prune scans below still run.
	freezeStart := time.Now()
	var chunkBuilds, indexBuilds int
	durableThrough, derr := lastCommittedLedger(cat, nil) // chunk-granularity, no hot DB read
	if derr != nil {
		if ctx.Err() != nil {
			return
		}
		cfg.Fatalf("streaming: lifecycle tick: derive durable through: %v", derr)
		return
	}
	highestComplete, haveComplete := lastCompleteChunkAtID(durableThrough)
	rangeEnd := lastChunk
	if haveComplete && highestComplete < rangeEnd {
		rangeEnd = highestComplete
	}
	if haveComplete && start >= 0 && start <= int64(rangeEnd) {
		plan, perr := resolve(cfg.ExecConfig, chunk.ID(start), rangeEnd) //nolint:gosec // start >= 0
		if perr != nil {
			if ctx.Err() != nil {
				return
			}
			cfg.Fatalf("streaming: lifecycle tick: resolve [%d,%s]: %v", start, rangeEnd, perr)
			return
		}
		chunkBuilds, indexBuilds = len(plan.ChunkBuilds), len(plan.IndexBuilds)
		if eerr := executePlan(ctx, plan, cfg.ExecConfig); eerr != nil {
			// CLEAN-SHUTDOWN FIX: a cancelled ctx makes executePlan return ctx.Err()
			// (every task's slot-acquire/wait observes the errgroup cancel). That is
			// a shutdown, NOT an op failure — return before any Fatalf.
			if ctx.Err() != nil {
				return
			}
			cfg.Fatalf("streaming: lifecycle tick: %v", eerr)
			return
		}
	}
	// else: no complete chunk in range (young network / empty store) — skip
	// production. The discard and prune scans still run: a past-retention hot DB
	// or stale key can exist with no producible range.
	metrics.Freeze(chunkBuilds, indexBuilds, time.Since(freezeStart))
	if logger != nil && (chunkBuilds > 0 || indexBuilds > 0) {
		logger.WithField("chunk_builds", chunkBuilds).
			WithField("index_builds", indexBuilds).
			Info("streaming: lifecycle freeze stage complete")
	}

	// Stage 2 — discard scan.
	discardStart := time.Now()
	discardOps, err := eligibleDiscardOps(cfg, cat, through)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		cfg.Fatalf("streaming: lifecycle tick: eligible discard ops: %v", err)
		return
	}
	for _, op := range discardOps {
		if oerr := op(); oerr != nil {
			if ctx.Err() != nil {
				return
			}
			cfg.Fatalf("streaming: lifecycle tick: discard op: %v", oerr)
			return
		}
	}
	metrics.Discard(len(discardOps), time.Since(discardStart))
	if logger != nil && len(discardOps) > 0 {
		logger.WithField("discarded", len(discardOps)).Info("streaming: lifecycle discard stage complete")
	}

	// Live hot-chunk gauge after the discard stage (the live + awaiting-discard set).
	if hot, herr := cat.HotChunkKeys(); herr == nil {
		metrics.LiveHotChunks(len(hot))
	}

	// Stage 3 — prune scan.
	pruneStart := time.Now()
	pruneOps, err := eligiblePruneOps(cfg, cat, through)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		cfg.Fatalf("streaming: lifecycle tick: eligible prune ops: %v", err)
		return
	}
	for _, op := range pruneOps {
		if oerr := op(); oerr != nil {
			if ctx.Err() != nil {
				return
			}
			cfg.Fatalf("streaming: lifecycle tick: prune op: %v", oerr)
			return
		}
	}
	metrics.Prune(len(pruneOps), time.Since(pruneStart))
	if logger != nil && len(pruneOps) > 0 {
		logger.WithField("pruned", len(pruneOps)).Info("streaming: lifecycle prune stage complete")
	}

	// Cold-tier footprint gauge after the prune stage (post-deletion size).
	if bytes, berr := coldTierBytes(cat.Layout()); berr == nil {
		metrics.ColdTierBytes(bytes)
	}
}

// lifecycleQueueDepth is the lifecycle notification buffer depth — far above the
// at-most-one boundary a healthy daemon holds in flight. A FULL buffer means
// freeze has fallen this many boundaries behind ingestion, which is a fatal
// condition the ingestion-side notify() reports (see runIngestionLoop).
const lifecycleQueueDepth = 8

// lifecycleLoop is the event-driven lifecycle goroutine. Each notification
// carries the just-completed chunk id; the loop DRAINS the buffered channel to
// the most-recent id (one tick covers every chunk queued behind it, since the
// plan range is [floor, lastChunk] and chunk ids only increase) and runs one
// tick up to it. It selects on BOTH ctx.Done() (return, clean shutdown) AND the
// channel — so it never blocks forever and never fatals on shutdown.
// Notifications arrive from exactly one source (ingestion: each boundary plus
// the startup seed, whose tick doubles as startup convergence). Between
// notifications the goroutine is idle, and idle means quiescent.
func lifecycleLoop(ctx context.Context, cfg LifecycleConfig, cat *catalog.Catalog, ch <-chan chunk.ID) {
	for {
		select {
		case <-ctx.Done():
			return
		case lastChunk := <-ch:
			// Drain to the most-recent queued chunk: one tick over [floor, lastChunk]
			// subsumes every earlier boundary still sitting in the buffer.
		drain:
			for {
				select {
				case lastChunk = <-ch:
				case <-ctx.Done():
					return
				default:
					break drain
				}
			}
			runLifecycleTick(ctx, cfg, cat, lastChunk)
		}
	}
}
