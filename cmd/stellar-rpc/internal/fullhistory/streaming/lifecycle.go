package streaming

import (
	"context"
	"log"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// The lifecycle goroutine runs one tick per notification (sent by the ingestion
// loop at start — the startup seed — and at every chunk boundary, carrying the
// just-completed chunk id), in three stages:
//
//  1. plan-and-execute — the SAME resolve + executePlan catch-up uses, over
//     [floor, lastChunk]. This is where a just-closed chunk freezes (from its hot
//     DB via backfillSource's hot branch) and the current window's index folds it
//     in. lastChunk is the id ingestion handed over — "how far to go"; what to
//     build, discard, and prune is read from the catalog.
//  2. discard scan — retire hot DBs the cold artifacts now fully serve (or that
//     fell past retention).
//  3. prune scan — sweep demoted and past-retention files, both key families.
//
// The retention floor plays two roles with OPPOSITE safe directions, kept
// separate (design "Lifecycle"):
//
//   - As a RETENTION boundary (the prune scan, the reader gate) erring low is
//     harmless — an extra chunk lingers briefly, or a read lands on already-
//     pruned data and returns not-found via the reader's missing-file rule.
//   - As a PRODUCTION boundary erring low is DANGEROUS — planning a build below
//     existing storage demands chunks from a bulk source nobody validated it can
//     produce. So the tick's plan range never starts below existing storage:
//     start is RAISED to lowestMaterializedChunk when the floor sits lower.
//     Extending the bottom of storage (retention widening) is exclusively catch-
//     up's job; producibility is enforced lazily there, per chunk, by the
//     buildTxhashIndex .bin precondition during the build (no pre-flight gate).
//
// The two goroutines (ingestion, lifecycle) share NO state: the tick is a pure
// function of the catalog, deriving everything from durable keys on every run.

// LifecycleConfig is the dependency bundle the lifecycle tick and loop read. It
// COMPOSES the scheduler's ExecConfig (resolve/executePlan share one set of
// postconditions and one worker pool with catch-up) and adds the retention knob
// plus an injectable fatal sink.
//
// RetentionChunks is the sliding-floor width (0 means "fixed earliest-ledger
// floor only", no sliding retention). Fatalf is the abort sink for the error
// policy: a tick whose executePlan fails (retries exhausted) aborts the daemon,
// because startup is the recovery path. Production wires log.Fatalf via
// WithLifecycleDefaults; tests inject a recorder so an abort is observable
// without killing the test process.
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
	cfg.ExecConfig = cfg.WithDefaults()
	if cfg.Fatalf == nil {
		cfg.Fatalf = log.Fatalf
	}
	return cfg
}

// effectiveRetentionFloor is the lower bound of the retention window, chunk-
// aligned: the first ledger of the lowest in-scope chunk. It combines the
// sliding retention floor (lastCompleteChunkAt(upperBound) - retentionChunks +
// 1, when retentionChunks > 0) with the fixed earliest-ledger floor, taking the
// HIGHER of the two.
//
// upperBound is ingestion's progress (completeThrough at runtime; the catch-up
// loop passes max(network tip, derived watermark)). The signed slidingChunk
// math is the underflow guard: a young store or a large retentionChunks drives
// slidingChunk negative, which max(..., 0) clamps to chunk 0 before mapping to
// its first ledger — never a uint32 wrap to MaxUint32.
func effectiveRetentionFloor(upperBound, retentionChunks, earliest uint32) uint32 {
	sliding := uint32(chunk.FirstLedgerSeq) // GenesisLedger
	if retentionChunks > 0 {
		slidingChunk := lastCompleteChunkAt(upperBound) - int64(retentionChunks) + 1
		sliding = chunkFirstLedger(max(slidingChunk, 0))
	}
	return max(sliding, earliest)
}

// lastCompleteChunkAt is the inverse of chunk.ID.LastLedger: the largest chunk
// whose last ledger is <= ledger, as a SIGNED int64 so a sub-genesis ledger
// (the watermark sentinel) maps to -1 ("before the first chunk") rather than
// wrapping. E.g. lastCompleteChunkAt(chunk 0's last ledger) == 0; a ledger
// below the first chunk's last ledger yields -1.
//
// The cast-before-subtract keeps the whole computation in int64: ledger is
// uint32, so (ledger - 1) would underflow for ledger 0; int64(ledger) - 1 does
// not. With chunk c spanning [c*L + 2, (c+1)*L + 1], the largest c whose last
// ledger <= ledger is (ledger - 2)/L when ledger >= 2; the form below
// ((ledger - FirstLedgerSeq + 1) - 1)/L - ... is normalized to match the
// design's (ledger-1)/L - 1 only after accounting for FirstLedgerSeq, so it is
// derived directly from the chunk geometry instead.
func lastCompleteChunkAt(ledger uint32) int64 {
	// chunk c's last ledger is (c+1)*L + FirstLedgerSeq - 1. The largest c with
	// that value <= ledger is floor((ledger - FirstLedgerSeq + 1)/L) - 1, i.e.
	// floor((ledger + 1 - FirstLedgerSeq)/L) - 1. Below the first chunk's last
	// ledger this is negative (the sentinel).
	return (int64(ledger)+1-int64(chunk.FirstLedgerSeq))/int64(chunk.LedgersPerChunk) - 1
}

// chunkFirstLedger maps a non-negative signed chunk index to its first ledger.
// It is the signed-domain companion of chunk.ID.FirstLedger used by
// effectiveRetentionFloor after the max(..., 0) clamp.
func chunkFirstLedger(c int64) uint32 {
	return chunk.ID(c).FirstLedger() //nolint:gosec // c >= 0 (clamped) and bounded by real chunk ids
}

// chunkIDOfLedger maps a ledger to its chunk, signed so the watermark sentinel
// (below genesis) yields a negative index instead of panicking like
// chunk.IDFromLedger. The tick only ever feeds it completeThrough, which is >=
// FirstLedgerSeq-1; a sentinel maps to chunk -1 ("before the first chunk").
func chunkIDOfLedger(ledger uint32) int64 {
	if ledger < chunk.FirstLedgerSeq {
		return -1
	}
	return int64(chunk.IDFromLedger(ledger))
}

// lastCompleteChunkAtID is lastCompleteChunkAt mapped to a chunk.ID for the
// resolver's rangeEnd, clamped at 0 (a negative result means no complete chunk
// exists; resolve's inverted-range guard then makes the plan empty when
// rangeEnd < rangeStart). The caller guards the negative case before using it.
func lastCompleteChunkAtID(ledger uint32) (chunk.ID, bool) {
	c := lastCompleteChunkAt(ledger)
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
func lowestMaterializedChunk(cat *Catalog) (chunk.ID, bool, error) {
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

// runLifecycleTick runs ONE tick for the just-completed chunk lastChunk that
// ingestion handed over. through is derived from lastChunk (its last ledger), so
// every stage sees the same snapshot and a boundary committing mid-tick can't
// make one stage contradict another (the new chunk is simply next tick's work).
// The three stages run in order.
//
// lastChunk is the unit of "how far to go": the plan range is [floor, lastChunk]
// (start raised to existing storage), and the discard/prune scans key off
// through = lastChunk.LastLedger(). What to build/discard/prune is read from the
// catalog, not from lastChunk.
//
// CLEAN-SHUTDOWN (binding): if executePlan returns an error AND ctx was
// canceled, the tick returns WITHOUT calling Fatalf — cancellation is a
// shutdown request, never an op failure. Only a genuine failure (ctx still
// live) aborts the daemon via Fatalf, per the error policy.
//
//nolint:gocognit,gocyclo,cyclop,funlen // an inherently multi-stage sequence (plan → discard → prune)
func runLifecycleTick(ctx context.Context, cfg LifecycleConfig, cat *Catalog, lastChunk chunk.ID) {
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

	// Stage 1 — plan-and-execute (the freeze + index fold). Timed and counted as
	// one phase; the plan's sizes are the chunk/index build counts (0/0 when there
	// is no producible range, still reported so the empty-tick rate is visible).
	//
	// rangeEnd is the just-completed chunk ingestion handed over (lastChunk), but
	// CLAMPED to the highest chunk that is actually complete in durable storage:
	// the production stage must never target the live or a not-yet-complete chunk
	// (its hot DB is held open by ingestion, and freezing it would race a live
	// writer — and on a young network nothing is complete at all). In the running
	// daemon lastChunk IS that highest-complete chunk, so the clamp is a no-op
	// there; it only bites on the seed/young-network/recovery edges. A negative
	// result (no complete chunk) makes the range empty — production is skipped,
	// while the discard and prune scans below still run.
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
	if haveComplete && start >= 0 && start <= int64(rangeEnd) { //nolint:nestif // plan-and-execute guard
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
			// CLEAN-SHUTDOWN FIX: a canceled ctx makes executePlan return ctx.Err()
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
	if bytes, berr := coldTierBytes(cat.layout); berr == nil {
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
func lifecycleLoop(ctx context.Context, cfg LifecycleConfig, cat *Catalog, ch <-chan chunk.ID) {
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
