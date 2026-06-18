package streaming

import (
	"context"
	"log"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// The lifecycle goroutine runs one tick per doorbell notification (rung by the
// ingestion loop at start and at every chunk boundary), in three stages:
//
//  1. plan-and-execute — the SAME resolve + executePlan catch-up uses, over
//     [floor, completeThrough]. This is where a just-closed chunk freezes (from
//     its hot DB via catchupSource's hot branch) and the current window's index
//     folds it in.
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
//     up's job, the one path that runs validateRangeProducible.
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
	cfg.ExecConfig = cfg.ExecConfig.WithDefaults()
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

// runLifecycleTick runs ONE tick. It derives completeThrough ONCE — so every
// stage sees the same snapshot and a boundary committing mid-tick can't make
// one stage contradict another (the new chunk is simply next tick's work) —
// then runs the three stages in order.
//
// CLEAN-SHUTDOWN (binding): if executePlan returns an error AND ctx was
// cancelled, the tick returns WITHOUT calling Fatalf — cancellation is a
// shutdown request, never an op failure. Only a genuine failure (ctx still
// live) aborts the daemon via Fatalf, per the error policy.
func runLifecycleTick(ctx context.Context, cfg LifecycleConfig, cat *Catalog) {
	metrics := cfg.metrics()
	logger := cfg.Logger

	// One derivation per tick — all stages share this snapshot.
	through, err := deriveCompleteThrough(cat)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		cfg.Fatalf("streaming: lifecycle tick: derive completeThrough: %v", err)
		return
	}

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
	rangeEnd, hasEnd := lastCompleteChunkAtID(through)
	freezeStart := time.Now()
	var chunkBuilds, indexBuilds int
	if hasEnd && start >= 0 {
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
	if bytes, berr := coldTierBytes(cat.layout); berr == nil {
		metrics.ColdTierBytes(bytes)
	}
}

// lifecycleLoop is the event-driven lifecycle goroutine. It selects on BOTH
// ctx.Done() (return, clean shutdown) AND the doorbell (run a tick) — so it
// never blocks forever and never fatals on shutdown. Notifications arrive from
// exactly one source (ingestion's hot-chunk-set changes: each boundary plus the
// one at ingestion start, whose tick doubles as startup convergence). Between
// notifications the goroutine is idle, and idle means quiescent.
func lifecycleLoop(ctx context.Context, cfg LifecycleConfig, cat *Catalog, doorbell <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-doorbell:
			runLifecycleTick(ctx, cfg, cat)
		}
	}
}
