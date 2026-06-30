package lifecycle

import (
	"context"
	"log"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// The lifecycle tick runs three stages in order: (1) plan-and-execute (the same
// resolve+executePlan as catch-up, over [floor, lastChunk]); (2) discard scan;
// (3) prune scan. The tick is a pure function of the catalog — the two goroutines
// share no state.
//
// The retention floor has two roles with OPPOSITE safe directions (design
// "Lifecycle"): as a RETENTION boundary erring low is harmless (an extra chunk
// lingers, or a read returns not-found via the missing-file rule); as a
// PRODUCTION boundary erring low is DANGEROUS (it would plan a build below
// existing storage from an unvalidated source). So the plan range never starts
// below storage — start is RAISED to lowestMaterializedChunk; extending the
// bottom is catch-up's job, producibility enforced lazily per chunk.

// LifecycleConfig bundles the tick/loop dependencies. It composes the scheduler's
// ExecConfig (shared postconditions + worker pool with catch-up) plus the
// retention knob and an injectable fatal sink.
type LifecycleConfig struct {
	backfill.ExecConfig

	// RetentionChunks bounds the sliding retention floor's width. 0 disables the
	// sliding floor (the fixed earliest-ledger floor alone applies).
	RetentionChunks uint32

	// Fatalf aborts the daemon on a tick op failure. WithLifecycleDefaults fills
	// log.Fatalf when unset; tests override it.
	Fatalf func(format string, args ...any)
}

// WithLifecycleDefaults returns a copy with ExecConfig and Fatalf defaults
// applied. Called once at startup before launching the loop.
func (cfg LifecycleConfig) WithLifecycleDefaults() LifecycleConfig {
	cfg.ExecConfig = cfg.ExecConfig.WithDefaults()
	if cfg.Fatalf == nil {
		cfg.Fatalf = log.Fatalf
	}
	return cfg
}

// lastCompleteChunkAtID maps geometry.LastCompleteChunkAt to a chunk.ID;
// ok=false when no complete chunk exists (negative result).
func lastCompleteChunkAtID(ledger uint32) (chunk.ID, bool) {
	c := geometry.LastCompleteChunkAt(ledger)
	if c < 0 {
		return 0, false
	}
	return chunk.ID(c), true //nolint:gosec // c >= 0
}

// lowestMaterializedChunk is the lowest chunk holding any chunk:* artifact key
// or hot:chunk key — the bottom of existing storage, and the production-boundary
// anchor (the plan never starts below it). ok=false on an empty catalog.
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

// runLifecycleTick runs one tick over the three stages for just-completed chunk
// lastChunk. through = lastChunk.LastLedger() is the single snapshot every stage
// shares, so a boundary committing mid-tick can't make stages contradict (it's
// next tick's work). Plan range is [floor, lastChunk] (start raised to storage);
// discard/prune key off through.
//
// CLEAN-SHUTDOWN (binding): on an op error with ctx cancelled, return WITHOUT
// Fatalf — cancellation is a shutdown, not a failure. Only a genuine failure
// (ctx still live) aborts via Fatalf.
func runLifecycleTick(ctx context.Context, cfg LifecycleConfig, cat *catalog.Catalog, lastChunk chunk.ID) {
	metrics := observability.MetricsOrNop(cfg.Metrics)
	logger := cfg.Logger

	// The one snapshot every stage shares.
	through := lastChunk.LastLedger()

	earliest, _, err := cat.EarliestLedger()
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		cfg.Fatalf("streaming: lifecycle tick: read earliest ledger: %v", err)
		return
	}
	floor := EffectiveRetentionFloor(through, cfg.RetentionChunks, earliest)

	// Progress gauges: derived last-committed ledger and effective retention floor.
	metrics.LastCommitted(through, floor)
	if logger != nil {
		logger.WithField("through", through).
			WithField("floor", floor).
			Debug("streaming: lifecycle tick — derived snapshot")
	}

	// Plan start = chunkID(floor), RAISED to lowestMaterializedChunk when higher
	// — the production-boundary rule (never plan below existing storage).
	start := ChunkIDOfLedger(floor)
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

	// Stage 1 — plan-and-execute (freeze + index fold).
	//
	// rangeEnd is lastChunk CLAMPED to the highest durably-complete chunk: the
	// production stage must never target the live or not-yet-complete chunk (whose
	// hot DB ingestion holds open). In the running daemon lastChunk IS that chunk,
	// so the clamp is a no-op; it only bites on seed/young-network/recovery edges.
	// No complete chunk ⇒ empty range, production skipped, scans below still run.
	freezeStart := time.Now()
	durableThrough, derr := LastCommittedLedger(cat, nil) // chunk-granularity, no hot DB read
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
		// Plan-and-execute over [start, rangeEnd] via the same entry point catch-up
		// uses (resolve → executePlan → Freeze metric, recorded internally).
		if eerr := backfill.RunBackfill(ctx, cfg.ExecConfig, chunk.ID(start), rangeEnd); eerr != nil { //nolint:gosec // start >= 0
			// CLEAN-SHUTDOWN: a cancelled ctx makes RunBackfill return ctx.Err() —
			// a shutdown, not an op failure. Return before any Fatalf.
			if ctx.Err() != nil {
				return
			}
			cfg.Fatalf("streaming: lifecycle tick: run backfill [%d,%s]: %v", start, rangeEnd, eerr)
			return
		}
	} else {
		// No complete chunk in range: skip production but report an empty freeze so
		// the empty-tick rate stays visible. Scans below still run.
		metrics.Freeze(time.Since(freezeStart))
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

	// Live hot-chunk gauge after the discard stage.
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
}

// LifecycleQueueDepth is the notification buffer depth — far above the at-most-one
// boundary a healthy daemon holds in flight. A FULL buffer means freeze has fallen
// this many boundaries behind ingestion, a fatal condition notify() reports.
const LifecycleQueueDepth = 8

// RunLoop is the event-driven lifecycle goroutine. Each notification carries
// the just-completed chunk id; the loop drains the buffer to the most-recent id
// (one tick over [floor, lastChunk] subsumes the rest) and runs one tick. It
// selects on both ctx.Done() and the channel, so it never blocks or fatals on
// shutdown.
func RunLoop(ctx context.Context, cfg LifecycleConfig, cat *catalog.Catalog, ch <-chan chunk.ID) {
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
