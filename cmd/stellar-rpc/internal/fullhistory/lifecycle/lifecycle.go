package lifecycle

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// The lifecycle tick runs three stages in order: (1) plan-and-execute (the same
// resolve+executePlan as backfill, over [floor, lastChunk]); (2) discard scan;
// (3) prune scan. The tick is a pure function of the catalog — the two goroutines
// share no state.
//
// The retention floor has two roles with OPPOSITE safe directions (design
// "Lifecycle"): as a RETENTION boundary erring low is harmless (an extra chunk
// lingers, or a read returns not-found via the missing-file rule); as a
// PRODUCTION boundary erring low would in principle plan a build below existing
// storage — but producibility is enforced lazily per chunk in resolve, so the
// plan simply spans [floor, lastChunk] and extending the bottom is backfill's job.

// Config bundles the tick/loop dependencies. It composes the scheduler's
// ExecConfig (shared postconditions + worker pool with backfill) plus the
// retention knob.
type Config struct {
	backfill.ExecConfig

	// RetentionChunks bounds the sliding retention floor's width. 0 disables the
	// sliding floor (the fixed earliest-ledger floor alone applies).
	RetentionChunks uint32
}

// WithLifecycleDefaults returns a copy with the embedded ExecConfig defaults
// applied. Called once at startup before launching the loop.
func (cfg Config) WithLifecycleDefaults() Config {
	cfg.ExecConfig = cfg.WithDefaults()
	return cfg
}

// runOps runs each op in order, returning the first error. It checks ctx between
// ops so a shutdown mid-scan stops promptly without starting the next storage op;
// the ctx error is surfaced up through Loop for supervise to classify as clean.
func runOps(ctx context.Context, ops []func() error) error {
	for _, op := range ops {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := op(); err != nil {
			return err
		}
	}
	return nil
}

// runLifecycle runs one tick over the three stages for just-completed chunk
// lastChunk. through = lastChunk.LastLedger() is the single snapshot every stage
// shares, so a boundary committing mid-tick can't make stages contradict (it's
// next tick's work). Plan range is [floor, lastChunk] (start raised to storage);
// discard/prune key off through.
//
// It returns the first stage error WITHOUT classifying it: Loop propagates it to
// run's errgroup and supervise decides clean-vs-restart (a canceled ctx surfaces
// as a ctx error supervise treats as a clean shutdown).
func runLifecycle(ctx context.Context, cfg Config, cat *catalog.Catalog, lastChunk chunk.ID) error {
	metrics := observability.MetricsOrNop(cfg.Metrics)
	logger := cfg.Logger

	// The one snapshot every stage shares.
	through := lastChunk.LastLedger()

	earliest, _, err := cat.EarliestLedger()
	if err != nil {
		return fmt.Errorf("read earliest ledger: %w", err)
	}
	floor := EffectiveRetentionFloor(through, cfg.RetentionChunks, earliest)

	// Progress gauges: derived last-committed ledger and effective retention floor.
	metrics.LastCommitted(through, floor)
	if logger != nil {
		logger.WithField("through", through).
			WithField("floor", floor).
			Debug("streaming: lifecycle tick — derived snapshot")
	}

	// Stage 1 — plan-and-execute (freeze + index fold) over [floor, lastChunk], via
	// the same entry point backfill uses (resolve → executePlan → Freeze metric,
	// recorded internally). A canceled ctx makes RunBackfill return ctx.Err(), which
	// propagates up for supervise to treat as a clean shutdown.
	//
	// No rangeEnd clamp to the highest-complete chunk and no floor raise to
	// lowestMaterializedChunk (both traced dead, #25): the Loop only ever fires for
	// a genuinely completed lastChunk (the upstream boundary-handoff fence + seed
	// guard), and recovery leaves chunk-aligned watermarks, so neither clamp can
	// fire with a consequence beyond re-download churn. The only guard left is the
	// empty-range check (floor above lastChunk when retention outran production).
	freezeStart := time.Now()
	start := ChunkIDOfLedger(floor)
	if start >= 0 && start <= int64(lastChunk) {
		if eerr := backfill.RunBackfill(ctx, cfg.ExecConfig, chunk.ID(start), lastChunk); eerr != nil { //nolint:gosec // start in [0, lastChunk]
			return fmt.Errorf("run backfill [%d,%s]: %w", start, lastChunk, eerr)
		}
	} else {
		// floor above lastChunk: nothing to produce, but report an empty freeze so
		// the empty-tick rate stays visible. Scans below still run.
		metrics.Freeze(time.Since(freezeStart))
	}

	// Stage 2 — discard scan.
	discardStart := time.Now()
	discardOps, err := eligibleDiscardOps(cfg, cat, through)
	if err != nil {
		return fmt.Errorf("eligible discard ops: %w", err)
	}
	if err := runOps(ctx, discardOps); err != nil {
		return fmt.Errorf("discard op: %w", err)
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
	pruneOps, prunedArtifacts, err := eligiblePruneOps(cfg, cat, through)
	if err != nil {
		return fmt.Errorf("eligible prune ops: %w", err)
	}
	if err := runOps(ctx, pruneOps); err != nil {
		return fmt.Errorf("prune op: %w", err)
	}
	metrics.Prune(prunedArtifacts, time.Since(pruneStart))
	if logger != nil && prunedArtifacts > 0 {
		logger.WithField("pruned", prunedArtifacts).Info("streaming: lifecycle prune stage complete")
	}
	return nil
}

// BoundarySignal couples ingestion (the producer) to the lifecycle Loop (the
// consumer): ingestion stores the latest completed chunk id and pings a
// 1-buffered wake; the Loop blocks on the wake, then reads the latest id. A
// latest-CELL (not a queue) means a slow lifecycle can never fall behind — one
// tick over [floor, latest] subsumes every skipped boundary — so there is no
// bounded buffer to overflow and thus no "fell behind" fatal path. Safe for one
// producer and one consumer.
type BoundarySignal struct {
	latest atomic.Uint32
	wake   chan struct{}
}

// NewBoundarySignal returns a ready signal with an empty latest cell.
func NewBoundarySignal() *BoundarySignal {
	return &BoundarySignal{wake: make(chan struct{}, 1)}
}

// Publish records c as the latest completed chunk and wakes the Loop. The wake is
// non-blocking: a pending wake already covers this boundary (the Loop will read
// the newest latest when it runs), so a full buffer is dropped, never blocked on.
func (s *BoundarySignal) Publish(c chunk.ID) {
	s.latest.Store(uint32(c))
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// latestChunk returns the most recently published completed chunk id. A wake is
// only ever sent by Publish, AFTER it stores the cell, so a received wake proves a
// value is present — no separate "was anything published" flag is needed.
func (s *BoundarySignal) latestChunk() chunk.ID {
	return chunk.ID(s.latest.Load())
}

// Loop is the event-driven lifecycle goroutine. It blocks on the boundary signal's
// wake, reads the latest completed chunk id, and runs one tick over
// [floor, lastChunk] (which subsumes every boundary skipped while it was busy). It
// selects on ctx.Done() too, so it never blocks past shutdown.
//
// It returns the first tick error to its caller (run() joins it with ingestion in
// an errgroup, so supervise decides clean-vs-restart). A cancellation observed at
// the select returns nil; a cancellation mid-tick returns the tick's wrapped ctx
// error — both are clean, since supervise keys off the daemon ctx, not this return.
func Loop(ctx context.Context, cfg Config, cat *catalog.Catalog, sig *BoundarySignal) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sig.wake:
			if err := runLifecycle(ctx, cfg, cat, sig.latestChunk()); err != nil {
				return err
			}
		}
	}
}
