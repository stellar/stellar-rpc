package lifecycle

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
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
// PRODUCTION boundary erring low is DANGEROUS (it would plan a build below
// existing storage from an unvalidated source). So the plan range never starts
// below storage — start is RAISED to lowestMaterializedChunk; extending the
// bottom is backfill's job, producibility enforced lazily per chunk.

// Config bundles the tick/loop dependencies. It composes the scheduler's
// ExecConfig (shared postconditions + worker pool with backfill) plus the
// retention knob and an injectable fatal sink.
type Config struct {
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
func (cfg Config) WithLifecycleDefaults() Config {
	cfg.ExecConfig = cfg.WithDefaults()
	if cfg.Fatalf == nil {
		cfg.Fatalf = log.Fatalf
	}
	return cfg
}

// abortTick centralizes the tick's error policy so each stage is one line.
// nil err → false (continue). A non-nil err aborts the tick (returns true); it
// calls Fatalf only when ctx is still live — a canceled ctx is a clean
// shutdown, not a failure. "what" names the failing step.
func (cfg Config) abortTick(ctx context.Context, err error, what string) bool {
	if err == nil {
		return false
	}
	if ctx.Err() == nil {
		cfg.Fatalf("lifecycle tick: %s: %v", what, err)
	}
	return true
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

// runLifecycle runs one tick over the three stages for just-completed chunk
// lastChunk. through = lastChunk.LastLedger() is the single snapshot every stage
// shares, so a boundary committing mid-tick can't make stages contradict (it's
// next tick's work). Plan range is [floor, lastChunk] (start raised to storage);
// discard/prune key off through.
//
// CLEAN-SHUTDOWN (binding): on an op error with ctx canceled, return WITHOUT
// Fatalf — cancellation is a shutdown, not a failure. Only a genuine failure
// (ctx still live) aborts via Fatalf.
//
//nolint:cyclop // linear 3-stage pipeline; the branch count is uniform abortTick guards, not real complexity
func runLifecycle(ctx context.Context, cfg Config, cat *catalog.Catalog, lastChunk chunk.ID) {
	metrics := observability.MetricsOrNop(cfg.Metrics)
	logger := cfg.Logger

	// The one snapshot every stage shares.
	through := lastChunk.LastLedger()

	earliest, _, err := cat.EarliestLedger()
	if cfg.abortTick(ctx, err, "read earliest ledger") {
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
	if cfg.abortTick(ctx, err, "lowest materialized chunk") {
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
	if cfg.abortTick(ctx, derr, "derive durable through") {
		return
	}
	highestComplete, haveComplete := lastCompleteChunkAtID(durableThrough)
	rangeEnd := lastChunk
	if haveComplete && highestComplete < rangeEnd {
		rangeEnd = highestComplete
	}
	if haveComplete && start >= 0 && start <= int64(rangeEnd) {
		// Plan-and-execute over [start, rangeEnd] via the same entry point backfill
		// uses (resolve → executePlan → Freeze metric, recorded internally). A
		// canceled ctx makes RunBackfill return ctx.Err(), which abortTick treats
		// as a clean shutdown (no Fatalf).
		eerr := backfill.RunBackfill(ctx, cfg.ExecConfig, chunk.ID(start), rangeEnd) //nolint:gosec // start >= 0
		if cfg.abortTick(ctx, eerr, fmt.Sprintf("run backfill [%d,%s]", start, rangeEnd)) {
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
	if cfg.abortTick(ctx, err, "eligible discard ops") {
		return
	}
	for _, op := range discardOps {
		if cfg.abortTick(ctx, op(), "discard op") {
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
	pruneOps, prunedArtifacts, err := eligiblePruneOps(cfg, cat, through)
	if cfg.abortTick(ctx, err, "eligible prune ops") {
		return
	}
	for _, op := range pruneOps {
		if cfg.abortTick(ctx, op(), "prune op") {
			return
		}
	}
	metrics.Prune(prunedArtifacts, time.Since(pruneStart))
	if logger != nil && prunedArtifacts > 0 {
		logger.WithField("pruned", prunedArtifacts).Info("streaming: lifecycle prune stage complete")
	}
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
	set    atomic.Bool
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
	s.set.Store(true)
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// take returns the latest published chunk id; ok=false when nothing has been
// published (chunk 0 is a valid id, so a separate flag distinguishes it).
func (s *BoundarySignal) take() (chunk.ID, bool) {
	if !s.set.Load() {
		return 0, false
	}
	return chunk.ID(s.latest.Load()), true
}

// Loop is the event-driven lifecycle goroutine. It blocks on the boundary signal's
// wake, reads the latest completed chunk id, and runs one tick over
// [floor, lastChunk] (which subsumes every boundary skipped while it was busy). It
// selects on ctx.Done() too, so it never blocks past shutdown.
func Loop(ctx context.Context, cfg Config, cat *catalog.Catalog, sig *BoundarySignal) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sig.wake:
			lastChunk, ok := sig.take()
			if !ok {
				continue
			}
			runLifecycle(ctx, cfg, cat, lastChunk)
		}
	}
}
