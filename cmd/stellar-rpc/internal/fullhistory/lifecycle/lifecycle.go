package lifecycle

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
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

	// Retention is the floor policy, bound once at startup from the validated
	// earliest_ledger pin.
	Retention geometry.Retention

	// Router unpublishes a discarded hot chunk's handle so deferred deletion can
	// close it (see deletion.go). Nil in the bounded backfill / test case, where
	// no handle is published.
	Router HandleDiscarder

	// grace is the deferred-deletion wait before destroying demoted hot chunks. It
	// is 0 until the read server sets a request deadline (design: T = max request
	// timeout + margin); with no in-flight readers a zero wait is safe.
	grace time.Duration

	// opRetryAttempts / opRetryBackoff bound the per-op retry the discard/prune
	// sweeps use (see runOps). Not config-wired: production always runs the
	// WithLifecycleDefaults constants, so these are unexported internals (a test
	// seam), not an advertised knob. Zero values fall back to the defaults in
	// WithLifecycleDefaults.
	opRetryAttempts int
	opRetryBackoff  time.Duration
}

const (
	defaultOpRetryAttempts = 3
	defaultOpRetryBackoff  = 5 * time.Second
)

// WithLifecycleDefaults returns a copy with the embedded ExecConfig defaults and
// the op-retry defaults applied. Called once at startup before launching the loop.
func (cfg Config) WithLifecycleDefaults() Config {
	cfg.ExecConfig = cfg.WithDefaults()
	if cfg.opRetryAttempts < 1 {
		cfg.opRetryAttempts = defaultOpRetryAttempts
	}
	if cfg.opRetryBackoff <= 0 {
		cfg.opRetryBackoff = defaultOpRetryBackoff
	}
	return cfg
}

// runOps runs each op in order, retrying a failed op a bounded number of times on
// a fixed pause before giving up. The discard/prune ops are idempotent file
// deletions, so a transient failure (a busy file, a slow fsync) is exactly the
// retryable kind — retrying in place avoids canceling ingestion through the shared
// errgroup and forcing a whole-daemon restart (which relaunches captive core) for
// a retryable file operation. It checks ctx between ops (and the backoff aborts on
// ctx cancellation) so a shutdown mid-scan stops promptly; the ctx error surfaces
// up through Loop for supervise to classify as clean.
//
// It returns how many ops ran to success (all of them on a nil error) so the caller
// can meter the work actually done even when a later op fails — the completed ops
// already retired their DBs / swept their artifacts and won't re-list next scan.
func runOps(ctx context.Context, cfg Config, ops []func() error) (int, error) {
	// A zero-value Config (no WithLifecycleDefaults, e.g. a test harness) runs each
	// op exactly once.
	attempts := max(cfg.opRetryAttempts, 1)
	for i, op := range ops {
		if err := ctx.Err(); err != nil {
			return i, err
		}
		// attempts total tries == 1 initial + (attempts-1) retries, fixed pause.
		//nolint:gosec // attempts >= 1, so attempts-1 >= 0
		bo := backoff.WithMaxRetries(backoff.NewConstantBackOff(cfg.opRetryBackoff), uint64(attempts-1))
		if err := backoff.Retry(op, backoff.WithContext(bo, ctx)); err != nil {
			return i, err
		}
	}
	return len(ops), nil
}

// runLifecycle runs one tick over the three stages for just-completed chunk
// lastChunk — the single snapshot every stage shares, so a boundary committing
// mid-tick can't make stages contradict (it's next tick's work). Plan range is
// [floor, lastChunk] (start raised to storage); discard/prune key off lastChunk.
// Every stage compares in the chunk domain.
//
// It returns the first stage error WITHOUT classifying it: Loop propagates it to
// run's errgroup and supervise decides clean-vs-restart (a canceled ctx surfaces
// as a ctx error supervise treats as a clean shutdown).
func runLifecycle(ctx context.Context, cfg Config, cat *catalog.Catalog, lastChunk chunk.ID) error {
	metrics := observability.MetricsOrNop(cfg.Metrics)
	logger := cfg.Logger

	floor := cfg.Retention.FloorAt(int64(lastChunk))

	// Retention-floor gauge only. The last-committed gauge is owned by the ingestion
	// loop (which holds the true, possibly mid-chunk value); re-emitting it here from
	// the chunk-aligned lastChunk would regress it on every tick.
	metrics.RetentionFloor(floor.FirstLedger())
	logger.WithField("last_chunk", lastChunk.String()).
		WithField("floor_chunk", floor.String()).
		Debug("lifecycle tick — derived snapshot")

	// Stage 1 — plan-and-execute (freeze + index rebuild) over [floor, lastChunk], via
	// the same entry point backfill uses (resolve → executePlan → Freeze metric,
	// recorded internally). A canceled ctx makes RunBackfill return ctx.Err(), which
	// propagates up for supervise to treat as a clean shutdown. lastChunk is always
	// a completed chunk (boundary fence + post-backfill seed), so the only guard
	// needed is the empty-range check (floor above lastChunk when retention outran
	// production). An empty range emits no Freeze sample — the Discard/Prune samples
	// below carry empty-tick visibility.
	if floor <= lastChunk {
		if eerr := backfill.RunBackfill(ctx, cfg.ExecConfig, floor, lastChunk); eerr != nil {
			return fmt.Errorf("run backfill [%s,%s]: %w", floor, lastChunk, eerr)
		}
	}

	// Stage 2 — discard scan. Demote each eligible hot chunk (unpublish its handle,
	// mark it transient) and collect it in pending for destruction at end of run.
	var pending hotDeletions
	discardStart := time.Now()
	discardChunks, err := eligibleDiscardChunks(cat, floor, lastChunk)
	if err != nil {
		return fmt.Errorf("eligible discard chunks: %w", err)
	}
	demoteOps := make([]func() error, len(discardChunks))
	for i, c := range discardChunks {
		demoteOps[i] = func() error { return pending.demote(cfg.Router, cat, c) }
	}
	// Meter the DBs actually demoted (one op per DB) BEFORE the error check, so a
	// mid-scan failure still counts what completed rather than losing it: the demoted
	// DBs carry a transient key that won't re-list as ready next scan.
	discarded, err := runOps(ctx, cfg, demoteOps)
	metrics.Discard(discarded, time.Since(discardStart))
	if err != nil {
		return fmt.Errorf("discard demote: %w", err)
	}
	if discarded > 0 {
		logger.WithField("discarded", discarded).Info("lifecycle discard stage complete")
	}

	// Live hot-chunk gauge after the discard stage.
	hot, err := cat.HotChunkKeys()
	if err != nil {
		return fmt.Errorf("read hot chunk keys: %w", err)
	}
	metrics.LiveHotChunks(len(hot))

	// Stage 3 — prune scan.
	pruneStart := time.Now()
	pruneOps, pruneWeights, err := eligiblePruneOps(cat, floor)
	if err != nil {
		return fmt.Errorf("eligible prune ops: %w", err)
	}
	// Sum the artifacts swept by the ops that actually completed (each op carries its
	// own artifact weight — the chunk family collapses many artifacts into one op).
	// Metered BEFORE the error check so a mid-sweep failure keeps the completed count.
	completed, err := runOps(ctx, cfg, pruneOps)
	prunedArtifacts := 0
	for _, w := range pruneWeights[:completed] {
		prunedArtifacts += w
	}
	metrics.Prune(prunedArtifacts, time.Since(pruneStart))
	if err != nil {
		return fmt.Errorf("prune op: %w", err)
	}
	if prunedArtifacts > 0 {
		logger.WithField("pruned", prunedArtifacts).Info("lifecycle prune stage complete")
	}

	// End of run: destroy the hot chunks demoted in stage 2, once every stage has
	// finished (design: wait the grace period once, then delete).
	pending.destroy(ctx, cfg, cat)
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
