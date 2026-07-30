package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
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

	// Registry unpublishes a discarded hot chunk's handle and closes it once idle so
	// deferred deletion can retire it (see deletion.go). Nil in tests, where no
	// handle is published (the bench runs no lifecycle).
	Registry HandleRetirer

	// Grace is the deferred-deletion wait before destroying demoted resources.
	// Unset (<= 0) takes defaultGrace via WithLifecycleDefaults; tests override it
	// small. TODO(#772): derive from the read server's request deadline
	// (T = max request timeout + margin) and boot-validate T exceeds that timeout.
	Grace time.Duration

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
	// defaultGrace is the placeholder deferred-deletion wait until the read server
	// derives it from the request deadline (#772). Chosen to comfortably outlast
	// any plausible request; the wait falls once per run, minutes against runs
	// hours apart.
	defaultGrace = 5 * time.Minute
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
	if cfg.Grace <= 0 {
		cfg.Grace = defaultGrace
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
// The stages run INDEPENDENTLY and their errors are joined at the end: each scan
// reads only durable state, so a failed freeze must not gate the discard/prune
// scans or the end-of-run destroys — otherwise a persistently failing stage (a
// full disk failing the freeze is the sharp case) would also wedge the very
// reclamation that could clear it. A chunk whose freeze failed is protected by
// eligibility, not by ordering: nothing is discardable without durably frozen
// artifacts and coverage. executePlan joins its workers before returning
// (errgroup), so no build is in flight when the debris scan runs after a failed
// stage 1. A canceled ctx still aborts everything, including the grace wait.
//
// It returns the joined stage errors WITHOUT classifying them: Loop propagates
// them to run's errgroup and supervise decides clean-vs-restart (a canceled ctx
// surfaces as a ctx error supervise treats as a clean shutdown).
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
	var errs []error
	if floor <= lastChunk {
		if eerr := backfill.RunBackfill(ctx, cfg.ExecConfig, floor, lastChunk); eerr != nil {
			errs = append(errs, fmt.Errorf("run backfill [%s,%s]: %w", floor, lastChunk, eerr))
		}
	}

	// Stage 2 — discard scan. Demote each eligible hot chunk (unpublish its handle,
	// mark it transient) and collect it in pending for destruction at end of run.
	var pending pendingDeletions
	discardStart := time.Now()
	discardChunks, err := eligibleDiscardChunks(cat, floor, lastChunk)
	if err != nil {
		errs = append(errs, fmt.Errorf("eligible discard chunks: %w", err))
	}
	demoteOps := make([]func() error, len(discardChunks))
	for i, c := range discardChunks {
		demoteOps[i] = func() error { return pending.demoteHotChunk(cfg.Registry, cat, c) }
	}
	// Meter the demote ops that completed (one per DB) BEFORE the error check, so a
	// mid-scan failure still counts what finished. This counts demotes, not destroys:
	// a chunk whose destroy stays reader-busy re-lists next run (the retry path) and
	// is re-counted — rare, since the grace wait outlasts requests, and it
	// self-reports via the destroy-skipped warning.
	discarded, err := runOps(ctx, cfg, demoteOps)
	metrics.Discard(discarded, time.Since(discardStart))
	if err != nil {
		errs = append(errs, fmt.Errorf("discard demote: %w", err))
	}
	if discarded > 0 {
		logger.WithField("discarded", discarded).Info("lifecycle discard stage complete")
	}

	// Live hot-chunk gauge after the discard stage.
	if hot, herr := cat.HotChunkKeys(); herr != nil {
		errs = append(errs, fmt.Errorf("read hot chunk keys: %w", herr))
	} else {
		metrics.LiveHotChunks(len(hot))
	}

	// Stage 3 — prune scan. Demote each eligible cold artifact (mark it pruning) and
	// defer its file/key destroy to end of run, alongside the discarded hot chunks.
	pruneStart := time.Now()
	idxCovs, chunkRefs, err := eligiblePruneTargets(cat, floor)
	if err != nil {
		errs = append(errs, fmt.Errorf("eligible prune targets: %w", err))
	}
	// One demote op per index coverage, one batched op for the chunk family; each
	// op carries its artifact weight so the meter sums only the ops that ran.
	pruneOps := make([]func() error, 0, len(idxCovs)+1)
	pruneWeights := make([]int, 0, len(idxCovs)+1)
	for _, cov := range idxCovs {
		pruneOps = append(pruneOps, func() error { return pending.demoteTxHashIndex(cat, cov) })
		pruneWeights = append(pruneWeights, 1)
	}
	if len(chunkRefs) > 0 {
		pruneOps = append(pruneOps, func() error { return pending.demoteChunkArtifacts(cat, chunkRefs) })
		pruneWeights = append(pruneWeights, len(chunkRefs))
	}
	// Metered BEFORE the error check so a mid-scan failure keeps the completed count.
	completed, err := runOps(ctx, cfg, pruneOps)
	prunedArtifacts := 0
	for _, w := range pruneWeights[:completed] {
		prunedArtifacts += w
	}
	metrics.Prune(prunedArtifacts, time.Since(pruneStart))
	if err != nil {
		errs = append(errs, fmt.Errorf("prune demote: %w", err))
	}
	if prunedArtifacts > 0 {
		logger.WithField("pruned", prunedArtifacts).Info("lifecycle prune stage complete")
	}

	// End of run: destroy everything demoted this run — discarded hot handles and
	// pruned cold files — after one grace wait (design: wait once, then delete).
	// Reached even when stages errored, so a failing freeze cannot wedge reclaim.
	pending.destroyAll(ctx, cfg)
	return errors.Join(errs...)
}

// BoundarySignal couples ingestion (the producer) to the lifecycle Loop (the
// consumer) as a pure wake-up: ingestion pings a 1-buffered wake at each chunk
// boundary, and the woken tick derives its anchor (the last complete chunk) from
// the catalog itself — the same derivation read-view acquisition runs against its
// snapshot. The signal carries no value, so a slow lifecycle can never fall
// behind — one tick subsumes every skipped boundary — and there is no bounded
// buffer to overflow and thus no "fell behind" fatal path. Safe for one producer
// and one consumer.
type BoundarySignal struct {
	wake chan struct{}
}

// NewBoundarySignal returns a ready signal with an empty latest cell.
func NewBoundarySignal() *BoundarySignal {
	return &BoundarySignal{wake: make(chan struct{}, 1)}
}

// Publish wakes the Loop. The wake is non-blocking: a pending wake already covers
// this boundary (the woken tick derives the newest anchor from the catalog), so a
// full buffer is dropped, never blocked on.
func (s *BoundarySignal) Publish() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Loop is the event-driven lifecycle goroutine. It blocks on the boundary signal's
// wake, derives the last complete chunk from the live catalog (the same
// Catalog.LastCompleteChunk derivation read-view acquisition runs against its
// snapshot, so the run and the queries share one floor-anchor implementation),
// and runs one tick over [floor, lastChunk] (which subsumes every boundary
// skipped while it was busy). The frontier at tick start may be newer than the
// boundary that woke it, which only pulls the next tick's work forward. It
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
			lastComplete, err := cat.LastCompleteChunk()
			if err != nil {
				// Includes ErrNoReadyHotChunk: the live chunk's key exists before
				// the loops start, so an empty scan marks a broken catalog.
				return fmt.Errorf("derive last complete chunk: %w", err)
			}
			if lastComplete < 0 {
				continue // young network: nothing complete yet
			}
			if err := runLifecycle(ctx, cfg, cat, chunk.ID(lastComplete)); err != nil { //nolint:gosec // >= 0
				return err
			}
		}
	}
}
