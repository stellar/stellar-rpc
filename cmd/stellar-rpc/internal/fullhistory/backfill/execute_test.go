package backfill

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// ---------------------------------------------------------------------------
// Executor test harness: runChunk/runIndex seams drive the dependency graph
// deterministically.
// ---------------------------------------------------------------------------

// execRecorder captures chunk/index execution interleaving for wait-ordering
// assertions. Mutex-guarded — the executor runs tasks on many goroutines.
type execRecorder struct {
	mu sync.Mutex
	// chunkDone[c] is true once chunk c's build returned.
	chunkDone map[chunk.ID]bool
	// indexSawAllDeps[w] is true if every in-coverage chunk was done when window
	// w's index build began.
	indexSawAllDeps map[geometry.TxHashIndexID]bool
	order           []string
}

func newExecRecorder() *execRecorder {
	return &execRecorder{chunkDone: map[chunk.ID]bool{}, indexSawAllDeps: map[geometry.TxHashIndexID]bool{}}
}

func (r *execRecorder) markChunkDone(c chunk.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunkDone[c] = true
	r.order = append(r.order, "chunk:"+c.String())
}

// indexBegan records whether all of window w's in-coverage chunks were already
// done — the wait-ordering invariant.
func (r *execRecorder) indexBegan(w geometry.TxHashIndexID, lo, hi chunk.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := true
	for c := lo; c <= hi; c++ {
		if !r.chunkDone[c] {
			all = false
			break
		}
		if c == hi {
			break
		}
	}
	r.indexSawAllDeps[w] = all
	r.order = append(r.order, "txhash_index:"+w.String())
}

// execTestCfg builds an ExecConfig with the task seams installed.
func execTestCfg(cat *catalog.Catalog, workers int, runChunk func(context.Context, ChunkBuild) error,
	runIndex func(context.Context, IndexBuild) error,
) ExecConfig {
	return ExecConfig{
		Catalog:  cat,
		Logger:   silentLogger(),
		Workers:  workers,
		runChunk: runChunk,
		runIndex: runIndex,
	}
}

// ---------------------------------------------------------------------------
// Wait ordering + no deadlock at Workers=1.
// ---------------------------------------------------------------------------

func TestExecutePlan_IndexWaitsOnInCoverageChunks_Workers1(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	rec := newExecRecorder()

	// Two windows, each with two chunk builds and one index build covering them.
	plan := Plan{
		ChunkBuilds: []ChunkBuild{
			{Chunk: 0, Artifacts: catalog.AllArtifacts()},
			{Chunk: 1, Artifacts: catalog.AllArtifacts()},
			{Chunk: 4, Artifacts: catalog.AllArtifacts()},
			{Chunk: 5, Artifacts: catalog.AllArtifacts()},
		},
		IndexBuilds: []IndexBuild{
			{Index: 0, Lo: 0, Hi: 1},
			{Index: 1, Lo: 4, Hi: 5},
		},
	}

	cfg := execTestCfg(cat, 1,
		func(_ context.Context, cb ChunkBuild) error {
			rec.markChunkDone(cb.Chunk)
			return nil
		},
		func(_ context.Context, b IndexBuild) error {
			rec.indexBegan(b.Index, b.Lo, b.Hi)
			return nil
		},
	)

	require.NoError(t, executePlan(context.Background(), plan, cfg),
		"Workers=1 must not deadlock — index builds wait on done-channels BEFORE acquiring the single slot")

	// Every index build saw all its in-coverage chunks already done.
	require.True(t, rec.indexSawAllDeps[0], "window 0 index must run after chunks 0,1")
	require.True(t, rec.indexSawAllDeps[1], "window 1 index must run after chunks 4,5")
	require.Len(t, rec.chunkDone, 4)
}

// A high worker count still honors the per-window dependency (no index build
// jumps ahead of its chunks) while running independent windows concurrently.
func TestExecutePlan_DependencyHoldsUnderConcurrency(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	rec := newExecRecorder()

	plan := Plan{
		ChunkBuilds: []ChunkBuild{
			{Chunk: 0, Artifacts: catalog.AllArtifacts()},
			{Chunk: 1, Artifacts: catalog.AllArtifacts()},
			{Chunk: 2, Artifacts: catalog.AllArtifacts()},
			{Chunk: 3, Artifacts: catalog.AllArtifacts()},
		},
		IndexBuilds: []IndexBuild{{Index: 0, Lo: 0, Hi: 3}},
	}

	cfg := execTestCfg(cat, 8,
		func(_ context.Context, cb ChunkBuild) error {
			// Stagger completion so a broken wait would observe a not-yet-done chunk.
			time.Sleep(time.Duration(uint32(cb.Chunk)+1) * 5 * time.Millisecond)
			rec.markChunkDone(cb.Chunk)
			return nil
		},
		func(_ context.Context, b IndexBuild) error {
			rec.indexBegan(b.Index, b.Lo, b.Hi)
			return nil
		},
	)

	require.NoError(t, executePlan(context.Background(), plan, cfg))
	require.True(t, rec.indexSawAllDeps[0],
		"the index build must wait on ALL four in-coverage chunk builds")
}

// An index build whose coverage chunks are already frozen (no ChunkBuild in plan)
// runs immediately — nothing to wait on (the risen-floor / re-derive case).
func TestExecutePlan_IndexWithNoInPlanDepsRunsImmediately(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	var ran atomic.Bool

	plan := Plan{
		// No chunk builds — every input already frozen.
		IndexBuilds: []IndexBuild{{Index: 0, Lo: 0, Hi: 3}},
	}
	cfg := execTestCfg(cat, 2,
		func(context.Context, ChunkBuild) error { return nil },
		func(context.Context, IndexBuild) error { ran.Store(true); return nil },
	)
	require.NoError(t, executePlan(context.Background(), plan, cfg))
	require.True(t, ran.Load(), "an index build with no in-plan deps runs without waiting")
}

// ---------------------------------------------------------------------------
// SUCCESS semantics: a failed chunk build leaves its done-channel open and
// cancels gctx, so the dependent index build unblocks via <-gctx.Done() and bails
// — never wedged, never running on a missing input; the plan always aborts.
// ---------------------------------------------------------------------------

func TestExecutePlan_FailedChunkAbortsPlanAndIndexNeverHangs(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)

	chunkErr := errors.New("chunk build boom")

	plan := Plan{
		ChunkBuilds: []ChunkBuild{{Chunk: 0, Artifacts: catalog.AllArtifacts()}},
		IndexBuilds: []IndexBuild{{Index: 0, Lo: 0, Hi: 0}},
	}

	cfg := execTestCfg(cat, 1,
		func(context.Context, ChunkBuild) error { return chunkErr },
		func(_ context.Context, _ IndexBuild) error {
			// Should bail via <-gctx.Done() and never reach here (guard).
			return errors.New("index build must bail via gctx, never run on a failed input")
		},
	)

	err := executePlan(context.Background(), plan, cfg)
	require.Error(t, err, "a task exhausting retries aborts the plan")
	require.ErrorIs(t, err, chunkErr, "the first error (the chunk failure) propagates")
}

// Production-path version (real buildThenSweep): if the index build wins the race
// and starts, buildTxhashIndex's loud .bin precondition backstops it. Either way
// no coverage key is written when an input chunk's .bin is not frozen.
func TestExecutePlan_FailedChunkHitsLoudPrecondition(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)

	plan := Plan{
		ChunkBuilds: []ChunkBuild{{Chunk: 0, Artifacts: catalog.NewArtifactSet(geometry.KindTxHash)}},
		IndexBuilds: []IndexBuild{{Index: 0, Lo: 0, Hi: 0}},
	}

	// runChunk fails (never freezes chunk:0:txhash); runIndex nil ⇒ real buildThenSweep.
	cfg := ExecConfig{
		Catalog: cat,
		Logger:  silentLogger(),
		Workers: 1,
		runChunk: func(context.Context, ChunkBuild) error {
			return errors.New("simulated chunk build failure: .bin never frozen")
		},
		// runIndex nil ⇒ executePlan uses the real buildThenSweep.
	}

	err := executePlan(context.Background(), plan, cfg)
	require.Error(t, err)

	// Precondition fired (chunk 0 txhash not frozen) ⇒ no coverage created.
	covs, qerr := cat.TxHashIndexKeys(0)
	require.NoError(t, qerr)
	require.Empty(t, covs, "no index coverage key may be written when the .bin precondition fails")
}

// ---------------------------------------------------------------------------
// Retry budget + zero-workers guard.
// ---------------------------------------------------------------------------

func TestExecutePlan_RetriesThenSucceeds(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	var attempts atomic.Int32

	plan := Plan{ChunkBuilds: []ChunkBuild{{Chunk: 0, Artifacts: catalog.AllArtifacts()}}}
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1, MaxRetries: 3,
		retryBackoff: time.Millisecond, // keep the real backoff waits negligible
		runChunk: func(context.Context, ChunkBuild) error {
			if attempts.Add(1) < 3 {
				return errors.New("transient")
			}
			return nil
		},
	}
	require.NoError(t, executePlan(context.Background(), plan, cfg))
	require.Equal(t, int32(3), attempts.Load(), "fn runs until it succeeds within the budget")
}

func TestExecutePlan_ExhaustsRetriesAndAborts(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	var attempts atomic.Int32

	plan := Plan{ChunkBuilds: []ChunkBuild{{Chunk: 0, Artifacts: catalog.AllArtifacts()}}}
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1, MaxRetries: 2,
		retryBackoff: time.Millisecond, // keep the real backoff waits negligible
		runChunk: func(context.Context, ChunkBuild) error {
			attempts.Add(1)
			return errors.New("always fails")
		},
	}
	require.Error(t, executePlan(context.Background(), plan, cfg))
	require.Equal(t, int32(3), attempts.Load(), "1 try + MaxRetries(2) = 3 attempts")
}

func TestExecutePlan_ZeroWorkersIsLoudNotADeadlock(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	cfg := ExecConfig{Catalog: cat, Logger: silentLogger(), Workers: 0}
	err := executePlan(context.Background(), Plan{ChunkBuilds: []ChunkBuild{{Chunk: 0}}}, cfg)
	require.ErrorContains(t, err, "Workers must be > 0",
		"a zero pool must be rejected, not deadlock")
}

// Context cancellation propagates: a parked chunk build returns promptly and the
// plan aborts.
func TestExecutePlan_ContextCancelAborts(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	ctx, cancel := context.WithCancel(context.Background())

	plan := Plan{ChunkBuilds: []ChunkBuild{
		{Chunk: 0, Artifacts: catalog.AllArtifacts()},
		{Chunk: 1, Artifacts: catalog.AllArtifacts()},
	}}
	var started sync.WaitGroup
	started.Add(1)
	var once sync.Once
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 2,
		runChunk: func(ctx context.Context, _ ChunkBuild) error {
			once.Do(started.Done)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	go func() { started.Wait(); cancel() }()
	require.Error(t, executePlan(ctx, plan, cfg))
}

// Cancellation while an index build is parked in its dependency wait must unblock
// it via <-gctx.Done() (covers the index wait loop; the prior test covers chunks).
func TestExecutePlan_ContextCancelUnblocksParkedIndexBuild(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	ctx, cancel := context.WithCancel(context.Background())

	plan := Plan{
		ChunkBuilds: []ChunkBuild{{Chunk: 0, Artifacts: catalog.AllArtifacts()}},
		IndexBuilds: []IndexBuild{{Index: 0, Lo: 0, Hi: 0}},
	}

	var started sync.WaitGroup
	started.Add(1)
	var once sync.Once
	var indexRan atomic.Bool

	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 2,
		runChunk: func(ctx context.Context, _ ChunkBuild) error {
			once.Do(started.Done)
			// Blocks until cancel ⇒ never closes done[0], forcing the index to park.
			<-ctx.Done()
			return ctx.Err()
		},
		runIndex: func(context.Context, IndexBuild) error {
			indexRan.Store(true) // must NEVER run on an input that never froze
			return nil
		},
	}

	// Cancel once the chunk build runs (index parked on done[0]).
	go func() { started.Wait(); cancel() }()
	require.Error(t, executePlan(ctx, plan, cfg), "cancel must abort the plan, not hang")
	require.False(t, indexRan.Load(),
		"a parked index build must bail via gctx, never run on a chunk that never froze")
}

// The real index-build path emits exactly one Rebuild per index build.
func TestExecutePlan_ReportsRebuildPerIndexBuild(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	rec := newRecordingMetrics()

	plan := Plan{
		ChunkBuilds: []ChunkBuild{
			{Chunk: 0, Artifacts: catalog.AllArtifacts()},
			{Chunk: 1, Artifacts: catalog.AllArtifacts()},
			{Chunk: 4, Artifacts: catalog.AllArtifacts()},
		},
		IndexBuilds: []IndexBuild{
			{Index: 0, Lo: 0, Hi: 1}, // 2 chunks folded
			{Index: 1, Lo: 4, Hi: 4}, // 1 chunk folded
		},
	}

	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 4, Metrics: rec,
		runChunk: func(context.Context, ChunkBuild) error { return nil },
		runIndex: func(context.Context, IndexBuild) error { return nil },
	}
	require.NoError(t, executePlan(context.Background(), plan, cfg))

	require.Len(t, rec.rebuild, 2, "one Rebuild reported per index build")
}

// ---------------------------------------------------------------------------
// withRetries backoff: exponential between attempts; a ctx-canceled wait aborts.
// ---------------------------------------------------------------------------

// The retry policy doubles from the base, caps at maxRetryBackoff, and Stops after
// exactly MaxRetries waits (so withRetries makes MaxRetries+1 attempts).
func TestRetryBackOff_DoublesCapsAndStops(t *testing.T) {
	cfg := ExecConfig{MaxRetries: 6, retryBackoff: time.Second}
	bo := cfg.retryBackOff()
	var delays []time.Duration
	for {
		d := bo.NextBackOff()
		if d == backoff.Stop {
			break
		}
		delays = append(delays, d)
	}
	require.Equal(t, []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, maxRetryBackoff, // 32s would exceed the cap
	}, delays, "×2 from base, capped at maxRetryBackoff, then Stop after MaxRetries waits")
}

// Zero base falls back to the default interval (so a fresh ExecConfig still backs off).
func TestRetryBackOff_ZeroBaseUsesDefault(t *testing.T) {
	cfg := ExecConfig{MaxRetries: 1, retryBackoff: 0}
	require.Equal(t, defaultRetryBackoff, cfg.retryBackOff().NextBackOff())
}

// withRetries makes 1 initial attempt + MaxRetries retries before giving up. A
// near-zero base keeps the real backoff waits negligible.
func TestWithRetries_AttemptsThenGivesUp(t *testing.T) {
	cfg := ExecConfig{MaxRetries: 3, retryBackoff: time.Nanosecond}
	calls := 0
	err := withRetries(context.Background(), cfg, func() error {
		calls++
		return errors.New("always fails")
	})
	require.Error(t, err)
	require.Equal(t, 4, calls, "1 initial attempt + 3 retries")
}

// A canceled ctx aborts the retries with ctx.Err() after the in-flight attempt.
func TestWithRetries_CtxCancelAborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := ExecConfig{MaxRetries: 5, retryBackoff: time.Hour}
	calls := 0
	err := withRetries(ctx, cfg, func() error {
		calls++
		return errors.New("fails")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls, "one attempt, then the canceled backoff stops the retries")
}
