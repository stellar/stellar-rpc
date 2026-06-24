package streaming

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Executor test harness. The runChunk/runIndex seams let a test drive the
// dependency graph deterministically: a fake chunk build records its order and
// optionally blocks on a release signal; a fake index build records the chunk
// states it observed at the instant it ran.
// ---------------------------------------------------------------------------

// execRecorder captures the interleaving of chunk and index task execution so a
// test can assert wait ordering. All access is mutex-guarded — the executor
// runs tasks on many goroutines.
type execRecorder struct {
	mu sync.Mutex
	// chunkDone[c] is true once the chunk build for c has returned.
	chunkDone map[chunk.ID]bool
	// indexSawAllDeps[w] records, for each index build's window, whether every
	// in-coverage chunk build had already completed when the index build began.
	indexSawAllDeps map[WindowID]bool
	order           []string
}

func newExecRecorder() *execRecorder {
	return &execRecorder{chunkDone: map[chunk.ID]bool{}, indexSawAllDeps: map[WindowID]bool{}}
}

func (r *execRecorder) markChunkDone(c chunk.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunkDone[c] = true
	r.order = append(r.order, "chunk:"+c.String())
}

// indexBegan records, for window w covering [lo,hi], whether all in-coverage
// chunks were already done — the invariant the wait ordering must guarantee.
func (r *execRecorder) indexBegan(w WindowID, lo, hi chunk.ID) {
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
	r.order = append(r.order, "index:"+w.String())
}

// execTestCfg builds an ExecConfig with the task seams installed. workers sets
// the pool size.
func execTestCfg(cat *Catalog, workers int, runChunk func(context.Context, ChunkBuild, ExecConfig) error,
	runIndex func(context.Context, IndexBuild, ExecConfig) error,
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
	cat, _ := smallWindowCatalog(t, 4)
	rec := newExecRecorder()

	// Two windows, each with two chunk builds and one index build covering them.
	plan := Plan{
		ChunkBuilds: []ChunkBuild{
			{Chunk: 0, Artifacts: AllArtifacts()},
			{Chunk: 1, Artifacts: AllArtifacts()},
			{Chunk: 4, Artifacts: AllArtifacts()},
			{Chunk: 5, Artifacts: AllArtifacts()},
		},
		IndexBuilds: []IndexBuild{
			{Window: 0, Lo: 0, Hi: 1},
			{Window: 1, Lo: 4, Hi: 5},
		},
	}

	cfg := execTestCfg(cat, 1,
		func(_ context.Context, cb ChunkBuild, _ ExecConfig) error {
			rec.markChunkDone(cb.Chunk)
			return nil
		},
		func(_ context.Context, b IndexBuild, _ ExecConfig) error {
			rec.indexBegan(b.Window, b.Lo, b.Hi)
			return nil
		},
	)

	require.NoError(t, executePlan(context.Background(), plan, cfg),
		"Workers=1 must not deadlock — index builds wait on done-channels BEFORE acquiring the single slot")

	// Every index build observed all of its in-coverage chunk builds as already
	// complete — the freeze-before-build dependency held.
	require.True(t, rec.indexSawAllDeps[0], "window 0 index must run after chunks 0,1")
	require.True(t, rec.indexSawAllDeps[1], "window 1 index must run after chunks 4,5")
	require.Len(t, rec.chunkDone, 4)
}

// A high worker count must also honor the per-window dependency (no index build
// jumps ahead of its own chunks) while running independent windows concurrently.
func TestExecutePlan_DependencyHoldsUnderConcurrency(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	rec := newExecRecorder()

	plan := Plan{
		ChunkBuilds: []ChunkBuild{
			{Chunk: 0, Artifacts: AllArtifacts()},
			{Chunk: 1, Artifacts: AllArtifacts()},
			{Chunk: 2, Artifacts: AllArtifacts()},
			{Chunk: 3, Artifacts: AllArtifacts()},
		},
		IndexBuilds: []IndexBuild{{Window: 0, Lo: 0, Hi: 3}},
	}

	cfg := execTestCfg(cat, 8,
		func(_ context.Context, cb ChunkBuild, _ ExecConfig) error {
			// Stagger completion so an unsynchronized index build would likely
			// observe a not-yet-done chunk if the wait were broken.
			time.Sleep(time.Duration(uint32(cb.Chunk)+1) * 5 * time.Millisecond)
			rec.markChunkDone(cb.Chunk)
			return nil
		},
		func(_ context.Context, b IndexBuild, _ ExecConfig) error {
			rec.indexBegan(b.Window, b.Lo, b.Hi)
			return nil
		},
	)

	require.NoError(t, executePlan(context.Background(), plan, cfg))
	require.True(t, rec.indexSawAllDeps[0],
		"the index build must wait on ALL four in-coverage chunk builds")
}

// An index build whose coverage chunks are ALREADY frozen (no ChunkBuild in the
// plan) must run immediately — there is no channel to wait on. Models the
// risen-floor / re-derive case where some inputs self-skipped.
func TestExecutePlan_IndexWithNoInPlanDepsRunsImmediately(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	var ran atomic.Bool

	plan := Plan{
		// No chunk builds — every input already frozen.
		IndexBuilds: []IndexBuild{{Window: 0, Lo: 0, Hi: 3}},
	}
	cfg := execTestCfg(cat, 2,
		func(context.Context, ChunkBuild, ExecConfig) error { return nil },
		func(context.Context, IndexBuild, ExecConfig) error { ran.Store(true); return nil },
	)
	require.NoError(t, executePlan(context.Background(), plan, cfg))
	require.True(t, ran.Load(), "an index build with no in-plan deps runs without waiting")
}

// ---------------------------------------------------------------------------
// SUCCESS semantics (item R2-2): a failed chunk build LEAVES its done-channel
// OPEN and returns the error, which cancels gctx. The dependent index build is
// therefore never wedged forever waiting on a failed input: it unblocks through
// the <-gctx.Done() case in its wait loop and bails with gctx.Err() — it never
// proceeds on a missing input. The plan ALWAYS aborts, and the index build never
// hangs (g.Wait returning is itself the proof).
// ---------------------------------------------------------------------------

func TestExecutePlan_FailedChunkAbortsPlanAndIndexNeverHangs(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)

	chunkErr := errors.New("chunk build boom")

	plan := Plan{
		ChunkBuilds: []ChunkBuild{{Chunk: 0, Artifacts: AllArtifacts()}},
		IndexBuilds: []IndexBuild{{Window: 0, Lo: 0, Hi: 0}},
	}

	cfg := execTestCfg(cat, 1,
		func(context.Context, ChunkBuild, ExecConfig) error { return chunkErr },
		func(_ context.Context, _ IndexBuild, _ ExecConfig) error {
			// Under SUCCESS semantics the failed chunk never closes its channel, so
			// this index build should bail through <-gctx.Done() and NEVER reach
			// here. (Left as a guard: if it ever did run, the plan still aborts.)
			return errors.New("index build must bail via gctx, never run on a failed input")
		},
	)

	// The plan aborts regardless of which branch the index build took.
	err := executePlan(context.Background(), plan, cfg)
	require.Error(t, err, "a task exhausting retries aborts the plan")
	require.ErrorIs(t, err, chunkErr, "the first error (the chunk failure) propagates")
}

// The production-path version: a REAL buildThenSweep. Under SUCCESS semantics
// (item R2-2) the failed chunk build leaves its done-channel open, so the index
// build normally bails via <-gctx.Done() before it ever runs. buildTxhashIndex's
// loud .bin precondition is KEPT as a cheap defensive backstop for the case the
// index build wins the race and starts anyway. Either way the invariant holds:
// NO coverage key is written when an input chunk's .bin is not frozen.
func TestExecutePlan_FailedChunkHitsLoudPrecondition(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)

	plan := Plan{
		ChunkBuilds: []ChunkBuild{{Chunk: 0, Artifacts: NewArtifactSet(KindTxHash)}},
		IndexBuilds: []IndexBuild{{Window: 0, Lo: 0, Hi: 0}},
	}

	// runChunk fails (never freezes chunk:0:txhash); runIndex is the REAL
	// buildThenSweep via the production path (cfg.runIndex left nil).
	cfg := ExecConfig{
		Catalog: cat,
		Logger:  silentLogger(),
		Workers: 1,
		runChunk: func(context.Context, ChunkBuild, ExecConfig) error {
			return errors.New("simulated chunk build failure: .bin never frozen")
		},
		// runIndex nil ⇒ executePlan uses the real buildThenSweep.
	}

	err := executePlan(context.Background(), plan, cfg)
	require.Error(t, err)

	// The real precondition fired: chunk 0's txhash is not "frozen", so
	// buildTxhashIndex refused before touching any key — no coverage was created.
	covs, qerr := cat.IndexKeys(0)
	require.NoError(t, qerr)
	require.Empty(t, covs, "no index coverage key may be written when the .bin precondition fails")
}

// ---------------------------------------------------------------------------
// Retry budget + zero-workers guard.
// ---------------------------------------------------------------------------

func TestExecutePlan_RetriesThenSucceeds(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	var attempts atomic.Int32

	plan := Plan{ChunkBuilds: []ChunkBuild{{Chunk: 0, Artifacts: AllArtifacts()}}}
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1, MaxRetries: 3,
		runChunk: func(context.Context, ChunkBuild, ExecConfig) error {
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
	cat, _ := smallWindowCatalog(t, 4)
	var attempts atomic.Int32

	plan := Plan{ChunkBuilds: []ChunkBuild{{Chunk: 0, Artifacts: AllArtifacts()}}}
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1, MaxRetries: 2,
		runChunk: func(context.Context, ChunkBuild, ExecConfig) error {
			attempts.Add(1)
			return errors.New("always fails")
		},
	}
	require.Error(t, executePlan(context.Background(), plan, cfg))
	require.Equal(t, int32(3), attempts.Load(), "1 try + MaxRetries(2) = 3 attempts")
}

func TestExecutePlan_ZeroWorkersIsLoudNotADeadlock(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	cfg := ExecConfig{Catalog: cat, Logger: silentLogger(), Workers: 0}
	err := executePlan(context.Background(), Plan{ChunkBuilds: []ChunkBuild{{Chunk: 0}}}, cfg)
	require.ErrorContains(t, err, "Workers must be > 0",
		"a zero pool must be rejected, not deadlock")
}

// Context cancellation propagates: a long-running chunk build observing a
// canceled context returns promptly and the whole plan aborts.
func TestExecutePlan_ContextCancelAborts(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	ctx, cancel := context.WithCancel(context.Background())

	plan := Plan{ChunkBuilds: []ChunkBuild{
		{Chunk: 0, Artifacts: AllArtifacts()},
		{Chunk: 1, Artifacts: AllArtifacts()},
	}}
	var started sync.WaitGroup
	started.Add(1)
	var once sync.Once
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 2,
		runChunk: func(ctx context.Context, _ ChunkBuild, _ ExecConfig) error {
			once.Do(started.Done)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	go func() { started.Wait(); cancel() }()
	require.Error(t, executePlan(ctx, plan, cfg))
}

// ---------------------------------------------------------------------------
// withRetries backoff (#816 comment pt4): a retry waits with EXPONENTIAL
// backoff between attempts (the design's "the lifecycle retries with backoff"),
// and a ctx-cancelled wait aborts immediately rather than burning the budget.
// ---------------------------------------------------------------------------

func TestWithRetries_ExponentialBackoffBetweenAttempts(t *testing.T) {
	var delays []time.Duration
	cfg := ExecConfig{
		MaxRetries:   3,
		RetryBackoff: time.Second,
		retrySleep: func(_ context.Context, d time.Duration) error {
			delays = append(delays, d)
			return nil
		},
	}
	calls := 0
	err := withRetries(context.Background(), cfg, func() error {
		calls++
		return errors.New("always fails")
	})
	require.Error(t, err)
	require.Equal(t, 4, calls, "1 initial attempt + 3 retries")
	// One backoff wait BEFORE each of the 3 retries (never before the first
	// attempt): base, 2*base, 4*base.
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}, delays)
}

func TestWithRetries_CtxCancelDuringBackoffAborts(t *testing.T) {
	cfg := ExecConfig{
		MaxRetries:   5,
		RetryBackoff: time.Second,
		retrySleep: func(_ context.Context, _ time.Duration) error {
			return context.Canceled // simulate ctx cancelled during the wait
		},
	}
	calls := 0
	err := withRetries(context.Background(), cfg, func() error {
		calls++
		return errors.New("fails")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls, "one attempt, then the cancelled backoff wait stops the retries")
}

func TestRetryBackoffFor_DoublesAndCaps(t *testing.T) {
	require.Equal(t, time.Second, retryBackoffFor(1, time.Second))
	require.Equal(t, 2*time.Second, retryBackoffFor(2, time.Second))
	require.Equal(t, 4*time.Second, retryBackoffFor(3, time.Second))
	require.Equal(t, maxRetryBackoff, retryBackoffFor(99, time.Second), "capped at maxRetryBackoff")
	require.Equal(t, defaultRetryBackoff, retryBackoffFor(1, 0), "zero base ⇒ default")
}
