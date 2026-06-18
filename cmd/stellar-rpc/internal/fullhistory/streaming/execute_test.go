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
// A failed chunk build still CLOSES its done-channel (broadcast is completion,
// not success). The dependent index build is therefore never wedged forever
// waiting on a failed input: it either wins the race against context
// cancellation and starts (then fails its precondition) or observes the
// cancel — both reach abort-and-restart. The plan ALWAYS aborts. The
// deterministic proof that the release mechanism is the close (not luck) is
// below: with cancellation removed (MaxRetries lets the chunk eventually
// succeed... no — here we prove the channel closes by NOT having the index
// build observe a hang).
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
			// Reached only if the index build won the race against gctx
			// cancellation — possible because the failed chunk closed its done
			// channel. If it loses the race it returns gctx.Err() from the wait
			// loop and never gets here; both outcomes abort the plan. The point of
			// the close is that this goroutine NEVER hangs forever — the test
			// completing (g.Wait returns) is itself the proof.
			return errors.New("index build should have failed its precondition")
		},
	)

	// The plan aborts regardless of which branch the index build took.
	err := executePlan(context.Background(), plan, cfg)
	require.Error(t, err, "a task exhausting retries aborts the plan")
	require.ErrorIs(t, err, chunkErr, "the first error (the chunk failure) propagates")
}

// The production-path version: a REAL buildThenSweep, whose .bin precondition is
// the load-bearing backstop. The chunk build (fake) fails to freeze the .bin, so
// the real index build hits buildTxhashIndex's loud precondition and aborts
// WITHOUT writing any coverage key.
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
