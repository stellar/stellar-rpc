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
// Executor test harness. The runChunk seam lets a test drive the chunk-build
// pool deterministically: a fake chunk build records its order and optionally
// blocks on a release signal.
// ---------------------------------------------------------------------------

// execRecorder captures chunk task execution so a test can assert completion.
// All access is mutex-guarded — the executor runs tasks on many goroutines.
type execRecorder struct {
	mu sync.Mutex
	// chunkDone[c] is true once the chunk build for c has returned.
	chunkDone map[chunk.ID]bool
	order     []string
}

func newExecRecorder() *execRecorder {
	return &execRecorder{chunkDone: map[chunk.ID]bool{}}
}

func (r *execRecorder) markChunkDone(c chunk.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunkDone[c] = true
	r.order = append(r.order, "chunk:"+c.String())
}

// execTestCfg builds an ExecConfig with the chunk-build seam installed. workers
// sets the pool size.
func execTestCfg(cat *Catalog, workers int, runChunk func(context.Context, ChunkBuild, ExecConfig) error) ExecConfig {
	return ExecConfig{
		Catalog:  cat,
		Logger:   silentLogger(),
		Workers:  workers,
		runChunk: runChunk,
	}
}

// ---------------------------------------------------------------------------
// No deadlock at Workers=1; every planned chunk build runs.
// ---------------------------------------------------------------------------

func TestExecutePlan_RunsEveryChunkBuild_Workers1(t *testing.T) {
	cat, _ := testCatalog(t)
	rec := newExecRecorder()

	plan := Plan{
		ChunkBuilds: []ChunkBuild{
			{Chunk: 0, Artifacts: AllArtifacts()},
			{Chunk: 1, Artifacts: AllArtifacts()},
			{Chunk: 4, Artifacts: AllArtifacts()},
			{Chunk: 5, Artifacts: AllArtifacts()},
		},
	}

	cfg := execTestCfg(cat, 1, func(_ context.Context, cb ChunkBuild, _ ExecConfig) error {
		rec.markChunkDone(cb.Chunk)
		return nil
	})

	require.NoError(t, executePlan(context.Background(), plan, cfg),
		"Workers=1 must not deadlock")
	require.Len(t, rec.chunkDone, 4)
}

// A high worker count runs every chunk build concurrently without losing any.
func TestExecutePlan_RunsEveryChunkBuildUnderConcurrency(t *testing.T) {
	cat, _ := testCatalog(t)
	rec := newExecRecorder()

	plan := Plan{
		ChunkBuilds: []ChunkBuild{
			{Chunk: 0, Artifacts: AllArtifacts()},
			{Chunk: 1, Artifacts: AllArtifacts()},
			{Chunk: 2, Artifacts: AllArtifacts()},
			{Chunk: 3, Artifacts: AllArtifacts()},
		},
	}

	cfg := execTestCfg(cat, 8, func(_ context.Context, cb ChunkBuild, _ ExecConfig) error {
		time.Sleep(time.Duration(uint32(cb.Chunk)+1) * 5 * time.Millisecond)
		rec.markChunkDone(cb.Chunk)
		return nil
	})

	require.NoError(t, executePlan(context.Background(), plan, cfg))
	require.Len(t, rec.chunkDone, 4)
}

// ---------------------------------------------------------------------------
// SUCCESS semantics (item R2-2): a failed chunk build returns the error, which
// cancels gctx; the plan ALWAYS aborts with the first error.
// ---------------------------------------------------------------------------

func TestExecutePlan_FailedChunkAbortsPlan(t *testing.T) {
	cat, _ := testCatalog(t)

	chunkErr := errors.New("chunk build boom")

	plan := Plan{
		ChunkBuilds: []ChunkBuild{{Chunk: 0, Artifacts: AllArtifacts()}},
	}

	cfg := execTestCfg(cat, 1, func(context.Context, ChunkBuild, ExecConfig) error { return chunkErr })

	err := executePlan(context.Background(), plan, cfg)
	require.Error(t, err, "a task exhausting retries aborts the plan")
	require.ErrorIs(t, err, chunkErr, "the chunk failure propagates")
}

// ---------------------------------------------------------------------------
// Retry budget + zero-workers guard.
// ---------------------------------------------------------------------------

func TestExecutePlan_RetriesThenSucceeds(t *testing.T) {
	cat, _ := testCatalog(t)
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
	cat, _ := testCatalog(t)
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
	cat, _ := testCatalog(t)
	cfg := ExecConfig{Catalog: cat, Logger: silentLogger(), Workers: 0}
	err := executePlan(context.Background(), Plan{ChunkBuilds: []ChunkBuild{{Chunk: 0}}}, cfg)
	require.ErrorContains(t, err, "Workers must be > 0",
		"a zero pool must be rejected, not deadlock")
}

// Context cancellation propagates: a long-running chunk build observing a
// canceled context returns promptly and the whole plan aborts.
func TestExecutePlan_ContextCancelAborts(t *testing.T) {
	cat, _ := testCatalog(t)
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
