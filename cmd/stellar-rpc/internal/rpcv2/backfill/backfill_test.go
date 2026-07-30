package backfill

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// runBackfill: resolve then executePlan. No upfront producibility gate — an
// unproducible chunk fatals per-chunk from backfillSource.
// ---------------------------------------------------------------------------

func TestRunBackfill_ResolvesThenExecutes(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)

	var chunksRun, indexRun atomic.Int32
	rec := newRecordingMetrics()
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 2, Metrics: rec,
		Process: ProcessConfig{Backend: zeroTxBackend(t)},
		runChunk: func(context.Context, ChunkBuild) error {
			chunksRun.Add(1)
			return nil
		},
		runIndex: func(context.Context, IndexBuild) error {
			indexRun.Add(1)
			return nil
		},
	}

	// Range [0,3]: 4 chunk builds + 1 terminal index build.
	require.NoError(t, RunBackfill(context.Background(), cfg, 0, 3))
	require.Equal(t, int32(4), chunksRun.Load())
	require.Equal(t, int32(1), indexRun.Load())

	// The pass reports exactly one Freeze stage (the plan-and-execute wall-clock).
	require.Len(t, rec.freeze, 1)
}

// No backend and nothing local fatals from backfillSource on the real processChunk
// path (no runChunk seam): the bulk-backend branch finds no backend and aborts.
func TestRunBackfill_NoBackendNoLocalCopyFatals(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process: ProcessConfig{}, // no backend, nothing local
	}
	err := RunBackfill(context.Background(), cfg, 0, 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "no bulk backend is configured")
}

// An inverted range (younger-than-one-chunk network) backfills nothing.
func TestRunBackfill_InvertedRangeIsNoop(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	var ran int
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process:  ProcessConfig{Backend: zeroTxBackend(t)},
		runChunk: func(context.Context, ChunkBuild) error { ran++; return nil },
	}
	require.NoError(t, RunBackfill(context.Background(), cfg, 5, 4))
	require.Zero(t, ran)
}
