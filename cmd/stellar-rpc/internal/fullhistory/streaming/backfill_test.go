package streaming

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// runBackfill end-to-end on the seamed executor: resolve the diff, then
// executePlan runs the resolved plan. There is NO upfront producibility gate
// (item R2-5); an unproducible chunk fatals from backfillSource per chunk when
// the executor reaches it (exercised below through the real processChunk path).
// ---------------------------------------------------------------------------

func TestRunBackfill_ResolvesThenExecutes(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)

	var chunksRun, indexRun atomic.Int32
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 2,
		Process: ProcessConfig{Backend: zeroTxBackend(t)},
		runChunk: func(context.Context, ChunkBuild, ExecConfig) error {
			chunksRun.Add(1)
			return nil
		},
		runIndex: func(context.Context, IndexBuild, ExecConfig) error {
			indexRun.Add(1)
			return nil
		},
	}

	// Fresh catalog, range [0,3] (window 0): resolve schedules 4 chunk builds +
	// 1 terminal index build.
	require.NoError(t, runBackfill(context.Background(), cfg, 0, 3))
	require.Equal(t, int32(4), chunksRun.Load())
	require.Equal(t, int32(1), indexRun.Load())
}

// No backend AND a genuine fall-through chunk (nothing local): the daemon still
// fatals — now from backfillSource itself when the executor reaches the chunk
// (item R2-5 folded the upfront gate into the per-chunk source selection). The
// REAL processChunk path runs (no runChunk seam), so backfillSource picks the
// (3) bulk-backend branch, finds no backend, and aborts the plan.
func TestRunBackfill_NoBackendNoLocalCopyFatals(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process: ProcessConfig{HotProbe: &fakeHotProbe{}}, // not "ready", no backend
	}
	err := runBackfill(context.Background(), cfg, 0, 0)
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
		runChunk: func(context.Context, ChunkBuild, ExecConfig) error { ran++; return nil },
	}
	require.NoError(t, runBackfill(context.Background(), cfg, 5, 4))
	require.Zero(t, ran)
}
