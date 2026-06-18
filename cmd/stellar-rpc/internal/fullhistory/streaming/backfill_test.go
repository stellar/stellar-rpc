package streaming

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// validateRangeProducible — the only thing runBackfill adds over executePlan.
// ---------------------------------------------------------------------------

// A configured bulk backend makes every chunk producible: the check passes
// without examining the catalog.
func TestValidateRangeProducible_BackendCoversEverything(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process: ProcessConfig{Backend: zeroTxBackend(t)},
	}
	require.NoError(t, validateRangeProducible(cfg, 0, 3),
		"a configured backend produces any fall-through chunk")
}

// No backend AND a genuine fall-through chunk (nothing local) is fatal before
// any work — the backfill would otherwise abort mid-flight on every retry.
func TestValidateRangeProducible_NoBackendNoLocalCopyFails(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process: ProcessConfig{HotProbe: &fakeHotProbe{}}, // not "ready"
	}
	err := validateRangeProducible(cfg, 0, 3)
	require.Error(t, err)
	require.ErrorContains(t, err, "no bulk backend is configured")
}

// No backend, but every requested chunk is already frozen ⇒ the resolver
// schedules no ChunkBuild, so there is nothing to validate and it passes. This
// is the steady-state restart whose range is entirely local.
func TestValidateRangeProducible_NoBackendButAllFrozen(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, KindLFS, KindEvents)
	}
	freezeCoverage(t, cat, 0, 0, 3)

	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process: ProcessConfig{HotProbe: &fakeHotProbe{}},
	}
	require.NoError(t, validateRangeProducible(cfg, 0, 3),
		"all-frozen range schedules no chunk build, so nothing needs a source")
}

// No backend, but a needed chunk is re-derivable from its frozen .pack (lfs not
// requested) ⇒ producible locally. Model the re-derive branch: chunk 0 has lfs
// frozen with a real pack on disk, only its .bin is missing.
func TestValidateRangeProducible_NoBackendPackReDerive(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)

	// chunk 0: lfs+events frozen with a real pack file present; .bin absent.
	writeArtifact(t, cat.layout.LedgerPackPath(0))
	freezeKinds(t, cat, 0, KindLFS, KindEvents)

	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process: ProcessConfig{HotProbe: &fakeHotProbe{}},
	}
	// Range [0,0]: resolve schedules a ChunkBuild for chunk 0 (its .bin is
	// missing) requesting ONLY txhash (lfs/events frozen). lfs not requested ⇒
	// the frozen .pack re-derives it locally ⇒ producible.
	require.NoError(t, validateRangeProducible(cfg, 0, 0))
}

// No backend, a needed chunk is complete in a "ready" hot tier ⇒ producible.
func TestValidateRangeProducible_NoBackendHotComplete(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	require.NoError(t, cat.FlipHotReady(0)) // hot:chunk:0 = "ready"

	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process: ProcessConfig{
			// Complete: the single DB's max committed seq reaches chunk 0's last ledger.
			HotProbe: &fakeHotProbe{ok: true, chunk: &fakeHotChunk{
				maxSeq: chunk.ID(0).LastLedger(), present: true,
			}},
		},
	}
	require.NoError(t, validateRangeProducible(cfg, 0, 0),
		"a ready+complete hot tier produces the chunk locally")
}

// No backend, a "ready" hot key whose tier is INCOMPLETE (and no pack) falls
// through to no-source ⇒ fatal, matching catchupSource's staleness fall-through.
func TestValidateRangeProducible_NoBackendHotIncompleteFails(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	require.NoError(t, cat.FlipHotReady(0))

	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process: ProcessConfig{
			HotProbe: &fakeHotProbe{ok: true, chunk: &fakeHotChunk{
				maxSeq: chunk.ID(0).FirstLedger(), present: true, // far short of LastLedger
			}},
		},
	}
	err := validateRangeProducible(cfg, 0, 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "no bulk backend is configured")
}

// ---------------------------------------------------------------------------
// runBackfill end-to-end on the seamed executor: validate passes (backend
// configured), then executePlan runs the resolved plan.
// ---------------------------------------------------------------------------

func TestRunBackfill_ValidatesThenExecutes(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)

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

// runBackfill aborts before any executePlan work when validation fails.
func TestRunBackfill_AbortsOnUnproducibleRange(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)

	var ran int
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process:  ProcessConfig{HotProbe: &fakeHotProbe{}}, // no backend, nothing local
		runChunk: func(context.Context, ChunkBuild, ExecConfig) error { ran++; return nil },
	}
	err := runBackfill(context.Background(), cfg, 0, 3)
	require.Error(t, err)
	require.ErrorContains(t, err, "no bulk backend is configured")
	require.Zero(t, ran, "no task runs when the range is not producible")
}

// An inverted range (younger-than-one-chunk network) backfills nothing.
func TestRunBackfill_InvertedRangeIsNoop(t *testing.T) {
	cat, _ := smallWindowCatalog(t, 4)
	var ran int
	cfg := ExecConfig{
		Catalog: cat, Logger: silentLogger(), Workers: 1,
		Process:  ProcessConfig{Backend: zeroTxBackend(t)},
		runChunk: func(context.Context, ChunkBuild, ExecConfig) error { ran++; return nil },
	}
	require.NoError(t, runBackfill(context.Background(), cfg, 5, 4))
	require.Zero(t, ran)
}
