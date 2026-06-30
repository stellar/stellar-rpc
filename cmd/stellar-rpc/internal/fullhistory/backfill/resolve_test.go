package backfill

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// resolve test helpers — set catalog state via the one-write protocol so resolve
// sees the durable keys production would.
// ---------------------------------------------------------------------------

// freezeKinds flips the given per-chunk kinds to "frozen" via the one-write protocol.
func freezeKinds(t *testing.T, cat *catalog.Catalog, chunkID chunk.ID, kinds ...geometry.Kind) {
	t.Helper()
	require.NoError(t, cat.MarkChunkFreezing(chunkID, kinds...))
	require.NoError(t, cat.FlipChunkFrozen(chunkID, kinds...))
}

// freezeCoverage marks and commits a frozen index coverage [0,hi] for window 0.
func freezeCoverage(t *testing.T, cat *catalog.Catalog, hi chunk.ID) {
	t.Helper()
	cov, err := cat.MarkTxHashIndexFreezing(0, 0, hi)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(cov))
}

// resolveCfg wires a minimal ExecConfig over a small-window catalog for resolve
// tests (resolve never runs a task, so the primitive deps stay nil).
func resolveCfg(cat *catalog.Catalog) ExecConfig {
	return ExecConfig{Catalog: cat, Logger: silentLogger(), Workers: 1}
}

// chunkSet collects the ChunkBuild chunk ids into a slice for assertions.
func chunkSet(p Plan) []chunk.ID {
	out := make([]chunk.ID, len(p.ChunkBuilds))
	for i, cb := range p.ChunkBuilds {
		out[i] = cb.Chunk
	}
	return out
}

// findChunkBuild returns the ChunkBuild for c, or ok=false.
func findChunkBuild(p Plan, c chunk.ID) (ChunkBuild, bool) {
	for _, cb := range p.ChunkBuilds {
		if cb.Chunk == c {
			return cb, true
		}
	}
	return ChunkBuild{}, false
}

// ---------------------------------------------------------------------------
// Inverted range guard.
// ---------------------------------------------------------------------------

func TestResolve_InvertedRangeIsEmpty(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)
	plan, err := resolve(resolveCfg(cat), 5, 4)
	require.NoError(t, err)
	require.True(t, plan.Empty(), "rangeEnd < rangeStart must yield an empty plan")
}

// ---------------------------------------------------------------------------
// Steady-state restart: a fully-frozen, finalized window resolves to nothing.
// ---------------------------------------------------------------------------

func TestResolve_SteadyStateRestartIsEmpty(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4) // window 0 = chunks [0,3]

	// Post-finalization steady state: ledgers+events frozen, terminal coverage
	// [0,3] frozen, .bins demoted+swept (no txhash keys remain).
	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents)
	}
	freezeCoverage(t, cat, 3)

	plan, err := resolve(resolveCfg(cat), 0, 3)
	require.NoError(t, err)
	require.True(t, plan.Empty(),
		"steady-state restart of a finalized window must schedule nothing, got %+v", plan)
}

// ---------------------------------------------------------------------------
// Risen floor: desired ⊆ stored (wider) coverage ⇒ nothing scheduled (the stale
// stored lo is the reader retention contract's problem, not a rebuild trigger).
// ---------------------------------------------------------------------------

func TestResolve_RisenFloorSchedulesNothing(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4) // window 0 = chunks [0,3]

	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents)
	}
	// Stored terminal coverage spans the whole window [0,3].
	freezeCoverage(t, cat, 3)

	// The floor rose to chunk 2: desired = [2,3] ⊆ stored [0,3].
	plan, err := resolve(resolveCfg(cat), 2, 3)
	require.NoError(t, err)
	require.Empty(t, plan.IndexBuilds, "a risen floor must not trigger a rebuild")
	require.Empty(t, plan.ChunkBuilds, "ledgers/events frozen for the in-range chunks")
}

// ---------------------------------------------------------------------------
// Window mid-roll at shutdown (stored_hi clause): downtime crosses the window
// boundary, so tail chunks (stored_hi, lastChunk] must be scheduled — lo-only
// classification would strand them.
// ---------------------------------------------------------------------------

func TestResolve_WindowMidRollAtShutdownSchedulesTail(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4) // window 0 = chunks [0,3]

	// At shutdown: coverage [0,1], chunks 0,1 fully frozen, chunks 2,3 not produced.
	for c := chunk.ID(0); c <= 1; c++ {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
	}
	freezeCoverage(t, cat, 1) // stored_hi = 1 < lastChunk(0) = 3

	// Restart catches up the now-complete window [0,3].
	plan, err := resolve(resolveCfg(cat), 0, 3)
	require.NoError(t, err)

	// Exactly one index build, covering the whole (now complete) window.
	require.Len(t, plan.IndexBuilds, 1)
	require.Equal(t, IndexBuild{Index: 0, Lo: 0, Hi: 3}, plan.IndexBuilds[0])

	// Tail chunks 2,3 scheduled for all kinds; chunks 0,1 self-skip.
	require.Equal(t, []chunk.ID{2, 3}, chunkSet(plan),
		"only the tail chunks (stored_hi, lastChunk] need work — lo-only classification would strand them")

	cb2, ok := findChunkBuild(plan, 2)
	require.True(t, ok)
	require.True(t, cb2.Artifacts.Has(geometry.KindLedgers))
	require.True(t, cb2.Artifacts.Has(geometry.KindEvents))
	require.True(t, cb2.Artifacts.Has(geometry.KindTxHash))
}

// Mid-roll re-derive: resolve requests txhash for every desired chunk whose .bin
// is not frozen, head chunks included.
func TestResolve_MidRollReDerivesMissingBins(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4)

	// ledgers+events frozen for all four chunks; .bin frozen only for 0,1.
	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents)
	}
	freezeKinds(t, cat, 0, geometry.KindTxHash)
	freezeKinds(t, cat, 1, geometry.KindTxHash)
	freezeCoverage(t, cat, 1) // current window, hi=1

	plan, err := resolve(resolveCfg(cat), 0, 3)
	require.NoError(t, err)

	require.Equal(t, []IndexBuild{{Index: 0, Lo: 0, Hi: 3}}, plan.IndexBuilds)
	// Only chunks 2,3 need a .bin (and only the .bin — ledgers/events are frozen).
	require.Equal(t, []chunk.ID{2, 3}, chunkSet(plan))
	for _, c := range []chunk.ID{2, 3} {
		cb, ok := findChunkBuild(plan, c)
		require.True(t, ok)
		require.Equal(t, catalog.NewArtifactSet(geometry.KindTxHash), cb.Artifacts,
			"head chunks' ledgers/events frozen ⇒ only txhash requested")
	}
}

// ---------------------------------------------------------------------------
// A finalized window the range ends in: the stored terminal coverage covers any
// desired sub-range ⇒ nothing scheduled.
// ---------------------------------------------------------------------------

func TestResolve_FinalizedWindowRangeEndsIn(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4) // windows: 0=[0,3], 1=[4,7]

	// Window 0 finalized: ledgers/events frozen, terminal coverage [0,3] frozen.
	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents)
	}
	freezeCoverage(t, cat, 3)

	// Range ends at chunk 2: desired [0,2] ⊆ stored [0,3].
	plan, err := resolve(resolveCfg(cat), 0, 2)
	require.NoError(t, err)
	require.True(t, plan.Empty(),
		"a finalized window the range ends in needs no rebuild, got %+v", plan)
}

// ---------------------------------------------------------------------------
// A range spanning a finalized window plus a fresh trailing window: the finalized
// one contributes nothing; the trailing one yields one non-terminal index build
// plus its chunks.
// ---------------------------------------------------------------------------

func TestResolve_SpanFinalizedPlusFreshTrailing(t *testing.T) {
	cat, _ := smallTxHashIndexCatalog(t, 4) // windows: 0=[0,3], 1=[4,7]

	// Window 0 fully finalized.
	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, geometry.KindLedgers, geometry.KindEvents)
	}
	freezeCoverage(t, cat, 3)

	// Window 1 untouched; range ends mid-window-1 at chunk 5.
	plan, err := resolve(resolveCfg(cat), 0, 5)
	require.NoError(t, err)

	// Only window 1's partial coverage [4,5] is built (non-terminal: hi=5 < 7).
	require.Len(t, plan.IndexBuilds, 1)
	require.Equal(t, IndexBuild{Index: 1, Lo: 4, Hi: 5}, plan.IndexBuilds[0])

	txLayout := cat.TxHashIndexLayout()
	require.False(t, txLayout.IsTerminalCoverage(geometry.TxHashIndexCoverage{Index: 1, Lo: 4, Hi: 5}),
		"a trailing partial window is non-terminal")

	// Chunks 4 and 5 need every kind (all absent); window-0 chunks self-skip.
	require.Equal(t, []chunk.ID{4, 5}, chunkSet(plan))
	for _, c := range []chunk.ID{4, 5} {
		cb, ok := findChunkBuild(plan, c)
		require.True(t, ok)
		require.Equal(t, catalog.AllArtifacts(), cb.Artifacts)
	}
}
