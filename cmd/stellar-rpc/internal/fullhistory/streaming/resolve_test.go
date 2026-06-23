package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// resolve test helpers — set catalog state directly through the Phase A
// one-write protocol so resolve sees exactly the durable keys production would.
// ---------------------------------------------------------------------------

// freezeKinds flips the given per-chunk kinds to "frozen" for chunkID via the
// one-write protocol (no real file content needed — resolve reads keys only).
func freezeKinds(t *testing.T, cat *Catalog, chunkID chunk.ID, kinds ...Kind) {
	t.Helper()
	require.NoError(t, cat.MarkChunkFreezing(chunkID, kinds...))
	require.NoError(t, cat.FlipChunkFrozen(chunkID, kinds...))
}

// resolveCfg wires a minimal ExecConfig over a catalog for resolve tests
// (resolve never runs a task, so the primitive deps stay nil).
func resolveCfg(cat *Catalog) ExecConfig {
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
	cat, _ := testCatalog(t)
	plan, err := resolve(resolveCfg(cat), 5, 4)
	require.NoError(t, err)
	require.True(t, plan.Empty(), "rangeEnd < rangeStart must yield an empty plan")
}

// ---------------------------------------------------------------------------
// Steady-state restart: a fully-frozen range resolves to nothing.
// ---------------------------------------------------------------------------

func TestResolve_SteadyStateRestartIsEmpty(t *testing.T) {
	cat, _ := testCatalog(t)

	// Every chunk in [0,3] has its ledgers + events frozen — the post-freeze
	// steady state.
	for c := chunk.ID(0); c <= 3; c++ {
		freezeKinds(t, cat, c, KindLedgers, KindEvents)
	}

	plan, err := resolve(resolveCfg(cat), 0, 3)
	require.NoError(t, err)
	require.True(t, plan.Empty(),
		"steady-state restart of fully-frozen chunks must schedule nothing, got %+v", plan)
}

// ---------------------------------------------------------------------------
// A range with a partly-frozen middle: only the un-frozen chunks are scheduled,
// and each scheduled chunk requests the ledgers artifact.
// ---------------------------------------------------------------------------

func TestResolve_SchedulesOnlyUnfrozenChunks(t *testing.T) {
	cat, _ := testCatalog(t)

	// Chunks 0,1,5 frozen (ledgers + events); 2,3,4 absent.
	for _, c := range []chunk.ID{0, 1, 5} {
		freezeKinds(t, cat, c, KindLedgers, KindEvents)
	}

	plan, err := resolve(resolveCfg(cat), 0, 5)
	require.NoError(t, err)

	require.Equal(t, []chunk.ID{2, 3, 4}, chunkSet(plan),
		"only the un-frozen chunks need work; frozen chunks self-skip")
	for _, c := range []chunk.ID{2, 3, 4} {
		cb, ok := findChunkBuild(plan, c)
		require.True(t, ok)
		require.True(t, cb.Artifacts.Has(KindLedgers), "an un-frozen chunk requests ledgers")
		require.Equal(t, AllArtifacts(), cb.Artifacts)
	}
}

// A "freezing" (not "frozen") key re-materializes: a partial/crashed freeze
// attempt is re-scheduled, never trusted.
func TestResolve_FreezingKeyReMaterializes(t *testing.T) {
	cat, _ := testCatalog(t)

	// Chunk 1 is mid-freeze ("freezing", not flipped to "frozen").
	require.NoError(t, cat.MarkChunkFreezing(1, KindLedgers))

	plan, err := resolve(resolveCfg(cat), 1, 1)
	require.NoError(t, err)
	require.Equal(t, []chunk.ID{1}, chunkSet(plan),
		"a freezing (not frozen) key must be re-scheduled")
	cb, ok := findChunkBuild(plan, 1)
	require.True(t, ok)
	require.True(t, cb.Artifacts.Has(KindLedgers))
}
