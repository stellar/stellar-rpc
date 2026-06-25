package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

func TestConfigPins(t *testing.T) {
	cat, _ := testCatalog(t)

	_, ok, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.False(t, ok, "pristine store has no earliest_ledger pin")

	require.NoError(t, cat.PutEarliestLedger(2))
	el, ok, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), el)

	_, ok, err = cat.ChunksPerTxhashIndex()
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, cat.PutChunksPerTxhashIndex(testCPI))
	cpi, ok, err := cat.ChunksPerTxhashIndex()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(testCPI), cpi)
}

func TestChunkArtifactKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	require.NoError(t, cat.MarkChunkFreezing(1, KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(2, KindEvents))

	refs, err := cat.ChunkArtifactKeys()
	require.NoError(t, err)
	require.Len(t, refs, 2)
	// Sorted by key: chunk:00000001:ledgers before chunk:00000002:events.
	require.Equal(t, ArtifactRef{Chunk: 1, Kind: KindLedgers, State: StateFreezing}, refs[0])
	require.Equal(t, ArtifactRef{Chunk: 2, Kind: KindEvents, State: StateFrozen}, refs[1])
}

// ---------------------------------------------------------------------------
// frozenCoverage: uniqueness + none-case.
// ---------------------------------------------------------------------------

func TestFrozenCoverageNone(t *testing.T) {
	cat, _ := testCatalog(t)

	_, ok, err := cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.False(t, ok, "no coverage at all")

	// A "freezing" coverage is not frozen.
	_, err = cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	_, ok, err = cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.False(t, ok, "freezing is not frozen")
}

func TestFrozenCoverageUnique(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	require.NoError(t, cat.CommitIndex(cov))

	got, ok, err := cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(5100), got.Lo)
	require.Equal(t, chunk.ID(5349), got.Hi)
}

func TestFrozenCoverageDetectsTwoFrozen(t *testing.T) {
	cat, _ := testCatalog(t)

	// Force the invariant-violating state directly through the store: two
	// frozen coverages in one window. FrozenCoverage must detect it, not pick
	// one.
	require.NoError(t, cat.store.Put(indexKey(5, 5100, 5349), string(StateFrozen)))
	require.NoError(t, cat.store.Put(indexKey(5, 5100, 5350), string(StateFrozen)))

	_, _, err := cat.FrozenCoverage(5)
	require.Error(t, err)
	require.Contains(t, err.Error(), "uniqueness invariant violated")
}

func TestGetHasMissReturnsCleanly(t *testing.T) {
	cat, _ := testCatalog(t)
	_, ok, err := cat.Get("nope")
	require.NoError(t, err)
	require.False(t, ok)
	has, err := cat.Has("nope")
	require.NoError(t, err)
	require.False(t, has)
}
