package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// PinEarliestLedger writes the sole config pin; EarliestLedger reads it back.
func TestConfigPins(t *testing.T) {
	cat, _ := testCatalog(t)

	// Pristine store: the pin is not set.
	_, ok, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.False(t, ok, "pristine store has no earliest_ledger pin")

	// The first-start commit writes it.
	require.NoError(t, cat.PinEarliestLedger(2))

	el, ok, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(2), el)
}

// ---------------------------------------------------------------------------
// Scans: HotChunkKeys (value-blind) vs ReadyHotChunkKeys (ready-only).
// ---------------------------------------------------------------------------

func TestHotChunkKeysValueBlindVsReadyOnly(t *testing.T) {
	cat, _ := testCatalog(t)

	require.NoError(t, cat.PutHotTransient(3))
	require.NoError(t, cat.FlipHotReady(5))
	require.NoError(t, cat.PutHotTransient(9))
	require.NoError(t, cat.FlipHotReady(12))

	all, err := cat.HotChunkKeys()
	require.NoError(t, err)
	require.Equal(t, []chunk.ID{3, 5, 9, 12}, all, "value-blind: every hot key")

	ready, err := cat.ReadyHotChunkKeys()
	require.NoError(t, err)
	require.Equal(t, []chunk.ID{5, 12}, ready, "ready-only excludes transient")
}

func TestChunkArtifactKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	require.NoError(t, cat.MarkChunkFreezing(1, geometry.KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(2, geometry.KindEvents))

	refs, err := cat.ChunkArtifactKeys()
	require.NoError(t, err)
	require.Len(t, refs, 2)
	// Sorted by key: chunk:00000001:ledgers before chunk:00000002:events.
	require.Equal(t, ArtifactRef{Chunk: 1, Kind: geometry.KindLedgers, State: geometry.StateFreezing}, refs[0])
	require.Equal(t, ArtifactRef{Chunk: 2, Kind: geometry.KindEvents, State: geometry.StateFrozen}, refs[1])
}

// ---------------------------------------------------------------------------
// frozenCoverage: uniqueness + none-case.
// ---------------------------------------------------------------------------

func TestFrozenCoverageNone(t *testing.T) {
	cat, _ := testCatalog(t)

	_, ok, err := cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.False(t, ok, "no coverage at all")

	// A "freezing" coverage is not frozen.
	_, err = cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	_, ok, err = cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.False(t, ok, "freezing is not frozen")
}

func TestFrozenCoverageUnique(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(cov))

	got, ok, err := cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, chunk.ID(5100), got.Lo)
	require.Equal(t, chunk.ID(5349), got.Hi)
}

func TestFrozenCoverageDetectsTwoFrozen(t *testing.T) {
	cat, _ := testCatalog(t)

	// Force the invariant-violating state directly through the store: two
	// frozen coverages in one index. FrozenTxHashIndex must detect it, not pick
	// one.
	require.NoError(t, cat.put(geometry.TxHashIndexKey(5, 5100, 5349), string(geometry.StateFrozen)))
	require.NoError(t, cat.put(geometry.TxHashIndexKey(5, 5100, 5350), string(geometry.StateFrozen)))

	_, _, err := cat.FrozenTxHashIndex(5)
	require.Error(t, err)
	require.Contains(t, err.Error(), "uniqueness invariant violated")
}

func TestGetHasMissReturnsCleanly(t *testing.T) {
	cat, _ := testCatalog(t)
	_, ok, err := cat.get("nope")
	require.NoError(t, err)
	require.False(t, ok)
	has, err := cat.has("nope")
	require.NoError(t, err)
	require.False(t, has)
}
