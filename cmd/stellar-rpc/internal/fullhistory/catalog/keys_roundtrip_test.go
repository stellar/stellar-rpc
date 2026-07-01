package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Round-trip every key family through the real metastore (the geometry key
// schema read back through the Catalog).
// ---------------------------------------------------------------------------

func TestRoundTripChunkKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	for _, kind := range geometry.AllKinds() {
		state, err := cat.State(42, kind)
		require.NoError(t, err)
		require.Equal(t, geometry.State(""), state, "absent key reads as empty State")
	}

	require.NoError(t, cat.MarkChunkFreezing(42, geometry.AllKinds()...))
	for _, kind := range geometry.AllKinds() {
		state, err := cat.State(42, kind)
		require.NoError(t, err)
		require.Equal(t, geometry.StateFreezing, state)
	}

	require.NoError(t, cat.FlipChunkFrozen(42, geometry.AllKinds()...))
	for _, kind := range geometry.AllKinds() {
		state, err := cat.State(42, kind)
		require.NoError(t, err)
		require.Equal(t, geometry.StateFrozen, state)
	}
}

func TestRoundTripIndexKey(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	require.Equal(t, geometry.StateFreezing, cov.State)

	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, geometry.StateFreezing, keys[0].State)
	require.Equal(t, chunk.ID(5100), keys[0].Lo)
	require.Equal(t, chunk.ID(5349), keys[0].Hi)
}
