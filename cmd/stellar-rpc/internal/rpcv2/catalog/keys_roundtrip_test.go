package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/storage/chunk"
)

// ---------------------------------------------------------------------------
// Round-trip every key family through the real catalog KV (the geometry key
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

// TestRoundTripHotKeys exercises the raw hot-key bracket primitives end to end:
// absent -> transient -> ready -> deleted (idempotent on a missing key). It lives
// here (not in lifecycle) because deleteHotKey is catalog-internal now that the
// create/discard choreography is behind the catalog.
func TestRoundTripHotKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	state, err := cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotState(""), state)

	require.NoError(t, cat.PutHotTransient(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotTransient, state)

	require.NoError(t, cat.FlipHotReady(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotReady, state)

	require.NoError(t, cat.deleteHotKey(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotState(""), state)
	// Idempotent on a missing key.
	require.NoError(t, cat.deleteHotKey(7))
}
