package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// ---------------------------------------------------------------------------
// Sweeps: the two deletion bodies.
// ---------------------------------------------------------------------------

func TestSweepChunkArtifacts(t *testing.T) {
	cat, root := testCatalog(t)
	_ = root

	// Set up a frozen ledgers + frozen events for chunk 3, with real files.
	lfsPath := cat.layout.LedgerPackPath(3)
	writeArtifact(t, lfsPath)
	require.NoError(t, cat.MarkChunkFreezing(3, geometry.KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(3, geometry.KindLedgers))

	eventsPaths := cat.layout.EventsPaths(3)
	for _, p := range eventsPaths {
		writeArtifact(t, p)
	}
	require.NoError(t, cat.MarkChunkFreezing(3, geometry.KindEvents))
	require.NoError(t, cat.FlipChunkFrozen(3, geometry.KindEvents))

	refs := []ArtifactRef{
		{Chunk: 3, Kind: geometry.KindLedgers, State: geometry.StateFrozen},
		{Chunk: 3, Kind: geometry.KindEvents, State: geometry.StateFrozen},
	}
	require.NoError(t, cat.SweepChunkArtifacts(refs))

	// Files gone.
	require.NoFileExists(t, lfsPath)
	for _, p := range eventsPaths {
		require.NoFileExists(t, p)
	}
	// Keys gone (key absent => file gone).
	for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
		s, err := cat.State(3, kind)
		require.NoError(t, err)
		require.Equal(t, geometry.State(""), s)
	}
}

func TestSweepChunkArtifactsIdempotentOnMissingFiles(t *testing.T) {
	cat, _ := testCatalog(t)

	// Key present, file never written (a "pruning" leftover whose file is
	// already gone).
	require.NoError(t, cat.store.Put(geometry.ChunkKey(8, geometry.KindLedgers), string(geometry.StatePruning)))
	require.NoError(t, cat.SweepChunkArtifacts([]ArtifactRef{
		{Chunk: 8, Kind: geometry.KindLedgers, State: geometry.StatePruning},
	}))
	s, err := cat.State(8, geometry.KindLedgers)
	require.NoError(t, err)
	require.Equal(t, geometry.State(""), s)
}

func TestSweepIndexKey(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.TxHashIndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, cat.CommitTxHashIndex(cov))

	// Re-read as frozen for the sweep.
	frozen, ok, err := cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, cat.SweepTxHashIndexKey(frozen))

	require.NoFileExists(t, idxPath)
	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys, "key absent => file gone")
}

func TestSweepIndexKeyFreezingDebris(t *testing.T) {
	cat, _ := testCatalog(t)

	// A crashed attempt: "freezing" key with a partial file.
	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.TxHashIndexFilePath(cov)
	writeArtifact(t, idxPath)

	require.NoError(t, cat.SweepTxHashIndexKey(cov))
	require.NoFileExists(t, idxPath)
	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys)
}

func TestSweepEmptyRefsNoop(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.SweepChunkArtifacts(nil))
}
