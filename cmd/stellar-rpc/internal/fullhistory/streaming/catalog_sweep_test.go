package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"
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
	require.NoError(t, cat.MarkChunkFreezing(3, KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(3, KindLedgers))

	eventsPaths := cat.layout.EventsPaths(3)
	for _, p := range eventsPaths {
		writeArtifact(t, p)
	}
	require.NoError(t, cat.MarkChunkFreezing(3, KindEvents))
	require.NoError(t, cat.FlipChunkFrozen(3, KindEvents))

	refs := []ArtifactRef{
		{Chunk: 3, Kind: KindLedgers, State: StateFrozen},
		{Chunk: 3, Kind: KindEvents, State: StateFrozen},
	}
	require.NoError(t, cat.SweepChunkArtifacts(refs))

	// Files gone.
	require.NoFileExists(t, lfsPath)
	for _, p := range eventsPaths {
		require.NoFileExists(t, p)
	}
	// Keys gone (key absent => file gone).
	for _, kind := range []Kind{KindLedgers, KindEvents} {
		s, err := cat.State(3, kind)
		require.NoError(t, err)
		require.Equal(t, State(""), s)
	}
}

func TestSweepChunkArtifactsIdempotentOnMissingFiles(t *testing.T) {
	cat, _ := testCatalog(t)

	// Key present, file never written (a "pruning" leftover whose file is
	// already gone).
	require.NoError(t, cat.store.Put(chunkKey(8, KindLedgers), string(StatePruning)))
	require.NoError(t, cat.SweepChunkArtifacts([]ArtifactRef{
		{Chunk: 8, Kind: KindLedgers, State: StatePruning},
	}))
	s, err := cat.State(8, KindLedgers)
	require.NoError(t, err)
	require.Equal(t, State(""), s)
}

func TestSweepIndexKey(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.IndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, cat.CommitIndex(cov))

	// Re-read as frozen for the sweep.
	frozen, ok, err := cat.FrozenCoverage(5)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, cat.SweepIndexKey(frozen))

	require.NoFileExists(t, idxPath)
	keys, err := cat.IndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys, "key absent => file gone")
}

func TestSweepIndexKeyFreezingDebris(t *testing.T) {
	cat, _ := testCatalog(t)

	// A crashed attempt: "freezing" key with a partial file.
	cov, err := cat.MarkIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.IndexFilePath(cov)
	writeArtifact(t, idxPath)

	require.NoError(t, cat.SweepIndexKey(cov))
	require.NoFileExists(t, idxPath)
	keys, err := cat.IndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys)
}

func TestSweepEmptyRefsNoop(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.SweepChunkArtifacts(nil))
}
