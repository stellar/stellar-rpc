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

// SweepTxHashIndexKey bases its frozen->pruning demote on the CURRENT durable
// value, not on cov.State, which a caller may have snapshotted before a
// concurrent CommitTxHashIndex promoted the key. Here the snapshot still reads
// "freezing" while the stored value is "frozen"; the sweep must take the demote
// path and still clean up. (The crash-window guarantee the demote provides —
// never unlink under a durably-frozen key — is exercised by the fault-injection
// harness tracked in the follow-up issue, not observable from end state here.)
func TestSweepIndexKeyStaleFreezingSnapshot(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349) // snapshot: State == freezing
	require.NoError(t, err)
	idxPath := cat.layout.TxHashIndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, cat.CommitTxHashIndex(cov)) // durable value is now frozen
	require.Equal(t, StateFreezing, cov.State, "the caller's snapshot is now stale")

	require.NoError(t, cat.SweepTxHashIndexKey(cov)) // swept via the stale snapshot

	require.NoFileExists(t, idxPath)
	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys, "key absent => file gone")
}

// A coverage whose key is already gone (a prior sweep finished) is a no-op — the
// re-read sees no key and the sweep must not resurrect one.
func TestSweepIndexKeyAbsentIsNoop(t *testing.T) {
	cat, _ := testCatalog(t)

	cov := TxHashIndexCoverage{
		Index: 5, Lo: 5100, Hi: 5349,
		Key:   txhashIndexKey(5, 5100, 5349),
		State: StateFrozen, // stale snapshot; nothing is actually stored
	}
	require.NoError(t, cat.SweepTxHashIndexKey(cov))

	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys, "sweep of an absent key must not resurrect it")
}

func TestSweepEmptyRefsNoop(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.SweepChunkArtifacts(nil))
}
