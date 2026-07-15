package catalog

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
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
	require.NoError(t, cat.put(geometry.ChunkKey(8, geometry.KindLedgers), string(geometry.StatePruning)))
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

// TestDiscardHotChunkResumesTransient mirrors the sweep siblings' crash-resume
// coverage for the hot-DB discard: a "transient" key (a discard that crashed after
// marking transient but before deleting the key) plus a leftover dir must be
// finished by the next DiscardHotChunk — the dir removed and the key deleted.
func TestDiscardHotChunkResumesTransient(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(4)

	// The mid-discard crash state: a "transient" key + a real leftover dir.
	require.NoError(t, cat.PutHotTransient(c))
	dir := cat.layout.HotChunkPath(c)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	require.NoError(t, cat.DiscardHotChunk(c))

	// The resume completed it: key gone, dir gone.
	state, err := cat.HotState(c)
	require.NoError(t, err)
	require.Equal(t, geometry.HotState(""), state, "transient key finished")
	require.NoDirExists(t, dir, "leftover hot dir swept")
}

// TestDiscardHotChunkAbsentKeyNoop: an absent hot key is a clean no-op (nothing
// to finish).
func TestDiscardHotChunkAbsentKeyNoop(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.DiscardHotChunk(chunk.ID(9)))
}

// ---------------------------------------------------------------------------
// Split sweeps (demote now, destroy later): the deferred-destroy shape the
// registry reaper runs, and the destroy-side guards.
// ---------------------------------------------------------------------------

// TestDemoteThenDestroyChunkArtifacts runs the two halves separately, as the
// prune stage does under a registry: after the demote the key is "pruning" and
// the file still exists; the (later) destroy unlinks and deletes the key.
func TestDemoteThenDestroyChunkArtifacts(t *testing.T) {
	cat, _ := testCatalog(t)

	path := cat.layout.LedgerPackPath(3)
	writeArtifact(t, path)
	require.NoError(t, cat.MarkChunkFreezing(3, geometry.KindLedgers))
	require.NoError(t, cat.FlipChunkFrozen(3, geometry.KindLedgers))
	refs := []ArtifactRef{{Chunk: 3, Kind: geometry.KindLedgers, State: geometry.StateFrozen}}

	require.NoError(t, cat.DemoteChunkArtifacts(refs))
	s, err := cat.State(3, geometry.KindLedgers)
	require.NoError(t, err)
	require.Equal(t, geometry.StatePruning, s, "demote flips frozen to pruning")
	require.FileExists(t, path, "demote deletes nothing")

	require.NoError(t, cat.DestroyChunkArtifacts(refs))
	require.NoFileExists(t, path)
	s, err = cat.State(3, geometry.KindLedgers)
	require.NoError(t, err)
	require.Equal(t, geometry.State(""), s)

	require.NoError(t, cat.DestroyChunkArtifacts(refs), "a re-run destroy (double-scheduled) is a no-op")
}

// TestDestroyChunkArtifactsSkipsRefrozenKey: a destroy that fires after the key
// went back to "frozen" (a re-frozen artifact is serving again) must leave the
// file and key alone.
func TestDestroyChunkArtifactsSkipsRefrozenKey(t *testing.T) {
	cat, _ := testCatalog(t)

	path := cat.layout.LedgerPackPath(3)
	writeArtifact(t, path)
	require.NoError(t, cat.put(geometry.ChunkKey(3, geometry.KindLedgers), string(geometry.StateFrozen)))

	// The ref says "pruning" (the scan's observation), but the durable key is
	// "frozen" again by destroy time.
	require.NoError(t, cat.DestroyChunkArtifacts([]ArtifactRef{
		{Chunk: 3, Kind: geometry.KindLedgers, State: geometry.StatePruning},
	}))
	require.FileExists(t, path, "a re-frozen artifact is never unlinked")
	s, err := cat.State(3, geometry.KindLedgers)
	require.NoError(t, err)
	require.Equal(t, geometry.StateFrozen, s, "the frozen key survives")
}

// TestDemoteThenDestroyTxHashIndexKey mirrors the chunk-family split for one
// index coverage, including destroy idempotence.
func TestDemoteThenDestroyTxHashIndexKey(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.TxHashIndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, cat.CommitTxHashIndex(cov))
	frozen, ok, err := cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, cat.DemoteTxHashIndexKey(frozen))
	require.FileExists(t, idxPath, "demote deletes nothing")

	require.NoError(t, cat.DestroyTxHashIndexKey(frozen))
	require.NoFileExists(t, idxPath)
	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys)

	require.NoError(t, cat.DestroyTxHashIndexKey(frozen), "a re-run destroy (double-scheduled) is a no-op")
}

// TestDestroyTxHashIndexKeySkipsRefrozenCoverage: a destroy that fires after
// the coverage went back to "frozen" must leave the .idx and key alone.
func TestDestroyTxHashIndexKeySkipsRefrozenCoverage(t *testing.T) {
	cat, _ := testCatalog(t)

	cov, err := cat.MarkTxHashIndexFreezing(5, 5100, 5349)
	require.NoError(t, err)
	idxPath := cat.layout.TxHashIndexFilePath(cov)
	writeArtifact(t, idxPath)
	require.NoError(t, cat.CommitTxHashIndex(cov)) // durable state: frozen

	cov.State = geometry.StatePruning // the scan's stale observation
	require.NoError(t, cat.DestroyTxHashIndexKey(cov))
	require.FileExists(t, idxPath, "a re-frozen coverage is never unlinked")
	frozen, ok, err := cat.FrozenTxHashIndex(5)
	require.NoError(t, err)
	require.True(t, ok, "the frozen coverage survives")
	require.Equal(t, cov.Key, frozen.Key)
}

// TestDestroyHotChunkGuards: destroy skips an absent key (already finished) and
// a "ready" key (the chunk is serving again); it removes only "transient" ones.
func TestDestroyHotChunkGuards(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(4)

	require.NoError(t, cat.DestroyHotChunk(c), "absent key is a no-op")

	dir := cat.layout.HotChunkPath(c)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.FlipHotReady(c))
	require.NoError(t, cat.DestroyHotChunk(c))
	require.DirExists(t, dir, "a ready hot DB is never removed")

	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.DestroyHotChunk(c))
	require.NoDirExists(t, dir, "a transient hot DB is removed")
	state, err := cat.HotState(c)
	require.NoError(t, err)
	require.Equal(t, geometry.HotState(""), state)
}
