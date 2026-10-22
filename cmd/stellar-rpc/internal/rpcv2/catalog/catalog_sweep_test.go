package catalog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
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

	require.NoError(t, cat.DemoteTxHashIndexKey(frozen))
	require.NoError(t, cat.DestroyTxHashIndexKey(frozen))

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

	require.NoError(t, cat.DemoteTxHashIndexKey(cov))
	require.NoError(t, cat.DestroyTxHashIndexKey(cov))
	require.NoFileExists(t, idxPath)
	keys, err := cat.TxHashIndexKeys(5)
	require.NoError(t, err)
	require.Empty(t, keys)
}

func TestSweepEmptyRefsNoop(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.SweepChunkArtifacts(nil))
}

// TestDestroyHotChunkResumesTransient mirrors the sweep siblings' crash-resume
// coverage for the hot-DB discard: a "transient" key (a discard that crashed after
// marking transient but before deleting the key) plus a leftover dir must be
// finished by the next run's DestroyHotChunk — the dir removed and the key deleted.
func TestDestroyHotChunkResumesTransient(t *testing.T) {
	cat, _ := testCatalog(t)
	c := chunk.ID(4)

	// The mid-discard crash state: a "transient" key + a real leftover dir.
	require.NoError(t, cat.PutHotTransient(c))
	dir := cat.layout.HotChunkPath(c)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	require.NoError(t, cat.DestroyHotChunk(c))

	// The resume completed it: key gone, dir gone.
	state, err := cat.HotState(c)
	require.NoError(t, err)
	require.Equal(t, geometry.HotState(""), state, "transient key finished")
	require.NoDirExists(t, dir, "leftover hot dir swept")
}

// TestDestroyHotChunkAbsentKeyNoop: an absent hot key is a clean no-op (nothing
// to finish).
func TestDestroyHotChunkAbsentKeyNoop(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.DestroyHotChunk(chunk.ID(9)))
}

// TestDestroyChunkArtifacts_SkipsUndemoted pins the exported-API guard: a "frozen"
// (un-demoted) ref passed by mistake is left intact — files and key survive; only
// after demotion does the same ref get destroyed.
func TestDestroyChunkArtifacts_SkipsUndemoted(t *testing.T) {
	cat, _ := testCatalog(t)
	const c chunk.ID = 3
	kind := geometry.KindLedgers
	for _, p := range cat.Layout().ArtifactPaths(c, kind) {
		writeArtifact(t, p)
	}
	require.NoError(t, cat.FlipChunkFrozen(c, kind))
	ref := ArtifactRef{Chunk: c, Kind: kind, State: geometry.StateFrozen}

	// Guard: a frozen (un-demoted) ref is not destroyed.
	require.NoError(t, cat.DestroyChunkArtifacts([]ArtifactRef{ref}))
	st, err := cat.State(c, kind)
	require.NoError(t, err)
	require.Equal(t, geometry.StateFrozen, st, "frozen ref left intact")
	for _, p := range cat.Layout().ArtifactPaths(c, kind) {
		require.FileExists(t, p)
	}

	// After demotion, the same ref is destroyed.
	require.NoError(t, cat.DemoteChunkArtifacts([]ArtifactRef{ref}))
	require.NoError(t, cat.DestroyChunkArtifacts([]ArtifactRef{ref}))
	st, err = cat.State(c, kind)
	require.NoError(t, err)
	require.Equal(t, geometry.State(""), st, "demoted ref destroyed")
	for _, p := range cat.Layout().ArtifactPaths(c, kind) {
		require.NoFileExists(t, p)
	}
}

// TestDestroyChunkArtifacts_DestroysFreezingDebris pins the guard's other allowed
// state: "freezing" debris (a crashed build, never demoted) is destroyed directly.
// A regression tightening the guard to "pruning"-only would silently strand this
// debris — its key re-collected by every prune scan, the destroy skipping it.
func TestDestroyChunkArtifacts_DestroysFreezingDebris(t *testing.T) {
	cat, _ := testCatalog(t)
	const c chunk.ID = 4
	kind := geometry.KindLedgers
	for _, p := range cat.Layout().ArtifactPaths(c, kind) {
		writeArtifact(t, p)
	}
	require.NoError(t, cat.MarkChunkFreezing(c, kind))
	ref := ArtifactRef{Chunk: c, Kind: kind, State: geometry.StateFreezing}

	require.NoError(t, cat.DestroyChunkArtifacts([]ArtifactRef{ref}))
	st, err := cat.State(c, kind)
	require.NoError(t, err)
	require.Equal(t, geometry.State(""), st, "freezing debris destroyed")
	for _, p := range cat.Layout().ArtifactPaths(c, kind) {
		require.NoFileExists(t, p)
	}
}

// TestDestroyTxHashIndexKey_SkipsUndemoted pins the same guard on the index
// family: a coverage whose key is still "frozen" is not destroyed; after the
// demote the same coverage is.
func TestDestroyTxHashIndexKey_SkipsUndemoted(t *testing.T) {
	cat, _ := testCatalog(t)
	w := geometry.TxHashIndexID(0)
	require.NoError(t, cat.put(geometry.TxHashIndexKey(w, 0, 1), string(geometry.StateFrozen)))
	covs, err := cat.TxHashIndexKeys(w)
	require.NoError(t, err)
	require.Len(t, covs, 1)
	cov := covs[0]
	idxPath := cat.Layout().TxHashIndexFilePath(cov)
	writeArtifact(t, idxPath)

	// Guard: the frozen (un-demoted) coverage is not destroyed.
	require.NoError(t, cat.DestroyTxHashIndexKey(cov))
	require.FileExists(t, idxPath, "frozen coverage's file left intact")
	live, err := cat.TxHashIndexKeys(w)
	require.NoError(t, err)
	require.Len(t, live, 1, "frozen coverage's key left intact")

	// After demotion, the same coverage is destroyed.
	require.NoError(t, cat.DemoteTxHashIndexKey(cov))
	require.NoError(t, cat.DestroyTxHashIndexKey(cov))
	require.NoFileExists(t, idxPath)
	live, err = cat.TxHashIndexKeys(w)
	require.NoError(t, err)
	require.Empty(t, live)
}

// TestDestroyHotChunk_SkipsUnmarked pins the guard on the hot family: a chunk
// whose key is still "ready" keeps its dir and key; once marked transient the
// same chunk is destroyed. An absent key stays a quiet no-op.
func TestDestroyHotChunk_SkipsUnmarked(t *testing.T) {
	cat, _ := testCatalog(t)
	const c chunk.ID = 6
	marker := filepath.Join(cat.Layout().HotChunkPath(c), "CURRENT")
	writeArtifact(t, marker)
	require.NoError(t, cat.FlipHotReady(c))

	// Guard: a ready (unmarked) chunk is not destroyed.
	require.NoError(t, cat.DestroyHotChunk(c))
	require.FileExists(t, marker, "ready chunk's dir left intact")
	hs, err := cat.HotState(c)
	require.NoError(t, err)
	require.Equal(t, geometry.HotReady, hs, "ready key left intact")

	// After the transient mark, the same chunk is destroyed.
	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.DestroyHotChunk(c))
	require.NoFileExists(t, marker)
	hs, err = cat.HotState(c)
	require.NoError(t, err)
	require.Equal(t, geometry.HotState(""), hs)

	// Absent key: quiet no-op.
	require.NoError(t, cat.DestroyHotChunk(c))
}
