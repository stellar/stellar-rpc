package catalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// TestSnapshotState_RepeatableAcrossWrites pins that an artifact state read
// through a snapshot keeps the value as of acquisition while a live read tracks
// later writes.
func TestSnapshotState_RepeatableAcrossWrites(t *testing.T) {
	cat, _ := testCatalog(t)
	const c chunk.ID = 42
	require.NoError(t, cat.put(geometry.ChunkKey(c, geometry.KindLedgers), string(geometry.StateFrozen)))

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer snap.Release()

	// After the snapshot: the ledgers key is pruned, and a second kind is frozen.
	require.NoError(t, cat.put(geometry.ChunkKey(c, geometry.KindLedgers), string(geometry.StatePruning)))
	require.NoError(t, cat.put(geometry.ChunkKey(c, geometry.KindEvents), string(geometry.StateFrozen)))

	// The snapshot still sees the frozen ledgers and no events key.
	st, err := snap.State(c, geometry.KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, geometry.StateFrozen, st)

	st, err = snap.State(c, geometry.KindEvents)
	require.NoError(t, err)
	assert.Equal(t, geometry.State(""), st, "a key added after the snapshot is absent in it")

	// A live read tracks the newer writes.
	st, err = cat.State(c, geometry.KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, geometry.StatePruning, st)
}

// TestSnapshotHotState_RepeatableAcrossWrites pins the same repeatable-read
// guarantee for the hot-DB state key, which flips as chunks are opened and
// discarded.
func TestSnapshotHotState_RepeatableAcrossWrites(t *testing.T) {
	cat, _ := testCatalog(t)
	const c chunk.ID = 7
	require.NoError(t, cat.FlipHotReady(c))

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer snap.Release()

	// After the snapshot the chunk is demoted toward discard.
	require.NoError(t, cat.PutHotTransient(c))

	hs, err := snap.HotState(c)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotReady, hs)

	hs, err = cat.HotState(c)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotTransient, hs)
}

// TestSnapshotState_Miss pins that an absent key reads as the empty state through
// a snapshot, same as the live read.
func TestSnapshotState_Miss(t *testing.T) {
	cat, _ := testCatalog(t)
	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer snap.Release()

	st, err := snap.State(99, geometry.KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, geometry.State(""), st)

	hs, err := snap.HotState(99)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotState(""), hs)
}

// TestSnapshotLastCompleteChunk_PinnedAcrossWrites pins that the floor anchor a
// query derives is fixed at acquisition: a chunk opened after the snapshot does
// not raise it, and one discarded after does not disturb the ready set it derives
// from.
func TestSnapshotLastCompleteChunk_PinnedAcrossWrites(t *testing.T) {
	cat, _ := testCatalog(t)
	for _, c := range []chunk.ID{5, 6, 7} {
		require.NoError(t, cat.FlipHotReady(c))
	}

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer snap.Release()

	// After the snapshot: open a higher chunk and discard the lowest.
	require.NoError(t, cat.FlipHotReady(8))
	require.NoError(t, cat.deleteHotKey(5))

	asOf, err := snap.LastCompleteChunk()
	require.NoError(t, err)
	assert.Equal(t, int64(6), asOf, "the snapshot anchor is 7-1, unchanged")

	live, err := cat.LastCompleteChunk()
	require.NoError(t, err)
	assert.Equal(t, int64(7), live, "the live anchor moved to 8-1")
}

// TestSnapshotLastCompleteChunk_SkipsTransient pins that only "ready" keys count
// in a snapshot read, matching the live derivation's filter.
func TestSnapshotLastCompleteChunk_SkipsTransient(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.FlipHotReady(10))
	require.NoError(t, cat.PutHotTransient(11)) // transient: never counts

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer snap.Release()

	asOf, err := snap.LastCompleteChunk()
	require.NoError(t, err)
	assert.Equal(t, int64(9), asOf, "transient 11 does not raise the anchor above 10-1")
}

// TestSnapshotLastCompleteChunk_EmptyErrors pins that an empty ready scan is
// ErrNoReadyHotChunk through a snapshot, same as the live read.
func TestSnapshotLastCompleteChunk_EmptyErrors(t *testing.T) {
	cat, _ := testCatalog(t)
	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer snap.Release()

	_, err = snap.LastCompleteChunk()
	require.ErrorIs(t, err, ErrNoReadyHotChunk)
	_, err = cat.LastCompleteChunk()
	require.ErrorIs(t, err, ErrNoReadyHotChunk)
}

// TestSnapshotAllTxHashIndexKeys_RepeatableAcrossRebuild pins that a
// getTransaction probe sees a fixed set of coverage generations even as an index
// rebuild swaps them.
func TestSnapshotAllTxHashIndexKeys_RepeatableAcrossRebuild(t *testing.T) {
	cat, _ := testCatalog(t)
	w := geometry.TxHashIndexID(0)
	old := geometry.TxHashIndexKey(w, 0, 42)
	require.NoError(t, cat.put(old, string(geometry.StateFrozen)))

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer snap.Release()

	// Rebuild after the snapshot: demote the old coverage, freeze a wider one.
	require.NoError(t, cat.put(old, string(geometry.StatePruning)))
	require.NoError(t, cat.put(geometry.TxHashIndexKey(w, 0, 99), string(geometry.StateFrozen)))

	asOf, err := snap.AllTxHashIndexKeys()
	require.NoError(t, err)
	require.Len(t, asOf, 1, "the snapshot sees only the pre-rebuild coverage")
	assert.Equal(t, chunk.ID(42), asOf[0].Hi)
	assert.Equal(t, geometry.StateFrozen, asOf[0].State)

	live, err := cat.AllTxHashIndexKeys()
	require.NoError(t, err)
	require.Len(t, live, 2, "live sees the pruning debris plus the new coverage")
}
