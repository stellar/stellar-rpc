package catalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// TestStateAsOf_RepeatableAcrossWrites pins that an artifact state read through a
// snapshot keeps the value as of admission while a live read tracks later writes.
func TestStateAsOf_RepeatableAcrossWrites(t *testing.T) {
	cat, _ := testCatalog(t)
	const c chunk.ID = 42
	require.NoError(t, cat.put(geometry.ChunkKey(c, geometry.KindLedgers), string(geometry.StateFrozen)))

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer cat.ReleaseSnapshot(snap)

	// After the snapshot: the ledgers key is pruned, and a second kind is frozen.
	require.NoError(t, cat.put(geometry.ChunkKey(c, geometry.KindLedgers), string(geometry.StatePruning)))
	require.NoError(t, cat.put(geometry.ChunkKey(c, geometry.KindEvents), string(geometry.StateFrozen)))

	// The snapshot still sees the frozen ledgers and no events key.
	st, err := cat.StateAsOf(snap, c, geometry.KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, geometry.StateFrozen, st)

	st, err = cat.StateAsOf(snap, c, geometry.KindEvents)
	require.NoError(t, err)
	assert.Equal(t, geometry.State(""), st, "a key added after the snapshot is absent in it")

	// A live read tracks the newer writes.
	st, err = cat.State(c, geometry.KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, geometry.StatePruning, st)
}

// TestHotStateAsOf_RepeatableAcrossWrites pins the same repeatable-read guarantee
// for the hot-DB state key, which flips as chunks are opened and discarded.
func TestHotStateAsOf_RepeatableAcrossWrites(t *testing.T) {
	cat, _ := testCatalog(t)
	const c chunk.ID = 7
	require.NoError(t, cat.FlipHotReady(c))

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer cat.ReleaseSnapshot(snap)

	// After the snapshot the chunk is demoted toward discard.
	require.NoError(t, cat.PutHotTransient(c))

	hs, err := cat.HotStateAsOf(snap, c)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotReady, hs)

	hs, err = cat.HotState(c)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotTransient, hs)
}

// TestStateAsOf_Miss pins that an absent key reads as the empty state through a
// snapshot, same as the live read.
func TestStateAsOf_Miss(t *testing.T) {
	cat, _ := testCatalog(t)
	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer cat.ReleaseSnapshot(snap)

	st, err := cat.StateAsOf(snap, 99, geometry.KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, geometry.State(""), st)

	hs, err := cat.HotStateAsOf(snap, 99)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotState(""), hs)
}

// TestReadyHotChunkKeysAsOf_FrontierPinned pins that the ready-hot frontier a
// query derives its floor from is fixed at admission: a chunk opened after the
// snapshot does not raise it, and one discarded after does not lower it.
func TestReadyHotChunkKeysAsOf_FrontierPinned(t *testing.T) {
	cat, _ := testCatalog(t)
	for _, c := range []chunk.ID{5, 6, 7} {
		require.NoError(t, cat.FlipHotReady(c))
	}

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer cat.ReleaseSnapshot(snap)

	// After the snapshot: open a higher chunk and discard the lowest.
	require.NoError(t, cat.FlipHotReady(8))
	require.NoError(t, cat.deleteHotKey(5))

	asOf, err := cat.ReadyHotChunkKeysAsOf(snap)
	require.NoError(t, err)
	assert.Equal(t, []chunk.ID{5, 6, 7}, asOf, "the snapshot frontier is 7, unchanged")

	live, err := cat.ReadyHotChunkKeys()
	require.NoError(t, err)
	assert.Equal(t, []chunk.ID{6, 7, 8}, live, "the live frontier moved to 8")
}

// TestReadyHotChunkKeysAsOf_SkipsTransient pins that only "ready" keys count in a
// snapshot read, matching the live ReadyHotChunkKeys filter.
func TestReadyHotChunkKeysAsOf_SkipsTransient(t *testing.T) {
	cat, _ := testCatalog(t)
	require.NoError(t, cat.FlipHotReady(10))
	require.NoError(t, cat.PutHotTransient(11)) // transient: never counts

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer cat.ReleaseSnapshot(snap)

	asOf, err := cat.ReadyHotChunkKeysAsOf(snap)
	require.NoError(t, err)
	assert.Equal(t, []chunk.ID{10}, asOf)
}

// TestTxHashIndexKeysAsOf_RepeatableAcrossRebuild pins that a getTransaction probe
// sees a fixed set of coverage generations even as an index rebuild swaps them.
func TestTxHashIndexKeysAsOf_RepeatableAcrossRebuild(t *testing.T) {
	cat, _ := testCatalog(t)
	w := geometry.TxHashIndexID(0)
	old := geometry.TxHashIndexKey(w, 0, 42)
	require.NoError(t, cat.put(old, string(geometry.StateFrozen)))

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer cat.ReleaseSnapshot(snap)

	// Rebuild after the snapshot: demote the old coverage, freeze a wider one.
	require.NoError(t, cat.put(old, string(geometry.StatePruning)))
	require.NoError(t, cat.put(geometry.TxHashIndexKey(w, 0, 99), string(geometry.StateFrozen)))

	asOf, err := cat.TxHashIndexKeysAsOf(snap, w)
	require.NoError(t, err)
	require.Len(t, asOf, 1, "the snapshot sees only the pre-rebuild coverage")
	assert.Equal(t, chunk.ID(42), asOf[0].Hi)
	assert.Equal(t, geometry.StateFrozen, asOf[0].State)

	live, err := cat.TxHashIndexKeys(w)
	require.NoError(t, err)
	require.Len(t, live, 2, "live sees the pruning debris plus the new coverage")
}

// TestFrozenTxHashIndexAsOf_PinnedAcrossRebuild pins that "the index" a probe
// resolves is fixed at admission across a rebuild. Each view holds exactly one
// frozen coverage, so INV-2 is respected in both.
func TestFrozenTxHashIndexAsOf_PinnedAcrossRebuild(t *testing.T) {
	cat, _ := testCatalog(t)
	w := geometry.TxHashIndexID(0)
	require.NoError(t, cat.put(geometry.TxHashIndexKey(w, 0, 42), string(geometry.StateFrozen)))

	snap, err := cat.NewSnapshot()
	require.NoError(t, err)
	defer cat.ReleaseSnapshot(snap)

	require.NoError(t, cat.put(geometry.TxHashIndexKey(w, 0, 42), string(geometry.StatePruning)))
	require.NoError(t, cat.put(geometry.TxHashIndexKey(w, 0, 99), string(geometry.StateFrozen)))

	cov, ok, err := cat.FrozenTxHashIndexAsOf(snap, w)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, chunk.ID(42), cov.Hi, "the snapshot resolves the coverage frozen at admission")

	cov, ok, err = cat.FrozenTxHashIndex(w)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, chunk.ID(99), cov.Hi, "live resolves the rebuilt coverage")
}
