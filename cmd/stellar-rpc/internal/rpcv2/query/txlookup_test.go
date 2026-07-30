package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// TestHotTxHashIndexes pins that every published hot chunk's tx index is returned,
// newest chunk first, and that an empty handle set yields no indexes.
func TestHotTxHashIndexes(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	require.NoError(t, cat.FlipHotReady(999)) // acquisition needs a ready live chunk

	empty, err := r.NewReadView()
	require.NoError(t, err)
	assert.Empty(t, empty.HotTxHashIndexes(), "no handles → no hot indexes")
	empty.Release()

	dbs := map[chunk.ID]*hotchunk.DB{}
	for _, c := range []chunk.ID{5, 6, 7} {
		db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		r.PublishHandle(c, db)
		dbs[c] = db
	}

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	got := a.HotTxHashIndexes()
	require.Len(t, got, 3)
	assert.Equal(t, dbs[7].Txhash(), got[0], "newest chunk first")
	assert.Equal(t, dbs[5].Txhash(), got[2], "oldest chunk last")
}

// TestTxHashCoverages pins that only frozen window coverages are returned, newest
// (by upper chunk) first, and freezing debris is excluded.
func TestColdTxHashIndexCoverages(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))

	require.NoError(t, cat.FlipHotReady(999)) // acquisition needs a ready live chunk
	// One frozen coverage per window 0,1,2 (distinct upper chunk), each the sole
	// coverage of its window so there is no predecessor to demote.
	for _, w := range []geometry.TxHashIndexID{0, 1, 2} {
		c := chunk.ID(uint32(w) * geometry.ChunksPerTxhashIndex)
		cov, err := cat.MarkTxHashIndexFreezing(w, c, c)
		require.NoError(t, err)
		require.NoError(t, cat.CommitTxHashIndex(cov))
	}
	// Freezing debris in window 3 — never committed, must be excluded.
	debris := chunk.ID(3 * geometry.ChunksPerTxhashIndex)
	_, err := cat.MarkTxHashIndexFreezing(3, debris, debris)
	require.NoError(t, err)

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	covs, err := a.ColdTxHashIndexCoverages()
	require.NoError(t, err)
	require.Len(t, covs, 3, "only the frozen coverages, not the freezing debris")
	for _, cov := range covs {
		assert.Equal(t, geometry.StateFrozen, cov.State)
	}
	assert.Equal(t, chunk.ID(2*geometry.ChunksPerTxhashIndex), covs[0].Hi, "newest coverage first")
	assert.Equal(t, chunk.ID(geometry.ChunksPerTxhashIndex), covs[1].Hi)
	assert.Equal(t, chunk.ID(0), covs[2].Hi)
}
