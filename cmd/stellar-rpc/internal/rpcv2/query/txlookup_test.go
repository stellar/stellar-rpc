package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
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
	inner := func(i int) txhash.HashIndex {
		gated, ok := got[i].(*windowGatedIndex)
		require.True(t, ok)
		return gated.inner
	}
	assert.Equal(t, dbs[7].Txhash(), inner(0), "newest chunk first")
	assert.Equal(t, dbs[5].Txhash(), inner(2), "oldest chunk last")
}

// stubIndex is a HashIndex whose Get always hits, answering seq.
type stubIndex struct{ seq uint32 }

func (s stubIndex) Get([32]byte) (uint32, error) { return s.seq, nil }

func TestWindowGatedIndex_OutOfWindowHitIsAMiss(t *testing.T) {
	view := &ReadView{floor: 5, latest: ledgerStamp{seq: chunk.ID(6).FirstLedger()}}
	oldest, latest := view.OldestLedger(), view.LatestLedger()

	for _, seq := range []uint32{oldest - 1, latest + 1} {
		gated := &windowGatedIndex{inner: stubIndex{seq: seq}, view: view}
		_, err := gated.Get([32]byte{1})
		assert.ErrorIs(t, err, stores.ErrNotFound, "seq %d is outside [%d, %d]", seq, oldest, latest)
	}

	for _, seq := range []uint32{oldest, latest} {
		gated := &windowGatedIndex{inner: stubIndex{seq: seq}, view: view}
		got, err := gated.Get([32]byte{1})
		require.NoError(t, err)
		assert.Equal(t, seq, got)
	}
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

	covs, err := a.coldTxHashIndexCoverages()
	require.NoError(t, err)
	require.Len(t, covs, 3, "only the frozen coverages, not the freezing debris")
	for _, cov := range covs {
		assert.Equal(t, geometry.StateFrozen, cov.State)
	}
	assert.Equal(t, chunk.ID(2*geometry.ChunksPerTxhashIndex), covs[0].Hi, "newest coverage first")
	assert.Equal(t, chunk.ID(geometry.ChunksPerTxhashIndex), covs[1].Hi)
	assert.Equal(t, chunk.ID(0), covs[2].Hi)
}

func TestColdTxIndexes(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	require.NoError(t, cat.FlipHotReady(999)) // acquisition needs a ready live chunk

	hashes := map[geometry.TxHashIndexID][32]byte{}
	seqs := map[geometry.TxHashIndexID]uint32{}
	for _, w := range []geometry.TxHashIndexID{0, 1} {
		c := chunk.ID(uint32(w) * geometry.ChunksPerTxhashIndex)
		cov, err := cat.MarkTxHashIndexFreezing(w, c, c)
		require.NoError(t, err)
		var h [32]byte
		h[0] = byte(w) + 1
		hashes[w], seqs[w] = h, c.FirstLedger()+5
		rpcv2test.WriteColdTxIndexFile(t, cat, cov, map[xdr.Hash]uint32{xdr.Hash(h): seqs[w]})
		require.NoError(t, cat.CommitTxHashIndex(cov))
	}
	// Freezing debris in window 3 — excluded from the probe set, so its missing
	// .idx file must never be opened.
	debris := chunk.ID(3 * geometry.ChunksPerTxhashIndex)
	_, err := cat.MarkTxHashIndexFreezing(3, debris, debris)
	require.NoError(t, err)

	// The returned indexes are window-gated; latest must cover the seeded seqs
	// or every hit reads as a miss.
	r.SetLatestLedger(seqs[1], 0)

	a, err := r.NewReadView()
	require.NoError(t, err)

	idxs, err := a.ColdTxIndexes()
	require.NoError(t, err)
	require.Len(t, idxs, 2, "one reader per frozen coverage, freezing debris excluded")

	got, err := idxs[0].Get(hashes[1])
	require.NoError(t, err)
	assert.Equal(t, seqs[1], got, "newest coverage's reader first")
	got, err = idxs[1].Get(hashes[0])
	require.NoError(t, err)
	assert.Equal(t, seqs[0], got)

	a.Release()
	_, err = idxs[0].Get(hashes[1])
	assert.ErrorIs(t, err, stores.ErrStoreClosed, "readers are view-owned: Release closes them")
}
