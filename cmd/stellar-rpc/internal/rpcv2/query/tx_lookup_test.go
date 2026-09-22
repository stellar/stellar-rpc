package query

import (
	"math"
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

// TestHotTxHashIndexes pins that every published hot chunk's tx index that
// meets the bounds is returned, newest chunk first, and that an empty handle
// set yields no indexes.
func TestHotTxHashIndexes(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 0))
	require.NoError(t, cat.FlipHotReady(999)) // acquisition needs a ready live chunk
	r.SetLatestLedger(chunk.ID(7).LastLedger(), CloseTimeAt(0))

	empty, err := r.NewReadView()
	require.NoError(t, err)
	assert.Empty(t, empty.HotTxHashIndexes(0, math.MaxUint32), "no handles → no hot indexes")
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

	inner := func(idx txhash.HashIndex) txhash.HashIndex {
		gated, ok := idx.(*windowGatedIndex)
		require.True(t, ok)
		return gated.inner
	}
	got := a.HotTxHashIndexes(0, math.MaxUint32)
	require.Len(t, got, 3)
	assert.Equal(t, dbs[7].Txhash(), inner(got[0]), "newest chunk first")
	assert.Equal(t, dbs[5].Txhash(), inner(got[2]), "oldest chunk last")

	got = a.HotTxHashIndexes(chunk.ID(6).FirstLedger()+1, chunk.ID(6).LastLedger())
	require.Len(t, got, 1, "only the chunk holding the bounds")
	assert.Equal(t, dbs[6].Txhash(), inner(got[0]))

	got = a.HotTxHashIndexes(chunk.ID(6).LastLedger(), math.MaxUint32)
	require.Len(t, got, 2, "bounds straddling a boundary reach both chunks")
	assert.Equal(t, dbs[7].Txhash(), inner(got[0]))
	assert.Equal(t, dbs[6].Txhash(), inner(got[1]))

	assert.Empty(t, a.HotTxHashIndexes(0, chunk.ID(4).LastLedger()), "bounds below every hot chunk")
	assert.Empty(t, a.HotTxHashIndexes(chunk.ID(8).FirstLedger(), math.MaxUint32), "bounds above latest")
}

func TestHotChunksCover(t *testing.T) {
	view := &ReadView{handles: &handleSet{byChunk: map[chunk.ID]*hotchunk.DB{5: nil, 6: nil, 7: nil}}}
	c := func(id uint32) chunk.ID { return chunk.ID(id) }

	assert.True(t, view.hotChunksCover(c(6).FirstLedger()+1, c(6).LastLedger()-1), "inside one hot chunk")
	assert.True(t, view.hotChunksCover(c(5).FirstLedger(), c(7).LastLedger()), "the whole hot run")
	assert.True(t, view.hotChunksCover(c(6).LastLedger(), c(7).FirstLedger()), "straddling a hot boundary")
	assert.False(t, view.hotChunksCover(c(4).LastLedger(), c(5).FirstLedger()), "reaching below the run")
	assert.False(t, view.hotChunksCover(c(7).LastLedger(), c(8).FirstLedger()), "reaching above the run")
	assert.False(t, view.hotChunksCover(c(9).FirstLedger(), c(9).LastLedger()), "outside the run")
}

// stubIndex is a HashIndex whose Get always hits, answering seq.
type stubIndex struct{ seq uint32 }

func (s stubIndex) Get([32]byte) (uint32, error) { return s.seq, nil }

func TestWindowGatedIndex_OutOfBoundsHitIsAMiss(t *testing.T) {
	lo, hi := chunk.ID(5).FirstLedger(), chunk.ID(6).FirstLedger()

	for _, seq := range []uint32{lo - 1, hi + 1} {
		gated := &windowGatedIndex{inner: stubIndex{seq: seq}, lo: lo, hi: hi}
		_, err := gated.Get([32]byte{1})
		assert.ErrorIs(t, err, stores.ErrNotFound, "seq %d is outside [%d, %d]", seq, lo, hi)
	}

	for _, seq := range []uint32{lo, hi} {
		gated := &windowGatedIndex{inner: stubIndex{seq: seq}, lo: lo, hi: hi}
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
	r.SetLatestLedger(seqs[1], CloseTimeAt(0))

	a, err := r.NewReadView()
	require.NoError(t, err)

	idxs, err := a.ColdTxIndexes(0, math.MaxUint32)
	require.NoError(t, err)
	require.Len(t, idxs, 2, "one reader per frozen coverage, freezing debris excluded")

	got, err := idxs[0].Get(hashes[1])
	require.NoError(t, err)
	assert.Equal(t, seqs[1], got, "newest coverage's reader first")
	got, err = idxs[1].Get(hashes[0])
	require.NoError(t, err)
	assert.Equal(t, seqs[0], got)

	only1, err := a.ColdTxIndexes(seqs[1], seqs[1])
	require.NoError(t, err)
	require.Len(t, only1, 1, "bounds inside window 1 reach only its index")
	got, err = only1[0].Get(hashes[1])
	require.NoError(t, err)
	assert.Equal(t, seqs[1], got)

	only0, err := a.ColdTxIndexes(0, chunk.ID(0).LastLedger())
	require.NoError(t, err)
	require.Len(t, only0, 1, "bounds inside window 0 reach only its index")
	_, err = only0[0].Get(hashes[1])
	assert.ErrorIs(t, err, stores.ErrNotFound, "window 0's index does not hold window 1's hash")

	a.Release()
	_, err = idxs[0].Get(hashes[1])
	assert.ErrorIs(t, err, stores.ErrStoreClosed, "readers are view-owned: Release closes them")
}

// TestColdTxIndexes_HotCoveredBoundsSkipTheColdTier pins the polling shortcut:
// when the published hot chunks cover the whole clamped range, a hot miss is
// final and the cold tier is not enumerated, even though a frozen coverage
// meets the bounds.
func TestColdTxIndexes_HotCoveredBoundsSkipTheColdTier(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 4))
	require.NoError(t, cat.FlipHotReady(999)) // acquisition needs a ready live chunk
	for _, c := range []chunk.ID{5, 6} {
		db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		r.PublishHandle(c, db)
	}
	// A frozen coverage over chunk 5 with no .idx behind it: probing it fails,
	// so a lookup that reaches it cannot end as a clean miss.
	cov, err := cat.MarkTxHashIndexFreezing(0, 5, 5)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(cov))
	latest := chunk.ID(6).FirstLedger() + 3
	r.SetLatestLedger(latest, CloseTimeAt(0))

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	for _, first := range []uint32{chunk.ID(5).FirstLedger(), chunk.ID(6).FirstLedger(), latest} {
		idxs, err := a.ColdTxIndexes(first, math.MaxUint32)
		require.NoError(t, err)
		assert.Empty(t, idxs, "bounds from %d are covered by hot chunks 5 and 6", first)
	}

	idxs, err := a.ColdTxIndexes(0, math.MaxUint32)
	require.NoError(t, err)
	require.Len(t, idxs, 1, "the floor chunk 4 is not hot, so the frozen coverage is probed")
	_, err = idxs[0].Get([32]byte{1})
	require.Error(t, err)
	assert.NotErrorIs(t, err, stores.ErrNotFound, "the missing .idx surfaces as a probe failure")
}
