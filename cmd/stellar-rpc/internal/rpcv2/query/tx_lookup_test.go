package query

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// openHotChunk seeds chunk c's hot DB with n zero-tx ledgers from its first
// ledger, marks it ready, and publishes the handle on r unless r is nil.
func openHotChunk(t *testing.T, cat *catalog.Catalog, r *Registry, c chunk.ID, n uint32) *hotchunk.DB {
	t.Helper()
	lcms := make([][]byte, 0, n)
	for seq := c.FirstLedger(); seq < c.FirstLedger()+n; seq++ {
		lcms = append(lcms, rpcv2test.ZeroTxLCMBytesAt(t, seq, int64(seq)))
	}
	var db *hotchunk.DB
	rpcv2test.SeedHotChunkLCMs(t, cat, c, func(d *hotchunk.DB) {
		db = d
		if r != nil {
			r.PublishHandle(c, d)
		}
	}, lcms...)
	return db
}

// coldIndexes enumerates the cold tier of a's probe set for [first, last].
func coldIndexes(t *testing.T, a *ReadView, first, last uint32) []txhash.HashIndex {
	t.Helper()
	_, cold := a.TxIndexes(first, last)
	idxs, err := cold()
	require.NoError(t, err)
	return idxs
}

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
	hot, _ := empty.TxIndexes(0, math.MaxUint32)
	assert.Empty(t, hot, "no handles → no hot indexes")
	empty.Release()

	dbs := map[chunk.ID]*hotchunk.DB{}
	for _, c := range []chunk.ID{5, 6, 7} {
		dbs[c] = openHotChunk(t, cat, r, c, 0)
	}

	a, err := r.NewReadView()
	require.NoError(t, err)
	defer a.Release()

	inner := func(idx txhash.HashIndex) txhash.HashIndex {
		gated, ok := idx.(*boundsGatedIndex)
		require.True(t, ok)
		return gated.inner
	}
	hot, _ = a.TxIndexes(0, math.MaxUint32)
	require.Len(t, hot, 3)
	assert.Equal(t, dbs[7].Txhash(), inner(hot[0]), "newest chunk first")
	assert.Equal(t, dbs[5].Txhash(), inner(hot[2]), "oldest chunk last")

	hot, _ = a.TxIndexes(chunk.ID(6).FirstLedger()+1, chunk.ID(6).LastLedger())
	require.Len(t, hot, 1, "only the chunk holding the bounds")
	assert.Equal(t, dbs[6].Txhash(), inner(hot[0]))

	hot, _ = a.TxIndexes(chunk.ID(6).LastLedger(), math.MaxUint32)
	require.Len(t, hot, 2, "bounds straddling a boundary reach both chunks")
	assert.Equal(t, dbs[7].Txhash(), inner(hot[0]))
	assert.Equal(t, dbs[6].Txhash(), inner(hot[1]))

	hot, _ = a.TxIndexes(0, chunk.ID(4).LastLedger())
	assert.Empty(t, hot, "bounds below every hot chunk")
	hot, cold := a.TxIndexes(chunk.ID(8).FirstLedger(), math.MaxUint32)
	assert.Empty(t, hot, "bounds above latest")
	assert.Nil(t, cold, "an empty clamped range has no cold tier either")
}

func TestHotChunksCover(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	c := func(id uint32) chunk.ID { return chunk.ID(id) }
	view := &ReadView{handles: &handleSet{byChunk: map[chunk.ID]*hotchunk.DB{
		5: openHotChunk(t, cat, nil, 5, 3), // partial: committed through its third ledger
		6: openHotChunk(t, cat, nil, 6, 2),
		7: openHotChunk(t, cat, nil, 7, 0), // published before its first commit
	}}}
	committed5 := c(5).FirstLedger() + 2

	assert.True(t, view.hotChunksCover(c(5).FirstLedger(), committed5), "within the committed ledgers")
	assert.True(t, view.hotChunksCover(committed5, committed5))
	assert.True(t, view.hotChunksCover(c(6).FirstLedger(), c(6).FirstLedger()+1))
	assert.False(t, view.hotChunksCover(c(5).FirstLedger(), committed5+1), "past the chunk's last commit")
	assert.False(t, view.hotChunksCover(committed5, c(6).FirstLedger()),
		"a partial chunk covers nothing beyond its last commit")
	assert.False(t, view.hotChunksCover(c(7).FirstLedger(), c(7).FirstLedger()), "an empty hot chunk")
	assert.False(t, view.hotChunksCover(c(4).LastLedger(), c(5).FirstLedger()), "no handle for chunk 4")
}

// stubIndex is a HashIndex whose Get always hits, answering seq.
type stubIndex struct{ seq uint32 }

func (s stubIndex) Get([32]byte) (uint32, error) { return s.seq, nil }

func TestBoundsGatedIndex_OutOfBoundsHitIsAMiss(t *testing.T) {
	lo, hi := chunk.ID(5).FirstLedger(), chunk.ID(6).FirstLedger()

	for _, seq := range []uint32{lo - 1, hi + 1} {
		gated := &boundsGatedIndex{inner: stubIndex{seq: seq}, lo: lo, hi: hi}
		_, err := gated.Get([32]byte{1})
		assert.ErrorIs(t, err, stores.ErrNotFound, "seq %d is outside [%d, %d]", seq, lo, hi)
	}

	for _, seq := range []uint32{lo, hi} {
		gated := &boundsGatedIndex{inner: stubIndex{seq: seq}, lo: lo, hi: hi}
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

	// The returned indexes are gated; latest must cover the seeded seqs or
	// every hit reads as a miss.
	r.SetLatestLedger(seqs[1], CloseTimeAt(0))

	a, err := r.NewReadView()
	require.NoError(t, err)

	idxs := coldIndexes(t, a, 0, math.MaxUint32)
	require.Len(t, idxs, 2, "one reader per frozen coverage, freezing debris excluded")

	got, err := idxs[0].Get(hashes[1])
	require.NoError(t, err)
	assert.Equal(t, seqs[1], got, "newest coverage's reader first")
	got, err = idxs[1].Get(hashes[0])
	require.NoError(t, err)
	assert.Equal(t, seqs[0], got)

	only1 := coldIndexes(t, a, seqs[1], seqs[1])
	require.Len(t, only1, 1, "bounds inside window 1 reach only its index")
	got, err = only1[0].Get(hashes[1])
	require.NoError(t, err)
	assert.Equal(t, seqs[1], got)

	only0 := coldIndexes(t, a, 0, chunk.ID(0).LastLedger())
	require.Len(t, only0, 1, "bounds inside window 0 reach only its index")
	_, err = only0[0].Get(hashes[1])
	assert.ErrorIs(t, err, stores.ErrNotFound, "window 0's index does not hold window 1's hash")

	a.Release()
	_, err = idxs[0].Get(hashes[1])
	assert.ErrorIs(t, err, stores.ErrStoreClosed, "readers are view-owned: Release closes them")
}

// TestColdTxIndexes_HotCoveredBoundsSkipTheColdTier pins the polling shortcut:
// when every ledger in the clamped range is committed to a hot chunk, a hot
// miss is final and the cold tier is not enumerated, even though a frozen
// coverage meets the bounds. A partial hot chunk counts only through its last
// commit.
func TestColdTxIndexes_HotCoveredBoundsSkipTheColdTier(t *testing.T) {
	cat := openTestCatalog(t, silentLogger())
	r := NewRegistry(cat, geometry.NewRetention(0, 4))
	require.NoError(t, cat.FlipHotReady(999)) // acquisition needs a ready live chunk
	openHotChunk(t, cat, r, 5, 2)             // partial: left by a restart, since served cold
	openHotChunk(t, cat, r, 6, 4)
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

	for _, first := range []uint32{chunk.ID(6).FirstLedger(), latest} {
		assert.Empty(t, coldIndexes(t, a, first, math.MaxUint32), "bounds from %d are committed in hot chunk 6", first)
	}
	assert.Empty(t, coldIndexes(t, a, chunk.ID(5).FirstLedger(), chunk.ID(5).FirstLedger()+1),
		"bounds within chunk 5's committed ledgers")

	for _, first := range []uint32{0, chunk.ID(5).FirstLedger()} {
		idxs := coldIndexes(t, a, first, math.MaxUint32)
		require.Len(t, idxs, 1, "bounds from %d reach ledgers no hot chunk committed, so the coverage is probed", first)
		_, err = idxs[0].Get([32]byte{1})
		require.Error(t, err)
		assert.NotErrorIs(t, err, stores.ErrNotFound, "the missing .idx surfaces as a probe failure")
	}
}
