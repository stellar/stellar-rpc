package adapters

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// sparseFixture seeds chunk 5's first four ledgers and chunk 6's first two,
// with latest pinned to chunk 6's first ledger — one below the last committed
// one, so clamping at latest is distinguishable from data running out. The
// retention floor is chunk 5, so OldestLedger sits on real data.
func sparseFixture(t *testing.T) (*LedgerReader, chunk.ID, chunk.ID) {
	t.Helper()
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	c0, c1 := testChunk, testChunk+1
	seedHotLedgers(t, cat, r, c0, seqRange(c0.FirstLedger(), c0.FirstLedger()+3)...)
	seedHotLedgers(t, cat, r, c1, c1.FirstLedger(), c1.FirstLedger()+1)
	r.SetLatestLedger(c1.FirstLedger())
	return NewLedgerReader(r), c0, c1
}

// emptyFixture is a genuine first start: the live chunk's key is ready (a
// catalog with no ready hot chunk at all is broken, and NewReadView rejects
// it), but nothing is committed yet, so the last committed ledger is
// earliest-1 and OldestLedger exceeds LatestLedger by one.
func emptyFixture(t *testing.T) *LedgerReader {
	t.Helper()
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	seedHotLedgers(t, cat, r, testChunk)
	r.SetLatestLedger(testChunk.FirstLedger() - 1)
	return NewLedgerReader(r)
}

func TestGetLatestLedgerSequence(t *testing.T) {
	reader, _, c1 := sparseFixture(t)
	got, err := reader.GetLatestLedgerSequence(context.Background())
	require.NoError(t, err)
	assert.Equal(t, c1.FirstLedger(), got)
}

func TestGetLatestLedgerSequence_EmptyStore(t *testing.T) {
	reader := emptyFixture(t)
	_, err := reader.GetLatestLedgerSequence(context.Background())
	assert.ErrorIs(t, err, store.ErrEmptyDB)
}

func TestGetLedgerRange(t *testing.T) {
	reader, c0, c1 := sparseFixture(t)
	got, err := reader.GetLedgerRange(context.Background())
	require.NoError(t, err)
	assert.Equal(t, store.LedgerRange{
		FirstLedger: store.LedgerInfo{Sequence: c0.FirstLedger(), CloseTime: closeTimeFor(c0.FirstLedger())},
		LastLedger:  store.LedgerInfo{Sequence: c1.FirstLedger(), CloseTime: closeTimeFor(c1.FirstLedger())},
	}, got)
}

func TestGetLedgerRange_EmptyStore(t *testing.T) {
	reader := emptyFixture(t)
	_, err := reader.GetLedgerRange(context.Background())
	assert.ErrorIs(t, err, store.ErrEmptyDB)
}

func TestGetLedger_PointRead(t *testing.T) {
	reader, c0, _ := sparseFixture(t)
	lcm, ok, err := reader.GetLedger(context.Background(), c0.FirstLedger()+2)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, c0.FirstLedger()+2, lcm.LedgerSequence())
	assert.Equal(t, closeTimeFor(c0.FirstLedger()+2), lcm.LedgerCloseTime())
}

func TestGetLedger_SubGenesisDoesNotPanic(t *testing.T) {
	reader, _, _ := sparseFixture(t)
	for _, seq := range []uint32{0, 1} {
		_, ok, err := reader.GetLedger(context.Background(), seq)
		assert.NoError(t, err)
		assert.False(t, ok)
	}
}

func TestGetLedger_OutsideWindow(t *testing.T) {
	reader, c0, c1 := sparseFixture(t)
	// c1.FirstLedger()+1 is committed but above the view's latest; the gate,
	// not the store, must produce the miss.
	for _, seq := range []uint32{c0.FirstLedger() - 1, c1.FirstLedger() + 1} {
		_, ok, err := reader.GetLedger(context.Background(), seq)
		assert.NoError(t, err)
		assert.False(t, ok)
	}
}

func TestStreamLedgerRange(t *testing.T) {
	reader, c0, c1 := sparseFixture(t)
	var seqs []uint32
	err := reader.StreamLedgerRange(context.Background(), c0.FirstLedger(), c1.FirstLedger()+500,
		func(lcm xdr.LedgerCloseMeta) error {
			assert.Equal(t, closeTimeFor(lcm.LedgerSequence()), lcm.LedgerCloseTime())
			seqs = append(seqs, lcm.LedgerSequence())
			return nil
		})
	require.NoError(t, err)
	assert.Equal(t, append(seqRange(c0.FirstLedger(), c0.FirstLedger()+3), c1.FirstLedger()), seqs,
		"streams what is committed, flat across the chunk border, clamped at latest")
}

func TestStreamLedgerRange_CallbackErrorStopsStream(t *testing.T) {
	reader, c0, _ := sparseFixture(t)
	boom := errors.New("boom")
	calls := 0
	err := reader.StreamLedgerRange(context.Background(), c0.FirstLedger(), c0.FirstLedger()+3,
		func(xdr.LedgerCloseMeta) error {
			calls++
			return boom
		})
	assert.ErrorIs(t, err, boom)
	assert.Equal(t, 1, calls)
}

func TestStreamLedgerRange_BelowFloorIsRangeError(t *testing.T) {
	reader, c0, _ := sparseFixture(t)
	var rangeErr *query.RangeError
	err := reader.StreamLedgerRange(context.Background(), 2, c0.FirstLedger(),
		func(xdr.LedgerCloseMeta) error { return nil })
	require.ErrorAs(t, err, &rangeErr)
	assert.Equal(t, uint32(2), rangeErr.Requested)
	assert.Equal(t, c0.FirstLedger(), rangeErr.Oldest)
}

func TestTxGetLedger_WalksContiguously(t *testing.T) {
	reader, c0, _ := sparseFixture(t)
	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	for seq := c0.FirstLedger(); seq <= c0.FirstLedger()+3; seq++ {
		lcm, ok, err := tx.GetLedger(context.Background(), seq)
		require.NoError(t, err)
		require.True(t, ok, "ledger %d", seq)
		assert.Equal(t, seq, lcm.LedgerSequence())
		assert.Equal(t, closeTimeFor(seq), lcm.LedgerCloseTime())
	}
}

func TestTxGetLedger_NonSequentialFailsLoudly(t *testing.T) {
	reader, c0, _ := sparseFixture(t)
	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	_, ok, err := tx.GetLedger(context.Background(), c0.FirstLedger())
	require.NoError(t, err)
	require.True(t, ok)

	// Skipping ahead breaks the walk contract; serving the wrong ledger's
	// data would be worse than an error.
	_, _, err = tx.GetLedger(context.Background(), c0.FirstLedger()+2)
	assert.ErrorContains(t, err, "non-sequential")
}

func TestTxGetLedger_GuardsBeforePriming(t *testing.T) {
	reader, c0, c1 := sparseFixture(t)
	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	// Sub-genesis and out-of-window sequences return clean misses without
	// consuming the walk iterator...
	for _, seq := range []uint32{0, 1, c0.FirstLedger() - 1, c1.FirstLedger() + 1} {
		_, ok, err := tx.GetLedger(context.Background(), seq)
		assert.NoError(t, err)
		assert.False(t, ok)
	}
	// ...so a walk primed afterwards still starts at its own sequence.
	lcm, ok, err := tx.GetLedger(context.Background(), c0.FirstLedger())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, c0.FirstLedger(), lcm.LedgerSequence())
}

func TestTxGetLedgerRange_DoesNotDisturbTheWalk(t *testing.T) {
	reader, c0, c1 := sparseFixture(t)
	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	// getTransactions reads the range first, then walks; the range's point
	// reads must not consume the walk iterator.
	lr, err := tx.GetLedgerRange(context.Background())
	require.NoError(t, err)
	assert.Equal(t, c0.FirstLedger(), lr.FirstLedger.Sequence)
	assert.Equal(t, c1.FirstLedger(), lr.LastLedger.Sequence)

	lcm, ok, err := tx.GetLedger(context.Background(), c0.FirstLedger()+1)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, c0.FirstLedger()+1, lcm.LedgerSequence())
}

func TestTxBatchGetLedgers(t *testing.T) {
	reader, c0, c1 := sparseFixture(t)
	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	got, err := tx.BatchGetLedgers(context.Background(), c0.FirstLedger(), c1.FirstLedger()+500)
	require.NoError(t, err)
	require.Len(t, got, 5, "four from chunk 5, one from chunk 6, clamped at latest")

	want := append(seqRange(c0.FirstLedger(), c0.FirstLedger()+3), c1.FirstLedger())
	for i, mc := range got {
		assert.Equal(t, want[i], uint32(mc.Header.Header.LedgerSeq))
		// Entry.Bytes is borrowed from the reader's scratch buffer; if the
		// adapter forgot to clone, earlier entries would decode to a later
		// ledger's bytes by the time the loop finishes.
		var lcm xdr.LedgerCloseMeta
		require.NoError(t, lcm.UnmarshalBinary(mc.Lcm))
		assert.Equal(t, want[i], lcm.LedgerSequence())
		assert.Equal(t, closeTimeFor(want[i]), lcm.LedgerCloseTime())
	}
}

func TestTxBatchGetLedgers_BelowFloorIsRangeError(t *testing.T) {
	reader, c0, _ := sparseFixture(t)
	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	var rangeErr *query.RangeError
	_, err = tx.BatchGetLedgers(context.Background(), 2, c0.FirstLedger())
	require.ErrorAs(t, err, &rangeErr)
}

func TestTxBatchGetLedgers_BeyondLatestIsEmpty(t *testing.T) {
	reader, _, c1 := sparseFixture(t)
	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	got, err := tx.BatchGetLedgers(context.Background(), c1.FirstLedger()+100, c1.FirstLedger()+200)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestTxDone_WithAndWithoutPriming(t *testing.T) {
	reader, c0, _ := sparseFixture(t)

	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	assert.NoError(t, tx.Done())

	tx, err = reader.NewTx(context.Background())
	require.NoError(t, err)
	_, _, err = tx.GetLedger(context.Background(), c0.FirstLedger())
	require.NoError(t, err)
	assert.NoError(t, tx.Done())
}

// TestTxGetLedger_WalkCrossesChunkBorder seeds chunk 5 densely (the walk
// contract assumes contiguous ledgers, so the border is only reachable through
// a full chunk) and walks the seam ledger by ledger.
func TestTxGetLedger_WalkCrossesChunkBorder(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds a full 10k-ledger chunk")
	}
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	c0, c1 := testChunk, testChunk+1
	seedHotLedgers(t, cat, r, c0, seqRange(c0.FirstLedger(), c0.LastLedger())...)
	seedHotLedgers(t, cat, r, c1, c1.FirstLedger(), c1.FirstLedger()+1)
	r.SetLatestLedger(c1.FirstLedger() + 1)
	reader := NewLedgerReader(r)

	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	for seq := c0.LastLedger() - 1; seq <= c1.FirstLedger()+1; seq++ {
		lcm, ok, err := tx.GetLedger(context.Background(), seq)
		require.NoError(t, err, "ledger %d", seq)
		require.True(t, ok, "ledger %d", seq)
		assert.Equal(t, seq, lcm.LedgerSequence())
	}
}

func TestBatchGetLedgers_ClonesBorrowedBytes(t *testing.T) {
	reader, c0, _ := sparseFixture(t)
	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	got, err := tx.BatchGetLedgers(context.Background(), c0.FirstLedger(), c0.FirstLedger()+1)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.False(t, bytes.Equal(got[0].Lcm, got[1].Lcm),
		"two entries decoding identically would mean both alias one scratch buffer")
}
