package adapters

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
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
	r.SetLatestLedger(c1.FirstLedger(), closeTimeFor(c1.FirstLedger()))
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
	r.SetLatestLedger(testChunk.FirstLedger()-1, 0)
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

func TestGetLedgerRange_BootStampFallsBackThenCaches(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	seedHotLedgers(t, cat, r, testChunk, testChunk.FirstLedger(), testChunk.FirstLedger()+1)
	// The boot seeding: OpenRegistry stamps the latest seq with close time 0
	// because the catalog knows no close times.
	r.SetLatestLedger(testChunk.FirstLedger()+1, 0)
	reader := NewLedgerReader(r)

	got, err := reader.GetLedgerRange(context.Background())
	require.NoError(t, err)
	assert.Equal(t, closeTimeFor(testChunk.FirstLedger()), got.FirstLedger.CloseTime,
		"a stamp miss still serves the real close time via the point read")
	assert.Equal(t, closeTimeFor(testChunk.FirstLedger()+1), got.LastLedger.CloseTime)

	// The fallback recorded the oldest edge, so the next view serves it from
	// memory.
	view, err := r.NewReadView()
	require.NoError(t, err)
	defer view.Release()
	ct, ok := view.OldestCloseTime()
	assert.True(t, ok, "first GetLedgerRange populates the oldest cache")
	assert.Equal(t, closeTimeFor(testChunk.FirstLedger()), ct)
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

func TestGetLedger_V1LedgerCloseMeta(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	raw, _ := lcmV1WithClassicTx(t, testChunk.FirstLedger())
	seedHotChunkLCMs(t, cat, r, testChunk, raw)
	r.SetLatestLedger(testChunk.FirstLedger(), closeTimeFor(testChunk.FirstLedger()))
	reader := NewLedgerReader(r)

	lcm, ok, err := reader.GetLedger(context.Background(), testChunk.FirstLedger())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, testChunk.FirstLedger(), lcm.LedgerSequence())
	assert.Equal(t, closeTimeFor(testChunk.FirstLedger()), lcm.LedgerCloseTime())
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

func TestTxBatchGetLedgers_HeaderMatchesFullDecode(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	rawV1, _ := lcmV1WithClassicTx(t, testChunk.FirstLedger())
	rawV2, _ := lcmWithTxs(t, testChunk.FirstLedger()+1, txSpec{})
	seedHotChunkLCMs(t, cat, r, testChunk, rawV1, rawV2)
	r.SetLatestLedger(testChunk.FirstLedger()+1, closeTimeFor(testChunk.FirstLedger()+1))
	reader := NewLedgerReader(r)

	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	got, err := tx.BatchGetLedgers(context.Background(), testChunk.FirstLedger(), testChunk.FirstLedger()+1)
	require.NoError(t, err)
	require.Len(t, got, 2)
	for i, raw := range [][]byte{rawV1, rawV2} {
		var full xdr.LedgerCloseMeta
		require.NoError(t, full.UnmarshalBinary(raw))
		assert.Equal(t, full.LedgerHeaderHistoryEntry(), got[i].Header,
			"the header-only decode must produce what the full decode would")
		assert.Equal(t, raw, got[i].Lcm)
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
	assert.NoError(t, tx.Done(), "a second Done must be a no-op, not a double release")

	tx, err = reader.NewTx(context.Background())
	require.NoError(t, err)
	_, _, err = tx.GetLedger(context.Background(), c0.FirstLedger())
	require.NoError(t, err)
	assert.NoError(t, tx.Done())
	assert.NoError(t, tx.Done(), "a second Done must be a no-op, not a double release")
}

func TestTxGetLedger_WalkCrossesChunkBorder(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds a full 10k-ledger chunk")
	}
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	c0, c1 := testChunk, testChunk+1
	seedHotLedgers(t, cat, r, c0, seqRange(c0.FirstLedger(), c0.LastLedger())...)
	seedHotLedgers(t, cat, r, c1, c1.FirstLedger(), c1.FirstLedger()+1)
	r.SetLatestLedger(c1.FirstLedger()+1, closeTimeFor(c1.FirstLedger()+1))
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

func TestTxGetLedger_WalkPastSpanCapIsExhaustedNotNonSequential(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds a full 10k-ledger chunk")
	}
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	c0, c1 := testChunk, testChunk+1
	seedHotLedgers(t, cat, r, c0, seqRange(c0.FirstLedger(), c0.LastLedger())...)
	seedHotLedgers(t, cat, r, c1, c1.FirstLedger(), c1.FirstLedger()+1, c1.FirstLedger()+2)
	r.SetLatestLedger(c1.FirstLedger()+2, closeTimeFor(c1.FirstLedger()+2))
	reader := NewLedgerReader(r)

	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	start := c0.FirstLedger()
	for seq := start; seq <= start+walkSpanCap; seq++ {
		_, ok, err := tx.GetLedger(context.Background(), seq)
		require.NoError(t, err, "ledger %d", seq)
		require.True(t, ok, "ledger %d", seq)
	}

	_, _, err = tx.GetLedger(context.Background(), start+walkSpanCap+1)
	require.ErrorContains(t, err, "exhausted its primed")
	assert.NotContains(t, err.Error(), "non-sequential")
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

func TestWalkSpanCapCoversTheHandlerScanLimit(t *testing.T) {
	assert.GreaterOrEqual(t, int(walkSpanCap), methods.LedgerScanLimit,
		"handler scan limits must never exceed one chunk's worth of ledgers")
}

func TestLedgerReaderTx_GetLedgerStopsOnCanceledContext(t *testing.T) {
	reader, c0, _ := sparseFixture(t)
	tx, err := reader.NewTx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	ctx, cancel := context.WithCancel(context.Background())
	_, found, err := tx.GetLedger(ctx, c0.FirstLedger())
	require.NoError(t, err)
	require.True(t, found)

	cancel()
	_, _, err = tx.GetLedger(ctx, c0.FirstLedger()+1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLedgerReaderTx_BatchGetLedgersStopsOnCanceledContext(t *testing.T) {
	r, first := sharedViewFixture(t)
	reader := NewLedgerReader(r)

	ctx, cancel := context.WithCancel(context.Background())
	tx, err := reader.NewTx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()
	cancel()

	_, err = tx.BatchGetLedgers(ctx, first, first+2)
	assert.ErrorIs(t, err, context.Canceled)
}
