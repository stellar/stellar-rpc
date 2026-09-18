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
func sparseFixture(t *testing.T) (context.Context, *LedgerReader, chunk.ID, chunk.ID) {
	t.Helper()
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	c0, c1 := testChunk, testChunk+1
	seedHotLedgers(t, cat, r, c0, seqRange(c0.FirstLedger(), c0.FirstLedger()+3)...)
	seedHotLedgers(t, cat, r, c1, c1.FirstLedger(), c1.FirstLedger()+1)
	r.SetLatestLedger(c1.FirstLedger(), query.CloseTimeAt(closeTimeFor(c1.FirstLedger())))
	return viewCtx(t, r), NewLedgerReader(), c0, c1
}

// emptyFixture is a genuine first start: the live chunk's key is ready (a
// catalog with no ready hot chunk at all is broken, and NewReadView rejects
// it), but nothing is committed yet, so the last committed ledger is
// earliest-1 and OldestLedger exceeds LatestLedger by one.
func emptyFixture(t *testing.T) (context.Context, *LedgerReader) {
	t.Helper()
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	seedHotLedgers(t, cat, r, testChunk)
	r.SetLatestLedger(testChunk.FirstLedger()-1, query.UnknownCloseTime())
	return viewCtx(t, r), NewLedgerReader()
}

func TestGetLatestLedgerSequence(t *testing.T) {
	ctx, reader, _, c1 := sparseFixture(t)
	got, err := reader.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, c1.FirstLedger(), got)
}

func TestGetLatestLedgerSequence_EmptyStore(t *testing.T) {
	ctx, reader := emptyFixture(t)
	_, err := reader.GetLatestLedgerSequence(ctx)
	assert.ErrorIs(t, err, store.ErrEmptyDB)
}

func TestGetLedgerRange(t *testing.T) {
	ctx, reader, c0, c1 := sparseFixture(t)
	got, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, store.LedgerRange{
		FirstLedger: store.LedgerInfo{Sequence: c0.FirstLedger(), CloseTime: closeTimeFor(c0.FirstLedger())},
		LastLedger:  store.LedgerInfo{Sequence: c1.FirstLedger(), CloseTime: closeTimeFor(c1.FirstLedger())},
	}, got)
}

func TestGetLedgerRange_EmptyStore(t *testing.T) {
	ctx, reader := emptyFixture(t)
	_, err := reader.GetLedgerRange(ctx)
	assert.ErrorIs(t, err, store.ErrEmptyDB)
}

func TestGetLedgerRange_BootStampFallsBackThenCaches(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	seedHotLedgers(t, cat, r, testChunk, testChunk.FirstLedger(), testChunk.FirstLedger()+1)
	// The boot seeding: OpenRegistry publishes the latest seq with no close
	// time, because the catalog records sequences and not timestamps.
	r.SetLatestLedger(testChunk.FirstLedger()+1, query.UnknownCloseTime())
	reader := NewLedgerReader()

	got, err := reader.GetLedgerRange(viewCtx(t, r))
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
	ctx, reader, c0, _ := sparseFixture(t)
	lcm, ok, err := store.GetLedger(ctx, reader, c0.FirstLedger()+2)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, c0.FirstLedger()+2, lcm.LedgerSequence())
	assert.Equal(t, closeTimeFor(c0.FirstLedger()+2), lcm.LedgerCloseTime())
}

func TestGetLedger_SubGenesisDoesNotPanic(t *testing.T) {
	ctx, reader, _, _ := sparseFixture(t)
	for _, seq := range []uint32{0, 1} {
		_, ok, err := store.GetLedger(ctx, reader, seq)
		assert.NoError(t, err)
		assert.False(t, ok)
	}
}

func TestGetLedger_OutsideWindow(t *testing.T) {
	ctx, reader, c0, c1 := sparseFixture(t)
	// c1.FirstLedger()+1 is committed but above the view's latest; the gate,
	// not the store, must produce the miss.
	for _, seq := range []uint32{c0.FirstLedger() - 1, c1.FirstLedger() + 1} {
		_, ok, err := store.GetLedger(ctx, reader, seq)
		assert.NoError(t, err)
		assert.False(t, ok)
	}
}

func TestGetLedger_V1LedgerCloseMeta(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	raw, _ := lcmV1WithClassicTx(t, testChunk.FirstLedger())
	seedHotChunkLCMs(t, cat, r, testChunk, raw)
	r.SetLatestLedger(testChunk.FirstLedger(), query.CloseTimeAt(closeTimeFor(testChunk.FirstLedger())))
	reader := NewLedgerReader()

	lcm, ok, err := store.GetLedger(viewCtx(t, r), reader, testChunk.FirstLedger())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, testChunk.FirstLedger(), lcm.LedgerSequence())
	assert.Equal(t, closeTimeFor(testChunk.FirstLedger()), lcm.LedgerCloseTime())
}

func TestStreamLedgerRange(t *testing.T) {
	ctx, reader, c0, c1 := sparseFixture(t)
	var seqs []uint32
	err := reader.StreamLedgerRange(ctx, c0.FirstLedger(), c1.FirstLedger()+500,
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
	ctx, reader, c0, _ := sparseFixture(t)
	boom := errors.New("boom")
	calls := 0
	err := reader.StreamLedgerRange(ctx, c0.FirstLedger(), c0.FirstLedger()+3,
		func(xdr.LedgerCloseMeta) error {
			calls++
			return boom
		})
	assert.ErrorIs(t, err, boom)
	assert.Equal(t, 1, calls)
}

func TestStreamLedgerRange_BelowFloorIsRangeError(t *testing.T) {
	ctx, reader, c0, _ := sparseFixture(t)
	var rangeErr *query.RangeError
	err := reader.StreamLedgerRange(ctx, 2, c0.FirstLedger(),
		func(xdr.LedgerCloseMeta) error { return nil })
	require.ErrorAs(t, err, &rangeErr)
	assert.Equal(t, uint32(2), rangeErr.Requested)
	assert.Equal(t, c0.FirstLedger(), rangeErr.Oldest)
}

// scanAll drains a Tx scan into sequences and clones of the borrowed bytes.
func scanAll(t *testing.T, tx store.LedgerReaderTx, start, end uint32) ([]uint32, [][]byte) {
	t.Helper()
	var seqs []uint32
	var raws [][]byte
	for l, err := range tx.ScanLedgers(context.Background(), start, end) {
		require.NoError(t, err)
		seqs = append(seqs, l.Sequence)
		raws = append(raws, bytes.Clone(l.Raw)) // the loan forbids retaining Raw
	}
	return seqs, raws
}

func TestTxScanLedgers_YieldsAscendingAndClampsAtLatest(t *testing.T) {
	ctx, reader, c0, c1 := sparseFixture(t)
	tx, err := reader.NewTx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	seqs, raws := scanAll(t, tx, c0.FirstLedger(), c1.FirstLedger()+500)
	want := append(seqRange(c0.FirstLedger(), c0.FirstLedger()+3), c1.FirstLedger())
	assert.Equal(t, want, seqs, "four from chunk 5, one from chunk 6, clamped at latest; the gap between is silent")
	for i, raw := range raws {
		var lcm xdr.LedgerCloseMeta
		require.NoError(t, lcm.UnmarshalBinary(raw))
		assert.Equal(t, want[i], lcm.LedgerSequence())
		assert.Equal(t, closeTimeFor(want[i]), lcm.LedgerCloseTime())
	}
}

func TestTxScanLedgers_LendsTheSameBytesGetLedgerDecodes(t *testing.T) {
	ctx, reader, c0, _ := sparseFixture(t)
	tx, err := reader.NewTx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	seqs, raws := scanAll(t, tx, c0.FirstLedger(), c0.FirstLedger()+3)
	require.Len(t, seqs, 4)
	for i, seq := range seqs {
		lcm, ok, err := store.GetLedger(ctx, reader, seq)
		require.NoError(t, err)
		require.True(t, ok)
		want, err := lcm.MarshalBinary()
		require.NoError(t, err)
		assert.Equal(t, want, raws[i], "ledger %d", seq)
	}
}

// A start below the retention floor is raised to it rather than surfacing
// ClampRange's *RangeError; the handler's gap check then names the caller's ledger.
func TestTxScanLedgers_BelowFloorStartsAtOldest(t *testing.T) {
	ctx, reader, c0, _ := sparseFixture(t)
	tx, err := reader.NewTx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	seqs, _ := scanAll(t, tx, 2, c0.FirstLedger()+1)
	assert.Equal(t, seqRange(c0.FirstLedger(), c0.FirstLedger()+1), seqs)
}

func TestTxScanLedgers_EmptyRanges(t *testing.T) {
	ctx, reader, c0, c1 := sparseFixture(t)
	tx, err := reader.NewTx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	for name, r := range map[string][2]uint32{
		"beyond latest":   {c1.FirstLedger() + 100, c1.FirstLedger() + 200},
		"start above end": {c0.FirstLedger() + 2, c0.FirstLedger()},
		"in-window gap":   {c0.FirstLedger() + 4, c1.FirstLedger() - 1},
	} {
		seqs, _ := scanAll(t, tx, r[0], r[1])
		assert.Empty(t, seqs, name)
	}
}

func TestTxScanLedgers_EmptyStoreYieldsNothing(t *testing.T) {
	ctx, reader := emptyFixture(t)
	tx, err := reader.NewTx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	seqs, _ := scanAll(t, tx, 2, 1_000_000)
	assert.Empty(t, seqs)
}

func TestTxScanLedgers_StopsOnCanceledContext(t *testing.T) {
	r, first := sharedViewFixture(t)
	tx, err := NewLedgerReader().NewTx(viewCtx(t, r))
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	t.Run("mid-scan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var got []uint32
		var scanErr error
		for l, err := range tx.ScanLedgers(ctx, first, first+2) {
			if err != nil {
				scanErr = err
				break
			}
			got = append(got, l.Sequence)
			cancel() // the next step must observe it
		}
		assert.Equal(t, []uint32{first}, got, "the step that ran before cancel is delivered")
		assert.ErrorIs(t, scanErr, context.Canceled)
	})
	t.Run("before the first step", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for l, err := range tx.ScanLedgers(ctx, first, first+2) {
			assert.ErrorIs(t, err, context.Canceled)
			assert.Zero(t, l, "the RawLedger beside an error is zero")
		}
	})
}

// The Tx is a snapshot: a scan cut short by a break leaves a later scan on the
// same Tx answering from the same state.
func TestTxScanLedgers_EarlyBreakThenRescan(t *testing.T) {
	ctx, reader, c0, _ := sparseFixture(t)
	tx, err := reader.NewTx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	for l, err := range tx.ScanLedgers(context.Background(), c0.FirstLedger(), c0.FirstLedger()+3) {
		require.NoError(t, err)
		assert.Equal(t, c0.FirstLedger(), l.Sequence)
		break
	}
	seqs, _ := scanAll(t, tx, c0.FirstLedger()+1, c0.FirstLedger()+3)
	assert.Equal(t, seqRange(c0.FirstLedger()+1, c0.FirstLedger()+3), seqs)
}

func TestTxDone_IsIdempotent(t *testing.T) {
	ctx, reader, _, _ := sparseFixture(t)
	tx, err := reader.NewTx(ctx)
	require.NoError(t, err)
	assert.NoError(t, tx.Done())
	assert.NoError(t, tx.Done(), "a second Done must be a no-op, not a double release")
}

func TestTxScanLedgers_CrossesChunkBorder(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds a full 10k-ledger chunk")
	}
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	c0, c1 := testChunk, testChunk+1
	seedHotLedgers(t, cat, r, c0, seqRange(c0.FirstLedger(), c0.LastLedger())...)
	seedHotLedgers(t, cat, r, c1, c1.FirstLedger(), c1.FirstLedger()+1)
	r.SetLatestLedger(c1.FirstLedger()+1, query.CloseTimeAt(closeTimeFor(c1.FirstLedger()+1)))
	tx, err := NewLedgerReader().NewTx(viewCtx(t, r))
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	seqs, _ := scanAll(t, tx, c0.LastLedger()-1, c1.FirstLedger()+1)
	assert.Equal(t, seqRange(c0.LastLedger()-1, c1.FirstLedger()+1), seqs)
}

func TestLedgerScanLimitMatchesChunkSpan(t *testing.T) {
	assert.Equal(t, methods.LedgerScanLimit, int(chunk.LedgersPerChunk),
		"the per-request scan bound has one value (chunk.LedgersPerChunk); "+
			"methods.LedgerScanLimit cannot derive from it (shared v1 code), so it is pinned here")
}

// TestGetLedgerRange_SeededWindowReadsNoLedgers is the standing guard on the
// close-time path: once both edges are stamped, a served daemon that is not
// ingesting must answer getLedgerRange without touching a ledger at all. Each
// read it does take costs a whole decompressed ledger to recover eight bytes,
// so a regression here is not a small one.
//
// Proved by the allocation budget rather than by breaking the store: a point
// read decompresses a whole ledger, so a served range that stays under a couple
// of kilobytes per call did not take one. The numbers are asserted too, which is
// what rules out answering cheaply by answering wrongly.
func TestGetLedgerRange_SeededWindowReadsNoLedgers(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	seedHotLedgers(t, cat, r, testChunk, testChunk.FirstLedger(), testChunk.FirstLedger()+1)
	r.SetLatestLedger(testChunk.FirstLedger()+1, query.UnknownCloseTime())
	reader := NewLedgerReader()

	// Seeding is the one read per edge the daemon pays before serving.
	require.NoError(t, SeedCloseTimes(r))

	want := store.LedgerRange{
		FirstLedger: store.LedgerInfo{
			Sequence: testChunk.FirstLedger(), CloseTime: closeTimeFor(testChunk.FirstLedger()),
		},
		LastLedger: store.LedgerInfo{
			Sequence: testChunk.FirstLedger() + 1, CloseTime: closeTimeFor(testChunk.FirstLedger() + 1),
		},
	}
	ctx := viewCtx(t, r)
	got, err := reader.GetLedgerRange(ctx)
	require.NoError(t, err)
	assert.Equal(t, want, got, "seeded stamps must answer with the real close times")

	// Both edges come from stamps, so nothing is read: a served range costs
	// about nothing per call.
	perCall := allocBytesPerRun(t, 200, func() {
		if _, err := reader.GetLedgerRange(ctx); err != nil {
			t.Error(err)
		}
	})
	assert.Less(t, perCall, uint64(2048),
		"a seeded GetLedgerRange allocated %d bytes per call; it should not be reading ledgers", perCall)
}

func TestWithLedgerRaw_LendsTheSameBytesGetLedgerDecodes(t *testing.T) {
	ctx, reader, c0, _ := sparseFixture(t)
	lcm, ok, err := store.GetLedger(ctx, reader, c0.FirstLedger())
	require.NoError(t, err)
	require.True(t, ok)
	want, err := lcm.MarshalBinary()
	require.NoError(t, err)

	var got []byte
	found, err := store.WithLedgerRaw(ctx, reader, c0.FirstLedger(), func(raw []byte) error {
		got = bytes.Clone(raw) // the loan forbids retaining raw
		return nil
	})
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, want, got)
}

func TestWithLedgerRaw_MissDoesNotRunFn(t *testing.T) {
	ctx, reader, c0, c1 := sparseFixture(t)
	// below the floor, in an in-window gap, and above latest
	for _, seq := range []uint32{c0.FirstLedger() - 1, c0.FirstLedger() + 10, c1.FirstLedger() + 1} {
		ran := false
		found, err := store.WithLedgerRaw(ctx, reader, seq, func([]byte) error {
			ran = true
			return nil
		})
		assert.NoError(t, err, "ledger %d", seq)
		assert.False(t, found, "ledger %d", seq)
		assert.False(t, ran, "fn must not run for an absent ledger %d", seq)
	}
}

func TestWithLedgerRaw_CallbackErrorSurfacesAsFound(t *testing.T) {
	ctx, reader, c0, _ := sparseFixture(t)
	boom := errors.New("boom")
	// found stays true: the ledger WAS there, the caller's own callback failed.
	found, err := store.WithLedgerRaw(ctx, reader, c0.FirstLedger(), func([]byte) error { return boom })
	assert.ErrorIs(t, err, boom)
	assert.True(t, found)
}
