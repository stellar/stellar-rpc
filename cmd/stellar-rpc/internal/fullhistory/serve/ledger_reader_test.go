package serve

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
)

// The seam fixture: a frozen cold chunk 0 covering ledgers [coldFirst, coldLast]
// and a live hot chunk 1 covering [hotFirst, hotLast], contiguous across the
// chunk-0/chunk-1 boundary. earliest_ledger is pinned to coldFirst so the
// servable floor lands inside the (partial) cold pack — a real frozen chunk
// spans its whole range, but the POC fixture keeps packs tiny.
const (
	coldFirst = 9999
	coldLast  = 10001 // chunk 0's last ledger
	hotFirst  = 10002 // chunk 1's first ledger
	hotLast   = 10003
)

// lcmWithCloseTime is ZeroTxLCMBytes with a caller-chosen close time, so a
// GetLedgerRange test can prove the close time was decoded from the served bytes.
func lcmWithCloseTime(t *testing.T, seq uint32, closeTime int64) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTime)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: nil},
			},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// writeColdLedgerRun writes a real cold ledger pack at c's layout path holding
// the contiguous run raws[i] at firstSeq+i.
func writeColdLedgerRun(t *testing.T, layout geometry.Layout, c chunk.ID, firstSeq uint32, raws [][]byte) {
	t.Helper()
	path := layout.LedgerPackPath(c)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	w, err := ledger.NewColdWriter(path, firstSeq, ledger.ColdWriterOptions{})
	require.NoError(t, err)
	for i, raw := range raws {
		require.NoError(t, w.AppendLedger(firstSeq+uint32(i), raw))
	}
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())
}

// buildSeamReader wires the cold+hot seam fixture and returns a reader over it
// plus the exact cold bytes keyed by sequence (so a read can be proven to have
// come from the cold pack, byte-for-byte).
func buildSeamReader(t *testing.T) (db.LedgerReader, map[uint32][]byte) {
	t.Helper()
	cat, layout := newTestCatalog(t)
	require.NoError(t, cat.PinEarliestLedger(coldFirst))

	cold := map[uint32][]byte{}
	var raws [][]byte
	for seq := uint32(coldFirst); seq <= coldLast; seq++ {
		b := lcmWithCloseTime(t, seq, int64(seq)*10)
		cold[seq] = b
		raws = append(raws, b)
	}
	writeColdLedgerRun(t, layout, chunk.ID(0), coldFirst, raws)
	freezeLedgers(t, cat, chunk.ID(0))

	db1 := openHotChunk(t, cat, layout, chunk.ID(1), hotFirst, hotLast)
	t.Cleanup(func() { _ = db1.Close() })

	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(hotLast))
	r.HotOpened(chunk.ID(1), db1)

	return NewLedgerReader(r, layout), cold
}

func TestLedgerReader_BatchGetLedgers_ColdHotSeam(t *testing.T) {
	lr, cold := buildSeamReader(t)
	tx, err := lr.NewTx(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	got, err := tx.BatchGetLedgers(t.Context(), coldFirst, hotLast)
	require.NoError(t, err)
	require.Len(t, got, hotLast-coldFirst+1, "full contiguous run across the seam")

	for i, mc := range got {
		wantSeq := uint32(coldFirst + i)
		assert.EqualValues(t, wantSeq, mc.Header.Header.LedgerSeq, "header sequence in order")

		// Decode the retained Lcm bytes independently: catches a missing copy of
		// the borrowed iterator buffer (which would alias every entry to the last).
		var lcm xdr.LedgerCloseMeta
		require.NoError(t, lcm.UnmarshalBinary(mc.Lcm))
		assert.Equal(t, wantSeq, lcm.LedgerSequence(), "retained Lcm decodes to its own sequence")

		if want, ok := cold[wantSeq]; ok {
			assert.Equal(t, want, mc.Lcm, "cold ledgers served byte-for-byte from the pack")
		}
	}
}

func TestLedgerReader_BatchGetLedgers_BelowFloorErrors(t *testing.T) {
	lr, _ := buildSeamReader(t)
	tx, err := lr.NewTx(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	_, err = tx.BatchGetLedgers(t.Context(), coldFirst-1, hotLast)
	require.ErrorIs(t, err, stores.ErrOutOfRange)
}

func TestLedgerReader_BatchGetLedgers_EndClampsToLatest(t *testing.T) {
	lr, _ := buildSeamReader(t)
	tx, err := lr.NewTx(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	got, err := tx.BatchGetLedgers(t.Context(), coldFirst, hotLast+50_000)
	require.NoError(t, err)
	require.Len(t, got, hotLast-coldFirst+1, "end clamps to latest")
	assert.EqualValues(t, hotLast, got[len(got)-1].Header.Header.LedgerSeq)
}

func TestLedgerReader_GetLedgerRange_EndpointsAndCloseTimes(t *testing.T) {
	lr, _ := buildSeamReader(t)

	lrange, err := lr.GetLedgerRange(t.Context())
	require.NoError(t, err)

	assert.EqualValues(t, coldFirst, lrange.FirstLedger.Sequence)
	assert.Equal(t, int64(coldFirst)*10, lrange.FirstLedger.CloseTime, "first close time decoded from cold bytes")
	assert.EqualValues(t, hotLast, lrange.LastLedger.Sequence)
	assert.EqualValues(t, 0, lrange.LastLedger.CloseTime, "hot ZeroTx ledgers carry close time 0")
}

func TestLedgerReader_GetLedger_BoundsAndFound(t *testing.T) {
	lr, _ := buildSeamReader(t)
	tx, err := lr.NewTx(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Done() }()

	// Below the floor is a not-found, not an error (matches v1 GetLedger).
	_, found, err := tx.GetLedger(t.Context(), coldFirst-1)
	require.NoError(t, err)
	assert.False(t, found, "below floor is not-found")

	// Above latest is likewise a not-found.
	_, found, err = tx.GetLedger(t.Context(), hotLast+1)
	require.NoError(t, err)
	assert.False(t, found, "above latest is not-found")

	// A cold-backed and a hot-backed hit both resolve.
	for _, seq := range []uint32{coldFirst, coldLast, hotFirst, hotLast} {
		lcm, found, err := tx.GetLedger(t.Context(), seq)
		require.NoError(t, err)
		require.True(t, found, "seq %d found", seq)
		assert.Equal(t, seq, lcm.LedgerSequence())
	}
}

func TestLedgerReader_GetLedgerRange_EmptyIsErrEmptyDB(t *testing.T) {
	cat, layout := newTestCatalog(t)
	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(0))
	lr := NewLedgerReader(r, layout)

	_, err := lr.GetLedgerRange(t.Context())
	require.ErrorIs(t, err, db.ErrEmptyDB)

	_, err = lr.GetLatestLedgerSequence(t.Context())
	require.ErrorIs(t, err, db.ErrEmptyDB)
}

func TestLedgerReader_UnsupportedPOCMethods(t *testing.T) {
	lr, _ := buildSeamReader(t)
	require.Error(t, lr.StreamAllLedgers(t.Context(), nil))
	require.Error(t, lr.StreamLedgerRange(t.Context(), coldFirst, hotLast, nil))
	_, _, _, err := lr.GetLedgerCountInRange(t.Context(), coldFirst, hotLast)
	require.Error(t, err)
}

// Step 4 integration: the real getLedgers handler, mounted over the adapter,
// returns a contiguous GetLedgersResponse across the cold->hot seam.
func TestLedgerReader_GetLedgersHandler_AcrossSeam(t *testing.T) {
	lr, _ := buildSeamReader(t)
	h := methods.NewGetLedgersHandler(lr, 200, 50, nil, silentLogger())

	requests, err := jrpc2.ParseRequests([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"getLedgers","params":{"startLedger":9999,"pagination":{"limit":10}}}`,
	))
	require.NoError(t, err)
	require.Len(t, requests, 1)

	res, err := h(t.Context(), requests[0].ToRequest())
	require.NoError(t, err)

	resp, ok := res.(protocol.GetLedgersResponse)
	require.True(t, ok, "handler returns a GetLedgersResponse")
	assert.EqualValues(t, hotLast, resp.LatestLedger)
	assert.EqualValues(t, coldFirst, resp.OldestLedger)
	require.Len(t, resp.Ledgers, hotLast-coldFirst+1)
	for i, li := range resp.Ledgers {
		assert.EqualValues(t, coldFirst+i, li.Sequence, "contiguous across the seam")
	}
}
