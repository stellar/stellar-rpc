package methods

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

const (
	expectedLatestLedgerSequence        uint32        = 960
	expectedLatestLedgerProtocolVersion uint32        = 20
	expectedLatestLedgerHashBytes       byte          = 42
	expectedLatestLedgerCloseTime       xdr.TimePoint = 125
)

type ConstantLedgerReader struct{}

func (ledgerReader *ConstantLedgerReader) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	return expectedLatestLedgerSequence, nil
}

func (ledgerReader *ConstantLedgerReader) GetLedgerRange(_ context.Context) (store.LedgerRange, error) {
	return store.LedgerRange{}, nil
}

func (ledgerReader *ConstantLedgerReader) NewTx(_ context.Context) (store.LedgerReaderTx, error) {
	return nil, errors.New("mock NewTx error")
}

func (ledgerReader *ConstantLedgerReader) GetLedger(_ context.Context,
	sequence uint32,
) (xdr.LedgerCloseMeta, bool, error) {
	return createLedger(expectedLatestLedgerHashBytes,
			sequence,
			expectedLatestLedgerCloseTime),
		true, nil
}

func (ledgerReader *ConstantLedgerReader) WithLedgerRaw(
	_ context.Context, sequence uint32, fn store.WithLedgerRawFn,
) (bool, error) {
	lcm := createLedger(expectedLatestLedgerHashBytes,
		sequence,
		expectedLatestLedgerCloseTime)
	raw, err := lcm.MarshalBinary()
	if err != nil {
		return false, err
	}
	return true, fn(raw)
}

func (ledgerReader *ConstantLedgerReader) StreamLedgerRange(
	_ context.Context,
	_ uint32,
	_ uint32,
	_ store.StreamLedgerFn,
) error {
	return nil
}

// memoLedgerReader is ConstantLedgerReader with a movable latest sequence,
// injectable errors, and a raw-read counter that shows when the memo is bypassed.
type memoLedgerReader struct {
	*ConstantLedgerReader

	latest   atomic.Uint32
	rawReads atomic.Int32
	seqErr   error
	rawErr   error
	raw      []byte // when set, served for every sequence instead of a fresh marshal
}

func newMemoLedgerReader(latest uint32) *memoLedgerReader {
	r := &memoLedgerReader{ConstantLedgerReader: &ConstantLedgerReader{}}
	r.latest.Store(latest)
	return r
}

func (r *memoLedgerReader) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	if r.seqErr != nil {
		return 0, r.seqErr
	}
	return r.latest.Load(), nil
}

func (r *memoLedgerReader) WithLedgerRaw(
	ctx context.Context, sequence uint32, fn store.WithLedgerRawFn,
) (bool, error) {
	r.rawReads.Add(1)
	if r.rawErr != nil {
		return false, r.rawErr
	}
	if r.raw != nil {
		return true, fn(r.raw)
	}
	return r.ConstantLedgerReader.WithLedgerRaw(ctx, sequence, fn)
}

func MakeTxSet() xdr.GeneralizedTransactionSet {
	txset := xdr.GeneralizedTransactionSet{
		V: 1,
		V1TxSet: &xdr.TransactionSetV1{
			PreviousLedgerHash: xdr.Hash{},
			Phases:             nil,
		},
	}
	return txset
}

func MakeLedgerHeader(ledgerSequence uint32, protocolVersion uint32, closeTime xdr.TimePoint) xdr.LedgerHeader {
	header := xdr.LedgerHeader{
		LedgerSeq:     xdr.Uint32(ledgerSequence),
		LedgerVersion: xdr.Uint32(protocolVersion),
		ScpValue: xdr.StellarValue{
			CloseTime: closeTime,
			TxSetHash: xdr.Hash{},
			Upgrades:  nil,
		},
	}
	return header
}

func createLedger(hash byte, ledgerSeq uint32, closeTime xdr.TimePoint) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash:   xdr.Hash{hash},
				Header: MakeLedgerHeader(ledgerSeq, expectedLatestLedgerProtocolVersion, closeTime),
			},
			TxSet:        MakeTxSet(), // minimal empty
			TxProcessing: nil,
			Ext:          xdr.LedgerCloseMetaExt{},
		},
	}
}

// callGetLatestLedger runs the handler and returns the rendered result bytes.
func callGetLatestLedger(t *testing.T, h jrpc2.Handler) json.RawMessage {
	t.Helper()
	respI, err := h(t.Context(), &jrpc2.Request{})
	require.NoError(t, err)
	body, ok := respI.(json.RawMessage)
	require.True(t, ok, "getLatestLedger must return pre-rendered json.RawMessage, got %T", respI)
	return body
}

func decodeLatestLedger(t *testing.T, body json.RawMessage) protocol.GetLatestLedgerResponse {
	t.Helper()
	var resp protocol.GetLatestLedgerResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	return resp
}

func TestGetLatestLedger(t *testing.T) {
	getLatestLedgerHandler := NewGetLatestLedgerHandler(&ConstantLedgerReader{})
	latestLedgerResp := decodeLatestLedger(t, callGetLatestLedger(t, getLatestLedgerHandler))

	expectedLedger := createLedger(expectedLatestLedgerHashBytes,
		expectedLatestLedgerSequence,
		expectedLatestLedgerCloseTime)

	var receivedHeader xdr.LedgerHeader
	err := xdr.SafeUnmarshalBase64(latestLedgerResp.LedgerHeader, &receivedHeader)
	require.NoError(t, err, "error unmarshaling received ledger header: %v", err)
	var receivedMetadata xdr.LedgerCloseMeta
	err = xdr.SafeUnmarshalBase64(latestLedgerResp.LedgerMetadata, &receivedMetadata)
	require.NoError(t, err, "error unmarshaling received ledger metadata: %v", err)

	assert.Equal(t, expectedLedger.LedgerHash().HexString(), latestLedgerResp.Hash)
	// Header check ensures sequence, protocol version, and close time match
	assert.Equal(t, expectedLedger.V1.LedgerHeader.Header, receivedHeader)
	assert.Equal(t, expectedLedger, receivedMetadata)
}

// TestGetLatestLedgerAcceptsEmptyParams verifies that getLatestLedger accepts
// requests with empty params objects, fixing https://github.com/stellar/stellar-rpc/issues/551
func TestGetLatestLedgerAcceptsEmptyParams(t *testing.T) {
	getLatestLedgerHandler := NewGetLatestLedgerHandler(&ConstantLedgerReader{})

	// Test with empty params object: params: {}
	emptyParamsRequest := `{
"jsonrpc": "2.0",
"id": 1,
"method": "getLatestLedger",
"params": {}
}`
	requests, err := jrpc2.ParseRequests([]byte(emptyParamsRequest))
	require.NoError(t, err)
	require.Len(t, requests, 1)

	latestLedgerRespI, err := getLatestLedgerHandler(t.Context(), requests[0].ToRequest())
	require.NoError(t, err, "getLatestLedger should accept empty params object")
	body, ok := latestLedgerRespI.(json.RawMessage)
	require.True(t, ok)
	assert.Equal(t, expectedLatestLedgerSequence, decodeLatestLedger(t, body).Sequence)
}

// The rendered bytes must be exactly what marshaling the response struct gives,
// since the dispatcher writes them to the wire verbatim.
func TestGetLatestLedgerRenderMatchesStructMarshal(t *testing.T) {
	body := callGetLatestLedger(t, NewGetLatestLedgerHandler(&ConstantLedgerReader{}))

	raw, err := createLedger(expectedLatestLedgerHashBytes,
		expectedLatestLedgerSequence, expectedLatestLedgerCloseTime).MarshalBinary()
	require.NoError(t, err)
	expected, err := latestLedgerResponse(xdr.LedgerCloseMetaView(raw), expectedLatestLedgerSequence)
	require.NoError(t, err)
	expectedJSON, err := json.Marshal(expected)
	require.NoError(t, err)
	assert.Equal(t, expectedJSON, []byte(body)) //nolint:testifylint // byte parity is the point, not JSON equality
}

func TestGetLatestLedgerServesMemoUntilLedgerAdvances(t *testing.T) {
	reader := newMemoLedgerReader(expectedLatestLedgerSequence)
	h := NewGetLatestLedgerHandler(reader)

	first := callGetLatestLedger(t, h)
	second := callGetLatestLedger(t, h)
	assert.Equal(t, int32(1), reader.rawReads.Load(), "second request must be served from the memo")
	assert.True(t, bytes.Equal(first, second))
	assert.Equal(t, expectedLatestLedgerSequence, decodeLatestLedger(t, second).Sequence)

	reader.latest.Store(expectedLatestLedgerSequence + 1)
	third := callGetLatestLedger(t, h)
	assert.Equal(t, int32(2), reader.rawReads.Load(), "a new latest ledger must re-render")
	assert.Equal(t, expectedLatestLedgerSequence+1, decodeLatestLedger(t, third).Sequence)

	callGetLatestLedger(t, h)
	assert.Equal(t, int32(2), reader.rawReads.Load())
}

// A request whose read view predates the newest render gets its own ledger but
// must not replace the memo with the older one.
func TestGetLatestLedgerOlderViewDoesNotEvictNewerRender(t *testing.T) {
	reader := newMemoLedgerReader(expectedLatestLedgerSequence + 1)
	h := NewGetLatestLedgerHandler(reader)
	callGetLatestLedger(t, h)
	require.Equal(t, int32(1), reader.rawReads.Load())

	reader.latest.Store(expectedLatestLedgerSequence)
	older := callGetLatestLedger(t, h)
	assert.Equal(t, expectedLatestLedgerSequence, decodeLatestLedger(t, older).Sequence)
	assert.Equal(t, int32(2), reader.rawReads.Load())

	reader.latest.Store(expectedLatestLedgerSequence + 1)
	newer := callGetLatestLedger(t, h)
	assert.Equal(t, expectedLatestLedgerSequence+1, decodeLatestLedger(t, newer).Sequence)
	assert.Equal(t, int32(2), reader.rawReads.Load(), "the newer render must still be memoized")
}

func TestGetLatestLedgerConcurrentMissRendersOnce(t *testing.T) {
	reader := newMemoLedgerReader(expectedLatestLedgerSequence)
	h := NewGetLatestLedgerHandler(reader)

	const callers = 32
	bodies := make([]json.RawMessage, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			respI, err := h(t.Context(), &jrpc2.Request{})
			assert.NoError(t, err)
			body, ok := respI.(json.RawMessage)
			assert.True(t, ok)
			bodies[i] = body
		})
	}
	wg.Wait()

	assert.Equal(t, int32(1), reader.rawReads.Load(), "concurrent misses on one ledger must render it once")
	for _, body := range bodies[1:] {
		assert.True(t, bytes.Equal(bodies[0], body))
	}
}

func TestGetLatestLedgerErrorsAreNotMemoized(t *testing.T) {
	reader := newMemoLedgerReader(expectedLatestLedgerSequence)
	h := NewGetLatestLedgerHandler(reader)

	reader.seqErr = errors.New("boom")
	_, err := h(t.Context(), &jrpc2.Request{})
	require.ErrorContains(t, err, "could not get latest ledger sequence")
	reader.seqErr = nil

	reader.rawErr = errors.New("disk")
	_, err = h(t.Context(), &jrpc2.Request{})
	require.ErrorContains(t, err, "could not get latest ledger: disk")
	reader.rawErr = nil

	assert.Equal(t, expectedLatestLedgerSequence, decodeLatestLedger(t, callGetLatestLedger(t, h)).Sequence)
	assert.Equal(t, int32(2), reader.rawReads.Load())
}

// paddedLedger is a ledger whose marshaled size is about targetBytes, padded
// with ManageData-heavy transactions. pubnet ledgers ran ~2.1 MB median in
// September 2026.
func paddedLedger(tb testing.TB, seq uint32, targetBytes int) []byte {
	tb.Helper()
	const opsPerTx = 100
	ops := make([]xdr.Operation, 0, opsPerTx)
	for i := range opsPerTx {
		value := xdr.DataValue(bytes.Repeat([]byte{byte(i)}, 64))
		ops = append(ops, xdr.Operation{Body: xdr.OperationBody{
			Type: xdr.OperationTypeManageData,
			ManageDataOp: &xdr.ManageDataOp{
				DataName:  xdr.String64("padding-key-with-a-realistic-length"),
				DataValue: &value,
			},
		}})
	}
	envelope, err := xdr.NewTransactionEnvelope(xdr.EnvelopeTypeEnvelopeTypeTx, xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			SourceAccount: xdr.MustMuxedAddress("GA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVSGZ"),
			Fee:           100 * opsPerTx,
			Operations:    ops,
		},
	})
	require.NoError(tb, err)
	txSize, err := envelope.MarshalBinary()
	require.NoError(tb, err)

	txs := make([]xdr.TransactionEnvelope, targetBytes/len(txSize))
	for i := range txs {
		txs[i] = envelope
	}
	components := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: txs,
		},
	}}
	lcm := createLedger(expectedLatestLedgerHashBytes, seq, expectedLatestLedgerCloseTime)
	lcm.V1.TxSet = xdr.GeneralizedTransactionSet{
		V: 1,
		V1TxSet: &xdr.TransactionSetV1{
			Phases: []xdr.TransactionPhase{{V: 0, V0Components: &components}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(tb, err)
	return raw
}

// BenchmarkGetLatestLedger measures one request's handler-side work on a
// pubnet-sized ledger, rendered the way the jsonrpc dispatcher sends it (a
// json.RawMessage goes out verbatim). "uncached" rebuilds the handler per
// iteration, which is a memo miss; "cached" is the steady state between
// ledger closes.
func BenchmarkGetLatestLedger(b *testing.B) {
	reader := newMemoLedgerReader(expectedLatestLedgerSequence)
	reader.raw = paddedLedger(b, expectedLatestLedgerSequence, 2_100_000)
	req := &jrpc2.Request{}

	serve := func(b *testing.B, h jrpc2.Handler) {
		b.Helper()
		respI, err := h(b.Context(), req)
		if err != nil {
			b.Fatal(err)
		}
		wire, ok := respI.(json.RawMessage)
		if !ok {
			b.Fatalf("unexpected result type %T", respI)
		}
		b.SetBytes(int64(len(wire)))
	}

	b.Run("uncached", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			serve(b, NewGetLatestLedgerHandler(reader))
		}
	})
	b.Run("cached", func(b *testing.B) {
		h := NewGetLatestLedgerHandler(reader)
		b.ReportAllocs()
		for b.Loop() {
			serve(b, h)
		}
	})
}
