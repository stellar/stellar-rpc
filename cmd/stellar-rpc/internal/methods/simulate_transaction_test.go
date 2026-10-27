package methods

import (
	"context"
	"encoding"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/gxdr"
	"github.com/stellar/go-stellar-sdk/ingest"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/randxdr"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/preflight"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

//nolint:funlen
func TestLedgerEntryChange(t *testing.T) {
	entry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 100,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId: xdr.MustAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON"),
				Balance:   100,
				SeqNum:    1,
			},
		},
	}

	updatedEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 101,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId: xdr.MustAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON"),
				Balance:   200,
				SeqNum:    2,
			},
		},
	}

	entryXDR, err := entry.MarshalBinary()
	require.NoError(t, err)
	entryB64 := base64.StdEncoding.EncodeToString(entryXDR)

	updatedEntryXDR, err := updatedEntry.MarshalBinary()
	require.NoError(t, err)
	updatedEntryB64 := base64.StdEncoding.EncodeToString(updatedEntryXDR)

	key, err := entry.LedgerKey()
	require.NoError(t, err)
	keyXDR, err := key.MarshalBinary()
	require.NoError(t, err)
	keyB64 := base64.StdEncoding.EncodeToString(keyXDR)

	keyJs, err := xdr2json.ConvertInterface(key)
	require.NoError(t, err)
	entryJs, err := xdr2json.ConvertInterface(entry)
	require.NoError(t, err)
	updatedEntryJs, err := xdr2json.ConvertInterface(updatedEntry)
	require.NoError(t, err)

	for _, test := range []struct {
		name            string
		input           preflight.XDRDiff
		expectedOutput  protocol.LedgerEntryChange
		expectedBeforeJ json.RawMessage
		expectedAfterJ  json.RawMessage
	}{
		{
			name: "creation",
			input: preflight.XDRDiff{
				Before: nil,
				After:  entryXDR,
			},
			expectedOutput: protocol.LedgerEntryChange{
				Type:      protocol.LedgerEntryChangeTypeCreated,
				KeyXDR:    keyB64,
				BeforeXDR: nil,
				AfterXDR:  &entryB64,
			},
			expectedBeforeJ: nil,
			expectedAfterJ:  entryJs,
		},
		{
			name: "deletion",
			input: preflight.XDRDiff{
				Before: entryXDR,
				After:  nil,
			},
			expectedOutput: protocol.LedgerEntryChange{
				Type:      protocol.LedgerEntryChangeTypeDeleted,
				KeyXDR:    keyB64,
				BeforeXDR: &entryB64,
				AfterXDR:  nil,
			},
			expectedBeforeJ: entryJs,
			expectedAfterJ:  nil,
		},
		{
			name: "update",
			input: preflight.XDRDiff{
				Before: entryXDR,
				After:  updatedEntryXDR,
			},
			expectedOutput: protocol.LedgerEntryChange{
				Type:      protocol.LedgerEntryChangeTypeUpdated,
				KeyXDR:    keyB64,
				BeforeXDR: &entryB64,
				AfterXDR:  &updatedEntryB64,
			},
			expectedBeforeJ: entryJs,
			expectedAfterJ:  updatedEntryJs,
		},
	} {
		var change protocol.LedgerEntryChange
		change, err := LedgerEntryChangeFromXDRDiff(test.input, "", nil)
		require.NoError(t, err, test.name)
		require.Equal(t, test.expectedOutput, change)

		// test json roundtrip
		changeJSON, err := json.Marshal(change)
		require.NoError(t, err, test.name)
		var change2 protocol.LedgerEntryChange
		require.NoError(t, json.Unmarshal(changeJSON, &change2))
		require.Equal(t, change, change2, test.name)

		// test JSON output
		changeJs, err := LedgerEntryChangeFromXDRDiff(test.input, protocol.FormatJSON, nil)
		require.NoError(t, err, test.name)

		require.Equal(t, keyJs, changeJs.KeyJSON, test.name)
		require.Equal(t, test.expectedBeforeJ, changeJs.BeforeJSON, test.name)
		require.Equal(t, test.expectedAfterJ, changeJs.AfterJSON, test.name)
	}

	// Check the error case
	change, err := LedgerEntryChangeFromXDRDiff(preflight.XDRDiff{
		Before: nil,
		After:  nil,
	}, "", nil)
	require.ErrorIs(t, err, errMissingDiff)
	require.Equal(t, protocol.LedgerEntryChange{}, change)
}

// TestReadSimulationInput checks the envelope parts read from the XDR against
// the same parts of the decoded envelope.
func TestReadSimulationInput(t *testing.T) {
	marshal := func(v encoding.BinaryMarshaler) []byte {
		b, err := v.MarshalBinary()
		require.NoError(t, err)
		return b
	}
	gen := randxdr.NewGenerator()
	envelopeTypes := map[xdr.EnvelopeType]bool{}
	for range 1000 {
		shape := &gxdr.TransactionEnvelope{}
		gen.Next(shape, randxdr.LedgerCloseMetaPresets)
		var envelope xdr.TransactionEnvelope
		require.NoError(t, gxdr.Convert(shape, &envelope))
		envelopeTypes[envelope.Type] = true

		in, err := readSimulationInput(base64.StdEncoding.EncodeToString(marshal(envelope)))
		require.NoError(t, err)

		ops := envelope.Operations()
		require.Len(t, ops, in.opCount)
		require.Equal(t, envelope.Memo().Type, in.memoType)
		var footprint []byte
		if data, ok := (&ingest.LedgerTransaction{Envelope: envelope}).GetSorobanData(); ok {
			footprint = marshal(data.Resources.Footprint)
		}
		require.Equal(t, footprint, []byte(in.footprint))
		if len(ops) == 0 {
			continue
		}
		op := ops[0]
		require.Equal(t, marshal(op.Body), []byte(in.opBody))
		require.Equal(t, op.Body.Type, in.opType)
		require.Equal(t, op.Body.Type == xdr.OperationTypeInvokeHostFunction &&
			len(op.Body.MustInvokeHostFunctionOp().Auth) > 0, in.hasAuth)
		source := envelope.SourceAccount()
		if op.SourceAccount != nil {
			source = *op.SourceAccount
		}
		require.Equal(t, marshal(source.ToAccountId()), []byte(in.source))
	}
	require.Len(t, envelopeTypes, 3)
}

func TestReadSimulationInputLargeArgument(t *testing.T) {
	envelope := invokeHostFunctionEnvelope(t)
	vec := make(xdr.ScVec, 6000)
	for i := range vec {
		u := xdr.Uint32(i)
		vec[i] = xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
	}
	args := &vec
	envelope.V1.Tx.Operations[0].Body.InvokeHostFunctionOp.HostFunction.InvokeContract.Args = []xdr.ScVal{
		{Type: xdr.ScValTypeScvVec, Vec: &args},
	}
	envelopeB64, err := xdr.MarshalBase64(envelope)
	require.NoError(t, err)

	_, err = readSimulationInput(envelopeB64)
	require.NoError(t, err)
}

func TestSimulateTransactionRejectsHugeDeclaredCountsCheaply(t *testing.T) {
	request := jsonRPCRequest(t, protocol.SimulateTransactionMethodName,
		protocol.SimulateTransactionRequest{Transaction: hugeCountEnvelope(t)})
	handler := NewSimulateTransactionHandler(quietLogger(), &MockLedgerReader{}, nil, panicPreflightGetter{})

	requireCheapRejection(t, func() {
		resp, err := handler(t.Context(), request)
		require.NoError(t, err)
		require.Equal(t, protocol.SimulateTransactionResponse{Error: "Could not unmarshal transaction"}, resp)
	})
}

type panicPreflightGetter struct{}

func (panicPreflightGetter) GetPreflight(
	context.Context,
	preflight.GetterParameters,
) (preflight.Preflight, error) {
	panic("unexpected GetPreflight call")
}

type recordingPreflightGetter struct {
	called bool
	params preflight.GetterParameters
	result preflight.Preflight
	err    error
}

func (g *recordingPreflightGetter) GetPreflight(
	_ context.Context,
	params preflight.GetterParameters,
) (preflight.Preflight, error) {
	g.called = true
	g.params = params
	return g.result, g.err
}

func TestSimulateTransactionFeeBumpMissingSorobanData(t *testing.T) {
	logger := log.New()
	ledgerReader := &MockLedgerReader{}
	handler := NewSimulateTransactionHandler(logger, ledgerReader, nil, panicPreflightGetter{})

	txEnvelope := feeBumpExtendFootprintMissingSorobanData(t)
	txB64, err := xdr.MarshalBase64(txEnvelope)
	require.NoError(t, err)

	requestJSON := fmt.Sprintf(`{
"jsonrpc": "2.0",
"id": 1,
"method": "simulateTransaction",
"params": { "transaction": "%s" }
}`, txB64)
	requests, err := jrpc2.ParseRequests([]byte(requestJSON))
	require.NoError(t, err)
	require.Len(t, requests, 1)

	resp, err := handler(t.Context(), requests[0].ToRequest())
	require.NoError(t, err)

	simResp, ok := resp.(protocol.SimulateTransactionResponse)
	require.True(t, ok)
	require.Equal(t,
		"To perform a SimulateTransaction for ExtendFootprintTtl or RestoreFootprint operations,"+
			" SorobanTransactionData must be provided",
		simResp.Error,
	)
}

func TestSimulateTransactionPassesLedgerTimeFromLatestLedgerMetaToPreflight(t *testing.T) {
	logger := log.New()
	ledgerReader := &MockLedgerReader{}
	getter := &recordingPreflightGetter{
		result: preflight.Preflight{
			MinFee: 123,
		},
	}
	handler := NewSimulateTransactionHandler(logger, ledgerReader, nil, getter)

	ledgerReader.
		On("GetLatestLedgerSequence", mock.Anything).
		Return(uint32(77), nil).
		Once()
	ledgerReader.
		On("GetLedger", mock.Anything, uint32(77)).
		Return(createLedger(expectedLatestLedgerHashBytes, 77, xdr.TimePoint(1_700_000_123)), true, nil).
		Once()

	txEnvelope := invokeHostFunctionEnvelope(t)
	txB64, err := xdr.MarshalBase64(txEnvelope)
	require.NoError(t, err)

	requestJSON := fmt.Sprintf(`{
"jsonrpc": "2.0",
"id": 1,
"method": "simulateTransaction",
"params": { "transaction": "%s" }
}`, txB64)
	requests, err := jrpc2.ParseRequests([]byte(requestJSON))
	require.NoError(t, err)
	require.Len(t, requests, 1)

	resp, err := handler(t.Context(), requests[0].ToRequest())
	require.NoError(t, err)

	simResp, ok := resp.(protocol.SimulateTransactionResponse)
	require.True(t, ok)
	require.Empty(t, simResp.Error)
	require.True(t, getter.called, "preflight getter should be called")
	require.Equal(t, uint32(77), getter.params.LedgerSeq)
	require.Equal(t, uint64(1_700_000_123), getter.params.LedgerTime)

	ledgerReader.AssertExpectations(t)
}

func TestSimulateTransactionReturnsErrorWhenLatestLedgerMetaFails(t *testing.T) {
	logger := log.New()
	ledgerReader := &MockLedgerReader{}
	handler := NewSimulateTransactionHandler(logger, ledgerReader, nil, panicPreflightGetter{})

	ledgerReader.
		On("GetLatestLedgerSequence", mock.Anything).
		Return(uint32(77), nil).
		Once()
	ledgerReader.
		On("GetLedger", mock.Anything, uint32(77)).
		Return(xdr.LedgerCloseMeta{}, false, errors.New("latest ledger meta lookup failed")).
		Once()

	txEnvelope := invokeHostFunctionEnvelope(t)
	txB64, err := xdr.MarshalBase64(txEnvelope)
	require.NoError(t, err)

	requestJSON := fmt.Sprintf(`{
"jsonrpc": "2.0",
"id": 1,
"method": "simulateTransaction",
"params": { "transaction": "%s" }
}`, txB64)
	requests, err := jrpc2.ParseRequests([]byte(requestJSON))
	require.NoError(t, err)
	require.Len(t, requests, 1)

	resp, err := handler(t.Context(), requests[0].ToRequest())
	require.NoError(t, err)

	simResp, ok := resp.(protocol.SimulateTransactionResponse)
	require.True(t, ok)
	require.Equal(t, "latest ledger meta lookup failed", simResp.Error)
	require.Equal(t, uint32(77), simResp.LatestLedger)

	ledgerReader.AssertExpectations(t)
}

// TestSimulateTransactionReturnsErrorWhenLatestLedgerMetaNotFound verifies the !ok
// path inside getLatestLedgerPreflightInfo: when WithLedgerRaw finds no ledger,
// meaning the ledger meta is absent (e.g. trimmed from the retention window right
// after GetLatestLedgerSequence returned), the handler must surface a descriptive
// error and still echo back the latest ledger sequence.
func TestSimulateTransactionReturnsErrorWhenLatestLedgerMetaNotFound(t *testing.T) {
	logger := log.New()
	ledgerReader := &MockLedgerReader{}
	handler := NewSimulateTransactionHandler(logger, ledgerReader, nil, panicPreflightGetter{})

	ledgerReader.
		On("GetLatestLedgerSequence", mock.Anything).
		Return(uint32(77), nil).
		Once()
	// ok=false, err=nil: meta is absent but the DB call itself succeeded.
	ledgerReader.
		On("GetLedger", mock.Anything, uint32(77)).
		Return(xdr.LedgerCloseMeta{}, false, nil).
		Once()

	txEnvelope := invokeHostFunctionEnvelope(t)
	txB64, err := xdr.MarshalBase64(txEnvelope)
	require.NoError(t, err)

	requestJSON := fmt.Sprintf(`{
"jsonrpc": "2.0",
"id": 1,
"method": "simulateTransaction",
"params": { "transaction": "%s" }
}`, txB64)
	requests, err := jrpc2.ParseRequests([]byte(requestJSON))
	require.NoError(t, err)
	require.Len(t, requests, 1)

	resp, err := handler(t.Context(), requests[0].ToRequest())
	require.NoError(t, err)

	simResp, ok := resp.(protocol.SimulateTransactionResponse)
	require.True(t, ok)
	require.Equal(t, "missing meta for latest ledger (77)", simResp.Error)
	require.Equal(t, uint32(77), simResp.LatestLedger)

	ledgerReader.AssertExpectations(t)
}

// TestSimulateTransactionPassesFieldsFromV2LedgerMetaToPreflight verifies that when
// the DB returns a V2 LedgerCloseMeta, getLatestLedgerPreflightInfo correctly
// extracts closeTime, protocolVersion, and bucketListSize from the V2 fields and
// passes them all to the preflight getter.
func TestSimulateTransactionPassesFieldsFromV2LedgerMetaToPreflight(t *testing.T) {
	logger := log.New()
	ledgerReader := &MockLedgerReader{}
	getter := &recordingPreflightGetter{
		result: preflight.Preflight{MinFee: 123},
	}
	handler := NewSimulateTransactionHandler(logger, ledgerReader, nil, getter)

	const (
		ledgerSeq       = uint32(99)
		protocolVersion = uint32(22)
		closeTime       = xdr.TimePoint(1_700_005_000)
		bucketListSize  = uint64(8192)
	)

	v2Meta := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq:     xdr.Uint32(ledgerSeq),
					LedgerVersion: xdr.Uint32(protocolVersion),
					ScpValue: xdr.StellarValue{
						CloseTime: closeTime,
					},
				},
			},
			TotalByteSizeOfLiveSorobanState: xdr.Uint64(bucketListSize),
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{},
			},
		},
	}

	ledgerReader.
		On("GetLatestLedgerSequence", mock.Anything).
		Return(ledgerSeq, nil).
		Once()
	ledgerReader.
		On("GetLedger", mock.Anything, ledgerSeq).
		Return(v2Meta, true, nil).
		Once()

	txEnvelope := invokeHostFunctionEnvelope(t)
	txB64, err := xdr.MarshalBase64(txEnvelope)
	require.NoError(t, err)

	requestJSON := fmt.Sprintf(`{
"jsonrpc": "2.0",
"id": 1,
"method": "simulateTransaction",
"params": { "transaction": "%s" }
}`, txB64)
	requests, err := jrpc2.ParseRequests([]byte(requestJSON))
	require.NoError(t, err)
	require.Len(t, requests, 1)

	resp, err := handler(t.Context(), requests[0].ToRequest())
	require.NoError(t, err)

	simResp, ok := resp.(protocol.SimulateTransactionResponse)
	require.True(t, ok)
	require.Empty(t, simResp.Error)
	require.True(t, getter.called, "preflight getter should be called")
	require.Equal(t, ledgerSeq, getter.params.LedgerSeq)
	require.Equal(t, uint64(closeTime), getter.params.LedgerTime)
	require.Equal(t, protocolVersion, getter.params.ProtocolVersion)
	require.Equal(t, bucketListSize, getter.params.BucketListSize)

	ledgerReader.AssertExpectations(t)
}

// TestSimulateTransactionReturnsErrorForUnknownLedgerMetaVersion verifies the
// version check inside getLatestLedgerPreflightInfo: a LedgerCloseMeta whose
// version is neither 1 nor 2 must produce a descriptive error and echo back the
// latest ledger sequence.
func TestSimulateTransactionReturnsErrorForUnknownLedgerMetaVersion(t *testing.T) {
	logger := log.New()
	ledgerReader := &MockLedgerReader{}
	handler := NewSimulateTransactionHandler(logger, ledgerReader, nil, panicPreflightGetter{})

	// V0 is encodable but predates the versions the handler reads, so it fails the version check.
	unknownVersionMeta := xdr.LedgerCloseMeta{V: 0, V0: &xdr.LedgerCloseMetaV0{}}

	ledgerReader.
		On("GetLatestLedgerSequence", mock.Anything).
		Return(uint32(77), nil).
		Once()
	ledgerReader.
		On("GetLedger", mock.Anything, uint32(77)).
		Return(unknownVersionMeta, true, nil).
		Once()

	txEnvelope := invokeHostFunctionEnvelope(t)
	txB64, err := xdr.MarshalBase64(txEnvelope)
	require.NoError(t, err)

	requestJSON := fmt.Sprintf(`{
"jsonrpc": "2.0",
"id": 1,
"method": "simulateTransaction",
"params": { "transaction": "%s" }
}`, txB64)
	requests, err := jrpc2.ParseRequests([]byte(requestJSON))
	require.NoError(t, err)
	require.Len(t, requests, 1)

	resp, err := handler(t.Context(), requests[0].ToRequest())
	require.NoError(t, err)

	simResp, ok := resp.(protocol.SimulateTransactionResponse)
	require.True(t, ok)
	require.Equal(t, "latest ledger (77) meta has unexpected version (0)", simResp.Error)
	require.Equal(t, uint32(77), simResp.LatestLedger)

	ledgerReader.AssertExpectations(t)
}

// TestSimulateTransactionCloseTimeIsAnchoredToLatestLedgerSequence verifies a
// consistency invariant for preflight parameters: the LedgerTime passed to
// preflight must come from the same ledger meta as the LedgerSeq chosen for the
// request.
//
// In this test, the selected latest ledger is 77 with close time T77. The
// handler must pass both values through together, rather than mixing LedgerSeq
// from one ledger with close time from a different ledger snapshot.
func TestSimulateTransactionCloseTimeIsAnchoredToLatestLedgerSequence(t *testing.T) {
	logger := log.New()
	ledgerReader := &MockLedgerReader{}
	getter := &recordingPreflightGetter{
		result: preflight.Preflight{MinFee: 1},
	}
	handler := NewSimulateTransactionHandler(logger, ledgerReader, nil, getter)

	// Use a fixed ledger snapshot: sequence 77 with close time T77.
	const (
		seqBeforeIngest   = uint32(77)
		closeTimeForSeq77 = xdr.TimePoint(1_700_000_077)
	)

	ledgerReader.
		On("GetLatestLedgerSequence", mock.Anything).
		Return(seqBeforeIngest, nil).
		Once()
	// The handler must load metadata for the exact sequence it selected above.
	ledgerReader.
		On("GetLedger", mock.Anything, seqBeforeIngest).
		Return(createLedger(0, seqBeforeIngest, closeTimeForSeq77), true, nil).
		Once()

	txEnvelope := invokeHostFunctionEnvelope(t)
	txB64, err := xdr.MarshalBase64(txEnvelope)
	require.NoError(t, err)

	requestJSON := fmt.Sprintf(`{
"jsonrpc": "2.0",
"id": 1,
"method": "simulateTransaction",
"params": { "transaction": "%s" }
}`, txB64)
	requests, err := jrpc2.ParseRequests([]byte(requestJSON))
	require.NoError(t, err)
	require.Len(t, requests, 1)

	resp, err := handler(t.Context(), requests[0].ToRequest())
	require.NoError(t, err)

	simResp, ok := resp.(protocol.SimulateTransactionResponse)
	require.True(t, ok)
	require.Empty(t, simResp.Error)
	require.True(t, getter.called, "preflight getter should be called")
	// Both LedgerSeq and LedgerTime must be sourced from ledger 77.
	require.Equal(t, seqBeforeIngest, getter.params.LedgerSeq)
	require.Equal(t, uint64(closeTimeForSeq77), getter.params.LedgerTime)
	// The expected calls confirm the handler selected ledger 77 and then loaded
	// the matching ledger meta for that same sequence.
	ledgerReader.AssertExpectations(t)
}

func feeBumpExtendFootprintMissingSorobanData(t *testing.T) xdr.TransactionEnvelope {
	t.Helper()

	sourceAccountID := xdr.MustAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	source := (&sourceAccountID).ToMuxedAccount()
	feeSourceAccountID := xdr.MustAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	feeSource := (&feeSourceAccountID).ToMuxedAccount()

	op := xdr.Operation{
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeExtendFootprintTtl,
			ExtendFootprintTtlOp: &xdr.ExtendFootprintTtlOp{
				Ext:      xdr.ExtensionPoint{V: 0},
				ExtendTo: xdr.Uint32(100),
			},
		},
	}

	tx := xdr.Transaction{
		SourceAccount: source,
		Fee:           xdr.Uint32(100),
		SeqNum:        xdr.SequenceNumber(1),
		Cond:          xdr.Preconditions{Type: xdr.PreconditionTypePrecondNone},
		Memo:          xdr.Memo{Type: xdr.MemoTypeMemoNone},
		Operations:    []xdr.Operation{op},
		Ext:           xdr.TransactionExt{V: 0},
	}

	innerV1 := xdr.TransactionV1Envelope{Tx: tx}
	innerTx := xdr.FeeBumpTransactionInnerTx{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1:   &innerV1,
	}
	feeBumpTx := xdr.FeeBumpTransaction{
		FeeSource: feeSource,
		Fee:       xdr.Int64(1000),
		InnerTx:   innerTx,
		Ext:       xdr.FeeBumpTransactionExt{V: 0},
	}
	feeBumpEnvelope := xdr.FeeBumpTransactionEnvelope{Tx: feeBumpTx}

	return xdr.TransactionEnvelope{
		Type:    xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		FeeBump: &feeBumpEnvelope,
	}
}

// capturingPreflightGetter records the GetterParameters it receives so tests can
// assert on how the handler populates them, and returns an empty (but valid)
// Preflight result.
type capturingPreflightGetter struct {
	called bool
	params preflight.GetterParameters
}

func (c *capturingPreflightGetter) GetPreflight(
	_ context.Context,
	params preflight.GetterParameters,
) (preflight.Preflight, error) {
	c.called = true
	c.params = params
	return preflight.Preflight{}, nil
}

func invokeHostFunctionEnvelope(t *testing.T) xdr.TransactionEnvelope {
	t.Helper()

	sourceAccountID := xdr.MustAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	source := (&sourceAccountID).ToMuxedAccount()
	contractID := xdr.ContractId{0xa, 0xb, 0xc}
	argSymbol := xdr.ScSymbol("world")

	op := xdr.Operation{
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeInvokeHostFunction,
			InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
				HostFunction: xdr.HostFunction{
					Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
					InvokeContract: &xdr.InvokeContractArgs{
						ContractAddress: xdr.ScAddress{
							Type:       xdr.ScAddressTypeScAddressTypeContract,
							ContractId: &contractID,
						},
						FunctionName: "hello",
						Args:         []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &argSymbol}},
					},
				},
			},
		},
	}

	tx := xdr.Transaction{
		SourceAccount: source,
		Fee:           xdr.Uint32(100),
		SeqNum:        xdr.SequenceNumber(1),
		Cond:          xdr.Preconditions{Type: xdr.PreconditionTypePrecondNone},
		Memo:          xdr.Memo{Type: xdr.MemoTypeMemoNone},
		Operations:    []xdr.Operation{op},
		Ext:           xdr.TransactionExt{V: 0},
	}

	return xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1:   &xdr.TransactionV1Envelope{Tx: tx},
	}
}

// TestSimulateTransactionThreadsUseUpgradedAuth verifies that the request's useUpgradedAuth flag is
// forwarded into the preflight GetterParameters (and defaults to false when omitted).
func TestSimulateTransactionThreadsUseUpgradedAuth(t *testing.T) {
	closeMeta := xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{LedgerVersion: 23},
			},
			// GeneralizedTransactionSet has no zero arm; the ledger must round-trip through bytes.
			TxSet:                           xdr.GeneralizedTransactionSet{V: 1, V1TxSet: &xdr.TransactionSetV1{}},
			TotalByteSizeOfLiveSorobanState: 100,
		},
	}

	txB64, err := xdr.MarshalBase64(invokeHostFunctionEnvelope(t))
	require.NoError(t, err)

	for _, tc := range []struct {
		name        string
		paramsField string
		expected    bool
	}{
		{name: "useUpgradedAuth true is forwarded", paramsField: `, "useUpgradedAuth": true`, expected: true},
		{name: "useUpgradedAuth false is forwarded", paramsField: `, "useUpgradedAuth": false`, expected: false},
		{name: "useUpgradedAuth omitted defaults to false", paramsField: "", expected: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ledgerReader := &MockLedgerReader{}
			ledgerReader.On("GetLatestLedgerSequence", mock.Anything).Return(uint32(2), nil)
			ledgerReader.On("GetLedger", mock.Anything, uint32(2)).Return(closeMeta, true, nil)

			getter := &capturingPreflightGetter{}
			handler := NewSimulateTransactionHandler(log.New(), ledgerReader, nil, getter)

			requestJSON := fmt.Sprintf(`{
"jsonrpc": "2.0",
"id": 1,
"method": "simulateTransaction",
"params": { "transaction": "%s"%s }
}`, txB64, tc.paramsField)
			requests, err := jrpc2.ParseRequests([]byte(requestJSON))
			require.NoError(t, err)
			require.Len(t, requests, 1)

			_, err = handler(t.Context(), requests[0].ToRequest())
			require.NoError(t, err)

			require.True(t, getter.called, "GetPreflight should have been called")
			require.Equal(t, tc.expected, getter.params.UseUpgradedAuth)
		})
	}
}

func TestSimulationJSONStaysWithinBudget(t *testing.T) {
	result, err := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: new(xdr.Uint32)}.MarshalBinary()
	require.NoError(t, err)
	p := preflight.Preflight{Result: result}

	_, err = getSimulationResults(p, protocol.FormatJSON, xdr2json.NewBudget(4))
	require.ErrorContains(t, err, "exceeds")

	results, err := getSimulationResults(p, protocol.FormatJSON, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"u32":0}`, string(results[0].ReturnValueJSON))
}

func TestSimulationEventsShareTheJSONBudget(t *testing.T) {
	// Each "<" is six bytes in the response, so each value takes just over half the budget.
	str := xdr.ScString(strings.Repeat("<", maxSimulationJSONBytes/12+1))
	val := xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
	result, err := val.MarshalBinary()
	require.NoError(t, err)
	event, err := xdr.DiagnosticEvent{Event: xdr.ContractEvent{
		Type: xdr.ContractEventTypeDiagnostic,
		Body: xdr.ContractEventBody{V0: &xdr.ContractEventV0{Data: val}},
	}}.MarshalBinary()
	require.NoError(t, err)

	resp, err := formatResponse(preflight.Preflight{Events: [][]byte{event}}, protocol.FormatJSON, 0)
	require.NoError(t, err)
	require.Len(t, resp.EventsJSON, 1)

	_, err = formatResponse(preflight.Preflight{Result: result, Events: [][]byte{event}}, protocol.FormatJSON, 0)
	require.ErrorContains(t, err, "exceeds")
}
