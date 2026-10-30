package methods

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/gxdr"
	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	proto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/randxdr"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

func TestSendTransactionHash(t *testing.T) {
	ledgerReader := &MockLedgerReader{}
	ledgerReader.On("GetLedgerRange", mock.Anything).Return(store.LedgerRange{}, nil)
	handler := NewSendTransactionHandler(host.MakeNoOpDaemon(), quietLogger(), ledgerReader, passphrase)

	gen := randxdr.NewGenerator()
	for range 100 {
		shape := &gxdr.TransactionEnvelope{}
		gen.Next(shape, randxdr.LedgerCloseMetaPresets)
		var envelope xdr.TransactionEnvelope
		require.NoError(t, gxdr.Convert(shape, &envelope))
		envelopeB64, err := xdr.MarshalBase64(envelope)
		require.NoError(t, err)
		hash, err := network.HashTransactionInEnvelope(envelope, passphrase)
		require.NoError(t, err)

		resp, err := handler(t.Context(), jsonRPCRequest(t, protocol.SendTransactionMethodName,
			protocol.SendTransactionRequest{Transaction: envelopeB64}))
		require.NoError(t, err)
		require.Equal(t, protocol.SendTransactionResponse{
			Status: proto.TXStatusPending,
			Hash:   hex.EncodeToString(hash[:]),
		}, resp)
	}
}

func TestSendTransactionRejectsHugeDeclaredCountsCheaply(t *testing.T) {
	request := jsonRPCRequest(t, protocol.SendTransactionMethodName,
		protocol.SendTransactionRequest{Transaction: hugeCountEnvelope(t)})
	handler := NewSendTransactionHandler(host.MakeNoOpDaemon(), quietLogger(), &MockLedgerReader{}, passphrase)

	requireCheapRejection(t, func() {
		_, err := handler(t.Context(), request)
		require.EqualError(t, err, "[-32602] invalid_xdr")
	})
}

// coreStub answers every submission with one canned response.
type coreStub struct {
	*host.NoOpDaemon

	response proto.TXResponse
}

func (c *coreStub) CoreClient() host.CoreClient { return c }

func (c *coreStub) Info(context.Context) (*proto.InfoResponse, error) {
	return &proto.InfoResponse{}, nil
}

func (c *coreStub) SubmitTransaction(context.Context, string) (*proto.TXResponse, error) {
	return &c.response, nil
}

func callSendTransaction(
	t *testing.T, core proto.TXResponse, format string,
) (protocol.SendTransactionResponse, error) {
	t.Helper()
	tx, err := xdr.MarshalBase64(txEnvelope(1))
	require.NoError(t, err)

	handler := NewSendTransactionHandler(&coreStub{NoOpDaemon: host.MakeNoOpDaemon(), response: core},
		quietLogger(), &ConstantLedgerReader{}, passphrase)
	result, err := handler(t.Context(), jsonRPCRequest(t, protocol.SendTransactionMethodName,
		protocol.SendTransactionRequest{Transaction: tx, Format: format}))
	resp, _ := result.(protocol.SendTransactionResponse)
	return resp, err
}

// Core's error result and diagnostic events render exactly as their decoded values do.
func TestSendTransactionErrorResponse(t *testing.T) {
	result := transactionResult(false)
	id := xdr.ContractId{7}
	events := []xdr.DiagnosticEvent{
		{Event: diffEvent(xdr.ContractEventTypeDiagnostic, &id, diffStr("trapped"), diffSym("error"), diffU32(3))},
		{InSuccessfulContractCall: true, Event: diffContractEvent()},
	}

	resultJSON, err := xdr2json.ConvertInterface(result)
	require.NoError(t, err)
	eventsXDR := make([]string, len(events))
	eventsJSON := make([]json.RawMessage, len(events))
	for i, event := range events {
		eventsXDR[i], err = xdr.MarshalBase64(event)
		require.NoError(t, err)
		eventsJSON[i], err = xdr2json.ConvertInterface(event)
		require.NoError(t, err)
	}

	core := proto.TXResponse{Status: proto.TXStatusError}
	core.Error, err = xdr.MarshalBase64(result)
	require.NoError(t, err)
	core.DiagnosticEvents, err = xdr.MarshalBase64(events)
	require.NoError(t, err)

	resp, err := callSendTransaction(t, core, protocol.FormatBase64)
	require.NoError(t, err)
	assert.Equal(t, core.Error, resp.ErrorResultXDR)
	assert.Equal(t, eventsXDR, resp.DiagnosticEventsXDR)

	resp, err = callSendTransaction(t, core, protocol.FormatJSON)
	require.NoError(t, err)
	assert.Equal(t, resultJSON, resp.ErrorResultJSON)
	assert.Equal(t, eventsJSON, resp.DiagnosticEventsJSON)

	// Core omits diagnostic_events for classic transactions.
	core.DiagnosticEvents = ""
	resp, err = callSendTransaction(t, core, protocol.FormatBase64)
	require.NoError(t, err)
	assert.Equal(t, core.Error, resp.ErrorResultXDR)
	assert.Empty(t, resp.DiagnosticEventsXDR)

	resp, err = callSendTransaction(t, core, protocol.FormatJSON)
	require.NoError(t, err)
	assert.Equal(t, resultJSON, resp.ErrorResultJSON)
	assert.Empty(t, resp.DiagnosticEventsJSON)
}

func TestSendTransactionRejectsMalformedDiagnosticEvents(t *testing.T) {
	events, err := xdr.MarshalBase64([]xdr.DiagnosticEvent{{Event: diffContractEvent()}})
	require.NoError(t, err)
	raw, err := base64.StdEncoding.DecodeString(events)
	require.NoError(t, err)

	for name, malformed := range map[string][]byte{
		"truncated":      raw[:len(raw)-1],
		"trailing bytes": append(raw, 0, 0, 0, 0),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := callSendTransaction(t, proto.TXResponse{
				Status:           proto.TXStatusError,
				DiagnosticEvents: base64.StdEncoding.EncodeToString(malformed),
			}, protocol.FormatBase64)
			require.ErrorContains(t, err, "could not decode diagnostic events")
		})
	}
}
