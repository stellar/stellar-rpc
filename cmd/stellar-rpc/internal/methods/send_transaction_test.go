package methods

import (
	"encoding/hex"
	"testing"

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
