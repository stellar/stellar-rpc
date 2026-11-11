package host

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/gxdr"
	"github.com/stellar/go-stellar-sdk/randxdr"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestOperationCount(t *testing.T) {
	gen := randxdr.NewGenerator()
	for range 1000 {
		shape := &gxdr.TransactionEnvelope{}
		gen.Next(shape, randxdr.LedgerCloseMetaPresets)
		var envelope xdr.TransactionEnvelope
		require.NoError(t, gxdr.Convert(shape, &envelope))
		envelopeB64, err := xdr.MarshalBase64(envelope)
		require.NoError(t, err)

		count, err := operationCount(envelopeB64)
		require.NoError(t, err)
		require.Len(t, envelope.Operations(), count)
	}
	_, err := operationCount("AAAA")
	require.Error(t, err)
}
