package host

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestOperationCountMatchesDecodedEnvelope(t *testing.T) {
	source := xdr.MustMuxedAddress("MA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVAAAAAAAAAAAAAJLK")
	op := xdr.Operation{Body: xdr.OperationBody{
		Type:           xdr.OperationTypeBumpSequence,
		BumpSequenceOp: &xdr.BumpSequenceOp{BumpTo: 1},
	}}
	v1 := xdr.TransactionV1Envelope{Tx: xdr.Transaction{SourceAccount: source, Operations: []xdr.Operation{op, op, op}}}

	for name, envelope := range map[string]xdr.TransactionEnvelope{
		"v0": {
			Type: xdr.EnvelopeTypeEnvelopeTypeTxV0,
			V0:   &xdr.TransactionV0Envelope{Tx: xdr.TransactionV0{Operations: []xdr.Operation{op}}},
		},
		"v1": {Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &v1},
		"fee bump": {
			Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
			FeeBump: &xdr.FeeBumpTransactionEnvelope{Tx: xdr.FeeBumpTransaction{
				FeeSource: source,
				InnerTx:   xdr.FeeBumpTransactionInnerTx{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &v1},
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			blob, err := xdr.MarshalBase64(envelope)
			require.NoError(t, err)
			count, err := operationCount(blob)
			require.NoError(t, err)
			assert.Equal(t, len(envelope.Operations()), count)
		})
	}
}
