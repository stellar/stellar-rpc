package integrationtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func TestGetFeeStats(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	sorobanTxResponse, _ := test.UploadHelloWorldContract()
	var sorobanTxResult xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(sorobanTxResponse.ResultXDR, &sorobanTxResult))
	sorobanTotalFee := sorobanTxResult.FeeCharged
	var sorobanTxMeta xdr.TransactionMeta
	require.NoError(t, xdr.SafeUnmarshalBase64(sorobanTxResponse.ResultMetaXDR, &sorobanTxMeta))
	var sorobanFees xdr.SorobanTransactionMetaExtV1
	switch sorobanTxMeta.V {
	case 3:
		sorobanFees = *sorobanTxMeta.V3.SorobanMeta.Ext.V1
	case 4:
		sorobanFees = *sorobanTxMeta.V4.SorobanMeta.Ext.V1
	default:
		t.Fatalf("Unexpected meta version: %d", sorobanTxMeta.V)
	}
	sorobanResourceFeeCharged := sorobanFees.TotalRefundableResourceFeeCharged + sorobanFees.TotalNonRefundableResourceFeeCharged
	sorobanInclusionFee := uint64(sorobanTotalFee - sorobanResourceFeeCharged)

	seq, err := test.MasterAccount().GetSequenceNumber()
	require.NoError(t, err)
	// Submit classic transaction
	classicTxResponse := test.SendMasterOperation(
		&txnbuild.BumpSequence{BumpTo: seq + 100},
	)
	var classicTxResult xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(classicTxResponse.ResultXDR, &classicTxResult))
	classicFee := uint64(classicTxResult.FeeCharged)

	result, err := test.GetRPCLient().GetFeeStats(context.Background())
	if err != nil {
		t.Fatalf("rpc call failed: %v", err)
	}
	expectedResult := protocol.GetFeeStatsResponse{
		SorobanInclusionFee: protocol.FeeDistribution{
			Max:              sorobanInclusionFee,
			Min:              sorobanInclusionFee,
			Mode:             sorobanInclusionFee,
			P10:              sorobanInclusionFee,
			P20:              sorobanInclusionFee,
			P30:              sorobanInclusionFee,
			P40:              sorobanInclusionFee,
			P50:              sorobanInclusionFee,
			P60:              sorobanInclusionFee,
			P70:              sorobanInclusionFee,
			P80:              sorobanInclusionFee,
			P90:              sorobanInclusionFee,
			P95:              sorobanInclusionFee,
			P99:              sorobanInclusionFee,
			TransactionCount: 9, // 1 here + 8 for upgrading limits
			LedgerCount:      result.SorobanInclusionFee.LedgerCount,
		},
		InclusionFee: protocol.FeeDistribution{
			Max:              classicFee,
			Min:              classicFee,
			Mode:             classicFee,
			P10:              classicFee,
			P20:              classicFee,
			P30:              classicFee,
			P40:              classicFee,
			P50:              classicFee,
			P60:              classicFee,
			P70:              classicFee,
			P80:              classicFee,
			P90:              classicFee,
			P95:              classicFee,
			P99:              classicFee,
			TransactionCount: 1,
			LedgerCount:      result.InclusionFee.LedgerCount,
		},
		LatestLedger: result.LatestLedger,
	}
	assert.Equal(t, expectedResult, result)

	// check ledgers separately
	assert.Greater(t, result.InclusionFee.LedgerCount, uint32(0))
	assert.Greater(t, result.SorobanInclusionFee.LedgerCount, uint32(0))
	assert.Greater(t, result.LatestLedger, uint32(0))
}
