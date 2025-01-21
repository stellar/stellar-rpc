package integrationtest

import (
	"context"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/keypair"
	proto "github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/protocol"
)

func TestSendTransactionSucceedsWithoutResults(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	test.SendMasterOperation(
		&txnbuild.SetOptions{HomeDomain: txnbuild.NewHomeDomain("soroban.com")},
	)
}

func TestSendTransactionSucceedsWithResults(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	response, contractHash := test.UploadHelloWorldContract()

	// Check the result is what we expect
	var transactionResult xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(response.ResultXDR, &transactionResult))
	opResults, ok := transactionResult.OperationResults()
	require.True(t, ok)
	invokeHostFunctionResult, ok := opResults[0].MustTr().GetInvokeHostFunctionResult()
	require.True(t, ok)
	require.Equal(t, xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess, invokeHostFunctionResult.Code)
	contractHashBytes := xdr.ScBytes(contractHash[:])
	expectedScVal := xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &contractHashBytes}
	var transactionMeta xdr.TransactionMeta
	require.NoError(t, xdr.SafeUnmarshalBase64(response.ResultMetaXDR, &transactionMeta))
	require.True(t, expectedScVal.Equals(transactionMeta.V3.SorobanMeta.ReturnValue))
	var resultXdr xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(response.ResultXDR, &resultXdr))
	expectedResult := xdr.TransactionResult{
		FeeCharged: resultXdr.FeeCharged,
		Result: xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxSuccess,
			Results: &[]xdr.OperationResult{
				{
					Code: xdr.OperationResultCodeOpInner,
					Tr: &xdr.OperationResultTr{
						Type: xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionResult: &xdr.InvokeHostFunctionResult{
							Code:    xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess,
							Success: (*resultXdr.Result.Results)[0].Tr.InvokeHostFunctionResult.Success,
						},
					},
				},
			},
		},
	}

	require.Equal(t, expectedResult, resultXdr)
}

func TestSendTransactionBadSequence(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	params := infrastructure.CreateTransactionParams(
		test.MasterAccount(),
		&txnbuild.SetOptions{HomeDomain: txnbuild.NewHomeDomain("soroban.com")},
	)
	params.IncrementSequenceNum = false
	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	tx, err = tx.Sign(infrastructure.StandaloneNetworkPassphrase, test.MasterKey())
	require.NoError(t, err)
	b64, err := tx.Base64()
	require.NoError(t, err)

	request := protocol.SendTransactionRequest{Transaction: b64}
	var result protocol.SendTransactionResponse
	client := test.GetRPCLient()
	err = client.CallResult(context.Background(), "sendTransaction", request, &result)
	require.NoError(t, err)

	require.NotZero(t, result.LatestLedger)
	require.NotZero(t, result.LatestLedgerCloseTime)
	expectedHashHex, err := tx.HashHex(infrastructure.StandaloneNetworkPassphrase)
	require.NoError(t, err)
	require.Equal(t, expectedHashHex, result.Hash)
	require.Equal(t, proto.TXStatusError, result.Status)
	var errorResult xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(result.ErrorResultXDR, &errorResult))
	require.Equal(t, xdr.TransactionResultCodeTxBadSeq, errorResult.Result.Code)
}

func TestSendTransactionFailedInsufficientResourceFee(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	params := infrastructure.PreflightTransactionParams(t, client,
		infrastructure.CreateTransactionParams(
			test.MasterAccount(),
			infrastructure.CreateUploadHelloWorldOperation(test.MasterAccount().GetAccountID()),
		),
	)

	// make the transaction fail due to insufficient resource fees
	params.Operations[0].(*txnbuild.InvokeHostFunction).Ext.SorobanData.ResourceFee /= 2

	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)

	require.NoError(t, err)
	tx, err = tx.Sign(infrastructure.StandaloneNetworkPassphrase, test.MasterKey())
	require.NoError(t, err)
	b64, err := tx.Base64()
	require.NoError(t, err)

	request := protocol.SendTransactionRequest{Transaction: b64}
	var result protocol.SendTransactionResponse
	err = client.CallResult(context.Background(), "sendTransaction", request, &result)
	require.NoError(t, err)

	require.Equal(t, proto.TXStatusError, result.Status)
	var errorResult xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(result.ErrorResultXDR, &errorResult))
	require.Equal(t, xdr.TransactionResultCodeTxSorobanInvalid, errorResult.Result.Code)

	require.NotEmpty(t, result.DiagnosticEventsXDR)
	var event xdr.DiagnosticEvent
	err = xdr.SafeUnmarshalBase64(result.DiagnosticEventsXDR[0], &event)
	require.NoError(t, err)
}

func TestSendTransactionFailedInLedger(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	kp := keypair.Root(infrastructure.StandaloneNetworkPassphrase)
	tx, err := txnbuild.NewTransaction(
		infrastructure.CreateTransactionParams(
			test.MasterAccount(),
			&txnbuild.Payment{
				// Destination doesn't exist, making the transaction fail
				Destination:   "GA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVSGZ",
				Amount:        "100000.0000000",
				Asset:         txnbuild.NativeAsset{},
				SourceAccount: "",
			},
		),
	)
	require.NoError(t, err)
	tx, err = tx.Sign(infrastructure.StandaloneNetworkPassphrase, kp)
	require.NoError(t, err)
	b64, err := tx.Base64()
	require.NoError(t, err)

	request := protocol.SendTransactionRequest{Transaction: b64}
	var result protocol.SendTransactionResponse
	err = client.CallResult(context.Background(), "sendTransaction", request, &result)
	require.NoError(t, err)

	expectedHashHex, err := tx.HashHex(infrastructure.StandaloneNetworkPassphrase)
	require.NoError(t, err)

	require.Equal(t, expectedHashHex, result.Hash)
	if !assert.Equal(t, proto.TXStatusPending, result.Status) {
		var txResult xdr.TransactionResult
		err := xdr.SafeUnmarshalBase64(result.ErrorResultXDR, &txResult)
		require.NoError(t, err)
		t.Logf("error: %#v\n", txResult)
	}
	require.NotZero(t, result.LatestLedger)
	require.NotZero(t, result.LatestLedgerCloseTime)

	response := test.GetTransaction(expectedHashHex)
	require.Equal(t, protocol.TransactionStatusFailed, response.Status)
	var transactionResult xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(response.ResultXDR, &transactionResult))
	require.Equal(t, xdr.TransactionResultCodeTxFailed, transactionResult.Result.Code)
	require.Greater(t, response.Ledger, result.LatestLedger)
	require.Greater(t, response.LedgerCloseTime, result.LatestLedgerCloseTime)
	require.GreaterOrEqual(t, response.LatestLedger, response.Ledger)
	require.GreaterOrEqual(t, response.LatestLedgerCloseTime, response.LedgerCloseTime)
}

func TestSendTransactionFailedInvalidXDR(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	request := protocol.SendTransactionRequest{Transaction: "abcdef"}
	var response protocol.SendTransactionResponse
	jsonRPCErr := client.CallResult(context.Background(), "sendTransaction", request, &response).(*jrpc2.Error)
	require.Equal(t, "invalid_xdr", jsonRPCErr.Message)
	require.Equal(t, jrpc2.InvalidParams, jsonRPCErr.Code)
}

func TestContractCreationWithConstructor(t *testing.T) {
	if infrastructure.GetCoreMaxSupportedProtocol() < 22 {
		t.Skip("Only test this for protocol >= 22")
	}
	test := infrastructure.NewTest(t, nil)

	test.UploadNoArgConstructorContract()

	client := test.GetRPCLient()

	params := infrastructure.PreflightTransactionParams(t, client,
		infrastructure.CreateTransactionParams(
			test.MasterAccount(),
			infrastructure.CreateCreateNoArgConstructorContractOperation(test.MasterAccount().GetAccountID()),
		),
	)

	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	infrastructure.SendSuccessfulTransaction(t, client, test.MasterKey(), tx)
}
