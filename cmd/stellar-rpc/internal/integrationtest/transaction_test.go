package integrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/keypair"
	proto "github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/toid"
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
	var retVal xdr.ScVal
	switch transactionMeta.V {
	case 3:
		retVal = transactionMeta.V3.SorobanMeta.ReturnValue
	case 4:
		retVal = *transactionMeta.V4.SorobanMeta.ReturnValue
	default:
		t.Fatalf("Unexpected protocol version: %d", transactionMeta.V)
	}
	require.True(t, expectedScVal.Equals(retVal))
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
	client := test.GetRPCLient()
	result, err := client.SendTransaction(context.Background(), request)
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
	result, err := client.SendTransaction(context.Background(), request)
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
	result, err := client.SendTransaction(context.Background(), request)
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
	_, err := client.SendTransaction(context.Background(), request)
	var jsonRPCErr *jrpc2.Error
	require.ErrorAs(t, err, &jsonRPCErr)
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

func TestContractEvents(t *testing.T) {
	if infrastructure.GetCoreMaxSupportedProtocol() < 23 {
		t.Skip("Only test this for protocol >= 23")
	}

	test := infrastructure.NewTest(t, nil)
	client := test.GetRPCLient()

	// Upload the contract wasm
	test.UploadEventsContract()

	// Create/deploy an instance of the contract
	creationOp := infrastructure.CreateCreateEventsContractOperation(test.MasterAccount().GetAccountID())
	params := infrastructure.PreflightTransactionParams(t, client,
		infrastructure.CreateTransactionParams(
			test.MasterAccount(),
			creationOp,
		),
	)

	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	infrastructure.SendSuccessfulTransaction(t, client, test.MasterKey(), tx)

	// Compute the contract ID based on the deployment operation.
	preimage := creationOp.HostFunction.MustCreateContractV2().ContractIdPreimage
	contractID := infrastructure.GetContractID(
		t,
		test.MasterAccount().GetAccountID(),
		preimage.MustFromAddress().Salt,
		infrastructure.StandaloneNetworkPassphrase,
	)
	require.NoError(t, err)
	params = infrastructure.PreflightTransactionParams(t, client,
		infrastructure.CreateTransactionParams(
			test.MasterAccount(),
			infrastructure.CreateIncrementOperation(
				xdr.ContractId(contractID),
				test.MasterAccount().GetAccountID(),
			),
		),
	)

	tx, err = txnbuild.NewTransaction(params)
	require.NoError(t, err)
	infrastructure.SendSuccessfulTransaction(t, client, test.MasterKey(), tx)

	//
	// Validate the events generated by the invocation
	//

	hash, err := tx.HashHex(infrastructure.StandaloneNetworkPassphrase)
	require.NoError(t, err)

	txResult, err := client.GetTransaction(t.Context(), protocol.GetTransactionRequest{
		Hash:   hash,
		Format: protocol.FormatJSON,
	})
	require.NoError(t, err)
	require.Equal(t, protocol.TransactionStatusSuccess, txResult.Status)
	assert.Len(t, txResult.Events.ContractEventsJSON, 1)
	assert.Len(t, txResult.Events.TransactionEventsJSON, 2)

	events, err := client.GetEvents(t.Context(), protocol.GetEventsRequest{
		StartLedger: txResult.Ledger,
		Pagination:  &protocol.PaginationOptions{Limit: 200},
		Format:      protocol.FormatJSON,
	})
	require.NoError(t, err)

	js, err := json.Marshal(events)
	require.NoError(t, err)
	t.Logf("getEvents response: %s", string(js))

	assert.Len(t, events.Events, 1+1+1 /* fee charged, contract, refund */)

	expectedToids := []*toid.ID{
		toid.New(
			int32(txResult.Ledger),
			0,
			0,
		),
		toid.New(
			int32(txResult.Ledger),
			txResult.ApplicationOrder,
			0,
		),
		toid.New(
			int32(txResult.Ledger),
			toid.TransactionMask,
			0,
		),
	}

	for i, e := range events.Events {
		assert.Equal(t, hash, e.TransactionHash)

		parts := strings.Split(e.ID, "-")
		require.Len(t, parts, 2)

		first, second := parts[0], parts[1]
		assert.Equal(t, fmt.Sprintf("%019d", expectedToids[i].ToInt64()), first)
		assert.Equal(t, "0000000000", second)
	}
}
