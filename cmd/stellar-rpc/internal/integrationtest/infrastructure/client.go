package infrastructure

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/client"
	"github.com/stellar/stellar-rpc/protocol"
)

func getTransaction(t testing.TB, client *client.Client, hash string) protocol.GetTransactionResponse {
	var result protocol.GetTransactionResponse
	for range 60 {
		var err error
		request := protocol.GetTransactionRequest{Hash: hash}
		result, err = client.GetTransaction(context.Background(), request)
		require.NoError(t, err)

		if result.Status == protocol.TransactionStatusNotFound {
			time.Sleep(time.Second)
			continue
		}

		return result
	}
	t.Fatal("GetTransaction timed out")
	return result
}

func logTransactionResult(t testing.TB, response protocol.GetTransactionResponse) {
	var txResult xdr.TransactionResult
	require.NoError(t, xdr.SafeUnmarshalBase64(response.ResultXDR, &txResult))
	t.Logf("error: %#v\n", txResult)

	var txMeta xdr.TransactionMeta
	require.NoError(t, xdr.SafeUnmarshalBase64(response.ResultMetaXDR, &txMeta))

	diagnosticEvents, err := txMeta.GetDiagnosticEvents()
	require.NoError(t, err)
	if len(diagnosticEvents) > 0 {
		t.Log("Diagnostic events:")
		for i, d := range diagnosticEvents {
			t.Logf("  %d: %s\n", i, d)
		}
	}
}

func SendSuccessfulTransaction(t testing.TB, client *client.Client, kp *keypair.Full,
	tx *txnbuild.Transaction,
) protocol.GetTransactionResponse {
	if kp != nil {
		var err error
		tx, err = tx.Sign(StandaloneNetworkPassphrase, kp)
		require.NoError(t, err)
	}
	b64, err := tx.Base64()
	require.NoError(t, err)

	request := protocol.SendTransactionRequest{Transaction: b64}
	result, err := client.SendTransaction(context.Background(), request)
	require.NoError(t, err)

	expectedHashHex, err := tx.HashHex(StandaloneNetworkPassphrase)
	require.NoError(t, err)

	require.Equal(t, expectedHashHex, result.Hash)
	if !assert.Equal(t, stellarcore.TXStatusPending, result.Status) {
		var txResult xdr.TransactionResult
		err := xdr.SafeUnmarshalBase64(result.ErrorResultXDR, &txResult)
		require.NoError(t, err)
		t.Logf("error: %#v\n", txResult)
	}
	require.NotZero(t, result.LatestLedger)
	require.NotZero(t, result.LatestLedgerCloseTime)

	response := getTransaction(t, client, expectedHashHex)
	if !assert.Equal(t, protocol.TransactionStatusSuccess, response.Status) {
		logTransactionResult(t, response)
		t.FailNow()
	}

	require.NotNil(t, response.ResultXDR)
	require.Greater(t, response.Ledger, result.LatestLedger)
	require.Greater(t, response.LedgerCloseTime, result.LatestLedgerCloseTime)
	require.GreaterOrEqual(t, response.LatestLedger, response.Ledger)
	require.GreaterOrEqual(t, response.LatestLedgerCloseTime, response.LedgerCloseTime)
	return response
}

func SimulateTransactionFromTxParams(t testing.TB, client *client.Client,
	params txnbuild.TransactionParams,
) protocol.SimulateTransactionResponse {
	savedAutoIncrement := params.IncrementSequenceNum
	params.IncrementSequenceNum = false
	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	params.IncrementSequenceNum = savedAutoIncrement
	txB64, err := tx.Base64()
	require.NoError(t, err)
	request := protocol.SimulateTransactionRequest{Transaction: txB64}
	response, err := client.SimulateTransaction(context.Background(), request)
	require.NoError(t, err)
	return response
}

func PreflightTransactionParamsLocally(t testing.TB, params txnbuild.TransactionParams,
	response protocol.SimulateTransactionResponse,
) txnbuild.TransactionParams {
	if !assert.Empty(t, response.Error) {
		t.Log(response.Error)
	}
	var transactionData xdr.SorobanTransactionData
	err := xdr.SafeUnmarshalBase64(response.TransactionDataXDR, &transactionData)
	require.NoError(t, err)

	op := params.Operations[0]
	switch v := op.(type) {
	case *txnbuild.InvokeHostFunction:
		require.Len(t, response.Results, 1)
		v.Ext = xdr.TransactionExt{
			V:           1,
			SorobanData: &transactionData,
		}
		var auth []xdr.SorobanAuthorizationEntry
		if response.Results[0].AuthXDR != nil {
			for _, b64 := range *response.Results[0].AuthXDR {
				var a xdr.SorobanAuthorizationEntry
				err := xdr.SafeUnmarshalBase64(b64, &a)
				require.NoError(t, err)
				auth = append(auth, a)
			}
		}
		v.Auth = auth
	case *txnbuild.ExtendFootprintTtl:
		require.Empty(t, response.Results)
		v.Ext = xdr.TransactionExt{
			V:           1,
			SorobanData: &transactionData,
		}
	case *txnbuild.RestoreFootprint:
		require.Empty(t, response.Results)
		v.Ext = xdr.TransactionExt{
			V:           1,
			SorobanData: &transactionData,
		}
	default:
		t.Fatalf("Wrong operation type %v", op)
	}

	params.Operations = []txnbuild.Operation{op}

	params.BaseFee += response.MinResourceFee
	return params
}

func PreflightTransactionParams(t testing.TB, client *client.Client, params txnbuild.TransactionParams,
) txnbuild.TransactionParams {
	response := SimulateTransactionFromTxParams(t, client, params)
	if !assert.Empty(t, response.Error) {
		t.Logf("Simulation failed: %s", response.Error)
		for i, evtb64 := range response.EventsXDR {
			t.Logf("Event #%d: %s", i, evtb64)
		}

		t.FailNow()
	}
	// The preamble should be zero except for the special restore case
	require.Nil(t, response.RestorePreamble)
	return PreflightTransactionParamsLocally(t, params, response)
}
