package infrastructure

import (
	"context"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/protocol"
)

// Client is a jrpc2 client which tolerates errors
type Client struct {
	url  string
	cli  *jrpc2.Client
	opts *jrpc2.ClientOptions
}

func NewClient(url string, opts *jrpc2.ClientOptions) *Client {
	c := &Client{url: url, opts: opts}
	c.refreshClient()
	return c
}

func (c *Client) refreshClient() {
	if c.cli != nil {
		c.cli.Close()
	}
	ch := jhttp.NewChannel(c.url, nil)
	c.cli = jrpc2.NewClient(ch, c.opts)
}

func (c *Client) CallResult(ctx context.Context, method string, params, result any) error {
	err := c.cli.CallResult(ctx, method, params, result)
	if err != nil {
		// This is needed because of https://github.com/creachadair/jrpc2/issues/118
		c.refreshClient()
	}
	return err
}

func (c *Client) Close() error {
	return c.cli.Close()
}

func getTransaction(t *testing.T, client *Client, hash string) protocol.GetTransactionResponse {
	var result protocol.GetTransactionResponse
	for i := 0; i < 60; i++ {
		request := protocol.GetTransactionRequest{Hash: hash}
		err := client.CallResult(context.Background(), "getTransaction", request, &result)
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

func SendSuccessfulTransaction(t *testing.T, client *Client, kp *keypair.Full, transaction *txnbuild.Transaction) protocol.GetTransactionResponse {
	tx, err := transaction.Sign(StandaloneNetworkPassphrase, kp)
	require.NoError(t, err)
	b64, err := tx.Base64()
	require.NoError(t, err)

	request := protocol.SendTransactionRequest{Transaction: b64}
	var result protocol.SendTransactionResponse
	require.NoError(t, client.CallResult(context.Background(), "sendTransaction", request, &result))

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
		var txResult xdr.TransactionResult
		require.NoError(t, xdr.SafeUnmarshalBase64(response.ResultXDR, &txResult))
		t.Logf("error: %#v\n", txResult)

		var txMeta xdr.TransactionMeta
		require.NoError(t, xdr.SafeUnmarshalBase64(response.ResultMetaXDR, &txMeta))

		if txMeta.V == 3 && txMeta.V3.SorobanMeta != nil {
			if len(txMeta.V3.SorobanMeta.Events) > 0 {
				t.Log("Contract events:")
				for i, e := range txMeta.V3.SorobanMeta.Events {
					t.Logf("  %d: %s\n", i, e)
				}
			}

			if len(txMeta.V3.SorobanMeta.DiagnosticEvents) > 0 {
				t.Log("Diagnostic events:")
				for i, d := range txMeta.V3.SorobanMeta.DiagnosticEvents {
					t.Logf("  %d: %s\n", i, d)
				}
			}
		}
	}

	require.NotNil(t, response.ResultXDR)
	require.Greater(t, response.Ledger, result.LatestLedger)
	require.Greater(t, response.LedgerCloseTime, result.LatestLedgerCloseTime)
	require.GreaterOrEqual(t, response.LatestLedger, response.Ledger)
	require.GreaterOrEqual(t, response.LatestLedgerCloseTime, response.LedgerCloseTime)
	return response
}

func SimulateTransactionFromTxParams(t *testing.T, client *Client, params txnbuild.TransactionParams) protocol.SimulateTransactionResponse {
	savedAutoIncrement := params.IncrementSequenceNum
	params.IncrementSequenceNum = false
	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	params.IncrementSequenceNum = savedAutoIncrement
	txB64, err := tx.Base64()
	require.NoError(t, err)
	request := protocol.SimulateTransactionRequest{Transaction: txB64}
	var response protocol.SimulateTransactionResponse
	err = client.CallResult(context.Background(), "simulateTransaction", request, &response)
	require.NoError(t, err)
	return response
}

func PreflightTransactionParamsLocally(t *testing.T, params txnbuild.TransactionParams, response protocol.SimulateTransactionResponse) txnbuild.TransactionParams {
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
		require.Len(t, response.Results, 0)
		v.Ext = xdr.TransactionExt{
			V:           1,
			SorobanData: &transactionData,
		}
	case *txnbuild.RestoreFootprint:
		require.Len(t, response.Results, 0)
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

func PreflightTransactionParams(t *testing.T, client *Client, params txnbuild.TransactionParams) txnbuild.TransactionParams {
	response := SimulateTransactionFromTxParams(t, client, params)
	// The preamble should be zero except for the special restore case
	require.Nil(t, response.RestorePreamble)
	return PreflightTransactionParamsLocally(t, params, response)
}
