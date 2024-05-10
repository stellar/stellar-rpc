package test

import (
	"context"
	"strconv"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/methods"
)

// buildSetOptionsTxParams constructs the parameters necessary for creating a transaction from the given account.
//
// account - the source account from which the transaction will originate. This account provides the starting sequence number.
//
// Returns a fully populated TransactionParams structure.
func buildSetOptionsTxParams(account txnbuild.SimpleAccount) txnbuild.TransactionParams {
	params := txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations: []txnbuild.Operation{
			&txnbuild.SetOptions{HomeDomain: txnbuild.NewHomeDomain("soroban.com")},
		},
		BaseFee:       txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
	}
	return params
}

// sendTransactions sends multiple transactions for testing purposes.
// It sends a total of three transactions, each from a new account sequence, and gathers the ledger
// numbers where these transactions were recorded.
//
// t - the testing framework handle for assertions.
// client - the JSON-RPC client used to send the transactions.
//
// Returns a slice of ledger numbers corresponding to where each transaction was recorded.
func sendTransactions(t *testing.T, client *jrpc2.Client) []uint32 {
	kp := keypair.Root(StandaloneNetworkPassphrase)
	address := kp.Address()

	var ledgers []uint32
	for i := 0; i <= 2; i++ {
		account := txnbuild.NewSimpleAccount(address, int64(i))
		tx, err := txnbuild.NewTransaction(buildSetOptionsTxParams(account))
		assert.NoError(t, err)

		txResponse := sendSuccessfulTransaction(t, client, kp, tx)
		ledgers = append(ledgers, txResponse.Ledger)
	}
	return ledgers
}

func TestGetTransactions(t *testing.T) {
	test := NewTest(t, nil)
	ch := jhttp.NewChannel(test.sorobanRPCURL(), nil)
	client := jrpc2.NewClient(ch, nil)

	ledgers := sendTransactions(t, client)

	// Get transactions across multiple ledgers
	var result methods.GetTransactionsResponse
	request := methods.GetTransactionsRequest{
		StartLedger: ledgers[0],
	}
	err := client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Equal(t, len(result.Transactions), 3)
	assert.Equal(t, result.Transactions[0].Ledger, ledgers[0])
	assert.Equal(t, result.Transactions[1].Ledger, ledgers[1])
	assert.Equal(t, result.Transactions[2].Ledger, ledgers[2])

	// Get transactions with limit
	request = methods.GetTransactionsRequest{
		StartLedger: ledgers[0],
		Pagination: &methods.TransactionsPaginationOptions{
			Limit: 1,
		},
	}
	err = client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Equal(t, len(result.Transactions), 1)
	assert.Equal(t, result.Transactions[0].Ledger, ledgers[0])

	// Get transactions using previous result's cursor
	c, err := strconv.ParseInt(result.Cursor, 10, 64)
	assert.NoError(t, err)
	cursor := toid.Parse(c)
	request = methods.GetTransactionsRequest{
		Pagination: &methods.TransactionsPaginationOptions{
			Cursor: &cursor,
			Limit:  5,
		},
	}
	err = client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Equal(t, len(result.Transactions), 2)
	assert.Equal(t, result.Transactions[0].Ledger, ledgers[1])
	assert.Equal(t, result.Transactions[1].Ledger, ledgers[2])
}
