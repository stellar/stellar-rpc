package integrationtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/protocol"
)

// buildSetOptionsTxParams constructs the parameters necessary for creating a transaction from the given account.
//
// account - the source account from which the transaction will originate. This account provides the starting sequence number.
//
// Returns a fully populated TransactionParams structure.
func buildSetOptionsTxParams(account txnbuild.SimpleAccount) txnbuild.TransactionParams {
	return infrastructure.CreateTransactionParams(
		&account,
		&txnbuild.SetOptions{HomeDomain: txnbuild.NewHomeDomain("soroban.com")},
	)
}

// sendTransactions sends multiple transactions for testing purposes.
// It sends a total of three transactions, each from a new account sequence, and gathers the ledger
// numbers where these transactions were recorded.
//
// t - the testing framework handle for assertions.
// client - the JSON-RPC client used to send the transactions.
//
// Returns a slice of ledger numbers corresponding to where each transaction was recorded.
func sendTransactions(t *testing.T, client *infrastructure.Client) []uint32 {
	kp := keypair.Root(infrastructure.StandaloneNetworkPassphrase)
	address := kp.Address()

	var ledgers []uint32
	for i := 0; i <= 2; i++ {
		account := txnbuild.NewSimpleAccount(address, int64(i))
		tx, err := txnbuild.NewTransaction(buildSetOptionsTxParams(account))
		assert.NoError(t, err)

		txResponse := infrastructure.SendSuccessfulTransaction(t, client, kp, tx)
		ledgers = append(ledgers, txResponse.Ledger)
	}
	return ledgers
}

func TestGetTransactions(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	client := test.GetRPCLient()

	ledgers := sendTransactions(t, client)

	test.MasterAccount()
	// Get transactions across multiple ledgers
	var result protocol.GetTransactionsResponse
	request := protocol.GetTransactionsRequest{
		StartLedger: ledgers[0],
	}
	err := client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Len(t, result.Transactions, 3)
	assert.Equal(t, result.Transactions[0].Ledger, ledgers[0])
	assert.Equal(t, result.Transactions[1].Ledger, ledgers[1])
	assert.Equal(t, result.Transactions[2].Ledger, ledgers[2])

	// Get transactions with limit
	request = protocol.GetTransactionsRequest{
		StartLedger: ledgers[0],
		Pagination: &protocol.TransactionsPaginationOptions{
			Limit: 1,
		},
	}
	err = client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Len(t, result.Transactions, 1)
	assert.Equal(t, result.Transactions[0].Ledger, ledgers[0])

	// Get transactions using previous result's cursor
	request = protocol.GetTransactionsRequest{
		Pagination: &protocol.TransactionsPaginationOptions{
			Cursor: result.Cursor,
			Limit:  5,
		},
	}
	err = client.CallResult(context.Background(), "getTransactions", request, &result)
	assert.NoError(t, err)
	assert.Len(t, result.Transactions, 2)
	assert.Equal(t, result.Transactions[0].Ledger, ledgers[1])
	assert.Equal(t, result.Transactions[1].Ledger, ledgers[2])
}
