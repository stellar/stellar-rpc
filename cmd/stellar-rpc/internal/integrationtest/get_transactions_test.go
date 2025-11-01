package integrationtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	client "github.com/stellar/go/clients/rpcclient"
	"github.com/stellar/go/keypair"
	protocol "github.com/stellar/go/protocols/rpc"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

// buildSetOptionsTxParams constructs the parameters necessary for creating a transaction from the given account.
//
// account - the source account from which the transaction will originate. This account provides the starting sequence number.
//
// Returns a fully populated TransactionParams structure.
func buildSetOptionsTxParams(account txnbuild.Account) txnbuild.TransactionParams {
	return infrastructure.CreateTransactionParams(
		account,
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
func sendTransactions(t *testing.T, client *client.Client) []uint32 {
	kp := keypair.Root(infrastructure.StandaloneNetworkPassphrase)
	address := kp.Address()

	account, err := client.LoadAccount(t.Context(), address)
	require.NoError(t, err)

	ledgers := make([]uint32, 0, 3)

	for i := 0; i <= 2; i++ {
		tx, err := txnbuild.NewTransaction(buildSetOptionsTxParams(account))
		require.NoError(t, err)

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
	request := protocol.GetTransactionsRequest{
		StartLedger: ledgers[0],
	}
	result, err := client.GetTransactions(context.Background(), request)
	assert.NoError(t, err)
	assert.Len(t, result.Transactions, 3)
	assert.Equal(t, result.Transactions[0].Ledger, ledgers[0])
	assert.Equal(t, result.Transactions[1].Ledger, ledgers[1])
	assert.Equal(t, result.Transactions[2].Ledger, ledgers[2])

	// Get transactions with limit
	request = protocol.GetTransactionsRequest{
		StartLedger: ledgers[0],
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 1,
		},
	}
	result, err = client.GetTransactions(context.Background(), request)
	assert.NoError(t, err)
	assert.Len(t, result.Transactions, 1)
	assert.Equal(t, result.Transactions[0].Ledger, ledgers[0])

	// Get transactions using previous result's cursor
	request = protocol.GetTransactionsRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Cursor: result.Cursor,
			Limit:  5,
		},
	}
	result, err = client.GetTransactions(context.Background(), request)
	assert.NoError(t, err)
	assert.Len(t, result.Transactions, 2)
	assert.Equal(t, result.Transactions[0].Ledger, ledgers[1])
	assert.Equal(t, result.Transactions[1].Ledger, ledgers[2])
}

func TestGetTransactionsEvents(t *testing.T) {
	if infrastructure.GetCoreMaxSupportedProtocol() < 23 {
		t.Skip("Only test this for protocol >= 23")
	}
	test := infrastructure.NewTest(t, nil)
	response, _, _ := test.CreateHelloWorldContract()
	assert.NotEmpty(t, response.Events.ContractEventsXDR)
	assert.Len(t, response.Events.ContractEventsXDR, 1)
	assert.Empty(t, response.Events.ContractEventsXDR[0])

	assert.Len(t, response.Events.TransactionEventsXDR, 2)
	assert.NotEmpty(t, response.DiagnosticEventsXDR)
}
