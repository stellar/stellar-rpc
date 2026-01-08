package integrationtest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	client "github.com/stellar/go-stellar-sdk/clients/rpcclient"
	"github.com/stellar/go-stellar-sdk/keypair"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/txnbuild"

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

//nolint:gocognit,cyclop,funlen
func TestGetTransactionsDataStore(t *testing.T) {
	gcsCfg := infrastructure.DefaultGCSTestConfig()
	gcsSetup := infrastructure.NewGCSTestSetup(t, gcsCfg)
	defer gcsSetup.Stop()

	// Add more ledgers to datastore to create overlap with local window
	// Datastore will contain ledgers [5, 50]
	datastoreStart := uint32(5)
	datastoreEnd := uint32(50)
	gcsSetup.AddLedgers(datastoreStart, datastoreEnd)

	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		DatastoreConfigFunc: gcsSetup.DatastoreConfigFunc(),
		NoParallel:          true,
	})
	cl := test.GetRPCLient()

	// Helper to wait for health condition
	waitUntil := func(cond func(h protocol.GetHealthResponse) bool, timeout time.Duration) protocol.GetHealthResponse {
		var last protocol.GetHealthResponse
		require.Eventually(t, func() bool {
			resp, err := cl.GetHealth(t.Context())
			require.NoError(t, err)
			last = resp
			return cond(resp)
		}, timeout, 100*time.Millisecond, "last health: %+v", last)
		return last
	}

	// Wait for system to stabilize with local window overlapping datastore
	// We want: local oldest to be within datastore range to guarantee overlap
	// With 15-ledger window and datastoreEnd=50, local will be around [45-50, 60-65]
	health := waitUntil(func(h protocol.GetHealthResponse) bool {
		// Wait until oldest is close to datastore end (within 10 ledgers)
		return h.OldestLedger >= datastoreEnd-10 && h.OldestLedger <= datastoreEnd+5
	}, 30*time.Second)

	oldest := health.OldestLedger
	latest := health.LatestLedger

	require.GreaterOrEqual(t, latest, oldest, "latest >= oldest")
	require.LessOrEqual(t, oldest, datastoreEnd, "should have overlap: oldest <= datastoreEnd")

	// Helper to make requests
	request := func(start uint32, limit uint) (protocol.GetTransactionsResponse, error) {
		req := protocol.GetTransactionsRequest{
			StartLedger: start,
			Pagination:  &protocol.LedgerPaginationOptions{Limit: limit},
		}
		return cl.GetTransactions(t.Context(), req)
	}

	// Helper to get ledger sequences from response
	getSeqs := func(resp protocol.GetTransactionsResponse) []uint32 {
		out := make([]uint32, len(resp.Transactions))
		for i, tx := range resp.Transactions {
			out[i] = tx.Ledger
		}
		return out
	}

	// Helper to validate transaction data
	assertTransactionValid := func(t *testing.T, tx protocol.TransactionInfo) {
		require.NotEmpty(t, tx.TransactionHash, "missing hash")
		require.NotZero(t, tx.Ledger, "zero ledger")
		require.NotEmpty(t, tx.EnvelopeXDR, "missing envelope")
		require.NotEmpty(t, tx.ResultXDR, "missing result")
		require.NotEmpty(t, tx.ResultMetaXDR, "missing meta")
		require.Contains(t, []string{"SUCCESS", "FAILED"}, tx.Status, "invalid status")
	}

	// ========================================================================
	// Test 1: Datastore Only - fetch from below local range
	// ========================================================================
	t.Run("datastore_only", func(t *testing.T) {
		// Request ledger well below local range (in datastore only)
		startLedger := datastoreStart + 5
		require.Less(t, startLedger, oldest, "start should be below local range")

		res, err := request(startLedger, 5)
		require.NoError(t, err, "should fetch from datastore")
		require.NotEmpty(t, res.Transactions, "should return transactions")

		seqs := getSeqs(res)
		t.Logf("  Fetched ledgers: %v", seqs)

		// Verify all returned ledgers are below local range
		for _, seq := range seqs {
			require.Less(t, seq, oldest, "ledger %d should be below local range %d", seq, oldest)
		}

		// Validate transaction data
		for _, tx := range res.Transactions {
			assertTransactionValid(t, tx)
		}

		// Verify response metadata reflects local range
		require.Equal(t, oldest, res.OldestLedger, "OldestLedger should be local oldest")
		require.Equal(t, latest, res.LatestLedger, "LatestLedger should be local latest")
	})

	// ========================================================================
	// Test 2: Local Only - fetch from current local range
	// ========================================================================
	t.Run("local_only", func(t *testing.T) {
		// Request ledger above datastore range (local only)
		startLedger := datastoreEnd + 1
		if startLedger < oldest {
			startLedger = oldest
		}
		require.GreaterOrEqual(t, startLedger, oldest, "start should be in local range")

		res, err := request(startLedger, 5)
		require.NoError(t, err, "should fetch from local")
		require.NotEmpty(t, res.Transactions, "should return transactions")

		seqs := getSeqs(res)
		t.Logf("  Fetched ledgers: %v", seqs)

		// Verify all returned ledgers are in local range
		for _, seq := range seqs {
			require.GreaterOrEqual(t, seq, oldest, "ledger %d should be >= oldest %d", seq, oldest)
			require.LessOrEqual(t, seq, latest, "ledger %d should be <= latest %d", seq, latest)
		}

		// Validate transaction data
		for _, tx := range res.Transactions {
			assertTransactionValid(t, tx)
		}

		require.Equal(t, oldest, res.OldestLedger, "OldestLedger should be local oldest")
		require.Equal(t, latest, res.LatestLedger, "LatestLedger should be local latest")
	})

	// ========================================================================
	// Test 3: Mixed - fetch across datastore and local boundary
	// ========================================================================
	t.Run("mixed_datastore_and_local", func(t *testing.T) {
		// Start well before local range, request enough to cross into local
		startLedger := oldest - 5
		if startLedger < datastoreStart {
			startLedger = datastoreStart
		}
		require.Less(t, startLedger, oldest, "start should be in datastore range")

		// Request enough transactions to span both ranges
		res, err := request(startLedger, 20)
		require.NoError(t, err, "should fetch across boundary")
		require.NotEmpty(t, res.Transactions, "should return transactions")

		seqs := getSeqs(res)
		t.Logf("  Fetched ledgers: %v", seqs)
		t.Logf("  Boundary at ledger: %d", oldest)

		// Verify we got transactions from both sources
		hasDatastore := false
		hasLocal := false
		for _, seq := range seqs {
			if seq < oldest {
				hasDatastore = true
			}
			if seq >= oldest {
				hasLocal = true
			}
		}

		require.True(t, hasDatastore, "should have transactions from datastore (<%d)", oldest)
		require.True(t, hasLocal, "should have transactions from local (>=%d)", oldest)

		// Verify transactions are ordered
		for i := 1; i < len(seqs); i++ {
			require.LessOrEqual(t, seqs[i-1], seqs[i],
				"ledgers should be ordered: %d before %d", seqs[i-1], seqs[i])
		}

		// Validate all transaction data
		for _, tx := range res.Transactions {
			assertTransactionValid(t, tx)
		}

		require.Equal(t, oldest, res.OldestLedger, "OldestLedger should be local oldest")
		require.Equal(t, latest, res.LatestLedger, "LatestLedger should be local latest")
	})

	// ========================================================================
	// Error Cases
	// ========================================================================
	t.Run("below_datastore_floor", func(t *testing.T) {
		belowFloor := datastoreStart - 3

		res, err := request(belowFloor, 3)

		if err != nil {
			t.Logf("  Below floor returned error (expected): %v", err)
		} else {
			require.Empty(t, res.Transactions,
				"should return empty for ledger %d below datastore floor %d",
				belowFloor, datastoreStart)
		}
	})

	t.Run("beyond_latest", func(t *testing.T) {
		beyondLatest := latest + 100

		res, err := request(beyondLatest, 3)

		if err != nil {
			t.Logf("  Beyond latest returned error (expected): %v", err)
		} else {
			require.Empty(t, res.Transactions,
				"should return empty for ledger %d beyond latest %d",
				beyondLatest, latest)
		}
	})
}
