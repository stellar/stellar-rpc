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

func TestGetTransactionsDataStore1(t *testing.T) {
	gcsCfg := infrastructure.DefaultGCSTestConfig()
	gcsSetup := infrastructure.NewGCSTestSetup(t, gcsCfg)
	defer gcsSetup.Stop()

	// add files to GCS
	gcsSetup.AddLedgers(5, 40)

	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		DatastoreConfigFunc: gcsSetup.DatastoreConfigFunc(),
		NoParallel:          true,
	})
	client := test.GetRPCLient() // at this point we're at like ledger 30

	waitUntil := func(cond func(h protocol.GetHealthResponse) bool, timeout time.Duration) protocol.GetHealthResponse {
		var last protocol.GetHealthResponse
		require.Eventually(t, func() bool {
			resp, err := client.GetHealth(t.Context())
			require.NoError(t, err)
			last = resp
			return cond(resp)
		}, timeout, 100*time.Millisecond, "last health: %+v", last)
		return last
	}
	ledgers := sendTransactions(t, client)

	request := func(start uint32, limit uint, cursor string) (protocol.GetTransactionsResponse, error) {
		req := protocol.GetTransactionsRequest{
			StartLedger: start,
			Pagination: &protocol.LedgerPaginationOptions{
				Limit:  limit,
				Cursor: cursor,
			},
		}
		return client.GetTransactions(t.Context(), req)
	}

	// ensure oldest > 40 so datastore set ([35..40]) is below local window
	health := waitUntil(func(h protocol.GetHealthResponse) bool {
		return uint(h.OldestLedger) > 40
	}, 30*time.Second)

	oldest := health.OldestLedger
	latest := health.LatestLedger
	require.GreaterOrEqual(t, latest, oldest)

	getSeqs := func(resp protocol.GetTransactionsResponse) []uint32 {
		out := make([]uint32, len(resp.Transactions))
		for i, l := range resp.Transactions {
			out[i] = l.TransactionDetails.Ledger
		}
		return out
	}
	// --- 1) datastore-only: entirely below oldest ---
	t.Run("datastore_only", func(t *testing.T) {
		res, err := request(35, 3, "")
		require.NoError(t, err)
		require.Len(t, res.Transactions, 3)
		require.Equal(t, []uint32{35, 35, 36}, getSeqs(res))
	})

	// --- 2) local-only: entirely at/above oldest ---
	t.Run("local_only", func(t *testing.T) {
		limit := 3
		res, err := request(ledgers[0], uint(limit), "")
		require.NoError(t, err)
		require.Len(t, res.Transactions, 3)
	})

	// --- 3) mixed: cross boundary (datastore then local) ---
	t.Run("mixed_datastore_and_local", func(t *testing.T) {
		// 39,40 from datastore; 41,42 from local
		require.GreaterOrEqual(t, latest, uint32(42), "need latest >= 42")
		res, err := request(39, 4, "")
		require.NoError(t, err)
		require.Len(t, res.Transactions, 4)

		// verify cursor continuity across boundary
		next, err := request(0, 2, res.Cursor)
		require.NoError(t, err)
		if len(next.Transactions) > 0 {
			require.EqualValues(t, 43, next.Transactions[0].TransactionHash)
		}
	})

	// --- 4) negative: below datastore floor (not available anywhere) ---
	t.Run("negative_below_datastore_floor", func(t *testing.T) {
		res, err := request(2, 3, "")
		// accept either an error or an empty page; but never data
		if err != nil {
			return
		}
		require.Empty(t, res.Transactions, "expected no ledgers when requesting below datastore floor")
	})

	// --- 5) negative: beyond latest ---
	t.Run("negative_beyond_latest", func(t *testing.T) {
		res, err := request(latest+1, 1, "")
		if err != nil {
			return
		}
		require.Empty(t, res.Transactions, "expected no ledgers when requesting beyond latest")
	})
}

/*
func TestGetTransactionsDataStore1(t *testing.T) {
	gcsCfg := infrastructure.DefaultGCSTestConfig()
	gcsSetup := infrastructure.NewGCSTestSetup(t, gcsCfg)
	defer gcsSetup.Stop()

	// add files to GCS
	gcsSetup.AddLedgers(5, 42)

	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		DatastoreConfigFunc: gcsSetup.DatastoreConfigFunc(),
		NoParallel:          true,
	})
	client := test.GetRPCLient()

	waitUntil := func(cond func(h protocol.GetHealthResponse) bool, timeout time.Duration) protocol.GetHealthResponse {
		var last protocol.GetHealthResponse
		require.Eventually(t, func() bool {
			resp, err := client.GetHealth(t.Context())
			require.NoError(t, err)
			last = resp
			return cond(resp)
		}, timeout, 100*time.Millisecond, "last health: %+v", last)
		return last
	}


	request := func(start uint32, limit uint, cursor string) (protocol.GetTransactionsResponse, error) {
		req := protocol.GetTransactionsRequest{
			StartLedger: start,
			Pagination: &protocol.LedgerPaginationOptions{
				Limit:  limit,
				Cursor: cursor,
			},
		}
		return client.GetTransactions(t.Context(), req)
	}

	// Helper to extract ledger sequences from response
	getSeqs := func(resp protocol.GetTransactionsResponse) []uint32 {
		out := make([]uint32, len(resp.Transactions))
		for i, tx := range resp.Transactions {
			out[i] = tx.TransactionDetails.Ledger
		}
		return out
	}

	// Helper to verify transactions are properly ordered
	assertTransactionsOrdered := func(t *testing.T, txs []protocol.TransactionInfo, desc string) {
		for i := 1; i < len(txs); i++ {
			prev := txs[i-1].TransactionDetails
			curr := txs[i].TransactionDetails
			// Ledger should be non-decreasing
			require.LessOrEqual(t, prev.Ledger, curr.Ledger,
				"%s: transactions not ordered - ledger %d after %d", desc, curr.Ledger, prev.Ledger)
			// If same ledger, verify we have proper transaction indices/ordering
			if prev.Ledger == curr.Ledger {
				// Transactions within same ledger should maintain order
				require.NotEmpty(t, prev.TransactionHash, "%s: missing transaction hash", desc)
				require.NotEmpty(t, curr.TransactionHash, "%s: missing transaction hash", desc)
			}
		}
	}

	// Helper to verify transaction data integrity
	assertTransactionValid := func(t *testing.T, tx protocol.TransactionInfo, desc string) {
		require.NotEmpty(t, tx.TransactionHash, "%s: missing transaction hash", desc)
		require.NotZero(t, tx.TransactionDetails.Ledger, "%s: zero ledger number", desc)
		require.NotEmpty(t, tx.TransactionDetails.EnvelopeXDR, "%s: missing envelope XDR", desc)
		require.NotEmpty(t, tx.TransactionDetails.ResultXDR, "%s: missing result XDR", desc)
		require.NotEmpty(t, tx.TransactionDetails.ResultMetaXDR, "%s: missing result meta XDR", desc)
	}

	// Ensure oldest > 40 so datastore set ([5..40]) is below local window
	health := waitUntil(func(h protocol.GetHealthResponse) bool {
		return uint(h.OldestLedger) > 40
	}, 30*time.Second)

	oldest := health.OldestLedger
	latest := health.LatestLedger
	require.GreaterOrEqual(t, latest, oldest, "latest should be >= oldest")
	require.Greater(t, oldest, uint32(40), "oldest should be > 40 for this test")


	// --- 1) Datastore-only: entirely below oldest ---
	t.Run("datastore_only", func(t *testing.T) {
		res, err := request(35, 3, "")
		require.NoError(t, err, "should successfully fetch from datastore")
		require.Len(t, res.Transactions, 3, "should return exactly 3 transactions")

		seqs := getSeqs(res)
		require.Equal(t, []uint32{35, 35, 36}, seqs, "should get transactions from ledgers 35-36")

		// Verify transaction data integrity
		for i, tx := range res.Transactions {
			assertTransactionValid(t, tx, fmt.Sprintf("datastore tx[%d]", i))
		}
		assertTransactionsOrdered(t, res.Transactions, "datastore-only")

		// Verify cursor is present and non-empty for pagination
		require.NotEmpty(t, res.Cursor, "should have cursor for pagination")

		// Verify ledger range in response
		require.GreaterOrEqual(t, res.OldestLedger, uint32(40), "oldest ledger should match request")
		require.GreaterOrEqual(t, res.LatestLedger, uint32(40), "latest should include returned ledgers")
	})

	// --- 2) Local-only: entirely at/above oldest ---
	t.Run("local_only", func(t *testing.T) {
		ledgers := sendTransactions(t, client)
		require.NotEmpty(t, ledgers, "should have sent some transactions")
		limit := 3
		startLedger := ledgers[0]
		require.GreaterOrEqual(t, startLedger, oldest, "test ledger should be in local range")

		res, err := request(startLedger, uint(limit), "")
		require.NoError(t, err, "should successfully fetch from local storage")
		require.Len(t, res.Transactions, 3, "should return exactly 3 transactions")

		// Verify ledger numbers are in the local range
		seqs := getSeqs(res)
		for _, seq := range seqs {
			require.GreaterOrEqual(t, seq, oldest, "local tx should be >= oldest ledger")
		}

		// Verify all start at or after the requested start
		require.GreaterOrEqual(t, seqs[0], startLedger, "first tx should be >= start ledger")

		// Verify transaction data and ordering
		for i, tx := range res.Transactions {
			assertTransactionValid(t, tx, fmt.Sprintf("local tx[%d]", i))
		}
		assertTransactionsOrdered(t, res.Transactions, "local-only")

		require.NotEmpty(t, res.Cursor, "should have cursor for pagination")
	})

	// --- 3) Mixed: cross boundary (datastore then local) ---
	t.Run("mixed_datastore_and_local", func(t *testing.T) {
		// Request starting from the datastore range, crossing into the local range
		ledgers := sendTransactions(t, client)
		require.NotEmpty(t, ledgers, "should have sent some transactions")
		startLedger := uint32(39)
		//require.Less(t, startLedger, oldest, "start should be in datastore range")
		//require.GreaterOrEqual(t, latest, uint32(42), "need latest >= 42 for this test")

		res, err := request(startLedger, 15, "")
		require.NoError(t, err, "should successfully fetch across boundary")
	//	require.Len(t, res.Transactions, 15, "should return exactly 15 transactions")

		seqs := getSeqs(res)
		require.Equal(t, startLedger, seqs[0], "should start at requested ledger")

		// Verify we got data from both sources
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
		require.True(t, hasDatastore, "should have transactions from datastore")
		require.True(t, hasLocal, "should have transactions from local storage")

		// Verify transaction validity and ordering across boundary
		for i, tx := range res.Transactions {
			assertTransactionValid(t, tx, fmt.Sprintf("mixed tx[%d]", i))
		}
		assertTransactionsOrdered(t, res.Transactions, "mixed datastore+local")

		// Verify cursor continuity across boundary
		require.NotEmpty(t, res.Cursor, "should have cursor")
		next, err := request(0, 2, res.Cursor)
		require.NoError(t, err, "cursor should work for next page")

		if len(next.Transactions) > 0 {
			// Next page should continue from where we left off
			lastSeq := seqs[len(seqs)-1]
			nextSeqs := getSeqs(next)
			require.GreaterOrEqual(t, nextSeqs[0], lastSeq,
				"next page should continue from last ledger %d, got %d", lastSeq, nextSeqs[0])
		}
	})

	// --- 4) Pagination across boundary ---
	t.Run("pagination_across_boundary", func(t *testing.T) {
		// Start well before boundary, paginate through it
		startLedger := uint32(38)
		require.Less(t, startLedger, oldest, "start in datastore range")

		var allSeqs []uint32
		cursor := ""
		pageCount := 0

		for pageCount < 5 { // limit iterations to prevent infinite loop
			res, err := request(startLedger, 2, cursor)
			require.NoError(t, err, "pagination request should succeed")

			if len(res.Transactions) == 0 {
				break
			}

			seqs := getSeqs(res)
			allSeqs = append(allSeqs, seqs...)
			assertTransactionsOrdered(t, res.Transactions, fmt.Sprintf("page %d", pageCount))

			if res.Cursor == "" {
				break
			}
			cursor = res.Cursor
			startLedger = 0 // Use cursor for subsequent requests
			pageCount++
		}

		require.NotEmpty(t, allSeqs, "should have retrieved some transactions")

		// Verify overall ordering across all pages
		for i := 1; i < len(allSeqs); i++ {
			require.LessOrEqual(t, allSeqs[i-1], allSeqs[i],
				"sequences across pages should be ordered: page had %d after %d",
				allSeqs[i], allSeqs[i-1])
		}

		// Verify we crossed the boundary
		require.Contains(t, allSeqs, uint32(39), "should include ledger before boundary")
		require.True(t, func() bool {
			for _, seq := range allSeqs {
				if seq >= oldest {
					return true
				}
			}
			return false
		}(), "should include ledgers after boundary (>= %d)", oldest)
	})

	// --- 5) Edge case: request exactly at boundary ---
	t.Run("exactly_at_boundary", func(t *testing.T) {
		res, err := request(oldest, 3, "")
		require.NoError(t, err, "requesting at oldest should succeed")
		require.NotEmpty(t, res.Transactions, "should return transactions at oldest ledger")

		seqs := getSeqs(res)
		require.GreaterOrEqual(t, seqs[0], oldest, "should start at or after oldest")
		assertTransactionsOrdered(t, res.Transactions, "at-boundary")
	})

	// --- 6) Large limit spanning multiple sources ---
	t.Run("large_limit_spanning_sources", func(t *testing.T) {
		res, err := request(37, 20, "")
		require.NoError(t, err, "large limit should succeed")

		if len(res.Transactions) > 0 {
			seqs := getSeqs(res)
			require.LessOrEqual(t, seqs[0], uint32(37), "should start at or after requested")
			assertTransactionsOrdered(t, res.Transactions, "large-limit")

			// Should span both sources
			minSeq := seqs[0]
			maxSeq := seqs[len(seqs)-1]
			if minSeq < oldest && maxSeq >= oldest {
				t.Logf("Successfully retrieved data spanning boundary: [%d..%d] across boundary at %d",
					minSeq, maxSeq, oldest)
			}
		}
	})

	// --- 7) Invalid cursor ---
	t.Run("invalid_cursor", func(t *testing.T) {
		_, err := request(oldest, 1, "invalid-cursor-12345")
		// Should either return error or handle gracefully
		if err == nil {
			t.Log("Invalid cursor handled gracefully (no error)")
		} else {
			t.Logf("Invalid cursor rejected with error: %v", err)
		}
	})

	// --- 8) Zero/invalid limits ---
	t.Run("zero_limit", func(t *testing.T) {
		res, err := request(oldest, 0, "")
		if err == nil {
			// If no error, should return empty or use default limit
			t.Logf("Zero limit returned %d transactions", len(res.Transactions))
		}
	})

	// --- 9) Negative: below datastore floor ---
	t.Run("negative_below_datastore_floor", func(t *testing.T) {
		// Ledger 2 is before our datastore range starts (5-40)
		res, err := request(2, 3, "")
		if err != nil {
			t.Logf("Request below datastore floor returned expected error: %v", err)
			return
		}

		// If no error, should return empty
		require.Empty(t, res.Transactions,
			"expected no ledgers when requesting below datastore floor (ledger 2 < 5)")

		// Cursor should indicate no more data
		if res.Cursor != "" {
			next, err := request(0, 1, res.Cursor)
			if err == nil {
				require.Empty(t, next.Transactions, "cursor from below-floor should not return data")
			}
		}
	})

	// --- 10) Negative: beyond latest ---
	t.Run("negative_beyond_latest", func(t *testing.T) {
		beyondLatest := latest + 100
		res, err := request(beyondLatest, 1, "")
		if err != nil {
			t.Logf("Request beyond latest returned expected error: %v", err)
			return
		}

		require.Empty(t, res.Transactions,
			"expected no ledgers when requesting beyond latest (requested %d, latest %d)",
			beyondLatest, latest)
	})

	// --- 11) Multiple full pages in datastore-only range ---
	t.Run("multiple_pages_datastore_only", func(t *testing.T) {
		startLedger := uint32(10)
		require.Less(t, startLedger, oldest, "should be in datastore range")

		// Get first page
		page1, err := request(startLedger, 2, "")
		require.NoError(t, err)
		if len(page1.Transactions) == 0 {
			t.Skip("No transactions in datastore range")
		}

		require.NotEmpty(t, page1.Cursor, "should have cursor for next page")

		// Get second page using cursor
		page2, err := request(0, 2, page1.Cursor)
		require.NoError(t, err)

		if len(page2.Transactions) > 0 {
			seqs1 := getSeqs(page1)
			seqs2 := getSeqs(page2)

			// Verify pages don't overlap
			lastOfPage1 := seqs1[len(seqs1)-1]
			firstOfPage2 := seqs2[0]
			require.LessOrEqual(t, lastOfPage1, firstOfPage2,
				"pages should not overlap: page1 ends at %d, page2 starts at %d",
				lastOfPage1, firstOfPage2)
		}
	})

	// --- 12) Request with start=0 (should use oldest?) ---
	t.Run("start_zero", func(t *testing.T) {
		res, err := request(0, 3, "")
		if err != nil {
			t.Logf("start=0 returned error: %v", err)
			return
		}

		if len(res.Transactions) > 0 {
			seqs := getSeqs(res)
			t.Logf("start=0 returned ledgers starting at %d (oldest=%d)", seqs[0], oldest)
		}
	})
}
*/

// TestGetTransactionsDataStore tests fetching transactions from datastore and local storage.
// Setup creates an overlap to guarantee no gaps: datastore=[5,50], local=[~45,~60]
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
	client := test.GetRPCLient()

	// Helper to wait for health condition
	waitUntil := func(cond func(h protocol.GetHealthResponse) bool, timeout time.Duration) protocol.GetHealthResponse {
		var last protocol.GetHealthResponse
		require.Eventually(t, func() bool {
			resp, err := client.GetHealth(t.Context())
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
		return client.GetTransactions(t.Context(), req)
	}

	// Helper to get ledger sequences from response
	getSeqs := func(resp protocol.GetTransactionsResponse) []uint32 {
		out := make([]uint32, len(resp.Transactions))
		for i, tx := range resp.Transactions {
			out[i] = tx.TransactionDetails.Ledger
		}
		return out
	}

	// Helper to validate transaction data
	assertTransactionValid := func(t *testing.T, tx protocol.TransactionInfo) {
		require.NotEmpty(t, tx.TransactionHash, "missing hash")
		require.NotZero(t, tx.TransactionDetails.Ledger, "zero ledger")
		require.NotEmpty(t, tx.TransactionDetails.EnvelopeXDR, "missing envelope")
		require.NotEmpty(t, tx.TransactionDetails.ResultXDR, "missing result")
		require.NotEmpty(t, tx.TransactionDetails.ResultMetaXDR, "missing meta")
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
