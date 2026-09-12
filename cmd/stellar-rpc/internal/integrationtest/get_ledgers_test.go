package integrationtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	client "github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func testGetLedgers(t *testing.T, client *client.Client) {
	// The test reads five ledgers, then pages past them with a cursor, so the
	// network must be far enough ahead that the second page has something in
	// it. Five ledgers is exactly the first page and leaves nothing over.
	var ledgerCount uint
	var oldestLedger uint32

	for ledgerCount < 15 {
		health, err := client.GetHealth(t.Context())
		require.NoError(t, err)

		ledgerCount = uint(health.LatestLedger) - uint(health.OldestLedger) + 1
		oldestLedger = health.OldestLedger

		time.Sleep(time.Second)
	}

	// Get first group of ledgers
	request := protocol.GetLedgersRequest{
		StartLedger: oldestLedger,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 5,
		},
	}

	result, err := client.GetLedgers(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, result.Ledgers, 5)
	prevLedgers := result.Ledgers

	// Get ledgers using previous result's cursor
	request = protocol.GetLedgersRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Cursor: result.Cursor,
			Limit:  8,
		},
	}
	result, err = client.GetLedgers(t.Context(), request)
	require.NoError(t, err)
	require.NotEmpty(t, result.Ledgers)
	require.LessOrEqual(t, len(result.Ledgers), 8)
	require.Equal(t, prevLedgers[len(prevLedgers)-1].Sequence+1, result.Ledgers[0].Sequence)

	// Test with JSON format
	request = protocol.GetLedgersRequest{
		StartLedger: oldestLedger + 1,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 1,
		},
		Format: protocol.FormatJSON,
	}
	result, err = client.GetLedgers(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, result.Ledgers, 1)
	require.NotEmpty(t, result.Ledgers[0].LedgerHeaderJSON)
	require.NotEmpty(t, result.Ledgers[0].LedgerMetadataJSON)

	// Test invalid requests
	invalidRequests := []protocol.GetLedgersRequest{
		{StartLedger: result.OldestLedger - 4}, // -3 to exceed data store
		// Far beyond the latest ledger: a ledger closes every second on this
		// network, so latest+1 can exist by the time the request arrives.
		{StartLedger: result.LatestLedger + 1000},
		{
			Pagination: &protocol.LedgerPaginationOptions{
				Cursor: "invalid",
			},
		},
		{
			Pagination: &protocol.LedgerPaginationOptions{
				Limit: 100_000,
			},
		},
	}

	for _, req := range invalidRequests {
		_, err = client.GetLedgers(t.Context(), req)
		require.Error(t, err, "request: %+v (oldest: %d, latest: %d)",
			req, result.OldestLedger, result.LatestLedger)
	}
}

func TestGetLedgers(t *testing.T) {
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{ApplyLimits: infrastructure.SkipLimitsUpgrade()})
	client := test.GetRPCLient()
	testGetLedgers(t, client)
}
