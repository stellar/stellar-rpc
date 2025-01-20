package integrationtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/protocol"
)

func TestGetLedgers(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	client := test.GetRPCLient()

	// Get all ledgers
	request := methods.GetLedgersRequest{
		StartLedger: 8,
		Pagination: &methods.LedgerPaginationOptions{
			Limit: 3,
		},
	}
	var result methods.GetLedgersResponse
	err := client.CallResult(context.Background(), "getLedgers", request, &result)
	require.NoError(t, err)
	assert.Len(t, result.Ledgers, 3)
	prevLedgers := result.Ledgers

	// Get ledgers using previous result's cursor
	request = methods.GetLedgersRequest{
		Pagination: &methods.LedgerPaginationOptions{
			Cursor: result.Cursor,
			Limit:  2,
		},
	}
	err = client.CallResult(context.Background(), "getLedgers", request, &result)
	require.NoError(t, err)
	assert.Len(t, result.Ledgers, 2)
	assert.Equal(t, prevLedgers[len(prevLedgers)-1].Sequence+1, result.Ledgers[0].Sequence)

	// Test with JSON format
	request = methods.GetLedgersRequest{
		StartLedger: 8,
		Pagination: &methods.LedgerPaginationOptions{
			Limit: 1,
		},
		Format: protocol.FormatJSON,
	}
	err = client.CallResult(context.Background(), "getLedgers", request, &result)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Ledgers[0].LedgerHeaderJSON)
	assert.NotEmpty(t, result.Ledgers[0].LedgerMetadataJSON)

	// Test invalid requests
	invalidRequests := []methods.GetLedgersRequest{
		{StartLedger: result.OldestLedger - 1},
		{StartLedger: result.LatestLedger + 1},
		{
			Pagination: &methods.LedgerPaginationOptions{
				Cursor: "invalid",
			},
		},
	}

	for _, req := range invalidRequests {
		err = client.CallResult(context.Background(), "getLedgers", req, &result)
		assert.Error(t, err)
	}
}
