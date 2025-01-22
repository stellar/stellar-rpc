package integrationtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/protocol"
)

func TestGetLedgers(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	client := test.GetRPCLient()

	// Get all ledgers
	request := protocol.GetLedgersRequest{
		StartLedger: 8,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 3,
		},
	}

	result, err := client.GetLedgers(context.Background(), request)
	require.NoError(t, err)
	assert.Len(t, result.Ledgers, 3)
	prevLedgers := result.Ledgers

	// Get ledgers using previous result's cursor
	request = protocol.GetLedgersRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Cursor: result.Cursor,
			Limit:  2,
		},
	}
	result, err = client.GetLedgers(context.Background(), request)
	require.NoError(t, err)
	assert.Len(t, result.Ledgers, 2)
	assert.Equal(t, prevLedgers[len(prevLedgers)-1].Sequence+1, result.Ledgers[0].Sequence)

	// Test with JSON format
	request = protocol.GetLedgersRequest{
		StartLedger: 8,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 1,
		},
		Format: protocol.FormatJSON,
	}
	result, err = client.GetLedgers(context.Background(), request)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Ledgers[0].LedgerHeaderJSON)
	assert.NotEmpty(t, result.Ledgers[0].LedgerMetadataJSON)

	// Test invalid requests
	invalidRequests := []protocol.GetLedgersRequest{
		{StartLedger: result.OldestLedger - 1},
		{StartLedger: result.LatestLedger + 1},
		{
			Pagination: &protocol.LedgerPaginationOptions{
				Cursor: "invalid",
			},
		},
	}

	for _, req := range invalidRequests {
		_, err = client.GetLedgers(context.Background(), req)
		assert.Error(t, err)
	}
}
