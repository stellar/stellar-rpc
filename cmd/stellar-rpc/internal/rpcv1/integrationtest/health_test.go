package integrationtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/limits"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure"
)

func TestHealth(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	result, err := test.GetRPCLient().GetHealth(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "healthy", result.Status)
	assert.Equal(t, uint32(limits.OneDayOfLedgers), result.LedgerRetentionWindow)
	assert.Positive(t, result.OldestLedger)
	assert.Positive(t, result.LatestLedger)
	assert.GreaterOrEqual(t, result.LatestLedger, result.OldestLedger)
	assert.Positive(t, result.LatestLedgerCloseTime)
	assert.Positive(t, result.OldestLedgerCloseTime)
	assert.GreaterOrEqual(t, result.LatestLedgerCloseTime, result.OldestLedgerCloseTime)
}
