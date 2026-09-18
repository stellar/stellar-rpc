package integrationtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func TestHealth(t *testing.T) {
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{ApplyLimits: infrastructure.SkipLimitsUpgrade()})
	result, err := test.GetRPCLient().GetHealth(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "healthy", result.Status)
	assert.Positive(t, result.OldestLedger)
	assert.Positive(t, result.LatestLedger)
	assert.GreaterOrEqual(t, result.LatestLedger, result.OldestLedger)
	assert.Positive(t, result.LatestLedgerCloseTime)
	assert.Positive(t, result.OldestLedgerCloseTime)
	assert.GreaterOrEqual(t, result.LatestLedgerCloseTime, result.OldestLedgerCloseTime)
}
