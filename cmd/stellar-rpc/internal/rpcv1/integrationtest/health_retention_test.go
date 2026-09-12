package integrationtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/limits"
)

func TestHealthRetentionWindow(t *testing.T) {
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{ApplyLimits: infrastructure.SkipLimitsUpgrade()})
	result, err := test.GetRPCLient().GetHealth(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint32(limits.OneDayOfLedgers), result.LedgerRetentionWindow)
}
