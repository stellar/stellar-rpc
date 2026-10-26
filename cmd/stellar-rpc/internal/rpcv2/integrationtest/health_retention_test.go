package integrationtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func TestHealthRetentionWindow(t *testing.T) {
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{ApplyLimits: infrastructure.SkipLimitsUpgrade()})
	result, err := test.GetRPCLient().GetHealth(t.Context())
	require.NoError(t, err)
	// retention_chunks = 0 is full history, which the API reports as no window.
	assert.Equal(t, uint32(0), result.LedgerRetentionWindow)
}
