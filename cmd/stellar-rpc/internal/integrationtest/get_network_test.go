package integrationtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func TestGetNetworkSucceeds(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	result, err := client.GetNetwork(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, infrastructure.FriendbotURL, result.FriendbotURL)
	assert.Equal(t, infrastructure.StandaloneNetworkPassphrase, result.Passphrase)
	assert.GreaterOrEqual(t, result.ProtocolVersions.MaxSupportedProtocolVersion, 24)
	assert.Greater(t, result.Limits.MaxContractSize, 0)
	assert.Greater(t, result.Limits.Tx.MaxInstructions, 0)
	assert.Greater(t, result.Limits.Ledger.MaxInstructions, 0)
	assert.Greater(t, result.Limits.FeeTransactionSize1KB, 0)
	assert.Greater(t, result.Limits.StateArchival.PersistentRentRateDenominator, 0)
	// Core should refuse to boot if the following doesn't hold
	assert.Equal(t, result.ProtocolVersions.MaxSupportedProtocolVersion, result.ProtocolVersions.CoreSupportedProtocolVersion)
}
