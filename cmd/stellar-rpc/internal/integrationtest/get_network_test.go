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
	assert.Positive(t, result.Limits.MaxContractSize)
	assert.Positive(t, result.Limits.Tx.MaxInstructions)
	assert.Positive(t, result.Limits.Ledger.MaxInstructions)
	assert.Positive(t, result.Limits.FeeTransactionSize1KB)
	assert.Positive(t, result.Limits.StateArchival.PersistentRentRateDenominator)
	// Core should refuse to boot if the following doesn't hold
	assert.Equal(t, result.ProtocolVersions.MaxSupportedProtocolVersion,
		result.ProtocolVersions.CoreSupportedProtocolVersion,
		"core supported protocol version and max supported ledger protocol versions out of sync")
}
