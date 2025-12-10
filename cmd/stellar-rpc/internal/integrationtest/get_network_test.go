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
	assert.GreaterOrEqual(t, result.ProtocolVersion, 24)
	assert.GreaterOrEqual(t, result.CoreSupportedProtocolVersion, 25)
}
