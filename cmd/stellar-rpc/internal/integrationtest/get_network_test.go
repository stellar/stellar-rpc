package integrationtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func TestGetNetworkSucceeds(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	client := test.GetRPCLient()

	result, err := client.GetNetwork(t.Context())
	require.NoError(t, err)
	assert.Equal(t, infrastructure.FriendbotURL, result.FriendbotURL)
	assert.Equal(t, infrastructure.StandaloneNetworkPassphrase, result.Passphrase)
	assert.GreaterOrEqual(t, result.ProtocolVersion, 20)
}
