package integrationtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

func setVersionInfoForTests() {
	version.CommitHash = "commitHash"
	version.BuildTimestamp = "buildTimestamp"
}

func TestGetVersionInfoSucceeds(t *testing.T) {
	setVersionInfoForTests()
	test := infrastructure.NewTest(t, nil)

	result, err := test.GetRPCLient().GetVersionInfo(t.Context())
	require.NoError(t, err)

	assert.Equal(t, "0.0.0", result.Version)
	assert.Equal(t, "buildTimestamp", result.BuildTimestamp)
	assert.Equal(t, "commitHash", result.CommitHash)
	assert.EqualValues(t, test.GetProtocolVersion(), result.ProtocolVersion)
	assert.NotEmpty(t, result.CaptiveCoreVersion)
}
