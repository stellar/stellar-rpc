package integrationtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func init() {
	// Initialize variables to non-empty values
	config.CommitHash = "commitHash"
	config.BuildTimestamp = "buildTimestamp"
}

func TestGetVersionInfoSucceeds(t *testing.T) {
	test := infrastructure.NewTest(t, nil)

	result, err := test.GetRPCLient().GetVersionInfo(context.Background())
	assert.NoError(t, err)

	assert.Equal(t, "0.0.0", result.Version)
	assert.Equal(t, "buildTimestamp", result.BuildTimestamp)
	assert.Equal(t, "commitHash", result.CommitHash)
	assert.Equal(t, test.GetProtocolVersion(), result.ProtocolVersion)
	assert.NotEmpty(t, result.CaptiveCoreVersion)
}
