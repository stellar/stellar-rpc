package infrastructure

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

func TestRPCv2ConfigParsesStrict(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("docker", rpcv2ConfigFilename))
	require.NoError(t, err)

	cfg, err := config.ParseConfig(data)
	require.NoError(t, err)

	assert.Empty(t, cfg.Backfill.DataStore.Type, "the test config must not reach a datastore")
	assert.True(t, *cfg.Service.Preflight.EnableDebug)
	assert.Equal(t, "debug", cfg.Logging.Level)
	assert.Equal(t, config.DefaultMaxHealthyLedgerLatency, *cfg.Service.Methods.GetHealth.MaxHealthyLedgerLatency)
	assert.Equal(t, config.DefaultClassicFeeWindowLedgers, *cfg.Service.FeeStats.ClassicFeeWindowLedgers)
}
