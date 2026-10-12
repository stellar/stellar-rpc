package infrastructure

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

// The test config is a copy of the sample with three deliberate differences.
// Comparing the two decoded files catches a sample default that changes
// without the copy following it.
func TestRPCv2ConfigParsesStrict(t *testing.T) {
	testCfg := decodeConfigFile(t, filepath.Join("docker", rpcv2ConfigFilename))
	sampleCfg := decodeConfigFile(t, filepath.Join("..", "..", "..", "rpcv2", "rpc-v2-sample-config.toml"))

	sampleCfg.Backfill.DataStore = config.DataStoreConfig{}
	enableDebug := true
	sampleCfg.Service.Preflight.EnableDebug = &enableDebug
	sampleCfg.Logging.Level = daemonLogLevel

	assert.Equal(t, sampleCfg.WithDefaults(), testCfg.WithDefaults())
}

func decodeConfigFile(t *testing.T, path string) config.Config {
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	cfg, err := config.DecodeConfig(data)
	require.NoError(t, err)
	return cfg
}
