package config

import (
	"runtime"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/network"
)

func TestLoadConfigPathPrecedence(t *testing.T) {
	var cfg Config

	cmd := &cobra.Command{}
	require.NoError(t, cfg.AddFlags(cmd))
	require.NoError(t, cmd.ParseFlags([]string{
		"--config-path", "./test.soroban.rpc.config",
		"--stellar-core-binary-path", "/usr/overridden/stellar-core",
		"--network-passphrase", "CLI test passphrase",
	}))

	require.NoError(t, cfg.SetValues(func(key string) (string, bool) {
		switch key {
		case "STELLAR_CORE_BINARY_PATH":
			return "/env/stellar-core", true
		case "DB_PATH":
			return "/env/overridden/db", true
		default:
			return "", false
		}
	}))
	require.NoError(t, cfg.Validate())

	assert.Equal(t, "/opt/stellar/stellar-rpc/etc/stellar-captive-core.cfg", cfg.CaptiveCoreConfigPath,
		"should read values from the config path file")
	assert.Equal(t, "CLI test passphrase", cfg.NetworkPassphrase, "cli flags should override --config-path values")
	assert.Equal(t, "/usr/overridden/stellar-core", cfg.StellarCoreBinaryPath,
		"cli flags should override --config-path values and env vars")
	assert.Equal(t, "/env/overridden/db", cfg.SQLiteDBPath, "env var should override config file")
	assert.Equal(t, 2*time.Second, cfg.CoreRequestTimeout, "default value should be used, if not set anywhere else")
}

func TestConfigLoadDefaults(t *testing.T) {
	// Set up a default config
	cfg := Config{}
	require.NoError(t, cfg.loadDefaults())

	// Check that the defaults are set
	assert.Equal(t, defaultHTTPEndpoint, cfg.Endpoint)
	assert.Equal(t, uint(runtime.NumCPU()), cfg.PreflightWorkerCount)
}

func TestConfigExtendedUserAgent(t *testing.T) {
	var cfg Config
	require.NoError(t, cfg.loadDefaults())
	assert.Equal(t, "stellar-rpc/0.0.0/123", cfg.ExtendedUserAgent("123"))
}

func TestConfigLoadFlagsDefaultValuesOverrideExisting(t *testing.T) {
	// Set up a config with an existing non-default value
	cfg := Config{
		NetworkPassphrase: "existing value",
		LogLevel:          logrus.InfoLevel,
		Endpoint:          "localhost:8000",
	}

	cmd := &cobra.Command{}
	require.NoError(t, cfg.AddFlags(cmd))
	// Set up a flag set with the default value
	require.NoError(t, cmd.ParseFlags([]string{
		"--network-passphrase", "",
		"--log-level", logrus.PanicLevel.String(),
	}))

	// Load the flags
	require.NoError(t, cfg.loadFlags())

	// Check that the flag value is set
	assert.Equal(t, "", cfg.NetworkPassphrase)
	assert.Equal(t, logrus.PanicLevel, cfg.LogLevel)

	// Check it didn't overwrite values which were not set in the flags
	assert.Equal(t, "localhost:8000", cfg.Endpoint)
}

func TestConfigLoadNetworkOption(t *testing.T) {
	// Generate structs networkParameters{networkName, historyArchiveURLs, networkPassphrase} for testnet and pubnet
	networkFlagOptions := generateNetworkParameters()

	for _, networkFlagOption := range networkFlagOptions {
		var cfg Config

		// Part confirming network option writes historyArchiveURLs and networkPassphrase
		cmd := &cobra.Command{}
		require.NoError(t, cfg.AddFlags(cmd))
		require.NoError(t, cmd.ParseFlags([]string{
			"--stellar-core-binary-path", "/usr/overridden/stellar-core",
			"--network", networkFlagOption.networkName,
		}))

		require.NoError(t, cfg.SetValues(func(_ string) (string, bool) {
			return "", false
		}))
		require.NoError(t, cfg.Validate())

		assert.Equal(t, cfg.HistoryArchiveURLs, networkFlagOption.historyArchiveURLs,
			"network flag should write historyArchiveURLs")
		assert.Equal(t, cfg.NetworkPassphrase, networkFlagOption.networkPassphrase,
			"network flag should write networkPassphrase")

		// Part confirming network option conflicts with networkPassphrase and/or historyArchiveURLs
		cmd = &cobra.Command{}
		require.NoError(t, cfg.AddFlags(cmd))
		require.NoError(t, cmd.ParseFlags([]string{
			"--stellar-core-binary-path", "/usr/overridden/stellar-core",
			"--network", networkFlagOption.networkName,
			"--network-passphrase", "should-not-be-set-with-network-flag",
			"--history-archive-urls", "should-not-be-set-with-network-flag",
		}))
		require.Error(t, cfg.SetValues(func(_ string) (string, bool) {
			return "", false
		}), "should not be able to set network option along with network-passphrase and/or history-archive-URLs")
	}
}

// Helper that generates a slice of structs containing relevant network testing parameters
func generateNetworkParameters() []networkParameters {
	testnet := networkParameters{
		networkName:        "testnet",
		historyArchiveURLs: network.TestNetworkhistoryArchiveURLs,
		networkPassphrase:  network.TestNetworkPassphrase,
	}
	pubnet := networkParameters{
		networkName:        "pubnet",
		historyArchiveURLs: network.PublicNetworkhistoryArchiveURLs,
		networkPassphrase:  network.PublicNetworkPassphrase,
	}
	futurenet := networkParameters{
		networkName:        "futurenet",
		historyArchiveURLs: []string{"http://history.stellar.org/dev/core-futurenet/core_futurenet_001/", "http://history.stellar.org/dev/core-futurenet/core_futurenet_002/", "http://history.stellar.org/dev/core-futurenet/core_futurenet_003/"},
		networkPassphrase:  "Test SDF Future Network ; October 2022",
	}
	return []networkParameters{testnet, pubnet, futurenet}
}

type networkParameters struct {
	networkName        string
	historyArchiveURLs []string
	networkPassphrase  string
}
