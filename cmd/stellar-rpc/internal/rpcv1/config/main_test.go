package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
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
	assert.Equal(t, uint(runtime.GOMAXPROCS(0)), cfg.PreflightWorkerCount)
}

// The concurrency defaults follow GOMAXPROCS, the CPUs the process may use,
// which can be fewer than the machine has.
func TestConfigConcurrencyDefaultsFollowGOMAXPROCS(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(3))
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte("MAX_REQUEST_BATCH_SIZE = 1\n"), 0o600))
	cfg := Config{}
	require.NoError(t, cfg.SetValues(func(key string) (string, bool) {
		return path, key == "SOROBAN_RPC_CONFIG_PATH"
	}))
	assert.Equal(t, uint(3), cfg.PreflightWorkerCount)
	assert.Equal(t, uint(3), cfg.PreflightWorkerQueueSize)
	assert.Equal(t, uint16(3), cfg.CaptiveCoreHTTPQueryThreadPoolSize)
}

// The preflight queue holds a whole batch of simulations by default.
func TestConfigPreflightQueueHoldsABatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte("MAX_REQUEST_BATCH_SIZE = 500\n"), 0o600))
	for _, tc := range []struct {
		env  map[string]string
		want uint
	}{
		{env: nil, want: max(uint(runtime.GOMAXPROCS(0)), 10)},
		{env: map[string]string{"SOROBAN_RPC_CONFIG_PATH": path}, want: 500},
		{env: map[string]string{"SOROBAN_RPC_CONFIG_PATH": path, "PREFLIGHT_WORKER_QUEUE_SIZE": "2"}, want: 2},
	} {
		var cfg Config
		require.NoError(t, cfg.SetValues(func(key string) (string, bool) {
			v, ok := tc.env[key]
			return v, ok
		}))
		assert.Equal(t, tc.want, cfg.PreflightWorkerQueueSize, tc.env)
	}
}

func TestConfigBufferedStorageBackend(t *testing.T) {
	const base = `
NETWORK_PASSPHRASE = "Test SDF Network ; September 2015"
HISTORY_ARCHIVE_URLS = ["http://localhost:1570"]
CAPTIVE_CORE_CONFIG_PATH = "/etc/stellar/captive-core.cfg"
STELLAR_CORE_BINARY_PATH = "/usr/bin/stellar-core"
`
	const datastore = base + `
SERVE_LEDGERS_FROM_DATASTORE = true

[datastore_config]
type = "GCS"

[datastore_config.params]
destination_bucket_path = "bucket/ledgers"
`
	const section = "\n[buffered_storage_backend_config]\n"
	for _, tc := range []struct {
		name    string
		toml    string
		want    ledgerbackend.BufferedStorageBackendConfig
		wantErr string
	}{
		{
			name: "missing section",
			toml: datastore,
			want: ledgerbackend.BufferedStorageBackendConfig{BufferSize: 100, NumWorkers: 10},
		},
		{
			name: "buffer size only",
			toml: datastore + section + "buffer_size = 50\n",
			want: ledgerbackend.BufferedStorageBackendConfig{BufferSize: 50, NumWorkers: 10},
		},
		{
			name:    "explicit zero workers",
			toml:    datastore + section + "num_workers = 0\n",
			want:    ledgerbackend.BufferedStorageBackendConfig{BufferSize: 100},
			wantErr: "invalid config value for buffered_storage_backend_config: number of workers must be > 0",
		},
		{
			name:    "more workers than buffer size",
			toml:    datastore + section + "buffer_size = 5\n",
			want:    ledgerbackend.BufferedStorageBackendConfig{BufferSize: 5, NumWorkers: 10},
			wantErr: "invalid config value for buffered_storage_backend_config: number of workers must be <= BufferSize",
		},
		{
			name: "datastore serving off",
			toml: base + section + "buffer_size = 5\n",
			want: ledgerbackend.BufferedStorageBackendConfig{BufferSize: 5, NumWorkers: 10},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			require.NoError(t, os.WriteFile(path, []byte(tc.toml), 0o600))
			var cfg Config
			require.NoError(t, cfg.SetValues(func(key string) (string, bool) {
				return path, key == "SOROBAN_RPC_CONFIG_PATH"
			}))
			assert.Equal(t, tc.want, cfg.BufferedStorageBackendConfig)
			if tc.wantErr == "" {
				require.NoError(t, cfg.Validate())
			} else {
				require.ErrorContains(t, cfg.Validate(), tc.wantErr)
			}
		})
	}
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
	assert.Empty(t, cfg.NetworkPassphrase)
	assert.Equal(t, logrus.PanicLevel, cfg.LogLevel)

	// Check it didn't overwrite values which were not set in the flags
	assert.Equal(t, "localhost:8000", cfg.Endpoint)
}

func TestConfigLoadNetworkOption(t *testing.T) {
	// Make structs networkParameters{networkName, historyArchiveURLs, networkPassphrase} for testnet/pubnet/futurenet
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
		historyArchiveURLs: network.FutureNetworkhistoryArchiveURLs,
		networkPassphrase:  network.FutureNetworkPassphrase,
	}
	return []networkParameters{testnet, pubnet, futurenet}
}

type networkParameters struct {
	networkName        string
	historyArchiveURLs []string
	networkPassphrase  string
}
