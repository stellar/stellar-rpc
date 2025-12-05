package config

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pelletier/go-toml"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/datastore"
)

const basicToml = `
HISTORY_ARCHIVE_URLS = [ "http://history-futurenet.stellar.org" ]
NETWORK_PASSPHRASE = "Test SDF Future Network ; October 2022"

# testing comments work ok
STELLAR_CORE_BINARY_PATH = "/usr/bin/stellar-core"
CAPTIVE_CORE_STORAGE_PATH = "/etc/stellar/stellar-rpc"
CAPTIVE_CORE_CONFIG_PATH = "/etc/stellar/stellar-rpc/captive-core.cfg"
`

func TestBasicTomlReading(t *testing.T) {
	cfg := Config{}
	require.NoError(t, parseToml(strings.NewReader(basicToml), false, &cfg))

	// Check the fields got read correctly
	assert.Equal(t, []string{"http://history-futurenet.stellar.org"}, cfg.HistoryArchiveURLs)
	assert.Equal(t, network.FutureNetworkPassphrase, cfg.NetworkPassphrase)
	assert.Equal(t, "/etc/stellar/stellar-rpc", cfg.CaptiveCoreStoragePath)
	assert.Equal(t, "/etc/stellar/stellar-rpc/captive-core.cfg", cfg.CaptiveCoreConfigPath)
}

func TestBasicTomlReadingStrictMode(t *testing.T) {
	invalidToml := `UNKNOWN = "key"`
	cfg := Config{}

	// Should ignore unknown fields when strict is not set
	require.NoError(t, parseToml(strings.NewReader(invalidToml), false, &cfg))

	// Should panic when unknown key is present and strict is set in the cli
	// flags
	require.EqualError(
		t,
		parseToml(strings.NewReader(invalidToml), true, &cfg),
		"invalid config: unexpected entry specified in toml file \"UNKNOWN\"",
	)

	// Should panic when unknown key is present and strict is set in the
	// config file
	invalidStrictToml := `
	STRICT = true
	UNKNOWN = "key"
`
	require.EqualError(
		t,
		parseToml(strings.NewReader(invalidStrictToml), false, &cfg),
		"invalid config: unexpected entry specified in toml file \"UNKNOWN\"",
	)

	// It succeeds with a valid config
	require.NoError(t, parseToml(strings.NewReader(basicToml), true, &cfg))
}

func TestBasicTomlWriting(t *testing.T) {
	// Set up a default config
	cfg := Config{}
	require.NoError(t, cfg.loadDefaults())

	// Output it to toml
	outBytes, err := cfg.MarshalTOML()
	require.NoError(t, err)

	out := string(outBytes)

	// Spot-check that the output looks right. Try to check one value for each
	// type of option. (string, duration, uint, etc...)
	assert.Contains(t, out, "ENDPOINT = \"localhost:8000\"")
	assert.Contains(t, out, "STELLAR_CORE_TIMEOUT = \"2s\"")
	assert.Contains(t, out, "STELLAR_CAPTIVE_CORE_HTTP_PORT = 11626")
	assert.Contains(t, out, "LOG_LEVEL = \"info\"")
	assert.Contains(t, out, "LOG_FORMAT = \"text\"")

	// Check that the output contains comments about each option
	assert.Contains(t, out, "# Network passphrase of the Stellar network transactions should be signed for")

	// Test that it wraps long lines.
	// Note the newline at char 80. This also checks it adds a space after the
	// comment when outputting multi-line comments, which go-toml does *not* do
	// by default.
	assert.Contains(t, out,
		`# configures history retention window for transactions and events, expressed in
# number of ledgers, the default value is 120960 which corresponds to about 7
# days of history`)
}

func TestRoundTrip(t *testing.T) {
	// Set up a default config
	cfg := Config{}
	require.NoError(t, cfg.loadDefaults())

	// Generate test values for every option, so we can round-trip test them all.
	for _, option := range cfg.options() {
		optType := reflect.ValueOf(option.ConfigKey).Elem().Type()
		switch v := option.ConfigKey.(type) {
		case *bool:
			*v = true
		case *string:
			switch option.ConfigKey {
			case &cfg.Network:
				// Network option sets the three config keys below, but requires them to be empty
				cfg.HistoryArchiveURLs = []string{}
				cfg.CaptiveCoreConfigPath = ""
				cfg.NetworkPassphrase = ""
				// Network option errors on inputs that are not a real network
				*v = "testnet"
			default:
				*v = "test"
			}
		case *uint:
			*v = 42
		case *uint16:
			*v = 22
		case *uint32:
			*v = 32
		case *time.Duration:
			*v = 5 * time.Second
		case *[]string:
			*v = []string{"a", "b"}
		case *logrus.Level:
			*v = logrus.InfoLevel
		case *LogFormat:
			*v = LogFormatText
		case *ledgerbackend.BufferedStorageBackendConfig:
			*v = defaultBufferedStorageBackendConfig()
		case *datastore.DataStoreConfig:
			*v = defaultDataStoreConfig()
		default:
			t.Fatalf("TestRoundTrip not implemented for type %s, on option %s, "+
				"please add a test value", optType.Kind(), option.Name)
		}
	}

	// Output it to toml
	outBytes, err := cfg.MarshalTOML()
	require.NoError(t, err)

	// t.Log(string(outBytes))

	// Parse it back
	require.NoError(
		t,
		parseToml(bytes.NewReader(outBytes), false, &cfg),
	)
}

func TestRoundTripDataStoreConfig(t *testing.T) {
	cfg := Config{}
	require.NoError(t, cfg.loadDefaults())
	require.Equal(t, ledgerbackend.BufferedStorageBackendConfig{}, cfg.BufferedStorageBackendConfig)
	require.Equal(t, datastore.DataStoreConfig{}, cfg.DataStoreConfig)

	outBytes, err := marshalTOML(&cfg)
	require.NoError(t, err)

	require.NoError(t, parseToml(bytes.NewReader(outBytes), false, &cfg))
	require.Equal(t, defaultBufferedStorageBackendConfig(), cfg.BufferedStorageBackendConfig)
	require.Equal(t, defaultDataStoreConfig(), cfg.DataStoreConfig)
}

func marshalTOML(cfg *Config) ([]byte, error) {
	tree, err := toml.TreeFromMap(map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	for _, option := range cfg.options() {
		key, ok := option.getTomlKey()
		if !ok {
			continue
		}
		value, err := option.marshalTOML()
		if err != nil {
			return nil, err
		}
		tree.Set(key, value)
	}
	return tree.Marshal()
}
