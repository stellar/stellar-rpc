package main

import (
	"os"
	"regexp"
	"testing"

	"github.com/pelletier/go-toml"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

func TestSampleConfig_ParsesStrict(t *testing.T) {
	data, err := os.ReadFile("rpc-v2-sample-config.toml")
	require.NoError(t, err)

	cfg, err := config.ParseConfig(data)
	require.NoError(t, err)

	assert.Equal(t, "/var/stellar/rpc-v2", cfg.Storage.DefaultDataDir)
	assert.Equal(t, config.DefaultEndpoint, cfg.Service.Endpoint)
	assert.Equal(t, "GCS", cfg.Backfill.DataStore.Type)
	assert.Equal(t, "/etc/stellar/captive-core.toml", cfg.Ingestion.CaptiveCoreConfig)
	assert.False(t, *cfg.Service.Preflight.EnableDebug,
		"the sample deliberately departs from the true default: it is a production starting point")
}

// Every value the sample shows, set or commented out, is the compiled default
// unless a comment there says otherwise. The fields cleared below are those
// exceptions. The strict parse also fails on a commented-out key the schema lacks.
func TestSampleConfig_ShownValuesAreDefaults(t *testing.T) {
	cfg, err := config.ParseConfig(uncommentedSample(t))
	require.NoError(t, err)

	cfg.Storage.DefaultDataDir = ""
	cfg.Service.Methods.QueueLimit = nil
	cfg.Service.Methods.MaxExecutionDuration = nil
	cfg.Service.Methods.GetNetwork.FriendbotURL = ""
	cfg.Service.Preflight = config.PreflightConfig{}
	cfg.Backfill.Workers = nil
	cfg.Backfill.DataStore = config.DataStoreConfig{}
	cfg.Ingestion.CaptiveCoreConfig = ""
	cfg.Ingestion.HistoryArchiveURLs = nil
	cfg.Ingestion.StellarCoreBinaryPath = ""
	cfg.Ingestion.CoreHTTPQueryThreadPoolSize = nil

	assert.Equal(t, config.Config{}.WithDefaults(), cfg.WithDefaults())
}

// Every schema key must appear in the sample, set or commented out. BindFlags
// registers one flag per key, named by its TOML path.
func TestSampleConfig_ListsEverySchemaKey(t *testing.T) {
	tree, err := toml.LoadBytes(uncommentedSample(t))
	require.NoError(t, err)

	fs := pflag.NewFlagSet("schema", pflag.ContinueOnError)
	config.BindFlags(fs)
	fs.VisitAll(func(f *pflag.Flag) {
		assert.True(t, tree.Has(f.Name), "the sample does not list %s", f.Name)
	})
}

// uncommentedSample turns every optional-key line in the sample (`#key = value`,
// no space after #, unlike prose comments) into a live key.
func uncommentedSample(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("rpc-v2-sample-config.toml")
	require.NoError(t, err)

	uncommented := regexp.MustCompile(`(?m)^#([a-z_]+ = )`).ReplaceAll(data, []byte("$1"))
	require.NotEqual(t, string(data), string(uncommented),
		"expected '#key = value' optional-key lines in the sample")
	return uncommented
}
