package config

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
)

// A fully-populated, documented-valid config. Every section present with
// non-default values so the parse-and-resolve round-trip is exercised end to
// end.
const fullValidConfig = `
[service]
endpoint = "0.0.0.0:9000"
admin_endpoint = "localhost:9001"
max_concurrent_requests = 7000
max_request_execution_duration = "30s"
request_execution_warning_threshold = "3s"

[service.fee_stats]
classic_fee_window_ledgers = 20
soroban_inclusion_fee_window_ledgers = 60

[service.methods]
queue_limit = 200
max_execution_duration = "8s"

[service.methods.getLedgers]
queue_limit = 500
max_items_per_response = 400
default_items_per_response = 40

[service.methods.getFeeStats]
max_execution_duration = "2s"

[retention]
earliest_ledger = "now"
retention_chunks = 100

[storage]
default_data_dir = "/var/lib/fullhistory"
catalog = "/mnt/catalog"
ledgers = "/mnt/ledgers"
events = "/mnt/events"
txhash_raw = "/mnt/txhash/raw"
txhash_index = "/mnt/txhash/index"
hot = "/mnt/hot"

[backfill]
workers = 8
max_retries = 5

[backfill.datastore]
type = "GCS"

[backfill.datastore.params]
destination_bucket_path = "my-bucket/ledgers"

[backfill.bsb]
buffer_size = 2000
num_workers = 40

[ingestion]
captive_core_config = "/etc/captive-core.toml"

[logging]
level = "debug"
format = "json"
`

// A minimal config: only the required keys, everything else defaulted.
const minimalValidConfig = `
[storage]
default_data_dir = "/data"

[backfill.datastore]
type = "GCS"

[backfill.datastore.params]
destination_bucket_path = "bucket/path"

[ingestion]
captive_core_config = "/etc/cc.toml"
`

func TestParseConfig_FullDocument(t *testing.T) {
	cfg, err := ParseConfig([]byte(fullValidConfig))
	require.NoError(t, err)

	assert.Equal(t, "0.0.0.0:9000", cfg.Service.Endpoint)
	assert.Equal(t, "localhost:9001", cfg.Service.AdminEndpoint)
	assert.Equal(t, uint(7000), *cfg.Service.MaxConcurrentRequests)
	assert.Equal(t, 30*time.Second, *cfg.Service.MaxRequestExecutionDuration)
	assert.Equal(t, 3*time.Second, *cfg.Service.RequestExecutionWarningThreshold)
	assert.Equal(t, uint32(20), *cfg.Service.FeeStats.ClassicFeeWindowLedgers)
	assert.Equal(t, uint32(60), *cfg.Service.FeeStats.SorobanInclusionFeeWindowLedgers)

	assert.Equal(t, "now", cfg.Retention.EarliestLedger)
	assert.Equal(t, uint32(100), *cfg.Retention.RetentionChunks)
	assert.Equal(t, "/var/lib/fullhistory", cfg.Storage.DefaultDataDir)
	assert.Equal(t, "/mnt/catalog", cfg.Storage.Catalog)
	assert.Equal(t, "/mnt/ledgers", cfg.Storage.Ledgers)
	assert.Equal(t, "/mnt/events", cfg.Storage.Events)
	assert.Equal(t, "/mnt/txhash/raw", cfg.Storage.TxhashRaw)
	assert.Equal(t, "/mnt/txhash/index", cfg.Storage.TxhashIndex)
	assert.Equal(t, "/mnt/hot", cfg.Storage.Hot)
	assert.Equal(t, 8, *cfg.Backfill.Workers)
	assert.Equal(t, 5, *cfg.Backfill.MaxRetries)
	assert.Equal(t, "GCS", cfg.Backfill.DataStore.Type)
	assert.Equal(t, "my-bucket/ledgers", cfg.Backfill.DataStore.Params["destination_bucket_path"])
	assert.Equal(t, uint32(2000), *cfg.Backfill.BSB.BufferSize)
	assert.Equal(t, uint32(40), *cfg.Backfill.BSB.NumWorkers)
	assert.Equal(t, "/etc/captive-core.toml", cfg.Ingestion.CaptiveCoreConfig)
	assert.Equal(t, "debug", cfg.Logging.Level)
	assert.Equal(t, "json", cfg.Logging.Format)
}

func TestParseConfig_MinimalAppliesDefaults(t *testing.T) {
	cfg, err := ParseConfig([]byte(minimalValidConfig))
	require.NoError(t, err)

	// Required keys preserved.
	assert.Equal(t, "/data", cfg.Storage.DefaultDataDir)
	assert.Equal(t, "bucket/path", cfg.Backfill.DataStore.Params["destination_bucket_path"])
	assert.Equal(t, "/etc/cc.toml", cfg.Ingestion.CaptiveCoreConfig)

	// Documented defaults filled.
	assert.Equal(t, runtime.GOMAXPROCS(0), *cfg.Backfill.Workers)
	assert.Equal(t, DefaultMaxRetries, *cfg.Backfill.MaxRetries)
	assert.Equal(t, uint32(backfill.DefaultBSBBufferSize), *cfg.Backfill.BSB.BufferSize)
	assert.Equal(t, uint32(backfill.DefaultBSBNumWorkers), *cfg.Backfill.BSB.NumWorkers)
	assert.Equal(t, uint32(backfill.DefaultBSBMaxRetries), *cfg.Backfill.BSB.MaxRetries)
	assert.Equal(t, backfill.DefaultBSBRetryWait, *cfg.Backfill.BSB.RetryWait)
	assert.Equal(t, uint32(0), *cfg.Retention.RetentionChunks)
	assert.Equal(t, DefaultEarliestLedger, cfg.Retention.EarliestLedger)
	assert.Equal(t, DefaultLogLevel, cfg.Logging.Level)
	assert.Equal(t, DefaultLogFormat, cfg.Logging.Format)
}

func TestDecodeConfig_RejectsPassphraseUnderDatastore(t *testing.T) {
	_, err := DecodeConfig([]byte(`
[backfill.datastore]
type = "GCS"
NetworkPassphrase = "oops"
`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NetworkPassphrase")
}

func TestDataStoreConfig_SDKConfigCarriesPassphrase(t *testing.T) {
	d := DataStoreConfig{
		Type:   "GCS",
		Params: map[string]string{"destination_bucket_path": "b/p"},
		Schema: DataStoreSchemaConfig{LedgersPerFile: 1, FilesPerPartition: 64000},
	}
	sdk := d.SDKConfig("Public Global Stellar Network ; September 2015")

	assert.Equal(t, "GCS", sdk.Type)
	assert.Equal(t, "b/p", sdk.Params["destination_bucket_path"])
	assert.Equal(t, uint32(1), sdk.Schema.LedgersPerFile)
	assert.Equal(t, uint32(64000), sdk.Schema.FilesPerPartition)
	assert.Equal(t, "Public Global Stellar Network ; September 2015", sdk.NetworkPassphrase)
}

func TestParseConfig_ExplicitZeroBSBMaxRetriesSurvives(t *testing.T) {
	cfg, err := ParseConfig([]byte(minimalValidConfig + "\n[backfill.bsb]\nmax_retries = 0\n"))
	require.NoError(t, err)

	assert.Zero(t, *cfg.Backfill.BSB.MaxRetries)
	assert.Zero(t, cfg.Backfill.BSB.SDKConfig().RetryLimit)
}

func TestParseConfig_ServiceDefaults(t *testing.T) {
	// An entirely absent [service] section is valid and fully defaulted.
	cfg, err := ParseConfig([]byte(minimalValidConfig))
	require.NoError(t, err)

	assert.Equal(t, DefaultEndpoint, cfg.Service.Endpoint)
	assert.Empty(t, cfg.Service.AdminEndpoint, "admin server disabled by default")
	assert.Equal(t, DefaultMaxConcurrentRequests, *cfg.Service.MaxConcurrentRequests)
	assert.Equal(t, DefaultMaxRequestExecutionDuration, *cfg.Service.MaxRequestExecutionDuration)
	assert.Equal(t, DefaultRequestExecutionWarningThreshold, *cfg.Service.RequestExecutionWarningThreshold)
	assert.Equal(t, DefaultClassicFeeWindowLedgers, *cfg.Service.FeeStats.ClassicFeeWindowLedgers)
	assert.Equal(t, DefaultSorobanInclusionFeeWindowLedgers, *cfg.Service.FeeStats.SorobanInclusionFeeWindowLedgers)

	m := cfg.Service.Methods
	assert.Nil(t, m.QueueLimit, "the wide-default tier stays unset")
	assert.Nil(t, m.MaxExecutionDuration, "the wide-default tier stays unset")

	assert.Equal(t, DefaultMethodQueueLimit, *m.GetHealth.QueueLimit)
	assert.Equal(t, DefaultMethodMaxExecutionDuration, *m.GetHealth.MaxExecutionDuration)
	assert.Equal(t, DefaultMaxHealthyLedgerLatency, *m.GetHealth.MaxHealthyLedgerLatency)

	assert.Equal(t, DefaultGetFeeStatsQueueLimit, *m.GetFeeStats.QueueLimit)
	assert.Equal(t, DefaultScanMethodMaxExecutionDuration, *m.GetLedgers.MaxExecutionDuration)
	assert.Equal(t, DefaultScanMethodMaxExecutionDuration, *m.GetEvents.MaxExecutionDuration)
	assert.Equal(t, DefaultMethodMaxExecutionDuration, *m.GetTransactions.MaxExecutionDuration)

	assert.Equal(t, DefaultGetEventsMaxItemsPerResponse, *m.GetEvents.MaxItemsPerResponse)
	assert.Equal(t, DefaultGetEventsDefaultItemsPerResponse, *m.GetEvents.DefaultItemsPerResponse)
	assert.Equal(t, DefaultGetTransactionsMaxItemsPerResponse, *m.GetTransactions.MaxItemsPerResponse)
	assert.Equal(t, DefaultGetLedgersMaxItemsPerResponse, *m.GetLedgers.MaxItemsPerResponse)
}

func TestParseConfig_MethodsCascade(t *testing.T) {
	t.Run("wide default applies to unspecified methods, per-method value survives", func(t *testing.T) {
		cfg, err := ParseConfig([]byte(fullValidConfig))
		require.NoError(t, err)
		m := cfg.Service.Methods

		// getLedgers set queue_limit = 500 explicitly; the wide tier is 200.
		assert.Equal(t, uint(500), *m.GetLedgers.QueueLimit)
		// getFeeStats set only its duration; queue falls to the wide tier — NOT
		// its compiled default of 100.
		assert.Equal(t, uint(200), *m.GetFeeStats.QueueLimit)
		assert.Equal(t, 2*time.Second, *m.GetFeeStats.MaxExecutionDuration)
		// Untouched methods take both wide-tier values.
		assert.Equal(t, uint(200), *m.GetHealth.QueueLimit)
		assert.Equal(t, 8*time.Second, *m.GetHealth.MaxExecutionDuration)
		// getLedgers' duration was not set per-method, so the wide tier wins over
		// its compiled 10s default.
		assert.Equal(t, 8*time.Second, *m.GetLedgers.MaxExecutionDuration)
	})

	t.Run("no wide tier: compiled per-method defaults", func(t *testing.T) {
		cfg, err := ParseConfig([]byte(minimalValidConfig))
		require.NoError(t, err)
		m := cfg.Service.Methods

		assert.Equal(t, DefaultMethodQueueLimit, *m.GetLedgers.QueueLimit)
		assert.Equal(t, DefaultGetFeeStatsQueueLimit, *m.GetFeeStats.QueueLimit)
	})

	t.Run("pagination caps have no wide tier", func(t *testing.T) {
		cfg, err := ParseConfig([]byte(fullValidConfig))
		require.NoError(t, err)
		m := cfg.Service.Methods

		// getLedgers overrode its caps; getEvents keeps its own compiled defaults
		// (10000/100), untouched by getLedgers' 400/40 or any wide value.
		assert.Equal(t, uint(400), *m.GetLedgers.MaxItemsPerResponse)
		assert.Equal(t, uint(40), *m.GetLedgers.DefaultItemsPerResponse)
		assert.Equal(t, DefaultGetEventsMaxItemsPerResponse, *m.GetEvents.MaxItemsPerResponse)
		assert.Equal(t, DefaultGetEventsDefaultItemsPerResponse, *m.GetEvents.DefaultItemsPerResponse)
	})
}

func TestParseConfig_ExplicitZeroPreserved(t *testing.T) {
	// An explicit zero must NOT be overwritten by the default — validateConfig
	// is what rejects an illegal zero (e.g. workers = 0), so the defaulting layer
	// must preserve it for that rejection to fire.
	const cfgText = `
[storage]
default_data_dir = "/d"
[backfill]
workers = 0
max_retries = 0
[service.methods.getLedgers]
queue_limit = 0
[ingestion]
captive_core_config = "/cc"
`
	cfg, err := ParseConfig([]byte(cfgText))
	require.NoError(t, err)
	assert.Equal(t, 0, *cfg.Backfill.Workers)
	assert.Equal(t, 0, *cfg.Backfill.MaxRetries)
	assert.Equal(t, uint(0), *cfg.Service.Methods.GetLedgers.QueueLimit)
}

func TestParseConfig_Malformed(t *testing.T) {
	_, err := ParseConfig([]byte(`this is = = not valid toml [[[`))
	require.Error(t, err)
}

// A typo'd key must be REJECTED, not silently dropped to a default. The pinned
// earliest_ledger key is committed immutably on first start, so a silent
// fallback would permanently pin the wrong value. Strict decoding catches the
// typo before any pin is written. The removed chunks_per_txhash_index key (and
// its whole [layout] section) is likewise rejected, not silently ignored.
func TestParseConfig_RejectsUnknownKeys(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{
			// chunks_per_txhash_index is no longer a config key — even spelled
			// correctly it (and the [layout] section) must now be rejected.
			name: "removed chunks_per_txhash_index key",
			text: `
[storage]
default_data_dir = "/d"
[layout]
chunks_per_txhash_index = 1000
[ingestion]
captive_core_config = "/cc"
`,
		},
		{
			name: "typo'd earliest_ledger",
			text: `
[storage]
default_data_dir = "/d"
[retention]
earliest_ledgr = "now"
[ingestion]
captive_core_config = "/cc"
`,
		},
		{
			name: "unknown top-level key",
			text: `
default_data_dirr = "/d"
[storage]
default_data_dir = "/d"
[ingestion]
captive_core_config = "/cc"
`,
		},
		{
			name: "unknown section",
			text: `
[storage]
default_data_dir = "/d"
[bogus_section]
foo = "bar"
[ingestion]
captive_core_config = "/cc"
`,
		},
		{
			name: "unknown nested key under known section",
			text: `
[storage]
default_data_dir = "/d"
[backfill.bsb]
bucket_path = "b/p"
bufer_size = 10
[ingestion]
captive_core_config = "/cc"
`,
		},
		{
			// default_data_dir moved to [storage] in #882 — its old home must fail
			// loudly, not be silently accepted.
			name: "default_data_dir under its pre-#882 [service] home",
			text: `
[service]
default_data_dir = "/d"
[ingestion]
captive_core_config = "/cc"
`,
		},
		{
			name: "typo'd method table",
			text: `
[storage]
default_data_dir = "/d"
[service.methods.getLegders]
queue_limit = 5
[ingestion]
captive_core_config = "/cc"
`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseConfig([]byte(tc.text))
			require.Error(t, err, "an unknown/typo'd key must be rejected, not silently defaulted")
			assert.Contains(t, err.Error(), "parse config")
		})
	}
}

func TestResolvePaths_DefaultsUnderDataDir(t *testing.T) {
	cfg, err := ParseConfig([]byte(minimalValidConfig))
	require.NoError(t, err)
	p := cfg.ResolvePaths()

	assert.Equal(t, "/data", p.DataDir)
	assert.Equal(t, filepath.Join("/data", "catalog", "rocksdb"), p.Catalog)
	assert.Equal(t, filepath.Join("/data", "ledgers"), p.Ledgers)
	assert.Equal(t, filepath.Join("/data", "events"), p.Events)
	assert.Equal(t, filepath.Join("/data", "txhash", "raw"), p.TxhashRaw)
	assert.Equal(t, filepath.Join("/data", "txhash", "index"), p.TxhashIndex)
	assert.Equal(t, filepath.Join("/data", "hot"), p.HotStorage)
}

func TestResolvePaths_OverridesWin(t *testing.T) {
	cfg, err := ParseConfig([]byte(fullValidConfig))
	require.NoError(t, err)
	p := cfg.ResolvePaths()

	assert.Equal(t, "/mnt/catalog", p.Catalog)
	assert.Equal(t, "/mnt/ledgers", p.Ledgers)
	assert.Equal(t, "/mnt/events", p.Events)
	assert.Equal(t, "/mnt/txhash/raw", p.TxhashRaw)
	assert.Equal(t, "/mnt/txhash/index", p.TxhashIndex)
	assert.Equal(t, "/mnt/hot", p.HotStorage)
}

func TestRoots_AllDistinct(t *testing.T) {
	cfg, err := ParseConfig([]byte(minimalValidConfig))
	require.NoError(t, err)
	roots := cfg.ResolvePaths().Roots()
	// Meta store + four immutable trees + hot storage = six roots.
	require.Len(t, roots, 6)
	assert.NotContains(t, roots, "/data", "the data dir parent is not itself a root")
}

func TestValidateRoots(t *testing.T) {
	base := Paths{
		Catalog:     "/data/catalog/rocksdb",
		Ledgers:     "/data/ledgers",
		Events:      "/data/events",
		TxhashRaw:   "/data/txhash/raw",
		TxhashIndex: "/data/txhash/index",
		HotStorage:  "/data/hot",
	}

	t.Run("default sibling layout is valid", func(t *testing.T) {
		require.NoError(t, base.ValidateRoots())
	})

	t.Run("duplicate roots rejected", func(t *testing.T) {
		p := base
		p.HotStorage = p.Catalog
		err := p.ValidateRoots()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "more than one store")
	})

	t.Run("duplicate via unclean spelling rejected", func(t *testing.T) {
		p := base
		p.HotStorage = "/data/../data/catalog/rocksdb/" // same tree, different spelling
		err := p.ValidateRoots()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "more than one store")
	})

	t.Run("nested roots are allowed", func(t *testing.T) {
		// Deliberate: prune and discard delete per-chunk paths only, never a
		// whole root, so a root inside another root loses nothing.
		p := base
		p.Events = "/data/ledgers/events"
		require.NoError(t, p.ValidateRoots())
	})
}
