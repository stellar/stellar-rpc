package streaming

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A fully-populated, documented-valid config. Every section present with
// non-default values so the parse-and-resolve round-trip is exercised end to
// end.
const fullValidConfig = `
[service]
default_data_dir = "/var/lib/fullhistory"

[retention]
earliest_ledger = "now"
retention_chunks = 100

[storage]
catalog = "/mnt/catalog"
ledgers = "/mnt/ledgers"
events = "/mnt/events"
txhash_raw = "/mnt/txhash/raw"
txhash_index = "/mnt/txhash/index"
hot = "/mnt/hot"

[backfill]
workers = 8
max_retries = 5

[backfill.bsb]
bucket_path = "my-bucket/ledgers"
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
[service]
default_data_dir = "/data"

[backfill.bsb]
bucket_path = "bucket/path"

[ingestion]
captive_core_config = "/etc/cc.toml"
`

func TestParseConfig_FullDocument(t *testing.T) {
	cfg, err := ParseConfig([]byte(fullValidConfig))
	require.NoError(t, err)

	assert.Equal(t, "/var/lib/fullhistory", cfg.Service.DefaultDataDir)
	assert.Equal(t, "now", cfg.Retention.EarliestLedger)
	assert.Equal(t, uint32(100), *cfg.Retention.RetentionChunks)
	assert.Equal(t, "/mnt/catalog", cfg.Storage.Catalog)
	assert.Equal(t, "/mnt/ledgers", cfg.Storage.Ledgers)
	assert.Equal(t, "/mnt/events", cfg.Storage.Events)
	assert.Equal(t, "/mnt/txhash/raw", cfg.Storage.TxhashRaw)
	assert.Equal(t, "/mnt/txhash/index", cfg.Storage.TxhashIndex)
	assert.Equal(t, "/mnt/hot", cfg.Storage.Hot)
	assert.Equal(t, 8, *cfg.Backfill.Workers)
	assert.Equal(t, 5, *cfg.Backfill.MaxRetries)
	assert.Equal(t, "my-bucket/ledgers", cfg.Backfill.BSB.BucketPath)
	assert.Equal(t, 2000, *cfg.Backfill.BSB.BufferSize)
	assert.Equal(t, 40, *cfg.Backfill.BSB.NumWorkers)
	assert.Equal(t, "/etc/captive-core.toml", cfg.Ingestion.CaptiveCoreConfig)
	assert.Equal(t, "debug", cfg.Logging.Level)
	assert.Equal(t, "json", cfg.Logging.Format)
}

func TestParseConfig_MinimalAppliesDefaults(t *testing.T) {
	cfg, err := ParseConfig([]byte(minimalValidConfig))
	require.NoError(t, err)

	// Required keys preserved.
	assert.Equal(t, "/data", cfg.Service.DefaultDataDir)
	assert.Equal(t, "bucket/path", cfg.Backfill.BSB.BucketPath)
	assert.Equal(t, "/etc/cc.toml", cfg.Ingestion.CaptiveCoreConfig)

	// Documented defaults filled.
	assert.Equal(t, runtime.GOMAXPROCS(0), *cfg.Backfill.Workers)
	assert.Equal(t, DefaultMaxRetries, *cfg.Backfill.MaxRetries)
	assert.Equal(t, DefaultBSBBufferSize, *cfg.Backfill.BSB.BufferSize)
	assert.Equal(t, DefaultBSBNumWorkers, *cfg.Backfill.BSB.NumWorkers)
	assert.Equal(t, uint32(0), *cfg.Retention.RetentionChunks)
	assert.Equal(t, DefaultEarliestLedger, cfg.Retention.EarliestLedger)
	assert.Equal(t, DefaultLogLevel, cfg.Logging.Level)
	assert.Equal(t, DefaultLogFormat, cfg.Logging.Format)
}

func TestParseConfig_ExplicitZeroPreserved(t *testing.T) {
	// An explicit zero must NOT be overwritten by the default — validateConfig
	// is what rejects an illegal zero (e.g. workers = 0), so the defaulting layer
	// must preserve it for that rejection to fire.
	const cfgText = `
[service]
default_data_dir = "/d"
[backfill]
workers = 0
max_retries = 0
[ingestion]
captive_core_config = "/cc"
`
	cfg, err := ParseConfig([]byte(cfgText))
	require.NoError(t, err)
	assert.Equal(t, 0, *cfg.Backfill.Workers)
	assert.Equal(t, 0, *cfg.Backfill.MaxRetries)
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
[service]
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
[service]
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
[service]
default_data_dir = "/d"
[ingestion]
captive_core_config = "/cc"
`,
		},
		{
			name: "unknown section",
			text: `
[service]
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
[service]
default_data_dir = "/d"
[backfill.bsb]
bucket_path = "b/p"
bufer_size = 10
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

func TestRootsToLock_AllDistinctRoots(t *testing.T) {
	cfg, err := ParseConfig([]byte(minimalValidConfig))
	require.NoError(t, err)
	roots := cfg.ResolvePaths().RootsToLock()
	// Meta store + four immutable trees + hot storage = six roots.
	require.Len(t, roots, 6)
	assert.NotContains(t, roots, "/data", "the data dir parent is not itself locked")
}
