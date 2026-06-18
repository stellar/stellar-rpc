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

[catch_up]
chunks_per_txhash_index = 500
workers = 8
max_retries = 5

[catch_up.bsb]
bucket_path = "my-bucket/ledgers"
buffer_size = 2000
num_workers = 40

[immutable_storage.ledgers]
path = "/mnt/ledgers"

[immutable_storage.events]
path = "/mnt/events"

[immutable_storage.txhash_raw]
path = "/mnt/txhash/raw"

[immutable_storage.txhash_index]
path = "/mnt/txhash/index"

[meta_store]
path = "/mnt/meta"

[streaming]
retention_chunks = 100
earliest_ledger = "now"
captive_core_config = "/etc/captive-core.toml"

[streaming.hot_storage]
path = "/mnt/hot"

[logging]
level = "debug"
format = "json"
`

// A minimal config: only the required keys, everything else defaulted.
const minimalValidConfig = `
[service]
default_data_dir = "/data"

[catch_up.bsb]
bucket_path = "bucket/path"

[streaming]
captive_core_config = "/etc/cc.toml"
`

func TestParseConfig_FullDocument(t *testing.T) {
	cfg, err := ParseConfig([]byte(fullValidConfig))
	require.NoError(t, err)

	assert.Equal(t, "/var/lib/fullhistory", cfg.Service.DefaultDataDir)
	assert.Equal(t, uint32(500), *cfg.CatchUp.ChunksPerTxhashIndex)
	assert.Equal(t, 8, *cfg.CatchUp.Workers)
	assert.Equal(t, 5, *cfg.CatchUp.MaxRetries)
	assert.Equal(t, "my-bucket/ledgers", cfg.CatchUp.BSB.BucketPath)
	assert.Equal(t, 2000, *cfg.CatchUp.BSB.BufferSize)
	assert.Equal(t, 40, *cfg.CatchUp.BSB.NumWorkers)
	assert.Equal(t, "/mnt/ledgers", cfg.ImmutableStorage.Ledgers.Path)
	assert.Equal(t, "/mnt/events", cfg.ImmutableStorage.Events.Path)
	assert.Equal(t, "/mnt/txhash/raw", cfg.ImmutableStorage.TxhashRaw.Path)
	assert.Equal(t, "/mnt/txhash/index", cfg.ImmutableStorage.TxhashIndex.Path)
	assert.Equal(t, "/mnt/meta", cfg.MetaStore.Path)
	assert.Equal(t, uint32(100), *cfg.Streaming.RetentionChunks)
	assert.Equal(t, "now", cfg.Streaming.EarliestLedger)
	assert.Equal(t, "/etc/captive-core.toml", cfg.Streaming.CaptiveCoreConfig)
	assert.Equal(t, "/mnt/hot", cfg.Streaming.HotStorage.Path)
	assert.Equal(t, "debug", cfg.Logging.Level)
	assert.Equal(t, "json", cfg.Logging.Format)
}

func TestParseConfig_MinimalAppliesDefaults(t *testing.T) {
	cfg, err := ParseConfig([]byte(minimalValidConfig))
	require.NoError(t, err)

	// Required keys preserved.
	assert.Equal(t, "/data", cfg.Service.DefaultDataDir)
	assert.Equal(t, "bucket/path", cfg.CatchUp.BSB.BucketPath)
	assert.Equal(t, "/etc/cc.toml", cfg.Streaming.CaptiveCoreConfig)

	// Documented defaults filled.
	assert.Equal(t, DefaultChunksPerTxhashIndex, *cfg.CatchUp.ChunksPerTxhashIndex)
	assert.Equal(t, runtime.GOMAXPROCS(0), *cfg.CatchUp.Workers)
	assert.Equal(t, DefaultMaxRetries, *cfg.CatchUp.MaxRetries)
	assert.Equal(t, DefaultBSBBufferSize, *cfg.CatchUp.BSB.BufferSize)
	assert.Equal(t, DefaultBSBNumWorkers, *cfg.CatchUp.BSB.NumWorkers)
	assert.Equal(t, uint32(0), *cfg.Streaming.RetentionChunks)
	assert.Equal(t, DefaultEarliestLedger, cfg.Streaming.EarliestLedger)
	assert.Equal(t, DefaultLogLevel, cfg.Logging.Level)
	assert.Equal(t, DefaultLogFormat, cfg.Logging.Format)
}

func TestParseConfig_ExplicitZeroPreserved(t *testing.T) {
	// An explicit zero must NOT be overwritten by the default — validateConfig
	// is what rejects an illegal zero (e.g. chunks_per_txhash_index), so the
	// defaulting layer must preserve it for that rejection to fire.
	const cfgText = `
[service]
default_data_dir = "/d"
[catch_up]
chunks_per_txhash_index = 0
workers = 0
max_retries = 0
[streaming]
captive_core_config = "/cc"
`
	cfg, err := ParseConfig([]byte(cfgText))
	require.NoError(t, err)
	assert.Equal(t, uint32(0), *cfg.CatchUp.ChunksPerTxhashIndex)
	assert.Equal(t, 0, *cfg.CatchUp.Workers)
	assert.Equal(t, 0, *cfg.CatchUp.MaxRetries)
}

func TestParseConfig_Malformed(t *testing.T) {
	_, err := ParseConfig([]byte(`this is = = not valid toml [[[`))
	require.Error(t, err)
}

// A typo'd key must be REJECTED, not silently dropped to a default. The two
// layout-defining keys (chunks_per_txhash_index, earliest_ledger) are pinned
// immutably on first start, so a silent fallback would permanently pin the
// wrong value. Strict decoding catches the typo before any pin is written.
func TestParseConfig_RejectsUnknownKeys(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{
			name: "typo'd chunks_per_txhash_index",
			text: `
[service]
default_data_dir = "/d"
[catch_up]
chunks_per_txhash_indx = 7
[streaming]
captive_core_config = "/cc"
`,
		},
		{
			name: "typo'd earliest_ledger",
			text: `
[service]
default_data_dir = "/d"
[streaming]
earliest_ledgr = "now"
captive_core_config = "/cc"
`,
		},
		{
			name: "unknown top-level key",
			text: `
default_data_dirr = "/d"
[service]
default_data_dir = "/d"
[streaming]
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
[streaming]
captive_core_config = "/cc"
`,
		},
		{
			name: "unknown nested key under known section",
			text: `
[service]
default_data_dir = "/d"
[catch_up.bsb]
bucket_path = "b/p"
bufer_size = 10
[streaming]
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
	assert.Equal(t, filepath.Join("/data", "meta", "rocksdb"), p.MetaStore)
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

	assert.Equal(t, "/mnt/meta", p.MetaStore)
	assert.Equal(t, "/mnt/ledgers", p.Ledgers)
	assert.Equal(t, "/mnt/events", p.Events)
	assert.Equal(t, "/mnt/txhash/raw", p.TxhashRaw)
	assert.Equal(t, "/mnt/txhash/index", p.TxhashIndex)
	assert.Equal(t, "/mnt/hot", p.HotStorage)
}

func TestLockRoots_AllDistinctRoots(t *testing.T) {
	cfg, err := ParseConfig([]byte(minimalValidConfig))
	require.NoError(t, err)
	roots := cfg.ResolvePaths().LockRoots()
	// Meta store + four immutable trees + hot storage = six roots.
	require.Len(t, roots, 6)
	assert.NotContains(t, roots, "/data", "the data dir parent is not itself locked")
}
