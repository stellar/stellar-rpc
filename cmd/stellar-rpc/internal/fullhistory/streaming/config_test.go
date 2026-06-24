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

[backfill]
workers = 8
max_retries = 5

[backfill.bsb]
bucket_path = "my-bucket/ledgers"
buffer_size = 2000
num_workers = 40

[immutable_storage]
path = "/mnt/cold"

[catalog]
path = "/mnt/catalog"

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

[backfill.bsb]
bucket_path = "bucket/path"

[streaming]
captive_core_config = "/etc/cc.toml"
`

func TestParseConfig_FullDocument(t *testing.T) {
	cfg, err := ParseConfig([]byte(fullValidConfig))
	require.NoError(t, err)

	assert.Equal(t, "/var/lib/fullhistory", cfg.Service.DefaultDataDir)
	assert.Equal(t, 8, *cfg.Backfill.Workers)
	assert.Equal(t, 5, *cfg.Backfill.MaxRetries)
	assert.Equal(t, "my-bucket/ledgers", cfg.Backfill.BSB.BucketPath)
	assert.Equal(t, 2000, *cfg.Backfill.BSB.BufferSize)
	assert.Equal(t, 40, *cfg.Backfill.BSB.NumWorkers)
	assert.Equal(t, "/mnt/cold", cfg.ImmutableStorage.Path)
	assert.Equal(t, "/mnt/catalog", cfg.Catalog.Path)
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
	assert.Equal(t, "bucket/path", cfg.Backfill.BSB.BucketPath)
	assert.Equal(t, "/etc/cc.toml", cfg.Streaming.CaptiveCoreConfig)

	// Documented defaults filled.
	assert.Equal(t, runtime.GOMAXPROCS(0), *cfg.Backfill.Workers)
	assert.Equal(t, DefaultMaxRetries, *cfg.Backfill.MaxRetries)
	assert.Equal(t, DefaultBSBBufferSize, *cfg.Backfill.BSB.BufferSize)
	assert.Equal(t, DefaultBSBNumWorkers, *cfg.Backfill.BSB.NumWorkers)
	assert.Equal(t, uint32(0), *cfg.Streaming.RetentionChunks)
	assert.Equal(t, DefaultEarliestLedger, cfg.Streaming.EarliestLedger)
	assert.Equal(t, DefaultLogLevel, cfg.Logging.Level)
	assert.Equal(t, DefaultLogFormat, cfg.Logging.Format)
}

func TestParseConfig_ExplicitZeroPreserved(t *testing.T) {
	// An explicit zero must NOT be overwritten by the default — validateConfig
	// is what rejects an illegal zero (e.g. workers), so the defaulting layer
	// must preserve it for that rejection to fire.
	const cfgText = `
[service]
default_data_dir = "/d"
[backfill]
workers = 0
max_retries = 0
[streaming]
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

// A typo'd key must be REJECTED, not silently defaulted: earliest_ledger is
// pinned immutably on first start, so a silent fallback would permanently pin
// the wrong value. Strict decoding catches the typo before any pin is written.
func TestParseConfig_RejectsUnknownKeys(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{
			name: "typo'd workers",
			text: `
[service]
default_data_dir = "/d"
[backfill]
workrs = 7
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
[backfill.bsb]
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
	assert.Equal(t, filepath.Join("/data", "catalog", "rocksdb"), p.Catalog)
	assert.Equal(t, "/data", p.Cold, "the cold root defaults to the data dir")
	assert.Equal(t, filepath.Join("/data", "hot"), p.HotStorage)
}

func TestResolvePaths_OverridesWin(t *testing.T) {
	cfg, err := ParseConfig([]byte(fullValidConfig))
	require.NoError(t, err)
	p := cfg.ResolvePaths()

	assert.Equal(t, "/mnt/catalog", p.Catalog)
	assert.Equal(t, "/mnt/cold", p.Cold)
	assert.Equal(t, "/mnt/hot", p.HotStorage)
}

func TestLockRoots_AllDistinctRoots(t *testing.T) {
	cfg, err := ParseConfig([]byte(minimalValidConfig))
	require.NoError(t, err)
	roots := cfg.ResolvePaths().LockRoots()
	// Meta store + cold-tier root + hot storage = three roots.
	require.Len(t, roots, 3)
	assert.Contains(t, roots, filepath.Join("/data", "catalog", "rocksdb"))
	assert.Contains(t, roots, "/data", "the cold-tier root (defaulting to the data dir) is locked")
	assert.Contains(t, roots, filepath.Join("/data", "hot"))
}
