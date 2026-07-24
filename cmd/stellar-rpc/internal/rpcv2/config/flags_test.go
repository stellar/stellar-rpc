package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newBoundFlagSet(t *testing.T) *pflag.FlagSet {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	BindFlags(fs)
	return fs
}

func TestBindFlags_LockstepWithTOMLSchema(t *testing.T) {
	fs := newBoundFlagSet(t)

	// Every TOML leaf has a flag — walked from the same struct BindFlags walks,
	// so this can only fail if BindFlags skipped a visit (or panicked on an
	// unsupported type, which is its own guard).
	count := 0
	walkLeaves(reflect.ValueOf(&Config{}).Elem(), "", func(path string, _ reflect.Value) {
		count++
		assert.NotNil(t, fs.Lookup(path), "TOML leaf %s has no flag", path)
	})
	assert.Greater(t, count, 40, "the schema has dozens of leaves; a tiny count means the walk broke")

	// Spot-check names across every section and nesting depth.
	for _, name := range []string{
		"storage.default_data_dir",
		"storage.hot",
		"retention.earliest_ledger",
		"retention.retention_chunks",
		"backfill.workers",
		"backfill.datastore.type",
		"backfill.datastore.params",
		"backfill.datastore.schema.ledgers_per_file",
		"backfill.bsb.retry_wait",
		"ingestion.captive_core_config",
		"ingestion.history_archive_urls",
		"logging.level",
		"service.endpoint",
		"service.max_concurrent_requests",
		"service.fee_stats.classic_fee_window_ledgers",
		"service.methods.queue_limit",
		"service.methods.getLedgers.queue_limit",
		"service.methods.getLedgers.max_items_per_response",
		"service.methods.getHealth.max_healthy_ledger_latency",
		"service.methods.getFeeStats.max_execution_duration",
	} {
		assert.NotNil(t, fs.Lookup(name), "expected flag --%s", name)
	}
}

func TestApplyFlags_NoFlagsSetIsANoOp(t *testing.T) {
	fs := newBoundFlagSet(t)
	cfg, err := DecodeConfig([]byte(minimalValidConfig))
	require.NoError(t, err)
	before := cfg

	require.NoError(t, ApplyFlags(&cfg, fs))
	assert.Equal(t, before, cfg)
}

func TestApplyFlags_OverridesFileValues(t *testing.T) {
	fs := newBoundFlagSet(t)
	require.NoError(t, fs.Parse([]string{
		"--storage.default_data_dir=/flag/data",
		"--service.endpoint=0.0.0.0:7777",
		"--service.fee_stats.classic_fee_window_ledgers=33",
		"--backfill.workers=3",
		"--ingestion.history_archive_urls=https://a.example,https://b.example",
		"--backfill.bsb.retry_wait=7s",
	}))

	cfg, err := DecodeConfig([]byte(minimalValidConfig))
	require.NoError(t, err)
	require.NoError(t, ApplyFlags(&cfg, fs))
	cfg = cfg.WithDefaults()

	assert.Equal(t, "/flag/data", cfg.Storage.DefaultDataDir)
	assert.Equal(t, "0.0.0.0:7777", cfg.Service.Endpoint)
	assert.Equal(t, uint32(33), *cfg.Service.FeeStats.ClassicFeeWindowLedgers)
	assert.Equal(t, 3, *cfg.Backfill.Workers)
	assert.Equal(t, []string{"https://a.example", "https://b.example"}, cfg.Ingestion.HistoryArchiveURLs)
	assert.Equal(t, 7*time.Second, cfg.Backfill.BSB.RetryWait)
}

// The precedence examples from issue #882: specificity beats source; within a
// tier, CLI beats file; compiled defaults are the last tier.
func TestApplyFlags_Precedence(t *testing.T) {
	const fileWithTiers = `
[storage]
default_data_dir = "/d"
[service.methods]
queue_limit = 10
[service.methods.getLedgers]
queue_limit = 5
[ingestion]
captive_core_config = "/cc"
`

	load := func(t *testing.T, text string, args ...string) Config {
		t.Helper()
		fs := newBoundFlagSet(t)
		require.NoError(t, fs.Parse(args))
		cfg, err := DecodeConfig([]byte(text))
		require.NoError(t, err)
		require.NoError(t, ApplyFlags(&cfg, fs))
		return cfg.WithDefaults()
	}

	t.Run("example A: CLI raises the default tier; explicit per-method file value survives", func(t *testing.T) {
		cfg := load(t, fileWithTiers, "--service.methods.queue_limit=30")
		m := cfg.Service.Methods

		assert.Equal(t, uint(5), *m.GetLedgers.QueueLimit, "tier 1 (file) beats a tier-2 CLI value")
		assert.Equal(t, uint(30), *m.GetHealth.QueueLimit, "tier 2: CLI beats file's 10")
		assert.Equal(t, uint(30), *m.GetFeeStats.QueueLimit, "tier 2 also beats getFeeStats' compiled 100")
	})

	t.Run("example B: CLI overrides one method only", func(t *testing.T) {
		cfg := load(t, `
[storage]
default_data_dir = "/d"
[service.methods.getLedgers]
queue_limit = 5
[ingestion]
captive_core_config = "/cc"
`, "--service.methods.getLedgers.queue_limit=50")
		m := cfg.Service.Methods

		assert.Equal(t, uint(50), *m.GetLedgers.QueueLimit, "tier 1: CLI beats file within the tier")
		assert.Equal(t, DefaultMethodQueueLimit, *m.GetHealth.QueueLimit, "tier 3: compiled default")
		assert.Equal(t, DefaultGetFeeStatsQueueLimit, *m.GetFeeStats.QueueLimit, "tier 3: compiled default")
	})

	t.Run("example D: durations cascade the same way", func(t *testing.T) {
		cfg := load(t, `
[storage]
default_data_dir = "/d"
[service.methods]
max_execution_duration = "8s"
[ingestion]
captive_core_config = "/cc"
`, "--service.methods.getEvents.max_execution_duration=20s")
		m := cfg.Service.Methods

		assert.Equal(t, 20*time.Second, *m.GetEvents.MaxExecutionDuration, "tier 1 (CLI)")
		assert.Equal(t, 8*time.Second, *m.GetLedgers.MaxExecutionDuration, "tier 2 (file) beats compiled 10s")
		assert.Equal(t, 8*time.Second, *m.GetHealth.MaxExecutionDuration, "tier 2 (file)")
	})
}
