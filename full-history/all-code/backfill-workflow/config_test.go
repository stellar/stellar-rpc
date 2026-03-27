package backfill

import (
	"testing"
)

// validConfig returns a minimal valid config for tests.
func validConfig() *Config {
	return &Config{
		Service: ServiceConfig{DefaultDataDir: "/data/stellar-rpc"},
		Backfill: BackfillConfig{
			ChunksPerTxHashIndex: 1000,
			BSB: &BSBConfig{
				BucketPath: "sdf-ledger-close-meta/v1/ledgers/pubnet",
			},
		},
	}
}

// mockConfigMetaStore is a simple in-memory meta store for config validation tests.
type mockConfigMetaStore struct {
	data map[string]string
}

func newMockConfigMetaStore() *mockConfigMetaStore {
	return &mockConfigMetaStore{data: make(map[string]string)}
}

func (m *mockConfigMetaStore) Get(key string) (string, error) {
	return m.data[key], nil
}

func (m *mockConfigMetaStore) Put(key, value string) error {
	m.data[key] = value
	return nil
}

func TestConfigNewSchema(t *testing.T) {
	tomlData := `
[service]
default_data_dir = "/data/stellar-rpc"

[backfill]
chunks_per_txhash_index = 1000

[immutable_storage.ledgers]
path = "/mnt/nvme/ledgers"

[immutable_storage.events]
path = "/mnt/nvme/events"

[immutable_storage.txhash_raw]
path = "/mnt/nvme/txhash/raw"

[immutable_storage.txhash_index]
path = "/mnt/nvme/txhash/index"

[backfill.bsb]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`
	cfg, err := ParseConfig([]byte(tomlData))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if cfg.ImmutableStorage.Ledgers.Path != "/mnt/nvme/ledgers" {
		t.Errorf("Ledgers.Path = %q", cfg.ImmutableStorage.Ledgers.Path)
	}
	if cfg.ImmutableStorage.Events.Path != "/mnt/nvme/events" {
		t.Errorf("Events.Path = %q", cfg.ImmutableStorage.Events.Path)
	}
	if cfg.ImmutableStorage.TxHashRaw.Path != "/mnt/nvme/txhash/raw" {
		t.Errorf("TxHashRaw.Path = %q", cfg.ImmutableStorage.TxHashRaw.Path)
	}
	if cfg.ImmutableStorage.TxHashIndex.Path != "/mnt/nvme/txhash/index" {
		t.Errorf("TxHashIndex.Path = %q", cfg.ImmutableStorage.TxHashIndex.Path)
	}
}

func TestConfigDefaults(t *testing.T) {
	tomlData := `
[service]
default_data_dir = "/data/stellar-rpc"

[backfill.bsb]
bucket_path = "bucket"
`
	cfg, err := ParseConfig([]byte(tomlData))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if cfg.ImmutableStorage.Ledgers.Path != "/data/stellar-rpc/ledgers" {
		t.Errorf("Ledgers.Path = %q", cfg.ImmutableStorage.Ledgers.Path)
	}
	if cfg.ImmutableStorage.Events.Path != "/data/stellar-rpc/events" {
		t.Errorf("Events.Path = %q", cfg.ImmutableStorage.Events.Path)
	}
	if cfg.ImmutableStorage.TxHashRaw.Path != "/data/stellar-rpc/txhash/raw" {
		t.Errorf("TxHashRaw.Path = %q", cfg.ImmutableStorage.TxHashRaw.Path)
	}
	if cfg.ImmutableStorage.TxHashIndex.Path != "/data/stellar-rpc/txhash/index" {
		t.Errorf("TxHashIndex.Path = %q", cfg.ImmutableStorage.TxHashIndex.Path)
	}
	if cfg.Backfill.ChunksPerTxHashIndex != 1000 {
		t.Errorf("ChunksPerTxHashIndex = %d, want 1000", cfg.Backfill.ChunksPerTxHashIndex)
	}
	if cfg.Backfill.BSB.BufferSize != 1000 {
		t.Errorf("BufferSize = %d, want 1000", cfg.Backfill.BSB.BufferSize)
	}
	if cfg.Backfill.BSB.NumWorkers != 20 {
		t.Errorf("NumWorkers = %d, want 20", cfg.Backfill.BSB.NumWorkers)
	}
	if cfg.MetaStore.Path != "/data/stellar-rpc/meta/rocksdb" {
		t.Errorf("MetaStore.Path = %q", cfg.MetaStore.Path)
	}
}

func TestConfigMissingDefaultDataDir(t *testing.T) {
	tomlData := `
[backfill.bsb]
bucket_path = "test"
`
	cfg, err := ParseConfig([]byte(tomlData))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	err = cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing default_data_dir")
	}
	if got := err.Error(); got != "service.default_data_dir is required" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestConfigMissingBSB(t *testing.T) {
	tomlData := `
[service]
default_data_dir = "/data"
`
	cfg, err := ParseConfig([]byte(tomlData))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	err = cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing BSB")
	}
	if got := err.Error(); got != "[backfill.bsb] is required" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestConfigBSBMissingBucketPath(t *testing.T) {
	tomlData := `
[service]
default_data_dir = "/data"

[backfill.bsb]
`
	cfg, err := ParseConfig([]byte(tomlData))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	err = cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing bucket_path")
	}
	if got := err.Error(); got != "backfill.bsb.bucket_path is required" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestChunkBoundaryExpansion(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	flags := CLIFlags{StartLedger: 5_000_000, EndLedger: 56_337_842, VerifyRecSplit: true}
	cfg.ApplyFlags(flags)

	if cfg.EffectiveStartLedger != 4_990_002 {
		t.Errorf("EffectiveStartLedger = %d, want 4990002", cfg.EffectiveStartLedger)
	}
	if cfg.EffectiveEndLedger != 56_340_001 {
		t.Errorf("EffectiveEndLedger = %d, want 56340001", cfg.EffectiveEndLedger)
	}
}

func TestChunkBoundaryExpansion_AlreadyAligned(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// chunk 0: [2, 10001], chunk 999: [9_990_002, 10_000_001]
	flags := CLIFlags{StartLedger: 2, EndLedger: 10_000_001, VerifyRecSplit: true}
	cfg.ApplyFlags(flags)

	if cfg.EffectiveStartLedger != 2 {
		t.Errorf("EffectiveStartLedger = %d, want 2", cfg.EffectiveStartLedger)
	}
	if cfg.EffectiveEndLedger != 10_000_001 {
		t.Errorf("EffectiveEndLedger = %d, want 10000001", cfg.EffectiveEndLedger)
	}
}

func TestValidateFlags(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// start < 2
	err := cfg.ValidateFlags(CLIFlags{StartLedger: 1, EndLedger: 100})
	if err == nil {
		t.Fatal("expected error for start < 2")
	}

	// end <= start
	err = cfg.ValidateFlags(CLIFlags{StartLedger: 100, EndLedger: 100})
	if err == nil {
		t.Fatal("expected error for end <= start")
	}

	// valid
	err = cfg.ValidateFlags(CLIFlags{StartLedger: 2, EndLedger: 10_000_001})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunksPerTxHashIndexImmutability(t *testing.T) {
	meta := newMockConfigMetaStore()

	// First run: no key present — writes it.
	cfg := validConfig()
	cfg.Backfill.ChunksPerTxHashIndex = 1000
	if err := cfg.ValidateAgainstMetaStore(meta); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if meta.data["config:chunks_per_txhash_index"] != "1000" {
		t.Errorf("key not persisted: %v", meta.data)
	}

	// Second run: same value — passes.
	if err := cfg.ValidateAgainstMetaStore(meta); err != nil {
		t.Fatalf("second run (same value): %v", err)
	}

	// Third run: different value — fails.
	cfg.Backfill.ChunksPerTxHashIndex = 500
	err := cfg.ValidateAgainstMetaStore(meta)
	if err == nil {
		t.Fatal("expected error for changed chunks_per_txhash_index")
	}
	if got := err.Error(); got != "chunks_per_txhash_index changed: meta store has 1000, config has 500" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestConfigBuildGeometry(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	geo := cfg.BuildGeometry()
	if geo.RangeSize != 10_000_000 {
		t.Errorf("RangeSize = %d", geo.RangeSize)
	}
	if geo.ChunkSize != 10_000 {
		t.Errorf("ChunkSize = %d", geo.ChunkSize)
	}
	if geo.ChunksPerTxHashIndex != 1000 {
		t.Errorf("ChunksPerTxHashIndex = %d", geo.ChunksPerTxHashIndex)
	}
}

func TestConfigCustomValues(t *testing.T) {
	tomlData := `
[service]
default_data_dir = "/data"

[meta_store]
path = "/ssd0/meta"

[immutable_storage.ledgers]
path = "/ssd1/ledgers"

[immutable_storage.events]
path = "/ssd1/events"

[immutable_storage.txhash_raw]
path = "/ssd1/txhash/raw"

[immutable_storage.txhash_index]
path = "/ssd1/txhash/index"

[backfill]
chunks_per_txhash_index = 100

[backfill.bsb]
bucket_path = "custom-bucket"
buffer_size = 500
num_workers = 10

[logging]
log_file = "/var/log/backfill.log"
error_file = "/var/log/backfill-error.log"
max_scope_depth = 3
`
	cfg, err := ParseConfig([]byte(tomlData))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if cfg.MetaStore.Path != "/ssd0/meta" {
		t.Errorf("MetaStore.Path = %q", cfg.MetaStore.Path)
	}
	if cfg.ImmutableStorage.Ledgers.Path != "/ssd1/ledgers" {
		t.Errorf("Ledgers.Path = %q", cfg.ImmutableStorage.Ledgers.Path)
	}
	if cfg.Backfill.ChunksPerTxHashIndex != 100 {
		t.Errorf("ChunksPerTxHashIndex = %d", cfg.Backfill.ChunksPerTxHashIndex)
	}
	if cfg.Backfill.BSB.BufferSize != 500 {
		t.Errorf("BufferSize = %d", cfg.Backfill.BSB.BufferSize)
	}
	if cfg.Backfill.BSB.NumWorkers != 10 {
		t.Errorf("NumWorkers = %d", cfg.Backfill.BSB.NumWorkers)
	}
	if cfg.Logging.LogFile != "/var/log/backfill.log" {
		t.Errorf("LogFile = %q", cfg.Logging.LogFile)
	}
	if cfg.Logging.ErrorFile != "/var/log/backfill-error.log" {
		t.Errorf("ErrorFile = %q", cfg.Logging.ErrorFile)
	}
	if cfg.Logging.MaxScopeDepth != 3 {
		t.Errorf("MaxScopeDepth = %d", cfg.Logging.MaxScopeDepth)
	}
}

func TestApplyFlags_DefaultWorkers(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	flags := CLIFlags{StartLedger: 2, EndLedger: 10_000_001, Workers: 0, VerifyRecSplit: true}
	cfg.ApplyFlags(flags)

	// Workers should default to GOMAXPROCS.
	if cfg.Workers <= 0 {
		t.Errorf("Workers = %d, should be > 0 (GOMAXPROCS)", cfg.Workers)
	}
}

func TestApplyFlags_DefaultMaxRetries(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	flags := CLIFlags{StartLedger: 2, EndLedger: 10_000_001, MaxRetries: 0, VerifyRecSplit: true}
	cfg.ApplyFlags(flags)

	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", cfg.MaxRetries)
	}
}
