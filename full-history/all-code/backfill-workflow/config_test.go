package backfill

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfigFile(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadConfigValid(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data/stellar-rpc"

[backfill]
start_ledger = 2
end_ledger = 10000001

[backfill.bsb]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if cfg.Service.DataDir != "/data/stellar-rpc" {
		t.Errorf("DataDir = %q", cfg.Service.DataDir)
	}
	if cfg.Backfill.StartLedger != 2 {
		t.Errorf("StartLedger = %d", cfg.Backfill.StartLedger)
	}
	if cfg.Backfill.EndLedger != 10000001 {
		t.Errorf("EndLedger = %d", cfg.Backfill.EndLedger)
	}
	if cfg.Backfill.BSB.BucketPath != "sdf-ledger-close-meta/v1/ledgers/pubnet" {
		t.Errorf("BucketPath = %q", cfg.Backfill.BSB.BucketPath)
	}
}

func TestConfigDefaultResolution(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data/test"

[backfill]
start_ledger = 2
end_ledger = 10000001

[backfill.bsb]
bucket_path = "test-bucket"
`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Check defaults
	if cfg.MetaStore.Path != "/data/test/meta/rocksdb" {
		t.Errorf("MetaStore.Path = %q", cfg.MetaStore.Path)
	}
	if cfg.ImmutableStores.LedgersBase != "/data/test/immutable/ledgers" {
		t.Errorf("LedgersBase = %q", cfg.ImmutableStores.LedgersBase)
	}
	if cfg.ImmutableStores.TxHashBase != "/data/test/immutable/txhash" {
		t.Errorf("TxHashBase = %q", cfg.ImmutableStores.TxHashBase)
	}
	if cfg.Logging.LogFile != "/data/test/logs/backfill.log" {
		t.Errorf("LogFile = %q", cfg.Logging.LogFile)
	}
	if cfg.Logging.ErrorFile != "/data/test/logs/backfill-error.log" {
		t.Errorf("ErrorFile = %q", cfg.Logging.ErrorFile)
	}
	if cfg.Backfill.ParallelRanges != 2 {
		t.Errorf("ParallelRanges = %d, want 2", cfg.Backfill.ParallelRanges)
	}
	if cfg.Backfill.FlushInterval != 100 {
		t.Errorf("FlushInterval = %d, want 100", cfg.Backfill.FlushInterval)
	}
	if cfg.Backfill.BSB.BufferSize != 1000 {
		t.Errorf("BufferSize = %d, want 1000", cfg.Backfill.BSB.BufferSize)
	}
	if cfg.Backfill.BSB.NumWorkers != 20 {
		t.Errorf("NumWorkers = %d, want 20", cfg.Backfill.BSB.NumWorkers)
	}
	if cfg.Backfill.BSB.NumInstancesPerRange != 20 {
		t.Errorf("NumInstancesPerRange = %d, want 20", cfg.Backfill.BSB.NumInstancesPerRange)
	}
}

func TestConfigMissingDataDir(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[backfill]
start_ledger = 2
end_ledger = 10000001

[backfill.bsb]
bucket_path = "test"
`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	err = cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing data_dir")
	}
	if !strings.Contains(err.Error(), "data_dir") {
		t.Errorf("error should mention data_dir: %v", err)
	}
}

func TestConfigBadStartLedgerAlignment(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[backfill]
start_ledger = 100
end_ledger = 10000001

[backfill.bsb]
bucket_path = "test"
`)

	cfg, _ := LoadConfig(path)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for misaligned start_ledger")
	}
	if !strings.Contains(err.Error(), "range-aligned") {
		t.Errorf("error should mention alignment: %v", err)
	}
}

func TestConfigBadEndLedgerAlignment(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[backfill]
start_ledger = 2
end_ledger = 10000000

[backfill.bsb]
bucket_path = "test"
`)

	cfg, _ := LoadConfig(path)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for misaligned end_ledger")
	}
	if !strings.Contains(err.Error(), "range-aligned") {
		t.Errorf("error should mention alignment: %v", err)
	}
}

func TestConfigEndBeforeStart(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[backfill]
start_ledger = 10000002
end_ledger = 10000001

[backfill.bsb]
bucket_path = "test"
`)

	cfg, _ := LoadConfig(path)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for end < start")
	}
	if !strings.Contains(err.Error(), "must be >") {
		t.Errorf("error should mention ordering: %v", err)
	}
}

func TestConfigBothBackends(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[backfill]
start_ledger = 2
end_ledger = 10000001

[backfill.bsb]
bucket_path = "test"

[backfill.captive_core]
binary_path = "/usr/bin/stellar-core"
config_path = "/etc/stellar.cfg"
`)

	cfg, _ := LoadConfig(path)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for both backends")
	}
	if !strings.Contains(err.Error(), "not both") {
		t.Errorf("error should mention exclusivity: %v", err)
	}
}

func TestConfigNeitherBackend(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[backfill]
start_ledger = 2
end_ledger = 10000001
`)

	cfg, _ := LoadConfig(path)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for no backend")
	}
	if !strings.Contains(err.Error(), "must be specified") {
		t.Errorf("error should mention backend requirement: %v", err)
	}
}

func TestConfigBSBMissingBucketPath(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[backfill]
start_ledger = 2
end_ledger = 10000001

[backfill.bsb]
`)

	cfg, _ := LoadConfig(path)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing bucket_path")
	}
	if !strings.Contains(err.Error(), "bucket_path") {
		t.Errorf("error should mention bucket_path: %v", err)
	}
}

func TestConfigBSBBadInstanceCount(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[backfill]
start_ledger = 2
end_ledger = 10000001

[backfill.bsb]
bucket_path = "test"
num_instances_per_range = 7
`)

	cfg, _ := LoadConfig(path)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for bad instance count")
	}
	if !strings.Contains(err.Error(), "divide") {
		t.Errorf("error should mention divisibility: %v", err)
	}
}

func TestConfigCaptiveCoreMissingFields(t *testing.T) {
	dir := t.TempDir()

	// Missing binary_path
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[backfill]
start_ledger = 2
end_ledger = 10000001

[backfill.captive_core]
config_path = "/etc/stellar.cfg"
`)

	cfg, _ := LoadConfig(path)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing binary_path")
	}
	if !strings.Contains(err.Error(), "binary_path") {
		t.Errorf("error should mention binary_path: %v", err)
	}
}

func TestConfigCustomValues(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[meta_store]
path = "/ssd0/meta"

[immutable_stores]
ledgers_base = "/ssd1/ledgers"
txhash_base = "/ssd2/txhash"

[backfill]
start_ledger = 2
end_ledger = 10000001
parallel_ranges = 4
flush_interval = 50

[backfill.bsb]
bucket_path = "custom-bucket"
buffer_size = 500
num_workers = 10
num_instances_per_range = 25

[logging]
log_file = "/var/log/backfill.log"
error_file = "/var/log/backfill-error.log"
`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if cfg.MetaStore.Path != "/ssd0/meta" {
		t.Errorf("MetaStore.Path = %q", cfg.MetaStore.Path)
	}
	if cfg.ImmutableStores.LedgersBase != "/ssd1/ledgers" {
		t.Errorf("LedgersBase = %q", cfg.ImmutableStores.LedgersBase)
	}
	if cfg.Backfill.ParallelRanges != 4 {
		t.Errorf("ParallelRanges = %d", cfg.Backfill.ParallelRanges)
	}
	if cfg.Backfill.FlushInterval != 50 {
		t.Errorf("FlushInterval = %d", cfg.Backfill.FlushInterval)
	}
	if cfg.Backfill.BSB.BufferSize != 500 {
		t.Errorf("BufferSize = %d", cfg.Backfill.BSB.BufferSize)
	}
	if cfg.Backfill.BSB.NumWorkers != 10 {
		t.Errorf("NumWorkers = %d", cfg.Backfill.BSB.NumWorkers)
	}
	if cfg.Backfill.BSB.NumInstancesPerRange != 25 {
		t.Errorf("NumInstancesPerRange = %d", cfg.Backfill.BSB.NumInstancesPerRange)
	}
	if cfg.Logging.LogFile != "/var/log/backfill.log" {
		t.Errorf("LogFile = %q", cfg.Logging.LogFile)
	}
}

func TestConfigMultipleRanges(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[backfill]
start_ledger = 2
end_ledger = 30000001

[backfill.bsb]
bucket_path = "test"
`)

	cfg, _ := LoadConfig(path)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Should cover 3 ranges: 0, 1, 2
	if cfg.Backfill.StartLedger != 2 {
		t.Errorf("StartLedger = %d", cfg.Backfill.StartLedger)
	}
	if cfg.Backfill.EndLedger != 30000001 {
		t.Errorf("EndLedger = %d", cfg.Backfill.EndLedger)
	}
}
