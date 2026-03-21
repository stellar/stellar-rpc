package backfill

import (
	"fmt"
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
	if cfg.ImmutableStores.ImmutableBase != "/data/test/immutable" {
		t.Errorf("ImmutableBase = %q", cfg.ImmutableStores.ImmutableBase)
	}
	if cfg.Logging.LogFile != "/data/test/logs/backfill.log" {
		t.Errorf("LogFile = %q", cfg.Logging.LogFile)
	}
	if cfg.Logging.ErrorFile != "/data/test/logs/backfill-error.log" {
		t.Errorf("ErrorFile = %q", cfg.Logging.ErrorFile)
	}
	if cfg.Backfill.Workers != 40 {
		t.Errorf("Workers = %d, want 40", cfg.Backfill.Workers)
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
	if cfg.Backfill.VerifyRecSplit == nil || !*cfg.Backfill.VerifyRecSplit {
		t.Errorf("VerifyRecSplit should default to true, got %v", cfg.Backfill.VerifyRecSplit)
	}
	if cfg.Logging.MaxScopeDepth != 0 {
		t.Errorf("MaxScopeDepth = %d, want 0", cfg.Logging.MaxScopeDepth)
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
immutable_base = "/ssd1/immutable"

[backfill]
start_ledger = 2
end_ledger = 10000001
chunks_per_txhash_index = 100
workers = 80
verify_recsplit = false

[backfill.bsb]
bucket_path = "custom-bucket"
buffer_size = 500
num_workers = 10

[logging]
log_file = "/var/log/backfill.log"
error_file = "/var/log/backfill-error.log"
max_scope_depth = 3
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
	if cfg.ImmutableStores.ImmutableBase != "/ssd1/immutable" {
		t.Errorf("ImmutableBase = %q", cfg.ImmutableStores.ImmutableBase)
	}
	if cfg.Backfill.ChunksPerTxHashIndex != 100 {
		t.Errorf("ChunksPerTxHashIndex = %d, want 100", cfg.Backfill.ChunksPerTxHashIndex)
	}
	if cfg.Backfill.Workers != 80 {
		t.Errorf("Workers = %d, want 80", cfg.Backfill.Workers)
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
		t.Errorf("MaxScopeDepth = %d, want 3", cfg.Logging.MaxScopeDepth)
	}
	if cfg.Backfill.VerifyRecSplit == nil || *cfg.Backfill.VerifyRecSplit {
		t.Errorf("VerifyRecSplit should be false when explicitly set, got %v", cfg.Backfill.VerifyRecSplit)
	}
}

func TestConfigChunksPerTxHashIndexDefault(t *testing.T) {
	dir := t.TempDir()
	path := writeConfigFile(t, dir, `
[service]
data_dir = "/data"

[backfill]
start_ledger = 2
end_ledger = 10000001

[backfill.bsb]
bucket_path = "test"
`)

	cfg, _ := LoadConfig(path)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if cfg.Backfill.ChunksPerTxHashIndex != 1000 {
		t.Errorf("ChunksPerTxHashIndex = %d, want 1000 (default)", cfg.Backfill.ChunksPerTxHashIndex)
	}

	geo := cfg.BuildGeometry()
	if geo.RangeSize != 10_000_000 {
		t.Errorf("Geometry.RangeSize = %d", geo.RangeSize)
	}
	if geo.ChunkSize != 10_000 {
		t.Errorf("Geometry.ChunkSize = %d", geo.ChunkSize)
	}
	if geo.ChunksPerTxHashIndex != 1000 {
		t.Errorf("Geometry.ChunksPerTxHashIndex = %d", geo.ChunksPerTxHashIndex)
	}
}

func TestConfigChunksPerTxHashIndexAllowed(t *testing.T) {
	tests := []struct {
		chunksPerIndex int
		endLedger      uint32
	}{
		{1, 10_001},
		{10, 100_001},
		{100, 1_000_001},
		{1000, 10_000_001},
	}

	for _, tt := range tests {
		dir := t.TempDir()
		path := writeConfigFile(t, dir, fmt.Sprintf(`
[service]
data_dir = "/data"

[backfill]
chunks_per_txhash_index = %d
start_ledger = 2
end_ledger = %d

[backfill.bsb]
bucket_path = "test"
`, tt.chunksPerIndex, tt.endLedger))

		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("LoadConfig (chunks_per_txhash_index=%d): %v", tt.chunksPerIndex, err)
		}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate (chunks_per_txhash_index=%d): %v", tt.chunksPerIndex, err)
			continue
		}

		geo := cfg.BuildGeometry()
		if geo.ChunksPerTxHashIndex != uint32(tt.chunksPerIndex) {
			t.Errorf("chunks_per_txhash_index=%d: ChunksPerTxHashIndex = %d, want %d",
				tt.chunksPerIndex, geo.ChunksPerTxHashIndex, tt.chunksPerIndex)
		}
	}
}

func TestConfigChunksPerTxHashIndexInvalid(t *testing.T) {
	invalid := []int{2, 5, 7, 50, 500, 2000}

	for _, cpi := range invalid {
		dir := t.TempDir()
		path := writeConfigFile(t, dir, fmt.Sprintf(`
[service]
data_dir = "/data"

[backfill]
chunks_per_txhash_index = %d
start_ledger = 2
end_ledger = 10000001

[backfill.bsb]
bucket_path = "test"
`, cpi))

		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("LoadConfig (chunks_per_txhash_index=%d): %v", cpi, err)
		}
		if err := cfg.Validate(); err == nil {
			t.Errorf("chunks_per_txhash_index=%d should be rejected but was accepted", cpi)
		} else if !strings.Contains(err.Error(), "not valid") {
			t.Errorf("chunks_per_txhash_index=%d: unexpected error: %v", cpi, err)
		}
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
