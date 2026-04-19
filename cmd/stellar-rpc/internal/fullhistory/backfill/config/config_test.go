package config

import (
	"reflect"
	"testing"
)

// minimalValidTOML returns the smallest TOML that parses + validates. Used
// as a starting point by tests that focus on a specific behavior without
// re-declaring the full schema.
func minimalValidTOML() []byte {
	return []byte(`
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"

[BACKFILL.BSB]
BUCKET_PATH = "bucket"
`)
}

func TestParseConfig_ServiceSection(t *testing.T) {
	data := []byte(`
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"
`)
	cfg, err := ParseConfig(data)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Service.DefaultDataDir != "/data/stellar-rpc" {
		t.Errorf("DefaultDataDir = %q", cfg.Service.DefaultDataDir)
	}
}

// TestParseConfig_FullSchema — every section decodes; a silently-ignored
// field shows up as the zero value of its type.
func TestParseConfig_FullSchema(t *testing.T) {
	data := []byte(`
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"

[META_STORE]
PATH = "/ssd0/meta"

[BACKFILL]
CHUNKS_PER_TXHASH_INDEX = 500

[BACKFILL.BSB]
BUCKET_PATH = "sdf-ledger-close-meta/v1/ledgers/pubnet"
BUFFER_SIZE = 250
NUM_WORKERS = 8

[IMMUTABLE_STORAGE.LEDGERS]
PATH = "/ssd1/ledgers"

[IMMUTABLE_STORAGE.EVENTS]
PATH = "/ssd1/events"

[IMMUTABLE_STORAGE.TXHASH_RAW]
PATH = "/ssd1/txhash/raw"

[IMMUTABLE_STORAGE.TXHASH_INDEX]
PATH = "/ssd1/txhash/index"

[LOGGING]
LEVEL = "debug"
FORMAT = "json"
`)
	cfg, err := ParseConfig(data)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}

	if cfg.Service.DefaultDataDir != "/data/stellar-rpc" {
		t.Errorf("Service.DefaultDataDir = %q", cfg.Service.DefaultDataDir)
	}
	if cfg.MetaStore.Path != "/ssd0/meta" {
		t.Errorf("MetaStore.Path = %q", cfg.MetaStore.Path)
	}
	if cfg.Backfill.ChunksPerTxHashIndex != 500 {
		t.Errorf("Backfill.ChunksPerTxHashIndex = %d", cfg.Backfill.ChunksPerTxHashIndex)
	}
	if cfg.Backfill.BSB == nil {
		t.Fatal("Backfill.BSB is nil — [BACKFILL.BSB] did not decode")
	}
	if cfg.Backfill.BSB.BucketPath != "sdf-ledger-close-meta/v1/ledgers/pubnet" {
		t.Errorf("BSB.BucketPath = %q", cfg.Backfill.BSB.BucketPath)
	}
	if cfg.Backfill.BSB.BufferSize != 250 {
		t.Errorf("BSB.BufferSize = %d", cfg.Backfill.BSB.BufferSize)
	}
	if cfg.Backfill.BSB.NumWorkers != 8 {
		t.Errorf("BSB.NumWorkers = %d", cfg.Backfill.BSB.NumWorkers)
	}
	if cfg.ImmutableStorage.Ledgers.Path != "/ssd1/ledgers" {
		t.Errorf("ImmutableStorage.Ledgers.Path = %q", cfg.ImmutableStorage.Ledgers.Path)
	}
	if cfg.ImmutableStorage.Events.Path != "/ssd1/events" {
		t.Errorf("ImmutableStorage.Events.Path = %q", cfg.ImmutableStorage.Events.Path)
	}
	if cfg.ImmutableStorage.TxHashRaw.Path != "/ssd1/txhash/raw" {
		t.Errorf("ImmutableStorage.TxHashRaw.Path = %q", cfg.ImmutableStorage.TxHashRaw.Path)
	}
	if cfg.ImmutableStorage.TxHashIndex.Path != "/ssd1/txhash/index" {
		t.Errorf("ImmutableStorage.TxHashIndex.Path = %q", cfg.ImmutableStorage.TxHashIndex.Path)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("Logging.Level = %q", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("Logging.Format = %q", cfg.Logging.Format)
	}
}

// TestConfig_MarshalTOMLRoundTrip — parse → marshal → parse yields an
// equal Config. Guards against accidentally missing struct tags.
func TestConfig_MarshalTOMLRoundTrip(t *testing.T) {
	data := []byte(`
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"

[META_STORE]
PATH = "/ssd0/meta"

[BACKFILL]
CHUNKS_PER_TXHASH_INDEX = 500

[BACKFILL.BSB]
BUCKET_PATH = "bucket"
BUFFER_SIZE = 250
NUM_WORKERS = 8

[IMMUTABLE_STORAGE.LEDGERS]
PATH = "/ssd1/ledgers"

[IMMUTABLE_STORAGE.EVENTS]
PATH = "/ssd1/events"

[IMMUTABLE_STORAGE.TXHASH_RAW]
PATH = "/ssd1/txhash/raw"

[IMMUTABLE_STORAGE.TXHASH_INDEX]
PATH = "/ssd1/txhash/index"

[LOGGING]
LEVEL = "debug"
FORMAT = "json"
`)
	original, err := ParseConfig(data)
	if err != nil {
		t.Fatalf("ParseConfig(original): %v", err)
	}
	marshaled, err := original.MarshalTOML()
	if err != nil {
		t.Fatalf("MarshalTOML: %v", err)
	}
	roundTripped, err := ParseConfig(marshaled)
	if err != nil {
		t.Fatalf("ParseConfig(round-tripped):\n%s\nerror: %v", marshaled, err)
	}
	if !reflect.DeepEqual(original, roundTripped) {
		t.Errorf("round-trip mismatch:\noriginal:      %+v\nround-tripped: %+v\nintermediate TOML:\n%s",
			original, roundTripped, marshaled)
	}
}

// TestValidate_ResolvesDefaults — Validate fills in every optional path
// and numeric default when the TOML omits them.
func TestValidate_ResolvesDefaults(t *testing.T) {
	cfg, err := ParseConfig(minimalValidTOML())
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	want := map[string]string{
		"MetaStore.Path":                    "/data/stellar-rpc/meta/rocksdb",
		"ImmutableStorage.Ledgers.Path":     "/data/stellar-rpc/ledgers",
		"ImmutableStorage.Events.Path":      "/data/stellar-rpc/events",
		"ImmutableStorage.TxHashRaw.Path":   "/data/stellar-rpc/txhash/raw",
		"ImmutableStorage.TxHashIndex.Path": "/data/stellar-rpc/txhash/index",
	}
	got := map[string]string{
		"MetaStore.Path":                    cfg.MetaStore.Path,
		"ImmutableStorage.Ledgers.Path":     cfg.ImmutableStorage.Ledgers.Path,
		"ImmutableStorage.Events.Path":      cfg.ImmutableStorage.Events.Path,
		"ImmutableStorage.TxHashRaw.Path":   cfg.ImmutableStorage.TxHashRaw.Path,
		"ImmutableStorage.TxHashIndex.Path": cfg.ImmutableStorage.TxHashIndex.Path,
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s = %q, want %q", k, got[k], w)
		}
	}

	if cfg.Backfill.ChunksPerTxHashIndex != 1000 {
		t.Errorf("Backfill.ChunksPerTxHashIndex = %d, want 1000", cfg.Backfill.ChunksPerTxHashIndex)
	}
	if cfg.Backfill.BSB.BufferSize != 1000 {
		t.Errorf("BSB.BufferSize = %d, want 1000", cfg.Backfill.BSB.BufferSize)
	}
	if cfg.Backfill.BSB.NumWorkers != 20 {
		t.Errorf("BSB.NumWorkers = %d, want 20", cfg.Backfill.BSB.NumWorkers)
	}
}
