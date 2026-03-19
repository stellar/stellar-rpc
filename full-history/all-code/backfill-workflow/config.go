package backfill

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
)

// =============================================================================
// TOML Configuration
// =============================================================================
//
// The backfill workflow is configured via a TOML file passed with --config.
// The Config struct mirrors the TOML structure exactly.
//
// Backend selection: Include exactly ONE of [backfill.bsb] or [backfill.captive_core].
// The presence of the section IS the backend selector. Both present → error.
// Neither present → error.
//
// Path defaults: If a path field is empty, it defaults to a subdirectory of data_dir.

// Config is the top-level configuration for the backfill workflow.
type Config struct {
	Service         ServiceConfig   `toml:"service"`
	MetaStore       MetaStoreConfig `toml:"meta_store"`
	ImmutableStores ImmutableConfig `toml:"immutable_stores"`
	Backfill        BackfillConfig  `toml:"backfill"`
	Logging         LoggingConfig   `toml:"logging"`
}

// ServiceConfig holds the base data directory.
type ServiceConfig struct {
	// DataDir is the root directory for all backfill data.
	// All other path defaults are derived from this directory.
	// REQUIRED.
	DataDir string `toml:"data_dir"`
}

// MetaStoreConfig holds the path to the meta store (RocksDB).
type MetaStoreConfig struct {
	// Path is the directory for the RocksDB meta store.
	// Default: {data_dir}/meta/rocksdb
	Path string `toml:"path"`
}

// ImmutableConfig holds paths for immutable (write-once) data files.
type ImmutableConfig struct {
	// LedgersBase is the base directory for LFS chunk files.
	// Default: {data_dir}/immutable/ledgers
	LedgersBase string `toml:"ledgers_base"`

	// TxHashBase is the base directory for txhash files (raw .bin + RecSplit indexes).
	// Default: {data_dir}/immutable/txhash
	TxHashBase string `toml:"txhash_base"`
}

// BackfillConfig holds parameters for the backfill pipeline.
type BackfillConfig struct {
	// ChunksPerTxHashIndex is the number of chunks in each txhash index group.
	// Controls the cadence of RecSplit index builds.
	// Allowed values: 1, 10, 100, 1000.
	// Default: 1000 (10M ledgers per index at 10K chunk size).
	ChunksPerTxHashIndex int `toml:"chunks_per_txhash_index"`

	// Workers is the maximum number of concurrent DAG tasks.
	// Default: 40.
	Workers int `toml:"workers"`

	// StartLedger is the first ledger to ingest (inclusive).
	// Must be range-aligned: (StartLedger - 2) % range_size == 0
	// Example valid values (at default 10M): 2, 10_000_002, 20_000_002
	// REQUIRED.
	StartLedger uint32 `toml:"start_ledger"`

	// EndLedger is the last ledger to ingest (inclusive).
	// Must be the last ledger of a range.
	// Example valid values (at default 10M): 10_000_001, 20_000_001, 30_000_001
	// REQUIRED.
	EndLedger uint32 `toml:"end_ledger"`

	// BSB holds GCS/BufferedStorageBackend configuration.
	// Include this section to use GCS as the ledger source.
	// Mutually exclusive with CaptiveCore.
	BSB *BSBConfig `toml:"bsb"`

	// CaptiveCore holds local stellar-core replay configuration.
	// Include this section to use CaptiveStellarCore as the ledger source.
	// Mutually exclusive with BSB.
	CaptiveCore *CaptiveCoreConfig `toml:"captive_core"`

	// VerifyRecSplit controls whether the RecSplit verify phase runs after building.
	// When true (default), all keys are looked up in the built indexes to confirm
	// correctness. When false, verification is skipped (faster but no lookup check).
	// nil = default true.
	VerifyRecSplit *bool `toml:"verify_recsplit"`
}

// BSBConfig holds GCS BufferedStorageBackend configuration.
type BSBConfig struct {
	// BucketPath is the GCS bucket path for ledger data.
	// Must be a bare path (NOT prefixed with gs://).
	// Example: "sdf-ledger-close-meta/v1/ledgers/pubnet"
	// REQUIRED.
	BucketPath string `toml:"bucket_path"`

	// BufferSize is the BSB prefetch buffer depth.
	// Default: 1000.
	BufferSize int `toml:"buffer_size"`

	// NumWorkers is the number of BSB download workers per instance.
	// Default: 20.
	NumWorkers int `toml:"num_workers"`
}

// CaptiveCoreConfig holds CaptiveStellarCore configuration.
type CaptiveCoreConfig struct {
	// BinaryPath is the path to the stellar-core binary.
	// REQUIRED.
	BinaryPath string `toml:"binary_path"`

	// ConfigPath is the path to the stellar-core config file.
	// REQUIRED.
	ConfigPath string `toml:"config_path"`
}

// LoggingConfig holds log file paths.
type LoggingConfig struct {
	// LogFile is the path to the main log file.
	// Default: {data_dir}/logs/backfill.log
	LogFile string `toml:"log_file"`

	// ErrorFile is the path to the error-only log file.
	// Default: {data_dir}/logs/backfill-error.log
	ErrorFile string `toml:"error_file"`

	// MaxScopeDepth controls verbosity by scope nesting depth.
	// 0 = all (default). 2 = range level only, 3 = +BSB/recsplit, 4 = everything.
	MaxScopeDepth int `toml:"max_scope_depth"`
}

// LoadConfig reads and parses a TOML configuration file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}

	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file %s: %w", path, err)
	}

	return &cfg, nil
}

// Validate checks all configuration constraints and resolves defaults.
// After Validate returns nil, all path fields are populated and all
// numeric fields have valid values.
func (c *Config) Validate() error {
	// 1. data_dir is required
	if c.Service.DataDir == "" {
		return fmt.Errorf("service.data_dir is required")
	}

	// 2. Resolve default paths from data_dir
	if c.MetaStore.Path == "" {
		c.MetaStore.Path = filepath.Join(c.Service.DataDir, "meta", "rocksdb")
	}
	if c.ImmutableStores.LedgersBase == "" {
		c.ImmutableStores.LedgersBase = filepath.Join(c.Service.DataDir, "immutable", "ledgers")
	}
	if c.ImmutableStores.TxHashBase == "" {
		c.ImmutableStores.TxHashBase = filepath.Join(c.Service.DataDir, "immutable", "txhash")
	}
	if c.Logging.LogFile == "" {
		c.Logging.LogFile = filepath.Join(c.Service.DataDir, "logs", "backfill.log")
	}
	if c.Logging.ErrorFile == "" {
		c.Logging.ErrorFile = filepath.Join(c.Service.DataDir, "logs", "backfill-error.log")
	}

	// 3. Validate and default chunks_per_txhash_index.
	allowedChunksPerTxHashIndex := map[int]bool{
		1:    true,
		10:   true,
		100:  true,
		1000: true,
	}
	if c.Backfill.ChunksPerTxHashIndex == 0 {
		c.Backfill.ChunksPerTxHashIndex = 1000
	}
	if !allowedChunksPerTxHashIndex[c.Backfill.ChunksPerTxHashIndex] {
		return fmt.Errorf("backfill.chunks_per_txhash_index=%d is not valid; must be one of: 1, 10, 100, 1000",
			c.Backfill.ChunksPerTxHashIndex)
	}

	// 4. Default workers.
	if c.Backfill.Workers <= 0 {
		c.Backfill.Workers = 40
	}

	rangeSize := uint32(c.Backfill.ChunksPerTxHashIndex) * geometry.ChunkSize

	// 5. Validate start_ledger alignment against configured range_size.
	if c.Backfill.StartLedger < geometry.FirstLedger {
		return fmt.Errorf("start_ledger %d must be >= %d", c.Backfill.StartLedger, geometry.FirstLedger)
	}
	if (c.Backfill.StartLedger-geometry.FirstLedger)%rangeSize != 0 {
		return fmt.Errorf("start_ledger %d is not range-aligned: (val-%d) %% %d must equal 0",
			c.Backfill.StartLedger, geometry.FirstLedger, rangeSize)
	}

	// 6. Validate end_ledger alignment against configured range_size.
	if c.Backfill.EndLedger <= c.Backfill.StartLedger {
		return fmt.Errorf("end_ledger %d must be > start_ledger %d",
			c.Backfill.EndLedger, c.Backfill.StartLedger)
	}
	if (c.Backfill.EndLedger-geometry.FirstLedger+1)%rangeSize != 0 {
		return fmt.Errorf("end_ledger %d is not range-aligned: (val-%d+1) %% %d must equal 0",
			c.Backfill.EndLedger, geometry.FirstLedger, rangeSize)
	}

	// 7. Exactly one of BSB or CaptiveCore must be specified.
	// The presence of the TOML section IS the backend selector.
	hasBSB := c.Backfill.BSB != nil
	hasCaptive := c.Backfill.CaptiveCore != nil
	if hasBSB && hasCaptive {
		return fmt.Errorf("exactly one of [backfill.bsb] or [backfill.captive_core] must be specified, not both")
	}
	if !hasBSB && !hasCaptive {
		return fmt.Errorf("exactly one of [backfill.bsb] or [backfill.captive_core] must be specified")
	}

	// 8. BSB validation
	if hasBSB {
		if c.Backfill.BSB.BucketPath == "" {
			return fmt.Errorf("backfill.bsb.bucket_path is required")
		}
		// Default BSB values
		if c.Backfill.BSB.BufferSize <= 0 {
			c.Backfill.BSB.BufferSize = 1000
		}
		if c.Backfill.BSB.NumWorkers <= 0 {
			c.Backfill.BSB.NumWorkers = 20
		}
	}

	// 9. CaptiveCore validation
	if hasCaptive {
		if c.Backfill.CaptiveCore.BinaryPath == "" {
			return fmt.Errorf("backfill.captive_core.binary_path is required")
		}
		if c.Backfill.CaptiveCore.ConfigPath == "" {
			return fmt.Errorf("backfill.captive_core.config_path is required")
		}
	}

	// 10. Default verify_recsplit to true
	if c.Backfill.VerifyRecSplit == nil {
		t := true
		c.Backfill.VerifyRecSplit = &t
	}

	return nil
}

// BuildGeometry returns a Geometry based on the configured chunks_per_txhash_index.
// Must be called after Validate (which defaults ChunksPerTxHashIndex if unset).
func (c *Config) BuildGeometry() geometry.Geometry {
	cpi := uint32(c.Backfill.ChunksPerTxHashIndex)
	return geometry.Geometry{
		RangeSize:      cpi * geometry.ChunkSize,
		ChunkSize:      geometry.ChunkSize,
		ChunksPerTxHashIndex: cpi,
	}
}
