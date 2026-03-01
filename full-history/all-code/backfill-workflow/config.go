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
	// StartLedger is the first ledger to ingest (inclusive).
	// Must satisfy: (StartLedger - 2) % 10,000,000 == 0
	// This ensures alignment with range boundaries.
	// Example valid values: 2, 10_000_002, 20_000_002
	// REQUIRED.
	StartLedger uint32 `toml:"start_ledger"`

	// EndLedger is the last ledger to ingest (inclusive).
	// Must satisfy: (EndLedger - 1) % 10,000,000 == 0
	// Example valid values: 10_000_001, 20_000_001, 30_000_001
	// REQUIRED.
	EndLedger uint32 `toml:"end_ledger"`

	// ParallelRanges controls how many 10M-ledger ranges are processed
	// concurrently. Each range spawns NumInstancesPerRange BSB instances.
	// Default: 2. Higher values increase GCS throughput but also memory usage.
	ParallelRanges int `toml:"parallel_ranges"`

	// FlushInterval is the number of ledgers between Level-1 flushes
	// (buffered writer flush to OS page cache, no fsync).
	// Default: 100.
	FlushInterval int `toml:"flush_interval"`

	// BSB holds GCS/BufferedStorageBackend configuration.
	// Include this section to use GCS as the ledger source.
	// Mutually exclusive with CaptiveCore.
	BSB *BSBConfig `toml:"bsb"`

	// CaptiveCore holds local stellar-core replay configuration.
	// Include this section to use CaptiveStellarCore as the ledger source.
	// Mutually exclusive with BSB.
	CaptiveCore *CaptiveCoreConfig `toml:"captive_core"`
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

	// NumInstancesPerRange is the number of BSB instances per 10M-ledger range.
	// Must divide 1000 evenly (so each instance gets equal chunks).
	// Default: 20 (each instance handles 50 chunks).
	NumInstancesPerRange int `toml:"num_instances_per_range"`
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

	// 3. Validate start_ledger alignment.
	// Range boundaries start at FirstLedger (2) and repeat every 10M ledgers.
	// A misaligned start_ledger would cause the first range to have fewer than
	// 10M ledgers, breaking the 1000-chunks-per-range assumption and making
	// chunk IDs inconsistent across ranges.
	if c.Backfill.StartLedger < geometry.FirstLedger {
		return fmt.Errorf("start_ledger %d must be >= %d", c.Backfill.StartLedger, geometry.FirstLedger)
	}
	if (c.Backfill.StartLedger-geometry.FirstLedger)%geometry.RangeSize != 0 {
		return fmt.Errorf("start_ledger %d is not range-aligned: (val-%d) %% %d must equal 0",
			c.Backfill.StartLedger, geometry.FirstLedger, geometry.RangeSize)
	}

	// 4. Validate end_ledger alignment.
	// end_ledger must be the last ledger of a range: (val - 1) % 10M == 0
	// This means end_ledger = N * 10M + 1 for some N.
	if c.Backfill.EndLedger <= c.Backfill.StartLedger {
		return fmt.Errorf("end_ledger %d must be > start_ledger %d",
			c.Backfill.EndLedger, c.Backfill.StartLedger)
	}
	if (c.Backfill.EndLedger-geometry.FirstLedger+1)%geometry.RangeSize != 0 {
		return fmt.Errorf("end_ledger %d is not range-aligned: (val-%d+1) %% %d must equal 0",
			c.Backfill.EndLedger, geometry.FirstLedger, geometry.RangeSize)
	}

	// 5. Exactly one of BSB or CaptiveCore must be specified.
	// The presence of the TOML section IS the backend selector.
	hasBSB := c.Backfill.BSB != nil
	hasCaptive := c.Backfill.CaptiveCore != nil
	if hasBSB && hasCaptive {
		return fmt.Errorf("exactly one of [backfill.bsb] or [backfill.captive_core] must be specified, not both")
	}
	if !hasBSB && !hasCaptive {
		return fmt.Errorf("exactly one of [backfill.bsb] or [backfill.captive_core] must be specified")
	}

	// 6. BSB validation
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
		if c.Backfill.BSB.NumInstancesPerRange <= 0 {
			c.Backfill.BSB.NumInstancesPerRange = 20
		}
		// NumInstancesPerRange must divide 1000 evenly so each instance gets
		// the same number of chunks. E.g., 20 instances → 50 chunks each.
		if geometry.ChunksPerRange%uint32(c.Backfill.BSB.NumInstancesPerRange) != 0 {
			return fmt.Errorf("backfill.bsb.num_instances_per_range=%d must divide %d evenly",
				c.Backfill.BSB.NumInstancesPerRange, geometry.ChunksPerRange)
		}
	}

	// 7. CaptiveCore validation
	if hasCaptive {
		if c.Backfill.CaptiveCore.BinaryPath == "" {
			return fmt.Errorf("backfill.captive_core.binary_path is required")
		}
		if c.Backfill.CaptiveCore.ConfigPath == "" {
			return fmt.Errorf("backfill.captive_core.config_path is required")
		}
	}

	// 8. Defaults for parallel_ranges and flush_interval
	if c.Backfill.ParallelRanges <= 0 {
		c.Backfill.ParallelRanges = 2
	}
	if c.Backfill.FlushInterval <= 0 {
		c.Backfill.FlushInterval = 100
	}

	return nil
}
