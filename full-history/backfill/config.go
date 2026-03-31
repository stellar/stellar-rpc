package backfill

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/BurntSushi/toml"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
)

// Config is the top-level configuration for the backfill workflow.
// Mirrors the TOML schema defined in the design doc (03-backfill-workflow.md).
type Config struct {
	Service          ServiceConfig          `toml:"service"`
	MetaStore        MetaStoreConfig        `toml:"meta_store"`
	ImmutableStorage ImmutableStorageConfig `toml:"immutable_storage"`
	Backfill         BackfillConfig         `toml:"backfill"`
	Logging          LoggingConfig          `toml:"logging"`

	// Populated by ApplyFlags — not in TOML.
	EffectiveStartLedger uint32 `toml:"-"`
	EffectiveEndLedger   uint32 `toml:"-"`
	Workers              int    `toml:"-"`
	VerifyRecSplit       bool   `toml:"-"`
	MaxRetries           int    `toml:"-"`
}

// ServiceConfig holds the base data directory.
type ServiceConfig struct {
	DefaultDataDir string `toml:"default_data_dir"`
}

// MetaStoreConfig holds the path to the meta store (RocksDB).
type MetaStoreConfig struct {
	Path string `toml:"path"`
}

// ImmutableStorageConfig holds per-type storage paths.
type ImmutableStorageConfig struct {
	Ledgers     StoragePathConfig `toml:"ledgers"`
	Events      StoragePathConfig `toml:"events"`
	TxHashRaw   StoragePathConfig `toml:"txhash_raw"`
	TxHashIndex StoragePathConfig `toml:"txhash_index"`
}

// StoragePathConfig is a single path entry.
type StoragePathConfig struct {
	Path string `toml:"path"`
}

// BackfillConfig holds parameters for the backfill pipeline.
type BackfillConfig struct {
	ChunksPerTxHashIndex int        `toml:"chunks_per_txhash_index"`
	BSB                  *BSBConfig `toml:"bsb"`
}

// BSBConfig holds GCS BufferedStorageBackend configuration.
type BSBConfig struct {
	BucketPath string `toml:"bucket_path"`
	BufferSize int    `toml:"buffer_size"`
	NumWorkers int    `toml:"num_workers"`
}

// LoggingConfig holds log file paths.
type LoggingConfig struct {
	LogFile       string `toml:"log_file"`
	ErrorFile     string `toml:"error_file"`
	MaxScopeDepth int    `toml:"max_scope_depth"`
}

// CLIFlags holds per-run parameters passed via command-line flags.
type CLIFlags struct {
	StartLedger    uint32
	EndLedger      uint32
	Workers        int
	VerifyRecSplit bool
	MaxRetries     int
}

// ParseConfig parses a TOML byte slice into a Config.
func ParseConfig(data []byte) (*Config, error) {
	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

// ApplyFlags applies CLI flags to the config and expands the ledger range
// outward to chunk boundaries.
func (c *Config) ApplyFlags(flags CLIFlags) {
	geo := c.BuildGeometry()

	// Expand start DOWN to the first ledger of its chunk.
	startChunkID := (flags.StartLedger - geometry.FirstLedger) / geo.ChunkSize
	c.EffectiveStartLedger = geo.ChunkFirstLedger(startChunkID)

	// Expand end UP to the last ledger of its chunk.
	endChunkID := (flags.EndLedger - geometry.FirstLedger) / geo.ChunkSize
	c.EffectiveEndLedger = geo.ChunkLastLedger(endChunkID)

	// Workers default to GOMAXPROCS.
	c.Workers = flags.Workers
	if c.Workers <= 0 {
		c.Workers = runtime.GOMAXPROCS(0)
	}

	c.VerifyRecSplit = flags.VerifyRecSplit
	c.MaxRetries = flags.MaxRetries
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
}

// Validate checks all configuration constraints and resolves defaults.
func (c *Config) Validate() error {
	if c.Service.DefaultDataDir == "" {
		return fmt.Errorf("service.default_data_dir is required")
	}

	// Resolve default paths.
	if c.MetaStore.Path == "" {
		c.MetaStore.Path = filepath.Join(c.Service.DefaultDataDir, "meta", "rocksdb")
	}
	if c.ImmutableStorage.Ledgers.Path == "" {
		c.ImmutableStorage.Ledgers.Path = filepath.Join(c.Service.DefaultDataDir, "ledgers")
	}
	if c.ImmutableStorage.Events.Path == "" {
		c.ImmutableStorage.Events.Path = filepath.Join(c.Service.DefaultDataDir, "events")
	}
	if c.ImmutableStorage.TxHashRaw.Path == "" {
		c.ImmutableStorage.TxHashRaw.Path = filepath.Join(c.Service.DefaultDataDir, "txhash", "raw")
	}
	if c.ImmutableStorage.TxHashIndex.Path == "" {
		c.ImmutableStorage.TxHashIndex.Path = filepath.Join(c.Service.DefaultDataDir, "txhash", "index")
	}
	if c.Logging.LogFile == "" {
		c.Logging.LogFile = filepath.Join(c.Service.DefaultDataDir, "logs", "backfill.log")
	}
	if c.Logging.ErrorFile == "" {
		c.Logging.ErrorFile = filepath.Join(c.Service.DefaultDataDir, "logs", "backfill-error.log")
	}

	// Default chunks_per_txhash_index.
	if c.Backfill.ChunksPerTxHashIndex == 0 {
		c.Backfill.ChunksPerTxHashIndex = 1000
	}

	// BSB is required.
	if c.Backfill.BSB == nil {
		return fmt.Errorf("[backfill.bsb] is required")
	}
	if c.Backfill.BSB.BucketPath == "" {
		return fmt.Errorf("backfill.bsb.bucket_path is required")
	}
	if c.Backfill.BSB.BufferSize <= 0 {
		c.Backfill.BSB.BufferSize = 1000
	}
	if c.Backfill.BSB.NumWorkers <= 0 {
		c.Backfill.BSB.NumWorkers = 20
	}

	return nil
}

// ValidateFlags checks CLI flag constraints. Must be called after ApplyFlags.
func (c *Config) ValidateFlags(flags CLIFlags) error {
	if flags.StartLedger < geometry.FirstLedger {
		return fmt.Errorf("start_ledger %d must be >= %d", flags.StartLedger, geometry.FirstLedger)
	}
	if flags.EndLedger <= flags.StartLedger {
		return fmt.Errorf("end_ledger %d must be > start_ledger %d", flags.EndLedger, flags.StartLedger)
	}
	return nil
}

// ValidateAgainstMetaStore checks that chunks_per_txhash_index has not changed
// since the first run. On first run (key absent), persists the current value.
func (c *Config) ValidateAgainstMetaStore(meta ConfigMetaStore) error {
	key := "config:chunks_per_txhash_index"
	existing, err := meta.Get(key)
	if err != nil {
		return fmt.Errorf("read %s from meta store: %w", key, err)
	}

	currentStr := strconv.Itoa(c.Backfill.ChunksPerTxHashIndex)

	if existing == "" {
		// First run — persist.
		if err := meta.Put(key, currentStr); err != nil {
			return fmt.Errorf("write %s to meta store: %w", key, err)
		}
		return nil
	}

	if existing != currentStr {
		return fmt.Errorf("chunks_per_txhash_index changed: meta store has %s, config has %s", existing, currentStr)
	}
	return nil
}

// ConfigMetaStore is the subset of the meta store interface needed by config validation.
type ConfigMetaStore interface {
	Get(key string) (string, error)
	Put(key, value string) error
}

// BuildGeometry returns a Geometry based on the configured chunks_per_txhash_index.
func (c *Config) BuildGeometry() geometry.Geometry {
	cpi := uint32(c.Backfill.ChunksPerTxHashIndex)
	if cpi == 0 {
		cpi = 1000
	}
	return geometry.Geometry{
		RangeSize:            cpi * geometry.ChunkSize,
		ChunkSize:            geometry.ChunkSize,
		ChunksPerTxHashIndex: cpi,
	}
}
