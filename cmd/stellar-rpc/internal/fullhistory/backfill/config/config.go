// Package config owns the backfill-subcommand's TOML schema, CLI-flag merge
// layer, and the pre-DAG validation rules from design-doc section
// "Validation before DAG construction".
//
// All TOML keys are UPPER_SNAKE_CASE; TOML is case-sensitive per spec.
package config

import (
	"fmt"

	"github.com/pelletier/go-toml"
)

// Config is the top-level backfill configuration. Fields whose tag is
// `toml:"-"` are populated by ApplyFlags after parse and do not round-trip.
type Config struct {
	Service          ServiceConfig          `toml:"SERVICE"`
	MetaStore        MetaStoreConfig        `toml:"META_STORE"`
	Backfill         BackfillConfig         `toml:"BACKFILL"`
	ImmutableStorage ImmutableStorageConfig `toml:"IMMUTABLE_STORAGE"`
	Logging          LoggingConfig          `toml:"LOGGING"`

	EffectiveStartLedger uint32 `toml:"-"`
	EffectiveEndLedger   uint32 `toml:"-"`
	Workers              int    `toml:"-"`
	MaxRetries           int    `toml:"-"`
	VerifyRecSplit       bool   `toml:"-"`
}

// ServiceConfig is the [SERVICE] section.
type ServiceConfig struct {
	// DefaultDataDir is the fall-back root for every storage path the
	// operator does not set explicitly.
	DefaultDataDir string `toml:"DEFAULT_DATA_DIR"`
}

// MetaStoreConfig is the optional [META_STORE] section. Defaults to
// {DEFAULT_DATA_DIR}/meta/rocksdb (lowercase) when absent.
//
// RocksDB tuning knobs are intentionally NOT operator-visible here —
// they are hardcoded in the Layer-2 metastore facade (slice #689).
type MetaStoreConfig struct {
	Path string `toml:"PATH"`
}

// BackfillConfig is the [BACKFILL] section plus its nested [BACKFILL.BSB].
//
// CHUNKS_PER_TXHASH_INDEX is layout-defining: once a backfill run has
// seeded the meta store with a value, later runs must match it or the
// on-disk txhash-index boundaries are silently corrupted.
type BackfillConfig struct {
	ChunksPerTxHashIndex uint32 `toml:"CHUNKS_PER_TXHASH_INDEX"`
	// BSB is pointer-valued so absence is distinguishable from an empty
	// struct — Validate treats nil as "section missing".
	BSB *BSBConfig `toml:"BSB"`
}

// BSBConfig is the nested [BACKFILL.BSB] table. BUCKET_PATH is required;
// BUFFER_SIZE / NUM_WORKERS default to 1000 / 20 when absent.
type BSBConfig struct {
	BucketPath string `toml:"BUCKET_PATH"`
	BufferSize int    `toml:"BUFFER_SIZE"`
	NumWorkers int    `toml:"NUM_WORKERS"`
}

// ImmutableStorageConfig mirrors the four [IMMUTABLE_STORAGE.*] sub-sections.
type ImmutableStorageConfig struct {
	Ledgers     StoragePathConfig `toml:"LEDGERS"`
	Events      StoragePathConfig `toml:"EVENTS"`
	TxHashRaw   StoragePathConfig `toml:"TXHASH_RAW"`
	TxHashIndex StoragePathConfig `toml:"TXHASH_INDEX"`
}

// StoragePathConfig holds a single PATH string.
type StoragePathConfig struct {
	Path string `toml:"PATH"`
}

// LoggingConfig is the [LOGGING] section. Values are plumbed through but
// not consumed in this slice; the logrus bootstrap lands in #685.
type LoggingConfig struct {
	// Level is one of "debug", "info", "warn", "error".
	Level string `toml:"LEVEL"`
	// Format is one of "text", "json".
	Format string `toml:"FORMAT"`
}

// ParseConfig decodes a TOML payload (typically the file passed via
// `--config`) into a Config.
func ParseConfig(data []byte) (*Config, error) {
	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

// MarshalTOML encodes the Config back to TOML. Useful for diagnostics
// ("what config is this run actually using?") and to satisfy the
// parse → marshal → parse round-trip acceptance criterion.
func (c *Config) MarshalTOML() ([]byte, error) {
	buf, err := toml.Marshal(*c)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	return buf, nil
}
