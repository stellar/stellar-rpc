package streaming

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/pelletier/go-toml"
)

// Config is the on-disk TOML schema for the full-history streaming daemon — the
// one --config file (design "Configuration"). Every section maps to a nested
// struct; optional scalars are pointers so an absent key is distinguishable
// from an explicit zero and the documented default applies in WithDefaults.
//
// The TOML form is the daemon's INPUT; validateConfig turns it (plus the
// catalog's pins and a network-tip backend) into the resolved StartConfig that
// startStreaming consumes. The two layout-defining values
// (chunks_per_txhash_index, earliest_ledger) are pinned immutably on first
// start and validated against their pins on every restart.
type Config struct {
	Service          ServiceConfig          `toml:"service"`
	Backfill         BackfillConfig         `toml:"backfill"`
	ImmutableStorage ImmutableStorageConfig `toml:"immutable_storage"`
	Catalog          CatalogConfig          `toml:"catalog"`
	Streaming        StreamingConfig        `toml:"streaming"`
	Logging          LoggingConfig          `toml:"logging"`
}

// ServiceConfig is [service].
type ServiceConfig struct {
	// DefaultDataDir is the base directory for the catalog and the default
	// storage paths. Required.
	DefaultDataDir string `toml:"default_data_dir"`
}

// BackfillConfig is [backfill] plus the nested [backfill.bsb].
type BackfillConfig struct {
	// Workers is the concurrent task-slot count for bulk catch-up. Default
	// GOMAXPROCS. Must be >= 1.
	Workers *int `toml:"workers"`

	// MaxRetries is per-task retries before the daemon aborts. Default
	// DefaultMaxRetries. Must be >= 0 (0 = run once, no retry).
	MaxRetries *int `toml:"max_retries"`

	// BSB is the Buffered Storage Backend — the default bulk LedgerBackend.
	BSB BSBConfig `toml:"bsb"`
}

// BSBConfig is [backfill.bsb] — the Buffered Storage Backend. Required unless
// another conformant LedgerBackend is wired as the bulk source.
type BSBConfig struct {
	// BucketPath is the remote object-store path for LedgerCloseMeta (no gs://
	// prefix for GCS). Required when BSB is the bulk source.
	BucketPath string `toml:"bucket_path"`

	// BufferSize is the prefetch buffer depth per connection. Default
	// DefaultBSBBufferSize.
	BufferSize *int `toml:"buffer_size"`

	// NumWorkers is the download workers per connection. Default
	// DefaultBSBNumWorkers.
	NumWorkers *int `toml:"num_workers"`
}

// ImmutableStorageConfig is [immutable_storage.*] — one optional path per
// artifact tree. An empty path means "default under default_data_dir".
type ImmutableStorageConfig struct {
	Ledgers StoragePathConfig `toml:"ledgers"`
}

// StoragePathConfig is one [immutable_storage.*] / [catalog] / [hot_storage]
// section: an optional path override.
type StoragePathConfig struct {
	Path string `toml:"path"`
}

// CatalogConfig is [catalog] — optional path override
// (default {default_data_dir}/catalog/rocksdb).
type CatalogConfig struct {
	Path string `toml:"path"`
}

// StreamingConfig is [streaming] plus the nested [streaming.hot_storage].
type StreamingConfig struct { //nolint:revive // matches the XxxConfig section-struct convention
	// RetentionChunks is the retention window in chunks; 0 = full history.
	// Default 0.
	RetentionChunks *uint32 `toml:"retention_chunks"`

	// EarliestLedger is the earliest ledger this daemon will ever have data
	// for: "genesis", "now", or a chunk-aligned decimal ledger. Default
	// "genesis". Pinned immutably on first start.
	EarliestLedger string `toml:"earliest_ledger"`

	// CaptiveCoreConfig is the path to the CaptiveStellarCore config file.
	// Required.
	CaptiveCoreConfig string `toml:"captive_core_config"`

	// HotStorage is [streaming.hot_storage].
	HotStorage StoragePathConfig `toml:"hot_storage"`
}

// LoggingConfig is [logging].
type LoggingConfig struct {
	// Level is debug/info/warn/error. Default "info".
	Level string `toml:"level"`
	// Format is text/json. Default "text".
	Format string `toml:"format"`
}

// Documented defaults (design "Configuration").
const (
	DefaultMaxRetries    int = 3
	DefaultBSBBufferSize int = 1000
	DefaultBSBNumWorkers int = 20

	DefaultEarliestLedger = "genesis"
	DefaultLogLevel       = "info"
	DefaultLogFormat      = "text"

	// EarliestGenesis / EarliestNow are the two symbolic earliest_ledger forms.
	EarliestGenesis = "genesis"
	EarliestNow     = "now"
)

// LoadConfig reads and parses the TOML config at path. It applies documented
// defaults but does NOT validate semantics or touch any pin — that is
// validateConfig's job, which needs the catalog and a tip backend. Unknown
// top-level/section keys are rejected so a typo'd key never silently keeps a
// default.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("streaming: read config %q: %w", path, err)
	}
	return ParseConfig(data)
}

// ParseConfig parses TOML bytes into a Config with defaults applied. Split from
// LoadConfig so tests parse in-memory documents without a temp file.
//
// Decoding is STRICT (Decoder.Strict(true)): any key in the document with no
// corresponding struct field is an error rather than silently ignored. This is
// what backs the LoadConfig docstring's "unknown keys are rejected" promise — a
// typo in an immutable, layout-defining key (earliest_ledger) must fail loudly,
// not silently fall back to a default and pin the wrong value on first start.
// go-toml v1's plain Unmarshal ignores
// unknown keys (it mirrors the encoding/json decoder), so strict decoding is
// required here.
func ParseConfig(data []byte) (Config, error) {
	var cfg Config
	if err := toml.NewDecoder(bytes.NewReader(data)).Strict(true).Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("streaming: parse config: %w", err)
	}
	return cfg.WithDefaults(), nil
}

// WithDefaults returns a copy of cfg with every documented default filled for
// an unset (nil pointer / empty string) field. Numeric pointers left nil are
// resolved to their defaults; explicit zeros are preserved (and later rejected
// by validateConfig where a zero is illegal, e.g. workers).
func (cfg Config) WithDefaults() Config {
	if cfg.Backfill.Workers == nil {
		v := runtime.GOMAXPROCS(0)
		cfg.Backfill.Workers = &v
	}
	if cfg.Backfill.MaxRetries == nil {
		v := DefaultMaxRetries
		cfg.Backfill.MaxRetries = &v
	}
	if cfg.Backfill.BSB.BufferSize == nil {
		v := DefaultBSBBufferSize
		cfg.Backfill.BSB.BufferSize = &v
	}
	if cfg.Backfill.BSB.NumWorkers == nil {
		v := DefaultBSBNumWorkers
		cfg.Backfill.BSB.NumWorkers = &v
	}
	if cfg.Streaming.RetentionChunks == nil {
		v := uint32(0)
		cfg.Streaming.RetentionChunks = &v
	}
	if cfg.Streaming.EarliestLedger == "" {
		cfg.Streaming.EarliestLedger = DefaultEarliestLedger
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = DefaultLogLevel
	}
	if cfg.Logging.Format == "" {
		cfg.Logging.Format = DefaultLogFormat
	}
	return cfg
}

// Paths resolves the on-disk paths the daemon uses, filling each unset storage
// path with its documented default under default_data_dir. It is the single
// place the {default_data_dir}/... layout lives, so locking and store-opening
// agree on every root.
type Paths struct {
	DataDir    string // default_data_dir (the data root)
	Catalog    string // catalog RocksDB dir
	Ledgers    string // immutable ledger packs root
	HotStorage string // per-chunk hot RocksDB root
}

// ResolvePaths fills every storage path, defaulting under default_data_dir per
// the design's directory layout. Relative overrides are kept relative (the
// caller's working dir resolves them); only the defaults are joined to the data
// dir.
func (cfg Config) ResolvePaths() Paths {
	dataDir := cfg.Service.DefaultDataDir
	pick := func(override, def string) string {
		if override != "" {
			return override
		}
		return def
	}
	return Paths{
		DataDir:    dataDir,
		Catalog:    pick(cfg.Catalog.Path, filepath.Join(dataDir, "catalog", "rocksdb")),
		Ledgers:    pick(cfg.ImmutableStorage.Ledgers.Path, filepath.Join(dataDir, "ledgers")),
		HotStorage: pick(cfg.Streaming.HotStorage.Path, filepath.Join(dataDir, "hot")),
	}
}

// LockRoots returns the distinct storage roots that must each carry a
// single-process flock: the catalog, every immutable_storage tree, and the
// hot_storage tree (design "Single-process enforcement"). The data dir itself
// is NOT locked — only the leaf roots a second daemon could independently point
// at; locking the shared parent would not catch two daemons with disjoint data
// dirs that nonetheless share one artifact tree.
func (p Paths) LockRoots() []string {
	return []string{
		p.Catalog,
		p.Ledgers,
		p.HotStorage,
	}
}
