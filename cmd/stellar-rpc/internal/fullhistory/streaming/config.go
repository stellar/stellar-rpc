package streaming

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/pelletier/go-toml"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// Config is the on-disk --config TOML schema for the streaming daemon (design
// "Configuration"). Optional scalars are pointers so an absent key is
// distinguishable from an explicit zero; defaults applied in WithDefaults.
// validateConfig turns it (plus the catalog's pins and a tip backend) into the
// resolved StartConfig. chunks_per_txhash_index and earliest_ledger are pinned
// immutably on first start and validated against their pins on every restart.
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
	// Base dir for the catalog and default storage paths. Required.
	DefaultDataDir string `toml:"default_data_dir"`
}

// BackfillConfig is [backfill] plus the nested [backfill.bsb].
type BackfillConfig struct {
	// Chunks per tx-hash index; defines the index layout, immutable once
	// stored. Default DefaultChunksPerTxhashIndex.
	ChunksPerTxhashIndex *uint32 `toml:"chunks_per_txhash_index"`

	// Concurrent task-slot count for bulk catch-up; >= 1. Default GOMAXPROCS.
	Workers *int `toml:"workers"`

	// Per-task retries before the daemon aborts; >= 0 (0 = run once). Default
	// DefaultMaxRetries.
	MaxRetries *int `toml:"max_retries"`

	// Buffered Storage Backend — the default bulk LedgerBackend.
	BSB BSBConfig `toml:"bsb"`
}

// BSBConfig is [backfill.bsb] — the Buffered Storage Backend. Required unless
// another conformant LedgerBackend is wired as the bulk source.
type BSBConfig struct {
	// Remote object-store path for LedgerCloseMeta (no gs:// prefix for GCS).
	// Required when BSB is the bulk source.
	BucketPath string `toml:"bucket_path"`

	// Prefetch buffer depth per connection; default DefaultBSBBufferSize.
	BufferSize *int `toml:"buffer_size"`

	// Download workers per connection; default DefaultBSBNumWorkers.
	NumWorkers *int `toml:"num_workers"`
}

// ImmutableStorageConfig is [immutable_storage.*] — one optional path per
// artifact tree. An empty path means "default under default_data_dir".
type ImmutableStorageConfig struct {
	Ledgers     StoragePathConfig `toml:"ledgers"`
	Events      StoragePathConfig `toml:"events"`
	TxhashRaw   StoragePathConfig `toml:"txhash_raw"`
	TxhashIndex StoragePathConfig `toml:"txhash_index"`
}

// StoragePathConfig is one [immutable_storage.*] / [catalog] / [hot_storage]
// section: an optional path override.
type StoragePathConfig struct {
	Path string `toml:"path"`
}

// CatalogConfig is [catalog]; default {default_data_dir}/catalog/rocksdb.
type CatalogConfig struct {
	Path string `toml:"path"`
}

// StreamingConfig is [streaming] plus the nested [streaming.hot_storage].
type StreamingConfig struct {
	// Retention window in chunks; 0 = full history. Default 0.
	RetentionChunks *uint32 `toml:"retention_chunks"`

	// Earliest ledger this daemon will ever have data for: "genesis", "now", or
	// a chunk-aligned decimal ledger. Pinned immutably on first start (see
	// Config). Default "genesis".
	EarliestLedger string `toml:"earliest_ledger"`

	// Path to the CaptiveStellarCore config file. Required.
	CaptiveCoreConfig string `toml:"captive_core_config"`

	HotStorage StoragePathConfig `toml:"hot_storage"`
}

// LoggingConfig is [logging].
type LoggingConfig struct {
	// debug/info/warn/error; default "info".
	Level string `toml:"level"`
	// text/json; default "text".
	Format string `toml:"format"`
}

// Documented defaults (design "Configuration"). DefaultChunksPerTxhashIndex
// aliases the txhash store's default (1000, = 10M ledgers per index) so the
// streaming pin and the cold index builder agree on the index size.
const (
	DefaultChunksPerTxhashIndex uint32 = txhash.DefaultChunksPerIndex
	DefaultMaxRetries           int    = 3
	DefaultBSBBufferSize        int    = 1000
	DefaultBSBNumWorkers        int    = 20

	DefaultLogLevel  = "info"
	DefaultLogFormat = "text"

	// The two symbolic earliest_ledger forms.
	EarliestGenesis = "genesis"
	EarliestNow     = "now"

	// Applied when earliest_ledger is unset.
	DefaultEarliestLedger = EarliestGenesis
)

// LoadConfig reads and parses the TOML config at path. It applies defaults but
// does NOT validate semantics or touch any pin — that is validateConfig's job.
// See ParseConfig.
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
// Decoding is STRICT (Decoder.Strict(true)): any unknown key is an error, not
// silently ignored (go-toml v1's plain Unmarshal ignores them). A typo in an
// immutable, layout-defining key (chunks_per_txhash_index, earliest_ledger)
// must fail loudly, not pin the wrong value on first start.
func ParseConfig(data []byte) (Config, error) {
	var cfg Config
	if err := toml.NewDecoder(bytes.NewReader(data)).Strict(true).Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("streaming: parse config: %w", err)
	}
	return cfg.WithDefaults(), nil
}

// WithDefaults returns a copy of cfg with every documented default filled for
// an unset (nil pointer / empty string) field. Explicit zeros are preserved
// (and later rejected by validateConfig where a zero is illegal).
func (cfg Config) WithDefaults() Config {
	if cfg.Backfill.ChunksPerTxhashIndex == nil {
		v := DefaultChunksPerTxhashIndex
		cfg.Backfill.ChunksPerTxhashIndex = &v
	}
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

// Paths is the resolved set of on-disk paths the daemon uses — the single place
// the {default_data_dir}/... layout lives, so locking and store-opening agree
// on every root.
type Paths struct {
	DataDir     string // the data root
	Catalog     string // catalog RocksDB dir
	Ledgers     string // immutable ledger packs root
	Events      string // immutable events segments root
	TxhashRaw   string // transient txhash .bin root
	TxhashIndex string // frozen txhash .idx root
	HotStorage  string // per-chunk hot RocksDB root
}

// ResolvePaths fills every storage path, defaulting under default_data_dir.
// Relative overrides are kept relative (resolved against the caller's working
// dir); only the defaults are joined to the data dir.
func (cfg Config) ResolvePaths() Paths {
	dataDir := cfg.Service.DefaultDataDir
	pick := func(override, def string) string {
		if override != "" {
			return override
		}
		return def
	}
	return Paths{
		DataDir:     dataDir,
		Catalog:     pick(cfg.Catalog.Path, filepath.Join(dataDir, "catalog", "rocksdb")),
		Ledgers:     pick(cfg.ImmutableStorage.Ledgers.Path, filepath.Join(dataDir, "ledgers")),
		Events:      pick(cfg.ImmutableStorage.Events.Path, filepath.Join(dataDir, "events")),
		TxhashRaw:   pick(cfg.ImmutableStorage.TxhashRaw.Path, filepath.Join(dataDir, "txhash", "raw")),
		TxhashIndex: pick(cfg.ImmutableStorage.TxhashIndex.Path, filepath.Join(dataDir, "txhash", "index")),
		HotStorage:  pick(cfg.Streaming.HotStorage.Path, filepath.Join(dataDir, "hot")),
	}
}

// RootsToLock returns the distinct storage roots that must each carry a
// single-process flock: the catalog, every immutable_storage tree, and the
// hot_storage tree (design "Single-process enforcement"). The data dir itself
// is NOT locked — only the leaf roots a second daemon could independently point
// at; locking the shared parent would miss two daemons with disjoint data dirs
// that share one artifact tree. Feed the result to LockRoots (config_lock.go).
func (p Paths) RootsToLock() []string {
	return []string{
		p.Catalog,
		p.Ledgers,
		p.Events,
		p.TxhashRaw,
		p.TxhashIndex,
		p.HotStorage,
	}
}
