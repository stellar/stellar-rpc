package fullhistory

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/pelletier/go-toml"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
)

// Config is the on-disk --config TOML schema for the streaming daemon (design
// "Configuration"). Optional scalars are pointers so an absent key is
// distinguishable from an explicit zero; defaults applied in WithDefaults.
// validateConfig turns it (plus the catalog's pins and a tip backend) into the
// resolved StartConfig.
//
// Sections are grouped by what a field GOVERNS, not by which phase touches it.
// earliest_ledger ([retention]) is the one PINNED field — written immutably on
// first start (PinEarliestLedger) and validated against its pin (abort on
// mismatch) on every restart; its field doc flags the set-once contract.
// (chunks_per_txhash_index is no longer configurable — it is the fixed
// geometry.ChunksPerTxhashIndex constant.)
type Config struct {
	Service   ServiceConfig   `toml:"service"`
	Retention RetentionConfig `toml:"retention"`
	Storage   StorageConfig   `toml:"storage"`
	Backfill  BackfillConfig  `toml:"backfill"`
	Ingestion IngestionConfig `toml:"ingestion"`
	Logging   LoggingConfig   `toml:"logging"`
}

// ServiceConfig is [service].
type ServiceConfig struct {
	// Base dir for the catalog and default storage paths. Required.
	DefaultDataDir string `toml:"default_data_dir"`
}

// RetentionConfig is [retention] — the two inputs to the retention floor:
// floor = max(sliding(retention_chunks), earliest_ledger).
type RetentionConfig struct {
	// Earliest ledger this daemon will ever have data for: "genesis", "now", or a
	// chunk-aligned decimal ledger. PINNED — written on first start
	// (PinEarliestLedger) and validated-or-abort on every restart, so it can never
	// change once data exists. Default "genesis".
	EarliestLedger string `toml:"earliest_ledger"`

	// Retention window in chunks; 0 = full history. Default 0.
	RetentionChunks *uint32 `toml:"retention_chunks"`
}

// StorageConfig is [storage] — one optional path per on-disk tree (consolidating
// what were the separate [catalog] / [immutable_storage.*] / [streaming.hot_storage]
// sections). An empty value defaults under [service].default_data_dir.
type StorageConfig struct {
	Catalog     string `toml:"catalog"`      // catalog RocksDB dir
	Ledgers     string `toml:"ledgers"`      // immutable ledger packs root
	Events      string `toml:"events"`       // immutable events segments root
	TxhashRaw   string `toml:"txhash_raw"`   // transient txhash .bin root
	TxhashIndex string `toml:"txhash_index"` // frozen txhash .idx root
	Hot         string `toml:"hot"`          // per-chunk hot RocksDB root
}

// BackfillConfig is [backfill] plus the nested [backfill.datastore] and [backfill.bsb].
// The datastore and BSB blocks reuse the SDK config types verbatim, so any SDK
// datastore (GCS, S3, Filesystem, ...) works as the bulk source — backfill needs only
// correct ledger metadata, not a specific store.
type BackfillConfig struct {
	// Concurrent task-slot count for bulk catch-up; >= 1. Default GOMAXPROCS.
	Workers *int `toml:"workers"`

	// Per-task retries before the daemon aborts; >= 0 (0 = run once). Default
	// DefaultMaxRetries.
	MaxRetries *int `toml:"max_retries"`

	// DataStore is the bulk ledger source. An empty Type means frontfill-only (no
	// backfill backend); otherwise any SDK datastore type is accepted.
	DataStore datastore.DataStoreConfig `toml:"datastore"`

	// BSB tunes the buffered-storage stream over DataStore; zero fields fall back to
	// the backfill defaults applied in backfill.NewBSBBackend.
	BSB ledgerbackend.BufferedStorageBackendConfig `toml:"bsb"`
}

// IngestionConfig is [ingestion] — the live-network ingestion settings.
type IngestionConfig struct {
	// Path to the CaptiveStellarCore config file. Required.
	CaptiveCoreConfig string `toml:"captive_core_config"`
}

// LoggingConfig is [logging].
type LoggingConfig struct {
	// debug/info/warn/error; default "info".
	Level string `toml:"level"`
	// text/json; default "text".
	Format string `toml:"format"`
}

// Documented defaults (design "Configuration").
const (
	DefaultMaxRetries int = 3

	DefaultLogLevel  = "info"
	DefaultLogFormat = "text"

	// EarliestGenesis and EarliestNow are the two symbolic earliest_ledger forms.
	EarliestGenesis = "genesis"
	EarliestNow     = "now"

	// DefaultEarliestLedger is applied when earliest_ledger is unset.
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
// silently ignored (go-toml v1's plain Unmarshal ignores them). A typo in the
// immutable, pinned earliest_ledger key must fail loudly, not pin the wrong
// value on first start; and the removed chunks_per_txhash_index key (and its
// whole [layout] section) is rejected, not silently ignored.
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
	if cfg.Backfill.Workers == nil {
		v := runtime.GOMAXPROCS(0)
		cfg.Backfill.Workers = &v
	}
	if cfg.Backfill.MaxRetries == nil {
		v := DefaultMaxRetries
		cfg.Backfill.MaxRetries = &v
	}
	if cfg.Retention.RetentionChunks == nil {
		v := uint32(0)
		cfg.Retention.RetentionChunks = &v
	}
	if cfg.Retention.EarliestLedger == "" {
		cfg.Retention.EarliestLedger = DefaultEarliestLedger
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
		Catalog:     pick(cfg.Storage.Catalog, filepath.Join(dataDir, "catalog", "rocksdb")),
		Ledgers:     pick(cfg.Storage.Ledgers, filepath.Join(dataDir, "ledgers")),
		Events:      pick(cfg.Storage.Events, filepath.Join(dataDir, "events")),
		TxhashRaw:   pick(cfg.Storage.TxhashRaw, filepath.Join(dataDir, "txhash", "raw")),
		TxhashIndex: pick(cfg.Storage.TxhashIndex, filepath.Join(dataDir, "txhash", "index")),
		HotStorage:  pick(cfg.Storage.Hot, filepath.Join(dataDir, "hot")),
	}
}

// RootsToLock returns the distinct storage roots that must each carry a
// single-process flock: the catalog, every immutable storage tree, and the
// hot-storage tree (design "Single-process enforcement"). The data dir itself
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

// NewLayoutFromPaths adapts a resolved Paths into a geometry.Layout, so the
// flocked roots (RootsToLock) and the Layout's data roots can never disagree. It
// is the streaming-package bridge over geometry.NewLayoutFromRoots, which takes
// plain strings to keep geometry free of any config dependency.
func NewLayoutFromPaths(p Paths) geometry.Layout {
	return geometry.NewLayoutFromRoots(p.Catalog, p.HotStorage, p.Ledgers, p.Events, p.TxhashRaw, p.TxhashIndex)
}
