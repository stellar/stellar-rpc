package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
)

// Config is the on-disk --config TOML schema for the full-history daemon (design
// "Configuration"). Optional scalars are pointers so an absent key is
// distinguishable from an explicit zero; defaults applied in WithDefaults.
// validateConfig turns it (plus the catalog's pins and a tip backend) into the
// resolved StartConfig.
//
// Sections are grouped by what a field GOVERNS, not by which phase touches it.
// earliest_ledger ([retention]) is the one PINNED field — written immutably on
// first start (PinEarliestLedger) and validated against its pin (abort on
// mismatch) on every restart; its field doc flags the set-once contract.
// (The tx-hash index width is not configurable — it is the fixed
// geometry.ChunksPerTxhashIndex constant.)
type Config struct {
	Service   ServiceConfig   `toml:"service"`
	Retention RetentionConfig `toml:"retention"`
	Storage   StorageConfig   `toml:"storage"`
	Backfill  BackfillConfig  `toml:"backfill"`
	Ingestion IngestionConfig `toml:"ingestion"`
	Logging   LoggingConfig   `toml:"logging"`
	Serve     ServeConfig     `toml:"serve"`
}

// ServeConfig is [serve] — the read-side JSON-RPC server (query POC). An empty
// endpoint disables serving (the default), so a pure ingestion/backfill daemon
// needs no [serve] section at all. The per-endpoint limits mirror the v1 RPC
// getLedgers/getTransactions/getEvents caps; they are pointers so an absent key
// is distinguishable from an explicit zero, with the v1 defaults filled in
// WithDefaults.
type ServeConfig struct {
	// Endpoint is the host:port the JSON-RPC server listens on; "" disables
	// serving. Use host:0 to bind an OS-assigned port (benchmark harness).
	Endpoint string `toml:"endpoint"`

	MaxLedgersLimit          *uint `toml:"max_ledgers_limit"`          // v1 default 200
	DefaultLedgersLimit      *uint `toml:"default_ledgers_limit"`      // v1 default 50
	MaxTransactionsLimit     *uint `toml:"max_transactions_limit"`     // v1 default 200
	DefaultTransactionsLimit *uint `toml:"default_transactions_limit"` // v1 default 50
	MaxEventsLimit           *uint `toml:"max_events_limit"`           // v1 default 10000
	DefaultEventsLimit       *uint `toml:"default_events_limit"`       // v1 default 100
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
	// Concurrent task-slot count for bulk backfill; >= 1. Default GOMAXPROCS.
	Workers *int `toml:"workers"`

	// Per-task retries before the daemon aborts; >= 0 (0 = run once). Default
	// DefaultMaxRetries.
	MaxRetries *int `toml:"max_retries"`

	// DataStore is the bulk ledger source. An empty Type means no lake — backfill
	// then replays through captive core from the history archives; otherwise any
	// SDK datastore type is accepted.
	DataStore datastore.DataStoreConfig `toml:"datastore"`

	// BSB tunes the buffered-storage stream over DataStore; zero fields fall back to
	// the backfill defaults applied in backfill.NewBSBBackend.
	BSB ledgerbackend.BufferedStorageBackendConfig `toml:"bsb"`
}

// IngestionConfig is [ingestion] — the live-network ingestion (captive-core)
// settings. The captive-core config FILE is the single source of truth for what
// it can hold (notably NETWORK_PASSPHRASE, read back at startup); the remaining
// keys are the things that don't live in that file — the plain history-archive
// URLs (the file's [HISTORY.*] entries are shell commands, not the URLs the SDK's
// archive client needs), and, optionally, the stellar-core binary path and the
// captive-core storage directory.
type IngestionConfig struct {
	// CaptiveCoreConfig is the path to the CaptiveStellarCore (stellar-core) config
	// file. Required for live ingestion. Must define NETWORK_PASSPHRASE.
	CaptiveCoreConfig string `toml:"captive_core_config"`
	// HistoryArchiveURLs are the plain history-archive URLs the SDK reads
	// checkpoints from. Required for live ingestion (not derivable from the
	// captive-core file's [HISTORY.*] get-commands).
	HistoryArchiveURLs []string `toml:"history_archive_urls"`
	// StellarCoreBinaryPath is the path to the stellar-core binary. Optional —
	// defaults to the "stellar-core" found on PATH.
	StellarCoreBinaryPath string `toml:"stellar_core_binary_path"`
	// CaptiveCoreStoragePath is captive core's BUCKET_DIR_PATH base; optional,
	// defaults to {default_data_dir}/captive-core.
	CaptiveCoreStoragePath string `toml:"captive_core_storage_path"`
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
	// LogFormatJSON is the only non-default logging.format value; anything else is
	// rejected by validateConfig.
	LogFormatJSON = "json"

	// EarliestGenesis and EarliestNow are the two symbolic earliest_ledger forms.
	EarliestGenesis = "genesis"
	EarliestNow     = "now"

	// DefaultEarliestLedger is applied when earliest_ledger is unset.
	DefaultEarliestLedger = EarliestGenesis
)

// [serve] per-endpoint limit defaults, copied verbatim from the v1 RPC config
// (cmd/stellar-rpc/internal/config/options.go: max/default ledgers 200/50,
// transactions 200/50, events 10000/100). Filled by WithDefaults so the server
// mounts identically whether or not [serve] pins them.
const (
	DefaultServeMaxLedgersLimit          uint = 200
	DefaultServeDefaultLedgersLimit      uint = 50
	DefaultServeMaxTransactionsLimit     uint = 200
	DefaultServeDefaultTransactionsLimit uint = 50
	DefaultServeMaxEventsLimit           uint = 10000
	DefaultServeDefaultEventsLimit       uint = 100
)

// LoadConfig reads and parses the TOML config at path. It applies defaults but
// does NOT validate semantics or touch any pin — that is validateConfig's job.
// See ParseConfig.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
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
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	return cfg.WithDefaults(), nil
}

// WithDefaults returns a copy of cfg with every documented default filled for
// an unset (nil pointer / empty string) field. Explicit zeros are preserved
// (and later rejected by validateConfig where a zero is illegal).
func (cfg Config) WithDefaults() Config {
	if cfg.Backfill.Workers == nil {
		v := backfill.DefaultWorkers()
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
	fillUint(&cfg.Serve.MaxLedgersLimit, DefaultServeMaxLedgersLimit)
	fillUint(&cfg.Serve.DefaultLedgersLimit, DefaultServeDefaultLedgersLimit)
	fillUint(&cfg.Serve.MaxTransactionsLimit, DefaultServeMaxTransactionsLimit)
	fillUint(&cfg.Serve.DefaultTransactionsLimit, DefaultServeDefaultTransactionsLimit)
	fillUint(&cfg.Serve.MaxEventsLimit, DefaultServeMaxEventsLimit)
	fillUint(&cfg.Serve.DefaultEventsLimit, DefaultServeDefaultEventsLimit)
	return cfg
}

// fillUint sets *p to def when the key was absent (nil pointer). An explicit
// zero is preserved, matching the pointer-defaulting contract of the other
// optional scalars above.
func fillUint(p **uint, def uint) {
	if *p == nil {
		v := def
		*p = &v
	}
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
// dir); only the defaults are joined to the data dir. The default tree is spelled
// ONCE, by geometry.NewLayout — production flows through here and every package's
// test helpers through NewLayout, so a rename to the tree can't leave the two
// disagreeing.
func (cfg Config) ResolvePaths() Paths {
	dataDir := cfg.Service.DefaultDataDir
	def := geometry.NewLayout(dataDir)
	pick := func(override, defPath string) string {
		if override != "" {
			return override
		}
		return defPath
	}
	return Paths{
		DataDir:     dataDir,
		Catalog:     pick(cfg.Storage.Catalog, def.CatalogPath()),
		Ledgers:     pick(cfg.Storage.Ledgers, def.LedgersRoot()),
		Events:      pick(cfg.Storage.Events, def.EventsRoot()),
		TxhashRaw:   pick(cfg.Storage.TxhashRaw, def.TxHashRawRoot()),
		TxhashIndex: pick(cfg.Storage.TxhashIndex, def.TxHashIndexRoot()),
		HotStorage:  pick(cfg.Storage.Hot, def.HotRoot()),
	}
}

// Roots returns the distinct storage roots: the catalog, every immutable
// storage tree, and the hot-storage tree. The data dir itself is NOT a root —
// only the leaf trees a second daemon could independently point at. Feed the
// result to ValidateRoots + PrepareRoots (config_roots.go).
func (p Paths) Roots() []string {
	return []string{
		p.Catalog,
		p.Ledgers,
		p.Events,
		p.TxhashRaw,
		p.TxhashIndex,
		p.HotStorage,
	}
}

// ValidateRoots rejects two storage roots resolving to the same path — the
// stores would write into each other's trees. Nesting is deliberately
// allowed: no operation ever removes a whole root (prune and discard delete
// per-chunk paths only), so a root inside another root loses nothing.
func (p Paths) ValidateRoots() error {
	seen := make(map[string]struct{}, len(p.Roots()))
	for _, r := range p.Roots() {
		a, err := filepath.Abs(r)
		if err != nil {
			return fmt.Errorf("resolve storage root %q: %w", r, err)
		}
		if _, dup := seen[a]; dup {
			return fmt.Errorf("storage root %q is configured for more than one store; every root must be a distinct tree", a)
		}
		seen[a] = struct{}{}
	}
	return nil
}

// NewLayoutFromPaths adapts a resolved Paths into a geometry.Layout, so the
// prepared roots (Roots) and the Layout's data roots can never disagree. It
// is the config package's bridge over geometry.NewLayoutFromRoots, which takes
// plain strings to keep geometry free of any config dependency.
func NewLayoutFromPaths(p Paths) geometry.Layout {
	return geometry.NewLayoutFromRoots(p.Catalog, p.HotStorage, p.Ledgers, p.Events, p.TxhashRaw, p.TxhashIndex)
}
