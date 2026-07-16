package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

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
	Serving   ServingConfig   `toml:"serving"`
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

// ServingConfig is [serving] — the read-service knobs. Limit and duration
// defaults are copied from the v1 RPC daemon's options so the two services
// behave the same under the same load. Per-method queue limits and per-method
// execution durations are NOT configurable here — they use the v1 defaults in
// code (the serve package's method table).
type ServingConfig struct {
	// Endpoint is the JSON-RPC HTTP listen address. Default "localhost:8000"
	// (the v1 daemon's default). The server always starts — during backfill it
	// answers every data method with a "backfill in progress" error and starts
	// serving data the moment backfill completes.
	Endpoint string `toml:"endpoint"`

	// AdminEndpoint is the admin HTTP listen address, serving GET /metrics
	// (Prometheus), GET /latency.json (exact-quantile latency snapshots), and
	// /debug/pprof. Empty (the default) disables the admin listener.
	AdminEndpoint string `toml:"admin_endpoint"`

	// MaxEventsLimit / DefaultEventsLimit bound getEvents page sizes. Defaults
	// 10000 / 100.
	MaxEventsLimit     *uint `toml:"max_events_limit"`
	DefaultEventsLimit *uint `toml:"default_events_limit"`

	// MaxTransactionsLimit / DefaultTransactionsLimit bound getTransactions
	// page sizes. Defaults 200 / 50.
	MaxTransactionsLimit     *uint `toml:"max_transactions_limit"`
	DefaultTransactionsLimit *uint `toml:"default_transactions_limit"`

	// MaxLedgersLimit / DefaultLedgersLimit bound getLedgers page sizes.
	// Defaults 200 / 50.
	MaxLedgersLimit     *uint `toml:"max_ledgers_limit"`
	DefaultLedgersLimit *uint `toml:"default_ledgers_limit"`

	// MaxRequestExecutionDuration is the HTTP-layer ceiling on any single
	// request; past it the server responds 504 and aborts the request.
	// Default "25s".
	MaxRequestExecutionDuration *time.Duration `toml:"max_request_execution_duration"`

	// RequestBacklogGlobalQueueLimit caps concurrently in-flight HTTP requests
	// across all methods. Default 5000.
	RequestBacklogGlobalQueueLimit *uint `toml:"request_backlog_global_queue_limit"`

	// MaxHealthyLedgerLatency is getHealth's freshness threshold: if the last
	// committed ledger closed longer ago than this, getHealth reports the
	// service unhealthy. Default "30s".
	MaxHealthyLedgerLatency *time.Duration `toml:"max_healthy_ledger_latency"`

	// LedgerReaderCache / EventReaderCache cap the cold per-chunk reader
	// caches (entries = chunks). 0 or unset means the registry's built-in
	// defaults (128 ledger readers, 32 event readers).
	LedgerReaderCache *int `toml:"ledger_reader_cache"`
	EventReaderCache  *int `toml:"event_reader_cache"`
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

	// CaptiveCoreHTTPPort is the live core's admin HTTP port. Default 11626;
	// 0 disables it. Applies only to the live ingestion core — backfill's
	// bounded replay cores never serve HTTP.
	CaptiveCoreHTTPPort *uint16 `toml:"captive_core_http_port"`

	// CaptiveCoreHTTPQueryPort is the live core's HTTP query-server port —
	// the backend getLedgerEntries reads from. Default 11628; 0 disables the
	// query server, and with it getLedgerEntries.
	CaptiveCoreHTTPQueryPort *uint16 `toml:"captive_core_http_query_port"`

	// CaptiveCoreHTTPQueryThreadPoolSize is the query server's worker count.
	// Default: the machine's CPU count.
	CaptiveCoreHTTPQueryThreadPoolSize *uint16 `toml:"captive_core_http_query_thread_pool_size"`

	// CaptiveCoreHTTPQuerySnapshotLedgers is how many recent ledgers the query
	// server keeps snapshots for. Default 4.
	CaptiveCoreHTTPQuerySnapshotLedgers *uint16 `toml:"captive_core_http_query_snapshot_ledgers"`
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

	// [serving] defaults, copied from the v1 RPC daemon's options.
	DefaultServingEndpoint                              = "localhost:8000"
	DefaultMaxEventsLimit                 uint          = 10000
	DefaultDefaultEventsLimit             uint          = 100
	DefaultMaxTransactionsLimit           uint          = 200
	DefaultDefaultTransactionsLimit       uint          = 50
	DefaultMaxLedgersLimit                uint          = 200
	DefaultDefaultLedgersLimit            uint          = 50
	DefaultMaxRequestExecutionDuration                  = 25 * time.Second
	DefaultRequestBacklogGlobalQueueLimit uint          = 5000
	DefaultMaxHealthyLedgerLatency                      = 30 * time.Second

	// [ingestion] captive-core HTTP defaults, copied from the v1 RPC daemon.
	DefaultCaptiveCoreHTTPPort                 uint16 = 11626
	DefaultCaptiveCoreHTTPQueryPort            uint16 = 11628
	DefaultCaptiveCoreHTTPQuerySnapshotLedgers uint16 = 4

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
	cfg.Serving = cfg.Serving.withDefaults()
	cfg.Ingestion = cfg.Ingestion.withDefaults()
	return cfg
}

func (s ServingConfig) withDefaults() ServingConfig {
	if s.Endpoint == "" {
		s.Endpoint = DefaultServingEndpoint
	}
	setUint(&s.MaxEventsLimit, DefaultMaxEventsLimit)
	setUint(&s.DefaultEventsLimit, DefaultDefaultEventsLimit)
	setUint(&s.MaxTransactionsLimit, DefaultMaxTransactionsLimit)
	setUint(&s.DefaultTransactionsLimit, DefaultDefaultTransactionsLimit)
	setUint(&s.MaxLedgersLimit, DefaultMaxLedgersLimit)
	setUint(&s.DefaultLedgersLimit, DefaultDefaultLedgersLimit)
	setUint(&s.RequestBacklogGlobalQueueLimit, DefaultRequestBacklogGlobalQueueLimit)
	if s.MaxRequestExecutionDuration == nil {
		v := DefaultMaxRequestExecutionDuration
		s.MaxRequestExecutionDuration = &v
	}
	if s.MaxHealthyLedgerLatency == nil {
		v := DefaultMaxHealthyLedgerLatency
		s.MaxHealthyLedgerLatency = &v
	}
	// LedgerReaderCache/EventReaderCache stay nil when unset: 0 reaches the
	// registry, which applies its own built-in defaults.
	return s
}

func (ing IngestionConfig) withDefaults() IngestionConfig {
	if ing.CaptiveCoreHTTPPort == nil {
		v := DefaultCaptiveCoreHTTPPort
		ing.CaptiveCoreHTTPPort = &v
	}
	if ing.CaptiveCoreHTTPQueryPort == nil {
		v := DefaultCaptiveCoreHTTPQueryPort
		ing.CaptiveCoreHTTPQueryPort = &v
	}
	if ing.CaptiveCoreHTTPQueryThreadPoolSize == nil {
		v := defaultQueryThreadPoolSize()
		ing.CaptiveCoreHTTPQueryThreadPoolSize = &v
	}
	if ing.CaptiveCoreHTTPQuerySnapshotLedgers == nil {
		v := DefaultCaptiveCoreHTTPQuerySnapshotLedgers
		ing.CaptiveCoreHTTPQuerySnapshotLedgers = &v
	}
	return ing
}

func setUint(p **uint, def uint) {
	if *p == nil {
		v := def
		*p = &v
	}
}

// defaultQueryThreadPoolSize is the CPU count clamped into uint16 (the SDK's
// query-server param width).
func defaultQueryThreadPoolSize() uint16 {
	n := runtime.NumCPU()
	if n > int(^uint16(0)) {
		return ^uint16(0)
	}
	if n < 1 {
		return 1
	}
	return uint16(n)
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
