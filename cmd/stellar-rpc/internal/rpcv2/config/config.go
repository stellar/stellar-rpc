package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pelletier/go-toml"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
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
//
// Every leaf key is also settable from the command line as a flag named by its
// dotted TOML path (--storage.default_data_dir, --service.methods.getLedgers.queue_limit);
// see BindFlags/ApplyFlags in flags.go. Flags override the file; the file stays
// the single source of truth and --config stays required.
type Config struct {
	Service   ServiceConfig   `toml:"service"`
	Retention RetentionConfig `toml:"retention"`
	Storage   StorageConfig   `toml:"storage"`
	Backfill  BackfillConfig  `toml:"backfill"`
	Ingestion IngestionConfig `toml:"ingestion"`
	Logging   LoggingConfig   `toml:"logging"`
}

// ServiceConfig is [service] — the JSON-RPC read-serving policy (issue #882).
// Everything here is dormant today: the read server arrives with #772, and
// #881 wires [service.fee_stats] into live ingestion — until then the values
// are only parsed, defaulted, and validated. The whole section is optional:
// absent keys get v1's defaults in WithDefaults.
//
// Key naming rule: camelCase table keys ONLY where the key is a wire identifier
// (the [service.methods.<methodName>] tables, named after the JSON-RPC method);
// snake_case for every other key. The decoder cannot ENFORCE the casing —
// go-toml matches keys case-insensitively, so a miscased method table is
// accepted — the rule exists so configs read like the wire protocol.
type ServiceConfig struct {
	// Endpoint is the address the JSON-RPC server listens on. Default "localhost:8000".
	Endpoint string `toml:"endpoint"`
	// AdminEndpoint serves pprof/metrics over plaintext HTTP. "" (the default)
	// disables the admin server; it should never be reachable from the internet.
	AdminEndpoint string `toml:"admin_endpoint"`

	// The three keys below are the HTTP-layer gate wrapped around the WHOLE mux,
	// exactly as in v1 (internal/jsonrpc): they always act, on every request, in
	// ADDITION to the per-method limits — max_concurrent_requests bounds the SUM
	// of in-flight requests across all methods, which no per-method limit can.
	MaxConcurrentRequests            *uint          `toml:"max_concurrent_requests"`
	MaxRequestExecutionDuration      *time.Duration `toml:"max_request_execution_duration"`
	RequestExecutionWarningThreshold *time.Duration `toml:"request_execution_warning_threshold"`

	FeeStats FeeStatsConfig `toml:"fee_stats"`
	Methods  MethodsConfig  `toml:"methods"`
}

// FeeStatsConfig is [service.fee_stats] — the sizes, in ledgers, of the two
// in-memory fee windows behind getFeeStats. Live ingestion will feed them when
// #881 lands; nothing consumes them yet. They live here and NOT in the
// getFeeStats method table because they size ingestion-time memory, not
// request handling. Both must be positive and are capped at
// limits.MaxFeeStatsRetentionWindow.
type FeeStatsConfig struct {
	ClassicFeeWindowLedgers          *uint32 `toml:"classic_fee_window_ledgers"`
	SorobanInclusionFeeWindowLedgers *uint32 `toml:"soroban_inclusion_fee_window_ledgers"`
}

// MethodsConfig is [service.methods]: one table per served JSON-RPC method,
// keyed by the method's wire name, plus an optional methods-wide DEFAULT tier —
// the two bare keys below — for the two fields every method has.
//
// Precedence per method and field: explicit per-method value → methods-wide
// default → compiled default (v1's values). Specificity beats source: a CLI
// --service.methods.queue_limit=30 raises the default tier but never overrides
// a per-method value set in the file. The cascade is resolved in WithDefaults;
// after it, every per-method field is non-nil.
//
// One struct field per method — never a map — so the strict decoder rejects an
// unknown method table instead of silently accepting it. (go-toml matches keys
// case-insensitively, so this catches misspellings but not wrong casing.)
type MethodsConfig struct {
	QueueLimit           *uint          `toml:"queue_limit"`
	MaxExecutionDuration *time.Duration `toml:"max_execution_duration"`

	GetHealth       HealthMethodConfig    `toml:"getHealth"`
	GetNetwork      MethodConfig          `toml:"getNetwork"`
	GetVersionInfo  MethodConfig          `toml:"getVersionInfo"`
	GetLatestLedger MethodConfig          `toml:"getLatestLedger"`
	GetTransaction  MethodConfig          `toml:"getTransaction"`
	GetTransactions PaginatedMethodConfig `toml:"getTransactions"`
	GetLedgers      PaginatedMethodConfig `toml:"getLedgers"`
	GetEvents       PaginatedMethodConfig `toml:"getEvents"`
	GetFeeStats     MethodConfig          `toml:"getFeeStats"`
}

// MethodConfig is one method's serving knobs: the per-method request backlog
// cap and the per-method execution budget (v1's request-backlog-*-queue-limit /
// max-*-execution-duration pairs).
type MethodConfig struct {
	QueueLimit           *uint          `toml:"queue_limit"`
	MaxExecutionDuration *time.Duration `toml:"max_execution_duration"`
}

// PaginatedMethodConfig extends MethodConfig for the cursor-paginated methods
// (getTransactions, getLedgers, getEvents): max_items_per_response caps the
// client's pagination limit, default_items_per_response applies when the
// request omits it. These two have NO methods-wide default tier — they exist
// on only three methods, so a wide default would mostly be a trap.
type PaginatedMethodConfig struct {
	QueueLimit           *uint          `toml:"queue_limit"`
	MaxExecutionDuration *time.Duration `toml:"max_execution_duration"`

	MaxItemsPerResponse     *uint `toml:"max_items_per_response"`
	DefaultItemsPerResponse *uint `toml:"default_items_per_response"`
}

// HealthMethodConfig extends MethodConfig for getHealth with the staleness
// threshold: the last committed ledger's close time must be within
// max_healthy_ledger_latency of now for the daemon to report healthy. It lives
// here because the staleness judgment is the getHealth handler's check policy
// (see rpcv2/health.go) — the daemon itself only exposes the raw signal.
type HealthMethodConfig struct {
	QueueLimit           *uint          `toml:"queue_limit"`
	MaxExecutionDuration *time.Duration `toml:"max_execution_duration"`

	MaxHealthyLedgerLatency *time.Duration `toml:"max_healthy_ledger_latency"`
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

// StorageConfig is [storage]: the data root plus one optional path per on-disk
// tree (consolidating what were the separate [catalog] / [immutable_storage.*] /
// [streaming.hot_storage] sections). An empty per-store value defaults under
// default_data_dir.
type StorageConfig struct {
	// Base dir for the catalog and default storage paths. Required.
	// (Moved here from [service] in #882 — it governs storage placement.)
	DefaultDataDir string `toml:"default_data_dir"`

	Catalog     string `toml:"catalog"`      // catalog RocksDB dir
	Ledgers     string `toml:"ledgers"`      // immutable ledger packs root
	Events      string `toml:"events"`       // immutable events segments root
	TxhashRaw   string `toml:"txhash_raw"`   // transient txhash .bin root
	TxhashIndex string `toml:"txhash_index"` // frozen txhash .idx root
	Hot         string `toml:"hot"`          // per-chunk hot RocksDB root
}

// BackfillConfig is [backfill] plus the nested [backfill.datastore] and [backfill.bsb].
type BackfillConfig struct {
	// Concurrent task-slot count for bulk backfill; >= 1. Default GOMAXPROCS.
	Workers *int `toml:"workers"`

	// Per-task retries before the daemon aborts; >= 0 (0 = run once). Default
	// DefaultMaxRetries.
	MaxRetries *int `toml:"max_retries"`

	// DataStore is the bulk ledger source. An empty Type means no lake — backfill
	// then replays through captive core from the history archives; otherwise any
	// SDK datastore type is accepted.
	DataStore DataStoreConfig `toml:"datastore"`

	// BSB tunes the buffered-storage stream over DataStore.
	BSB BSBConfig `toml:"bsb"`
}

// DataStoreConfig is [backfill.datastore] — the bulk ledger source. The key
// names match the SDK's datastore.DataStoreConfig, but the struct is a mirror
// rather than the SDK type itself: the SDK struct carries extra untagged fields
// (NetworkPassphrase, Compression), and go-toml matches untagged exported
// fields by name, so embedding it would silently admit those as file keys. In
// particular the network passphrase must never come from this file — the
// captive-core config is its single source, and the daemon copies it into the
// SDK config at startup (see SDKConfig).
type DataStoreConfig struct {
	// Type is the SDK datastore type: "GCS", "S3", "Filesystem", ... Empty
	// means no lake.
	Type string `toml:"type"`
	// Params are the datastore-type-specific parameters (e.g. GCS's
	// destination_bucket_path).
	Params map[string]string `toml:"params"`
	// Schema describes the lake's file layout. Optional when the lake carries a
	// manifest — the stream reads the schema from there.
	Schema DataStoreSchemaConfig `toml:"schema"`
}

// DataStoreSchemaConfig is [backfill.datastore.schema] — how the lake's ledger
// files are laid out. Must match how the lake was exported.
type DataStoreSchemaConfig struct {
	LedgersPerFile    uint32 `toml:"ledgers_per_file"`
	FilesPerPartition uint32 `toml:"files_per_partition"`
}

// SDKConfig converts the [backfill.datastore] section into the SDK's config
// type. networkPassphrase is the one read back from the captive-core file (empty
// = unknown, e.g. an injected test core): when non-empty, the SDK compares it
// against the lake's manifest and refuses a lake exported from a different
// network.
func (d DataStoreConfig) SDKConfig(networkPassphrase string) datastore.DataStoreConfig {
	return datastore.DataStoreConfig{
		Type:   d.Type,
		Params: d.Params,
		Schema: datastore.DataStoreSchema{
			LedgersPerFile:    d.Schema.LedgersPerFile,
			FilesPerPartition: d.Schema.FilesPerPartition,
		},
		NetworkPassphrase: networkPassphrase,
	}
}

// BSBConfig is [backfill.bsb] — tuning for the buffered-storage stream that
// downloads ledger objects from [backfill.datastore] during backfill. It mirrors
// the SDK's ledgerbackend.BufferedStorageBackendConfig rather than embedding it,
// for two reasons: the fields are pointers so an absent key is distinguishable
// from an explicit zero (max_retries = 0 genuinely disables retries), and the
// key is named max_retries to match [backfill].max_retries instead of the SDK's
// retry_limit. SDKConfig converts to the SDK type at the daemon boundary.
type BSBConfig struct {
	// BufferSize is how many downloaded ledgers are buffered in memory, keeping
	// the download ahead of ingest; >= 1. Default backfill.DefaultBSBBufferSize.
	BufferSize *uint32 `toml:"buffer_size"`
	// NumWorkers is how many object downloads run concurrently; >= 1. Default
	// backfill.DefaultBSBNumWorkers.
	NumWorkers *uint32 `toml:"num_workers"`
	// MaxRetries caps how many times ONE object download (a single ledger file
	// fetched from the datastore) is retried after a transient error, waiting
	// RetryWait between attempts; when the budget is exhausted the whole stream
	// fails. 0 = fail on the first error, no retries. This is a different knob
	// from [backfill].max_retries, which re-runs a whole chunk task. Default
	// backfill.DefaultBSBMaxRetries.
	MaxRetries *uint32 `toml:"max_retries"`
	// RetryWait is the pause between retries of one object download; >= 1ms
	// (validateConfig applies the same nanosecond-typo guard as the [service]
	// durations). Default backfill.DefaultBSBRetryWait.
	RetryWait *time.Duration `toml:"retry_wait"`
}

// SDKConfig converts the resolved [backfill.bsb] section into the SDK's config
// type. Call it only after WithDefaults, when every field is non-nil.
func (b BSBConfig) SDKConfig() ledgerbackend.BufferedStorageBackendConfig {
	return ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: *b.BufferSize,
		NumWorkers: *b.NumWorkers,
		RetryLimit: *b.MaxRetries,
		RetryWait:  *b.RetryWait,
	}
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

// [service] defaults — v1's values, kept identical so the two daemons serve
// under the same policy until an operator tunes them.
const (
	DefaultEndpoint = "localhost:8000"

	DefaultMaxConcurrentRequests            uint          = 5000
	DefaultMaxRequestExecutionDuration      time.Duration = 25 * time.Second
	DefaultRequestExecutionWarningThreshold time.Duration = 5 * time.Second

	// DefaultMethodQueueLimit and the three below it are the compiled
	// per-method defaults — the last tier of the methods cascade.
	DefaultMethodQueueLimit           uint          = 1000
	DefaultGetFeeStatsQueueLimit      uint          = 100
	DefaultMethodMaxExecutionDuration time.Duration = 5 * time.Second
	// DefaultScanMethodMaxExecutionDuration is the doubled budget v1 gives
	// getEvents and getLedgers, which scan wider ranges.
	DefaultScanMethodMaxExecutionDuration time.Duration = 10 * time.Second

	DefaultMaxHealthyLedgerLatency time.Duration = 30 * time.Second

	DefaultGetEventsMaxItemsPerResponse           uint = 10000
	DefaultGetEventsDefaultItemsPerResponse       uint = 100
	DefaultGetTransactionsMaxItemsPerResponse     uint = 200
	DefaultGetTransactionsDefaultItemsPerResponse uint = 50
	DefaultGetLedgersMaxItemsPerResponse          uint = 200
	DefaultGetLedgersDefaultItemsPerResponse      uint = 50

	DefaultClassicFeeWindowLedgers          uint32 = 10
	DefaultSorobanInclusionFeeWindowLedgers uint32 = 50
)

// LoadConfigWithFlags reads and parses the TOML config at path, with CLI
// overrides: decode the file (strict), overlay every flag the user actually set
// (nil fs = none), THEN resolve defaults — so a flag participates in the
// methods cascade at its own specificity tier before any default is filled. It
// does NOT validate semantics or touch any pin — that is validateConfig's job.
// See flags.go for the flag-name/TOML-path correspondence.
func LoadConfigWithFlags(path string, fs FlagOverrides) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	cfg, err := DecodeConfig(data)
	if err != nil {
		return Config{}, err
	}
	if fs != nil {
		if err := ApplyFlags(&cfg, fs); err != nil {
			return Config{}, err
		}
	}
	return cfg.WithDefaults(), nil
}

// ParseConfig parses TOML bytes into a Config with defaults applied. Split from
// LoadConfig so tests parse in-memory documents without a temp file.
func ParseConfig(data []byte) (Config, error) {
	cfg, err := DecodeConfig(data)
	if err != nil {
		return Config{}, err
	}
	return cfg.WithDefaults(), nil
}

// DecodeConfig strictly decodes TOML bytes into a Config WITHOUT applying
// defaults — the seam LoadConfigWithFlags uses to overlay CLI flags between
// decode and defaulting.
//
// Decoding is STRICT (Decoder.Strict(true)): any unknown key is an error, not
// silently ignored (go-toml v1's plain Unmarshal ignores them). A typo in the
// immutable, pinned earliest_ledger key must fail loudly, not pin the wrong
// value on first start; and the removed chunks_per_txhash_index key (and its
// whole [layout] section) is rejected, not silently ignored.
func DecodeConfig(data []byte) (Config, error) {
	var cfg Config
	if err := toml.NewDecoder(bytes.NewReader(data)).Strict(true).Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

// WithDefaults returns a copy of cfg with every documented default filled for
// an unset (nil pointer / empty string) field. Explicit zeros are preserved
// (and later rejected by validateConfig where a zero is illegal).
//
// For the per-method serving fields it also resolves the methods cascade:
// explicit per-method value → [service.methods] wide default → compiled
// default. After WithDefaults every per-method field is non-nil.
//
//nolint:funlen // one linear fill per config field; splitting it hides fields
func (cfg Config) WithDefaults() Config {
	if cfg.Backfill.Workers == nil {
		v := backfill.DefaultWorkers()
		cfg.Backfill.Workers = &v
	}
	if cfg.Backfill.MaxRetries == nil {
		v := DefaultMaxRetries
		cfg.Backfill.MaxRetries = &v
	}
	fillUint32(&cfg.Backfill.BSB.BufferSize, backfill.DefaultBSBBufferSize)
	fillUint32(&cfg.Backfill.BSB.NumWorkers, backfill.DefaultBSBNumWorkers)
	fillUint32(&cfg.Backfill.BSB.MaxRetries, backfill.DefaultBSBMaxRetries)
	fillDuration(&cfg.Backfill.BSB.RetryWait, backfill.DefaultBSBRetryWait)
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

	svc := &cfg.Service
	if svc.Endpoint == "" {
		svc.Endpoint = DefaultEndpoint
	}
	fillUint(&svc.MaxConcurrentRequests, DefaultMaxConcurrentRequests)
	fillDuration(&svc.MaxRequestExecutionDuration, DefaultMaxRequestExecutionDuration)
	fillDuration(&svc.RequestExecutionWarningThreshold, DefaultRequestExecutionWarningThreshold)
	fillUint32(&svc.FeeStats.ClassicFeeWindowLedgers, DefaultClassicFeeWindowLedgers)
	fillUint32(&svc.FeeStats.SorobanInclusionFeeWindowLedgers, DefaultSorobanInclusionFeeWindowLedgers)

	// The methods cascade. queue/dur fill one per-method field: an explicit
	// per-method value stays; else the [service.methods] wide default applies;
	// else the compiled default. The wide-tier fields themselves are left as
	// decoded — they are only ever read here.
	m := &svc.Methods
	queue := func(p **uint, compiled uint) {
		if *p != nil {
			return
		}
		if m.QueueLimit != nil {
			compiled = *m.QueueLimit
		}
		v := compiled
		*p = &v
	}
	dur := func(p **time.Duration, compiled time.Duration) {
		if *p != nil {
			return
		}
		if m.MaxExecutionDuration != nil {
			compiled = *m.MaxExecutionDuration
		}
		v := compiled
		*p = &v
	}

	queue(&m.GetHealth.QueueLimit, DefaultMethodQueueLimit)
	dur(&m.GetHealth.MaxExecutionDuration, DefaultMethodMaxExecutionDuration)
	fillDuration(&m.GetHealth.MaxHealthyLedgerLatency, DefaultMaxHealthyLedgerLatency)

	queue(&m.GetNetwork.QueueLimit, DefaultMethodQueueLimit)
	dur(&m.GetNetwork.MaxExecutionDuration, DefaultMethodMaxExecutionDuration)

	queue(&m.GetVersionInfo.QueueLimit, DefaultMethodQueueLimit)
	dur(&m.GetVersionInfo.MaxExecutionDuration, DefaultMethodMaxExecutionDuration)

	queue(&m.GetLatestLedger.QueueLimit, DefaultMethodQueueLimit)
	dur(&m.GetLatestLedger.MaxExecutionDuration, DefaultMethodMaxExecutionDuration)

	queue(&m.GetTransaction.QueueLimit, DefaultMethodQueueLimit)
	dur(&m.GetTransaction.MaxExecutionDuration, DefaultMethodMaxExecutionDuration)

	queue(&m.GetTransactions.QueueLimit, DefaultMethodQueueLimit)
	dur(&m.GetTransactions.MaxExecutionDuration, DefaultMethodMaxExecutionDuration)
	fillUint(&m.GetTransactions.MaxItemsPerResponse, DefaultGetTransactionsMaxItemsPerResponse)
	fillUint(&m.GetTransactions.DefaultItemsPerResponse, DefaultGetTransactionsDefaultItemsPerResponse)

	queue(&m.GetLedgers.QueueLimit, DefaultMethodQueueLimit)
	dur(&m.GetLedgers.MaxExecutionDuration, DefaultScanMethodMaxExecutionDuration)
	fillUint(&m.GetLedgers.MaxItemsPerResponse, DefaultGetLedgersMaxItemsPerResponse)
	fillUint(&m.GetLedgers.DefaultItemsPerResponse, DefaultGetLedgersDefaultItemsPerResponse)

	queue(&m.GetEvents.QueueLimit, DefaultMethodQueueLimit)
	dur(&m.GetEvents.MaxExecutionDuration, DefaultScanMethodMaxExecutionDuration)
	fillUint(&m.GetEvents.MaxItemsPerResponse, DefaultGetEventsMaxItemsPerResponse)
	fillUint(&m.GetEvents.DefaultItemsPerResponse, DefaultGetEventsDefaultItemsPerResponse)

	queue(&m.GetFeeStats.QueueLimit, DefaultGetFeeStatsQueueLimit)
	dur(&m.GetFeeStats.MaxExecutionDuration, DefaultMethodMaxExecutionDuration)

	return cfg
}

func fillUint(p **uint, v uint) {
	if *p == nil {
		*p = &v
	}
}

func fillUint32(p **uint32, v uint32) {
	if *p == nil {
		*p = &v
	}
}

func fillDuration(p **time.Duration, v time.Duration) {
	if *p == nil {
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
	dataDir := cfg.Storage.DefaultDataDir
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
