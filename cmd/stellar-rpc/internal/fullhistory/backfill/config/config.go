// Package config owns the backfill-subcommand's TOML schema, CLI-flag merge
// layer, and the pre-DAG validation rules listed in design-doc section
// "Validation before DAG construction".
//
// The package splits its responsibilities across three concerns but lives as
// a single Go package so a future refactor can freely move helpers between
// them without producing import cycles:
//
//   - config.go       — struct schema, ParseConfig, MarshalTOML
//   - flags.go        — CLIFlags + ApplyFlags (chunk-boundary widening)
//   - validate.go     — Validate, ValidateFlags, ValidateAgainstConfigStore,
//     ValidateAgainstBSB
//   - interfaces.go   — ConfigStore + BSBAvailabilityProbe contracts, plus
//     no-op stubs used until slices #689 and #688 land the
//     real RocksDB-backed and BSB-backed implementations.
//
// TOML convention: UPPER_SNAKE_CASE keys throughout. pelletier/go-toml (v1)
// treats TOML keys as case-sensitive per spec, so the struct tags must match
// the on-disk casing exactly.
package config

import (
	"fmt"

	"github.com/pelletier/go-toml"
)

// Config is the top-level backfill configuration.
//
// Field layout mirrors the TOML schema in design-doc section "Configuration":
// [SERVICE], [BACKFILL] (with nested [BACKFILL.BSB]),
// [IMMUTABLE_STORAGE.LEDGERS|EVENTS|TXHASH_RAW|TXHASH_INDEX],
// [LOGGING], [META_STORE]. All keys are UPPER_SNAKE_CASE on disk; Go field
// names remain idiomatic CamelCase via struct tags.
//
// Non-TOML fields populated by ApplyFlags (see flags.go) carry `toml:"-"`
// so they round-trip cleanly through MarshalTOML.
type Config struct {
	// Service holds the base data directory used as a default root for
	// any storage paths the operator does not set explicitly.
	Service ServiceConfig `toml:"SERVICE"`

	// MetaStore is an optional section; when unset, Validate defaults
	// Path to {DEFAULT_DATA_DIR}/META/ROCKSDB.
	MetaStore MetaStoreConfig `toml:"META_STORE"`

	// Backfill holds the pipeline-level knobs: CHUNKS_PER_TXHASH_INDEX
	// (layout-defining; must not change across runs) and the BSB
	// connection details.
	Backfill BackfillConfig `toml:"BACKFILL"`

	// ImmutableStorage holds the four per-data-type storage paths that
	// the design doc lists under [IMMUTABLE_STORAGE.*]. Each defaults to
	// a sub-directory of DEFAULT_DATA_DIR when Validate resolves defaults.
	ImmutableStorage ImmutableStorageConfig `toml:"IMMUTABLE_STORAGE"`

	// Logging carries the [LOGGING] section. This slice only plumbs the
	// values through — the logrus bootstrap that consumes them lands in
	// slice #685 with the pkg/logging rewrite.
	Logging LoggingConfig `toml:"LOGGING"`

	// --- Fields populated by ApplyFlags — never round-tripped via TOML. ---

	// EffectiveStartLedger / EffectiveEndLedger are the widened range
	// after chunk-boundary expansion. Always >= the operator-requested
	// range; never narrower.
	EffectiveStartLedger uint32 `toml:"-"`
	EffectiveEndLedger   uint32 `toml:"-"`

	// Workers is the resolved DAG slot count (0 sentinel replaced with
	// GOMAXPROCS by ApplyFlags).
	Workers int `toml:"-"`

	// MaxRetries is the resolved per-task retry budget.
	MaxRetries int `toml:"-"`

	// VerifyRecSplit mirrors the CLI flag.
	VerifyRecSplit bool `toml:"-"`
}

// ServiceConfig carries the single [SERVICE] key — DEFAULT_DATA_DIR — which
// operators are required to set and which serves as the fall-back root for
// the meta store, immutable storage trees, and log file paths.
type ServiceConfig struct {
	// DefaultDataDir is the base directory; if unset, Validate errors.
	DefaultDataDir string `toml:"DEFAULT_DATA_DIR"`
}

// MetaStoreConfig carries the optional [META_STORE] section. Only the
// RocksDB directory path is operator-visible; RocksDB tuning knobs are
// hardcoded in the Layer-2 metastore facade (slice #689) per MEMORY.md's
// decision "tuning knobs NOT in operator TOML".
type MetaStoreConfig struct {
	// Path is the meta-store RocksDB directory.
	Path string `toml:"PATH"`
}

// BackfillConfig holds the [BACKFILL] section plus its nested [BACKFILL.BSB]
// table. CHUNKS_PER_TXHASH_INDEX is layout-defining (design doc: "must not
// change across runs") — immutability is enforced by
// ValidateAgainstConfigStore against the meta-store key
// `config:chunks_per_txhash_index`.
type BackfillConfig struct {
	// ChunksPerTxHashIndex controls the cadence of txhash index builds;
	// defaults to 1_000 (= 10M ledgers per index) when absent.
	ChunksPerTxHashIndex uint32 `toml:"CHUNKS_PER_TXHASH_INDEX"`

	// BSB is the BufferedStorageBackend configuration — required per the
	// design doc, hence pointer-valued so absence is distinguishable from
	// empty-struct.
	BSB *BSBConfig `toml:"BSB"`
}

// BSBConfig holds the [BACKFILL.BSB] nested table. BUCKET_PATH is the only
// required key; BUFFER_SIZE and NUM_WORKERS default to 1_000 and 20 when
// absent or zero (matching the design doc's defaults).
type BSBConfig struct {
	// BucketPath is the remote object-store path (GCS: no gs:// prefix).
	BucketPath string `toml:"BUCKET_PATH"`

	// BufferSize is the prefetch depth per BSB connection.
	BufferSize int `toml:"BUFFER_SIZE"`

	// NumWorkers is the download-worker count per BSB connection.
	NumWorkers int `toml:"NUM_WORKERS"`
}

// ImmutableStorageConfig mirrors the four sub-sections of [IMMUTABLE_STORAGE]:
// LEDGERS, EVENTS, TXHASH_RAW, TXHASH_INDEX. Each carries a PATH key that
// defaults to {DEFAULT_DATA_DIR}/<type> when Validate resolves defaults.
type ImmutableStorageConfig struct {
	// Ledgers is the per-chunk ledger-pack-file tree root.
	Ledgers StoragePathConfig `toml:"LEDGERS"`

	// Events is the per-chunk events cold-segment tree root.
	Events StoragePathConfig `toml:"EVENTS"`

	// TxHashRaw is the per-chunk raw .bin tree root (transient; cleaned
	// up by CleanupTxHashTask after the RecSplit build).
	TxHashRaw StoragePathConfig `toml:"TXHASH_RAW"`

	// TxHashIndex is the per-txhash-index RecSplit .idx tree root
	// (permanent once built).
	TxHashIndex StoragePathConfig `toml:"TXHASH_INDEX"`
}

// StoragePathConfig is the shape shared by all four [IMMUTABLE_STORAGE.*]
// sub-sections — a single PATH string.
type StoragePathConfig struct {
	// Path is the filesystem root for this data type.
	Path string `toml:"PATH"`
}

// LoggingConfig carries the [LOGGING] section — LEVEL (debug/info/warn/error)
// and FORMAT (text/json). The logrus bootstrap that consumes these fields
// lands in slice #685; this slice only plumbs them through parse + flag merge.
type LoggingConfig struct {
	// Level is one of "debug", "info", "warn", "error".
	Level string `toml:"LEVEL"`

	// Format is one of "text", "json".
	Format string `toml:"FORMAT"`
}

// ParseConfig decodes a TOML byte slice (typically the contents of the file
// passed via `--config`) into a Config. Errors are wrapped so the caller can
// surface them to the operator with context about where parsing failed.
func ParseConfig(data []byte) (*Config, error) {
	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

// MarshalTOML encodes the Config back to TOML bytes. Exists to satisfy the
// "TOML round-trips" acceptance criterion from #684 — useful for diagnostics
// ("what config is this backfill actually running with?") and for producing
// a normalized on-disk form after defaults are resolved. Keys come out in
// the same UPPER_SNAKE_CASE the struct tags declare.
func (c *Config) MarshalTOML() ([]byte, error) {
	buf, err := toml.Marshal(*c)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	return buf, nil
}
