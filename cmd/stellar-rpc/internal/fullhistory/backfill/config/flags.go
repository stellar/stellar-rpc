package config

import (
	"runtime"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/geometry"
)

// Default CLI-flag fall-backs applied when the operator omits a flag (or
// passes the zero sentinel 0). Named-constant form keeps the defaults out
// of ApplyFlags' control flow.
const (
	// defaultMaxRetries matches the design-doc "--max-retries default 3"
	// setting, echoed in cmd.go's flag registration.
	defaultMaxRetries int = 3
)

// CLIFlags carries the flag values that `cmd.go` hands to the subcommand
// body. Fields mirror cobra flag names (CamelCase for Go idioms,
// dash-separated on the wire).
//
// ApplyFlags consumes CLIFlags and mutates the Config with (a) the widened
// [EffectiveStartLedger, EffectiveEndLedger] range and (b) the resolved
// worker count, retry budget, verify-recsplit toggle, and logging overrides.
type CLIFlags struct {
	// StartLedger / EndLedger are the operator-requested range. Widened
	// outward to chunk boundaries by ApplyFlags.
	StartLedger uint32
	EndLedger   uint32

	// Workers is the DAG slot count. 0 → GOMAXPROCS (sentinel convention
	// already advertised by cmd.go's help text "(0 = GOMAXPROCS)").
	Workers int

	// MaxRetries is the per-task retry budget. 0 → 3 (design-doc default).
	MaxRetries int

	// VerifyRecSplit toggles the RecSplit verify pass; defaults true via
	// cobra's flag registration so the zero value here means "operator
	// explicitly set false OR flag was never wired" — cmd.go supplies
	// the real value.
	VerifyRecSplit bool

	// LogLevel / LogFormat override the TOML [LOGGING] section when
	// non-empty. Logrus wiring lands in slice #685; this slice only
	// merges the values into cfg.Logging.
	LogLevel  string
	LogFormat string
}

// ApplyFlags merges CLI flag values into the Config: widens the ledger range
// outward to chunk boundaries, resolves worker-count and retry defaults,
// stores the verify-recsplit toggle, and overrides logging fields whenever
// the operator passed a non-empty value.
//
// Assumes Validate has been called first so CHUNKS_PER_TXHASH_INDEX is
// defaulted — BuildGeometry depends on that. Order matters at the
// subcommand layer; see cmd.go's Run function.
func (c *Config) ApplyFlags(flags CLIFlags) {
	geo := c.BuildGeometry()

	// Chunk-boundary expansion. Design-doc rule: start widens DOWN to the
	// first ledger of its chunk, end widens UP to the last. Never narrows.
	//
	// The subtraction `flags.StartLedger - geometry.FirstLedger` is safe
	// so long as StartLedger >= FirstLedger (2); ValidateFlags enforces
	// that, but ApplyFlags runs before ValidateFlags in the subcommand
	// pipeline by design (expansion feeds both the effective range and the
	// BSB-availability check that follows). Pre-check to avoid underflow.
	if flags.StartLedger >= geometry.FirstLedger {
		startChunkID := (flags.StartLedger - geometry.FirstLedger) / geo.ChunkSize
		c.EffectiveStartLedger = geo.ChunkFirstLedger(startChunkID)
	} else {
		c.EffectiveStartLedger = flags.StartLedger // ValidateFlags will reject
	}

	if flags.EndLedger >= geometry.FirstLedger {
		endChunkID := (flags.EndLedger - geometry.FirstLedger) / geo.ChunkSize
		c.EffectiveEndLedger = geo.ChunkLastLedger(endChunkID)
	} else {
		c.EffectiveEndLedger = flags.EndLedger // ValidateFlags will reject
	}

	// Workers: 0 sentinel → GOMAXPROCS. Matches the "(0 = GOMAXPROCS)"
	// help-text contract cmd.go advertises.
	c.Workers = flags.Workers
	if c.Workers <= 0 {
		c.Workers = runtime.GOMAXPROCS(0)
	}

	// MaxRetries: 0 sentinel → 3. Cobra's flag default is also 3, so the
	// common path is that flags.MaxRetries arrives already-set; this branch
	// covers direct programmatic callers (tests, future wrappers).
	c.MaxRetries = flags.MaxRetries
	if c.MaxRetries <= 0 {
		c.MaxRetries = defaultMaxRetries
	}

	c.VerifyRecSplit = flags.VerifyRecSplit

	// Logging overrides: only replace TOML values when the operator
	// actually passed a value. Empty string means "no CLI override".
	if flags.LogLevel != "" {
		c.Logging.Level = flags.LogLevel
	}
	if flags.LogFormat != "" {
		c.Logging.Format = flags.LogFormat
	}
}

// BuildGeometry returns a geometry.Geometry wired to this config's
// CHUNKS_PER_TXHASH_INDEX. ChunkSize remains the global geometry constant
// (10_000 — a layout invariant, never operator-configurable per the design
// doc). RangeSize is derived as ChunkSize * ChunksPerTxHashIndex so a
// non-default operator value produces self-consistent range boundaries.
func (c *Config) BuildGeometry() geometry.Geometry {
	cpi := c.Backfill.ChunksPerTxHashIndex
	if cpi == 0 {
		cpi = defaultChunksPerTxHashIndex
	}
	return geometry.Geometry{
		RangeSize:            cpi * geometry.ChunkSize,
		ChunkSize:            geometry.ChunkSize,
		ChunksPerTxHashIndex: cpi,
	}
}
