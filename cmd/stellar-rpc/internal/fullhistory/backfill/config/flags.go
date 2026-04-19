package config

import (
	"runtime"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/geometry"
)

const defaultMaxRetries = 3

// CLIFlags carries every flag cmd.go hands to the subcommand body.
//
// Workers and MaxRetries use 0 as a sentinel for "apply default"; empty
// LogLevel / LogFormat mean "do not override the TOML value".
type CLIFlags struct {
	StartLedger    uint32
	EndLedger      uint32
	Workers        int
	MaxRetries     int
	VerifyRecSplit bool
	LogLevel       string
	LogFormat      string
}

// ApplyFlags merges CLI flag values into c: widens the ledger range to
// chunk boundaries (outward only — never narrows), resolves default
// values, and overrides TOML logging fields when the operator passed
// non-empty values.
//
// Precondition: Validate has run, so ChunksPerTxHashIndex is populated.
func (c *Config) ApplyFlags(flags CLIFlags) {
	geo := c.BuildGeometry()

	// Chunk-boundary expansion: start widens DOWN, end widens UP.
	// LedgerToChunkID's subtraction is safe only when the ledger is
	// >= FirstLedger; ValidateFlags is what finally rejects out-of-range
	// inputs, so guard here to avoid underflow on bogus values that
	// slip in before ValidateFlags runs.
	if flags.StartLedger >= geometry.FirstLedger {
		c.EffectiveStartLedger = geo.ChunkFirstLedger(geo.LedgerToChunkID(flags.StartLedger))
	} else {
		c.EffectiveStartLedger = flags.StartLedger
	}
	if flags.EndLedger >= geometry.FirstLedger {
		c.EffectiveEndLedger = geo.ChunkLastLedger(geo.LedgerToChunkID(flags.EndLedger))
	} else {
		c.EffectiveEndLedger = flags.EndLedger
	}

	c.Workers = flags.Workers
	if c.Workers <= 0 {
		c.Workers = runtime.GOMAXPROCS(0)
	}

	c.MaxRetries = flags.MaxRetries
	if c.MaxRetries <= 0 {
		c.MaxRetries = defaultMaxRetries
	}

	c.VerifyRecSplit = flags.VerifyRecSplit

	if flags.LogLevel != "" {
		c.Logging.Level = flags.LogLevel
	}
	if flags.LogFormat != "" {
		c.Logging.Format = flags.LogFormat
	}
}

// BuildGeometry returns a geometry.Geometry wired to this Config's
// CHUNKS_PER_TXHASH_INDEX. ChunkSize stays the package constant — it is
// a layout invariant, not operator-configurable.
func (c *Config) BuildGeometry() geometry.Geometry {
	cpi := c.Backfill.ChunksPerTxHashIndex
	if cpi == 0 {
		cpi = defaultChunksPerTxHashIndex
	}
	return geometry.Geometry{
		ChunkSize:            geometry.ChunkSize,
		ChunksPerTxHashIndex: cpi,
	}
}
