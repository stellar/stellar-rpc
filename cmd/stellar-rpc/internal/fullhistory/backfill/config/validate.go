package config

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/geometry"
)

// ChunksPerTxHashIndexMetaKey is the meta-store key under which the
// first-run value of CHUNKS_PER_TXHASH_INDEX is persisted. Exported so the
// metastore facade in slice #689 (Layer-2) and any diagnostic tooling use
// the same string — redefining the literal in two places is exactly the
// corruption vector this key is protecting against.
const ChunksPerTxHashIndexMetaKey = "config:chunks_per_txhash_index"

// Default values for keys that the operator may omit. Grouped here so the
// single place a number lives is this file, and Validate / ApplyFlags pick
// them up by name rather than re-typing literals.
//
// The path-segment defaults (meta/rocksdb, ledgers, events, txhash/raw,
// txhash/index) are lowercase because they are filesystem directory names,
// not TOML keys — TOML-key case (UPPER_SNAKE_CASE) and filesystem-path case
// are separate concerns. See design-doc section "Directory Structure".
const (
	// defaultChunksPerTxHashIndex matches the design doc's recommended
	// default (10M ledgers per txhash index == 1000 chunks × 10K ledgers).
	defaultChunksPerTxHashIndex uint32 = 1000

	// defaultBSBBufferSize / defaultBSBNumWorkers match the design-doc
	// defaults for BSB when the operator does not override them.
	defaultBSBBufferSize int = 1000
	defaultBSBNumWorkers int = 20

	// Filesystem sub-directory conventions; joined onto DEFAULT_DATA_DIR
	// to produce default storage paths.
	defaultMetaStoreSubdir   = "meta/rocksdb"
	defaultLedgersSubdir     = "ledgers"
	defaultEventsSubdir      = "events"
	defaultTxHashRawSubdir   = "txhash/raw"
	defaultTxHashIndexSubdir = "txhash/index"
)

// Validate enforces the TOML-level constraints (required fields, shape of
// [BACKFILL.BSB]) and resolves default values for optional fields. It
// mutates the receiver in place; callers should treat a nil return as
// "config is now fully resolved and safe to consume."
//
// Validate is the TOML/struct-level half of pre-DAG validation. The CLI-flag
// half (ValidateFlags) and the meta-store immutability check
// (ValidateAgainstConfigStore) live alongside so the subcommand can call
// them in sequence.
//
// Rules enforced here (map to design-doc "Validation before DAG
// construction"):
//   - DEFAULT_DATA_DIR is required.
//   - [BACKFILL.BSB] is required and must have BUCKET_PATH.
//   - CHUNKS_PER_TXHASH_INDEX defaults to 1000 when 0.
//   - BSB BUFFER_SIZE / NUM_WORKERS default to 1000 / 20 when <= 0.
//   - Per-type storage paths default under DEFAULT_DATA_DIR when unset.
func (c *Config) Validate() error {
	// DEFAULT_DATA_DIR is the anchor for every filesystem-path default; if
	// it is missing, we cannot sensibly fall back to anything.
	if c.Service.DefaultDataDir == "" {
		return fmt.Errorf("[SERVICE].DEFAULT_DATA_DIR is required")
	}

	// BSB is required — backfill has no alternative ledger source. Pointer
	// nil-check distinguishes "section absent" from "section present but
	// empty" (latter is caught by BUCKET_PATH check below).
	if c.Backfill.BSB == nil {
		return fmt.Errorf("[BACKFILL.BSB] is required")
	}
	if c.Backfill.BSB.BucketPath == "" {
		return fmt.Errorf("[BACKFILL.BSB].BUCKET_PATH is required")
	}

	// Defaults for path keys the operator omitted. filepath.Join normalizes
	// separators for the host OS, which matters on Windows-authored TOML
	// imported onto Linux hosts (and vice versa).
	if c.MetaStore.Path == "" {
		c.MetaStore.Path = filepath.Join(c.Service.DefaultDataDir, defaultMetaStoreSubdir)
	}
	if c.ImmutableStorage.Ledgers.Path == "" {
		c.ImmutableStorage.Ledgers.Path = filepath.Join(c.Service.DefaultDataDir, defaultLedgersSubdir)
	}
	if c.ImmutableStorage.Events.Path == "" {
		c.ImmutableStorage.Events.Path = filepath.Join(c.Service.DefaultDataDir, defaultEventsSubdir)
	}
	if c.ImmutableStorage.TxHashRaw.Path == "" {
		c.ImmutableStorage.TxHashRaw.Path = filepath.Join(c.Service.DefaultDataDir, defaultTxHashRawSubdir)
	}
	if c.ImmutableStorage.TxHashIndex.Path == "" {
		c.ImmutableStorage.TxHashIndex.Path = filepath.Join(c.Service.DefaultDataDir, defaultTxHashIndexSubdir)
	}

	// Numeric defaults. 0 is the zero value TOML parse gives us when a key
	// is absent; treat it as "unset" and fill in the design-doc defaults.
	if c.Backfill.ChunksPerTxHashIndex == 0 {
		c.Backfill.ChunksPerTxHashIndex = defaultChunksPerTxHashIndex
	}
	if c.Backfill.BSB.BufferSize <= 0 {
		c.Backfill.BSB.BufferSize = defaultBSBBufferSize
	}
	if c.Backfill.BSB.NumWorkers <= 0 {
		c.Backfill.BSB.NumWorkers = defaultBSBNumWorkers
	}

	return nil
}

// ValidateFlags enforces the CLI-flag-level rules from design-doc section
// "Validation before DAG construction":
//   - --start-ledger >= FirstLedger (2)
//   - --end-ledger > --start-ledger
//   - --workers >= 1
//   - --max-retries >= 0
//
// Each error message names the offending flag so the operator can fix the
// invocation without having to grep validation source code. --verify-recsplit
// has no numeric constraint — cobra's bool flag type makes parse failures
// impossible, so there is no rule to enforce here.
func (c *Config) ValidateFlags(flags CLIFlags) error {
	if flags.StartLedger < geometry.FirstLedger {
		return fmt.Errorf("--start-ledger (%d) must be >= %d", flags.StartLedger, geometry.FirstLedger)
	}
	if flags.EndLedger <= flags.StartLedger {
		return fmt.Errorf("--end-ledger (%d) must be > --start-ledger (%d)", flags.EndLedger, flags.StartLedger)
	}
	if flags.Workers < 1 {
		return fmt.Errorf("--workers (%d) must be >= 1", flags.Workers)
	}
	if flags.MaxRetries < 0 {
		return fmt.Errorf("--max-retries (%d) must be >= 0", flags.MaxRetries)
	}
	return nil
}

// ValidateAgainstConfigStore enforces the design-doc rule that
// CHUNKS_PER_TXHASH_INDEX is layout-defining and must not change across
// runs. Three branches (first-run / match / mismatch) map directly to the
// design-doc section "Validation before DAG construction".
//
// Why string-valued (not integer): the meta-store key schema uses string
// values throughout (chunk-flag values are "1" — see design-doc Meta Store
// Keys). Staying string-typed at the interface boundary keeps the Layer-2
// facade in slice #689 from having to special-case one numeric key.
//
// Pre-condition: Validate has been called (so CHUNKS_PER_TXHASH_INDEX is
// already defaulted to 1000 if absent). A caller who skips Validate and
// passes a raw zero would persist "0" on first-run and lock the store to
// that value forever — the subcommand's call order guards against this.
func (c *Config) ValidateAgainstConfigStore(store ConfigStore) error {
	current := strconv.FormatUint(uint64(c.Backfill.ChunksPerTxHashIndex), 10)

	stored, found, err := store.Get(ChunksPerTxHashIndexMetaKey)
	if err != nil {
		return fmt.Errorf("read %s from config store: %w", ChunksPerTxHashIndexMetaKey, err)
	}

	// First-run branch: persist the current value as the seed. Subsequent
	// runs will compare against this value.
	if !found {
		if err := store.Put(ChunksPerTxHashIndexMetaKey, current); err != nil {
			return fmt.Errorf("seed %s to config store: %w", ChunksPerTxHashIndexMetaKey, err)
		}
		return nil
	}

	// Match branch: nothing to do.
	if stored == current {
		return nil
	}

	// Mismatch branch: hard abort. Operator must either revert
	// CHUNKS_PER_TXHASH_INDEX or start from a fresh data directory.
	return fmt.Errorf(
		"CHUNKS_PER_TXHASH_INDEX changed: config-store has %q, config TOML has %q — "+
			"this value is layout-defining and must not change across runs",
		stored, current,
	)
}

// ValidateAgainstBSB enforces the design-doc rule "expanded end must not
// exceed BSB-advertised max ledger". Returns:
//   - nil when the probe reports a max ledger >= EffectiveEndLedger;
//   - a wrapped probe error when the probe itself fails (operator can fix
//     credentials / connectivity and retry);
//   - a hard-abort error naming both values when the expanded range
//     out-runs BSB's advertised coverage.
//
// Pre-condition: ApplyFlags has been called so EffectiveEndLedger holds
// the widened range; calling ValidateAgainstBSB before ApplyFlags compares
// against a zero EffectiveEndLedger and always passes — misleading.
func (c *Config) ValidateAgainstBSB(probe BSBAvailabilityProbe) error {
	maxAvailable, err := probe.MaxAvailableLedger()
	if err != nil {
		return fmt.Errorf("probe BSB for max available ledger: %w", err)
	}
	if maxAvailable < c.EffectiveEndLedger {
		return fmt.Errorf(
			"BSB does not yet have enough history: expanded end is %d, "+
				"but BSB advertises max available ledger %d — "+
				"reduce --end-ledger or wait for more ledgers to land in BSB",
			c.EffectiveEndLedger, maxAvailable,
		)
	}
	return nil
}
