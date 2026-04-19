package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/geometry"
)

// ChunksPerTxHashIndexMetaKey is the Store key under which the first-run
// value of CHUNKS_PER_TXHASH_INDEX is seeded. Exported so the real
// metastore facade (#689) uses the exact same literal — the whole point
// of the immutability check is defeated if two sites disagree.
const ChunksPerTxHashIndexMetaKey = "config:chunks_per_txhash_index"

// Design-doc defaults — one source per value.
const (
	defaultChunksPerTxHashIndex uint32 = 1000
	defaultBSBBufferSize        int    = 1000
	defaultBSBNumWorkers        int    = 20

	defaultMetaStoreSubdir   = "meta/rocksdb"
	defaultLedgersSubdir     = "ledgers"
	defaultEventsSubdir      = "events"
	defaultTxHashRawSubdir   = "txhash/raw"
	defaultTxHashIndexSubdir = "txhash/index"
)

// Validate enforces the TOML-level rules (required fields, BSB shape) and
// fills in defaults for every optional key. Mutates c in place.
//
// Rules (from design-doc "Validation before DAG construction"):
//   - DEFAULT_DATA_DIR is required.
//   - [BACKFILL.BSB] is required with BUCKET_PATH.
//   - Numeric defaults: CHUNKS_PER_TXHASH_INDEX=1000, BUFFER_SIZE=1000, NUM_WORKERS=20.
//   - Path defaults under DEFAULT_DATA_DIR.
func (c *Config) Validate() error {
	if c.Service.DefaultDataDir == "" {
		return errors.New("[SERVICE].DEFAULT_DATA_DIR is required")
	}
	if c.Backfill.BSB == nil {
		return errors.New("[BACKFILL.BSB] is required")
	}
	if c.Backfill.BSB.BucketPath == "" {
		return errors.New("[BACKFILL.BSB].BUCKET_PATH is required")
	}

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

// ValidateFlags enforces the CLI-level rules:
//   - --start-ledger >= FirstLedger (2)
//   - --end-ledger > --start-ledger
//   - --workers >= 0 (0 is the documented sentinel for GOMAXPROCS; ApplyFlags resolves it)
//   - --max-retries >= 0
//
// Error messages name the offending flag so operators do not have to
// grep source.
func (c *Config) ValidateFlags(flags CLIFlags) error {
	if flags.StartLedger < geometry.FirstLedger {
		return fmt.Errorf("--start-ledger (%d) must be >= %d", flags.StartLedger, geometry.FirstLedger)
	}
	if flags.EndLedger <= flags.StartLedger {
		return fmt.Errorf("--end-ledger (%d) must be > --start-ledger (%d)", flags.EndLedger, flags.StartLedger)
	}
	// Workers is a signed int — cobra allows negative inputs. The 0
	// sentinel is documented ("0 = GOMAXPROCS"); anything below 0 is
	// an operator mistake.
	if flags.Workers < 0 {
		return fmt.Errorf("--workers (%d) must be >= 0 (0 means GOMAXPROCS)", flags.Workers)
	}
	if flags.MaxRetries < 0 {
		return fmt.Errorf("--max-retries (%d) must be >= 0", flags.MaxRetries)
	}
	return nil
}

// ValidateAgainstStore enforces CHUNKS_PER_TXHASH_INDEX immutability. The
// value gets stringified and stored under ChunksPerTxHashIndexMetaKey on
// first run; subsequent runs must match or the on-disk layout breaks.
//
// Pre-condition: Validate has run, so ChunksPerTxHashIndex is populated.
// Skipping Validate would seed "0" and permanently lock that value.
func (c *Config) ValidateAgainstStore(store Store) error {
	current := strconv.FormatUint(uint64(c.Backfill.ChunksPerTxHashIndex), 10)

	stored, found, err := store.Get(ChunksPerTxHashIndexMetaKey)
	if err != nil {
		return fmt.Errorf("read %s from store: %w", ChunksPerTxHashIndexMetaKey, err)
	}

	if !found {
		if err := store.Put(ChunksPerTxHashIndexMetaKey, current); err != nil {
			return fmt.Errorf("seed %s to store: %w", ChunksPerTxHashIndexMetaKey, err)
		}
		return nil
	}

	if stored == current {
		return nil
	}

	return fmt.Errorf(
		"CHUNKS_PER_TXHASH_INDEX changed: store has %q, config TOML has %q — "+
			"this value is layout-defining and must not change across runs",
		stored, current,
	)
}

// ValidateAgainstBSB enforces "expanded end must not exceed BSB max
// available". Requires ApplyFlags to have populated EffectiveEndLedger.
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
