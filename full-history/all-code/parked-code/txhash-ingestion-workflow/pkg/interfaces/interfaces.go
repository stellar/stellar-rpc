// =============================================================================
// pkg/interfaces/interfaces.go - Core Interfaces
// =============================================================================
//
// This package defines the core interfaces used throughout the txhash-ingestion-workflow.
// By coding to interfaces, we achieve:
//
//   1. TESTABILITY: Mock implementations can be injected for unit testing
//   2. DRY (Don't Repeat Yourself): Common patterns are abstracted once
//   3. SEPARATION OF CONCERNS: Data store vs meta store have distinct interfaces
//   4. FLEXIBILITY: Easy to swap implementations
//
// =============================================================================

package interfaces

import (
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/types"
)

// =============================================================================
// Iterator Interface
// =============================================================================

// Iterator abstracts RocksDB iteration for traversing key-value pairs.
//
// USAGE PATTERN:
//
//	iter := store.NewIteratorCF("0")
//	defer iter.Close()
//
//	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
//	    key := iter.Key()
//	    value := iter.Value()
//	    // process key-value
//	}
//
//	if err := iter.Error(); err != nil {
//	    // handle error
//	}
type Iterator interface {
	// SeekToFirst positions the iterator at the first key.
	SeekToFirst()

	// Valid returns true if the iterator is positioned at a valid key-value pair.
	Valid() bool

	// Next advances the iterator to the next key.
	Next()

	// Key returns the key at the current position.
	Key() []byte

	// Value returns the value at the current position.
	Value() []byte

	// Error returns any error encountered during iteration.
	Error() error

	// Close releases resources associated with the iterator.
	Close()
}

// =============================================================================
// TxHashStore Interface
// =============================================================================

// TxHashStore defines the interface for txHash->ledgerSeq storage operations.
//
// This interface abstracts the RocksDB store that holds the main data:
//   - 16 column families (0-f) partitioned by first hex char of txHash
//   - Each entry: 32-byte txHash -> 4-byte ledgerSeq
type TxHashStore interface {
	// WriteBatch writes entries to column families without holding a lock, returning timing info.
	WriteBatch(entriesByCF map[string][]types.Entry) (map[string]time.Duration, error)

	// Get retrieves the ledger sequence for a transaction hash.
	Get(txHash []byte) (value []byte, found bool, err error)

	// NewIteratorCF creates a new iterator for a specific column family.
	NewIteratorCF(cfName string) Iterator

	// NewScanIteratorCF creates a scan-optimized iterator for a specific column family.
	// This iterator is optimized for full sequential scans (e.g., counting entries):
	//   - Large readahead buffer for prefetching
	//   - Does not fill block cache (avoids cache pollution)
	//   - Auto-tunes readahead size
	NewScanIteratorCF(cfName string) Iterator

	// FlushAll flushes all column family MemTables to SST files on disk.
	FlushAll() error

	// CompactAll performs full compaction on all 16 column families.
	CompactAll() time.Duration

	// CompactCF compacts a single column family by name.
	CompactCF(cfName string) time.Duration

	// GetAllCFStats returns statistics for all 16 column families.
	GetAllCFStats() []types.CFStats

	// GetCFStats returns statistics for a specific column family.
	GetCFStats(cfName string) types.CFStats

	// Close releases all resources associated with the store.
	Close()

	// Path returns the filesystem path to the RocksDB store.
	Path() string
}

// =============================================================================
// MetaStore Interface
// =============================================================================

// MetaStore defines the interface for checkpoint and progress tracking.
//
// The meta store persists workflow state to enable crash recovery:
//   - Configuration (start/end ledger)
//   - Current phase
//   - Last committed ledger
//   - Per-CF entry counts
//   - Verification progress
type MetaStore interface {
	// GetStartLedger returns the configured start ledger.
	GetStartLedger() (uint32, error)

	// GetEndLedger returns the configured end ledger.
	GetEndLedger() (uint32, error)

	// SetConfig stores the start and end ledger configuration.
	SetConfig(startLedger, endLedger uint32) error

	// GetPhase returns the current workflow phase.
	GetPhase() (types.Phase, error)

	// SetPhase updates the current workflow phase.
	SetPhase(phase types.Phase) error

	// GetLastCommittedLedger returns the last fully committed ledger sequence.
	GetLastCommittedLedger() (uint32, error)

	// GetCFCounts returns the entry count for each column family.
	GetCFCounts() (map[string]uint64, error)

	// CommitBatchProgress atomically updates progress after a successful batch.
	CommitBatchProgress(lastLedger uint32, cfCounts map[string]uint64) error

	// GetVerifyCF returns the column family currently being verified.
	GetVerifyCF() (string, error)

	// SetVerifyCF updates the column family currently being verified.
	SetVerifyCF(cf string) error

	// Exists returns true if the meta store has been initialized.
	Exists() bool

	// LogState logs the current meta store state for debugging/monitoring.
	LogState(logger Logger)

	// Close releases all resources associated with the meta store.
	Close()
}

// =============================================================================
// Logger Interface
// =============================================================================

// Logger defines the interface for logging operations.
// Implementations write to log file and error file separately.
//
// SCOPING:
// Use WithScope() to create a child logger that prefixes messages with a scope name.
// This is useful for phase-specific logging:
//
//	ingestLog := logger.WithScope("INGEST")
//	ingestLog.Info("Processing...") // → [timestamp] [INGEST] Processing...
type Logger interface {
	// Info logs an informational message to the log file.
	Info(format string, args ...interface{})

	// Error logs an error message to the error file.
	Error(format string, args ...interface{})

	// Separator logs a visual separator line to the log file.
	Separator()

	// Sync forces a flush of all log buffers to disk.
	Sync()

	// Close closes all log files.
	Close()

	// WithScope creates a child logger that prefixes messages with the given scope.
	// Scopes can be nested: logger.WithScope("A").WithScope("B") → [A:B]
	WithScope(scope string) Logger
}

// =============================================================================
// MemoryMonitor Interface
// =============================================================================

// MemoryMonitor defines the interface for memory monitoring.
type MemoryMonitor interface {
	// Check reads current memory usage and logs a warning if threshold exceeded.
	Check() int64

	// CurrentRSSGB returns the current RSS in gigabytes.
	CurrentRSSGB() float64

	// PeakRSSGB returns the peak RSS observed in gigabytes.
	PeakRSSGB() float64

	// LogSummary logs a summary of memory usage.
	LogSummary(logger Logger)

	// Stop performs cleanup.
	Stop()
}
