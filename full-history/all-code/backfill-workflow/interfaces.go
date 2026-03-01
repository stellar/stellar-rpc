// Package backfill implements the offline backfill pipeline for historical
// Stellar ledger data. It ingests ledgers from GCS (via BufferedStorageBackend)
// or CaptiveStellarCore, writes LFS chunk files and raw txhash .bin files,
// then builds RecSplit indexes from the .bin files.
//
// All interfaces are defined here to enable dependency injection and testability.
// Every component in the pipeline depends on these interfaces, never on concrete
// implementations — allowing pure-logic unit tests with map-backed mocks.
package backfill

import (
	"context"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Logger
// =============================================================================

// Logger provides scoped, dual-output logging. Info messages go to the main log
// file, error messages go to both the main log and a dedicated error file.
//
// Scopes nest via WithScope: calling WithScope("RANGE") on a logger scoped to
// "BACKFILL" produces "[BACKFILL:RANGE]" prefixes. This gives each component
// a distinct, traceable identity in log output.
type Logger interface {
	// Info logs an informational message. Format string uses fmt.Sprintf semantics.
	Info(format string, args ...interface{})

	// Error logs an error message to both the main log and the error log.
	Error(format string, args ...interface{})

	// Separator logs a visual separator line for readability.
	Separator()

	// Sync flushes any buffered log data to disk.
	Sync()

	// Close flushes and closes all underlying file handles.
	Close()

	// WithScope returns a new Logger with an appended scope prefix.
	// Example: logger.WithScope("RANGE").WithScope("0000") → "[BACKFILL:RANGE:0000]"
	WithScope(scope string) Logger
}

// =============================================================================
// Memory Monitor
// =============================================================================

// MemoryMonitor tracks process RSS (Resident Set Size) and logs warnings when
// memory usage exceeds a configured threshold. It is checked at key points:
//   - After each chunk completion (every 10K ledgers)
//   - After each BSB instance completes its chunk slice
//   - During RecSplit build (after each CF index)
//   - In the 1-minute progress ticker
type MemoryMonitor interface {
	// Check reads current RSS and logs a warning if it exceeds the threshold.
	// Returns current RSS in bytes.
	Check() int64

	// CurrentRSSGB returns the current RSS in gigabytes.
	CurrentRSSGB() float64

	// PeakRSSGB returns the peak RSS observed since monitor creation.
	PeakRSSGB() float64

	// LogSummary logs a final memory usage summary.
	LogSummary(log Logger)

	// Stop halts any background monitoring goroutines.
	Stop()
}

// =============================================================================
// Ledger Source
// =============================================================================

// LedgerSource abstracts the backend that provides ledger data. Implementations
// include BufferedStorageBackend (GCS) and CaptiveStellarCore (local replay).
//
// Each BSB instance creates its own LedgerSource for its sub-range of chunks.
// The source is closed when the instance finishes all its chunks.
type LedgerSource interface {
	// GetLedger fetches a single ledger by sequence number.
	// Returns the full LedgerCloseMeta for processing.
	GetLedger(ctx context.Context, ledgerSeq uint32) (xdr.LedgerCloseMeta, error)

	// PrepareRange hints the backend to prefetch ledgers in [startSeq, endSeq].
	// For BSB, this configures the GCS streaming range.
	// For CaptiveStellarCore, this triggers catchup.
	//
	// On restart after a crash, the range is narrowed to only the non-skipped
	// chunks' ledger bounds — avoiding GCS bandwidth waste for already-ingested data.
	PrepareRange(ctx context.Context, startSeq, endSeq uint32) error

	// Close releases all resources held by this source.
	Close() error
}

// LedgerSourceFactory creates LedgerSource instances for a given sub-range.
// The factory holds shared configuration (bucket path, worker counts, etc.)
// and produces per-instance sources with specific ledger bounds.
type LedgerSourceFactory interface {
	// Create returns a new LedgerSource configured for [startLedger, endLedger].
	// Each BSB instance calls this once with its effective (non-skipped) range.
	Create(ctx context.Context, startLedger, endLedger uint32) (LedgerSource, error)
}

// =============================================================================
// Chunk Status
// =============================================================================

// ChunkStatus represents the completion state of a single chunk's files.
// Both LFS and TxHash flags must be set for the chunk to be considered complete.
//
// These flags are set atomically via a single RocksDB WriteBatch AFTER both
// the LFS files (.data + .index) and the txhash .bin file have been fsynced.
// If EITHER flag is absent, the chunk is treated as incomplete and both files
// are fully rewritten on restart — no partial reuse.
type ChunkStatus struct {
	LFSDone    bool // True if lfs_done flag is "1" in meta store
	TxHashDone bool // True if txhash_done flag is "1" in meta store
}

// IsComplete returns true only if both the LFS and TxHash files are confirmed
// durable on disk. This is the sole criterion for including a chunk in the
// skip-set during crash recovery.
func (cs ChunkStatus) IsComplete() bool { return cs.LFSDone && cs.TxHashDone }

// =============================================================================
// Backfill Meta Store
// =============================================================================

// BackfillMetaStore tracks range, chunk, and RecSplit state in a durable store
// (RocksDB with WAL always on). It is the single source of truth for crash
// recovery — all resume decisions are made by reading flags from this store.
//
// Key hierarchy:
//
//	range:{N:04d}:state                     → "INGESTING" | "RECSPLIT_BUILDING" | "COMPLETE"
//	range:{N:04d}:chunk:{C:06d}:lfs_done   → "1"
//	range:{N:04d}:chunk:{C:06d}:txhash_done → "1"
//	range:{N:04d}:recsplit:cf:{XX:02x}:done → "1"
//
// All flag writes happen AFTER the corresponding file has been fsynced to disk.
// This ordering guarantees that a flag being present implies the file is durable.
type BackfillMetaStore interface {
	// GetRangeState returns the current state string for a range.
	// Returns empty string if the range has no state yet.
	GetRangeState(rangeID uint32) (string, error)

	// SetRangeState sets the state for a range (e.g., "INGESTING", "RECSPLIT_BUILDING", "COMPLETE").
	SetRangeState(rangeID uint32, state string) error

	// IsChunkComplete returns true if both lfs_done and txhash_done flags are "1".
	// This is equivalent to checking ChunkStatus.IsComplete() but as a single call.
	IsChunkComplete(rangeID, chunkID uint32) (bool, error)

	// SetChunkComplete atomically marks both lfs_done and txhash_done flags
	// for the given chunk using a single RocksDB WriteBatch.
	//
	// INVARIANT: This MUST only be called AFTER both the LFS files (.data + .index)
	// and the txhash .bin file have been fsynced to durable storage. Calling this
	// before fsync would violate crash recovery guarantees — on restart, the chunk
	// would appear complete but its files might be partial/corrupt.
	//
	// The atomic WriteBatch ensures both flags are set together or neither is —
	// there is no crash window where one flag is set without the other.
	SetChunkComplete(rangeID, chunkID uint32) error

	// ScanChunkFlags reads all chunk flag pairs for a range (O(1000) scan).
	// Returns a map from chunkID to its status. Used by BuildSkipSet during
	// crash recovery to determine which chunks need re-ingestion.
	//
	// The scan does NOT assume contiguity — chunks may be completed in any order
	// by concurrent BSB instances.
	ScanChunkFlags(rangeID uint32) (map[uint32]ChunkStatus, error)

	// IsRecSplitCFDone returns true if the RecSplit index for a specific CF
	// (column family / hash nibble partition) has been built and its done flag set.
	IsRecSplitCFDone(rangeID uint32, cfIndex int) (bool, error)

	// SetRecSplitCFDone marks a single CF's RecSplit index as complete.
	// Called AFTER the index file has been fsynced to disk.
	SetRecSplitCFDone(rangeID uint32, cfIndex int) error

	// AllRangeIDs returns all range IDs that have any state in the meta store.
	// Used by the reconciler at startup to discover ranges that need attention.
	AllRangeIDs() ([]uint32, error)

	// Close releases all resources (closes the RocksDB database).
	Close()
}

// =============================================================================
// TxHash Entry
// =============================================================================

// TxHashEntry represents a single transaction hash with its ledger sequence.
// This is the unit of data written to raw .bin files during backfill ingestion
// and read back during RecSplit index building.
//
// On-disk format (36 bytes, no header):
//
//	[txhash:32 bytes][ledgerSeq:4 bytes big-endian]
type TxHashEntry struct {
	TxHash    [32]byte // SHA-256 hash of the transaction envelope
	LedgerSeq uint32   // Ledger sequence containing this transaction
}
