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
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
)

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

// BackfillMetaStore tracks chunk and index state in a durable store
// (RocksDB with WAL always on). It is the single source of truth for crash
// recovery — all resume decisions are made by reading flags from this store.
//
// Key hierarchy:
//
//	chunk:{C:06d}:lfs         → "1"
//	chunk:{C:06d}:txhash      → "1"
//	index:{N:04d}:txhashindex → "1"
//
// All flag writes happen AFTER the corresponding file has been fsynced to disk.
// This ordering guarantees that a flag being present implies the file is durable.
type BackfillMetaStore interface {
	// SetChunkFlags atomically sets both lfs and txhash flags for a chunk.
	// MUST only be called AFTER both LFS files and txhash file are fsynced.
	SetChunkFlags(chunkID uint32) error

	// IsChunkLFSDone checks whether chunk:{C}:lfs = "1".
	IsChunkLFSDone(chunkID uint32) (bool, error)

	// IsChunkTxHashDone checks whether chunk:{C}:txhash = "1".
	IsChunkTxHashDone(chunkID uint32) (bool, error)

	// DeleteChunkTxHashKey deletes chunk:{C}:txhash — called by cleanup_txhash.
	DeleteChunkTxHashKey(chunkID uint32) error

	// SetIndexTxHashIndex sets index:{N}:txhashindex = "1" after all CFs are built.
	SetIndexTxHashIndex(indexID uint32) error

	// IsIndexTxHashIndexDone checks whether index:{N}:txhashindex = "1".
	IsIndexTxHashIndexDone(indexID uint32) (bool, error)

	// ScanIndexChunkFlags reads lfs+txhash flags for all chunks in an index group.
	// Returns a map from chunkID to its status. Used by BuildSkipSet during
	// crash recovery to determine which chunks need re-ingestion.
	ScanIndexChunkFlags(indexID uint32, geo geometry.Geometry) (map[uint32]ChunkStatus, error)

	// AllIndexIDs returns all index IDs that have an index:N:txhashindex key.
	// Used by the reconciler at startup to discover indexes that need attention.
	AllIndexIDs() ([]uint32, error)

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
