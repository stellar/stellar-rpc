// Package backfill implements the offline backfill pipeline for historical
// Stellar ledger data. It ingests ledgers from GCS (via BufferedStorageBackend),
// writes LFS chunk files, raw txhash .bin files, and events cold segments,
// then builds RecSplit indexes from the .bin files.
//
// All interfaces are defined here to enable dependency injection and testability.
package backfill

import (
	"context"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Ledger Source
// =============================================================================

// LedgerSource abstracts the backend that provides ledger data.
type LedgerSource interface {
	GetLedger(ctx context.Context, ledgerSeq uint32) (xdr.LedgerCloseMeta, error)
	PrepareRange(ctx context.Context, startSeq, endSeq uint32) error
	Close() error
}

// LedgerSourceFactory creates LedgerSource for a given ledger range.
type LedgerSourceFactory interface {
	Create(ctx context.Context, startLedger, endLedger uint32) (LedgerSource, error)
}

// =============================================================================
// Backfill Meta Store
// =============================================================================

// BackfillMetaStore tracks chunk and index state in a durable store
// (RocksDB with WAL always on). It is the single source of truth for crash
// recovery — all resume decisions are made by reading flags from this store.
//
// Key hierarchy:
//
//	chunk:{C:08d}:lfs         → "1"
//	chunk:{C:08d}:txhash      → "1"
//	chunk:{C:08d}:events      → "1"
//	index:{N:08d}:txhash      → "1"
//
// Each flag is written independently after its output's fsync. No WriteBatch.
type BackfillMetaStore interface {
	// Individual flag setters — each writes a single Put after fsync.
	SetChunkLFS(chunkID uint32) error
	SetChunkTxHash(chunkID uint32) error
	SetChunkEvents(chunkID uint32) error

	// Flag readers.
	IsChunkLFSDone(chunkID uint32) (bool, error)
	IsChunkTxHashDone(chunkID uint32) (bool, error)
	IsChunkEventsDone(chunkID uint32) (bool, error)

	// DeleteChunkTxHashKey deletes chunk:{C}:txhash — called by cleanup_txhash.
	DeleteChunkTxHashKey(chunkID uint32) error

	// SetIndexTxHash sets index:{N}:txhash = "1" after all CFs are built.
	SetIndexTxHash(indexID uint32) error

	// IsIndexTxHashDone checks whether index:{N}:txhash = "1".
	IsIndexTxHashDone(indexID uint32) (bool, error)

	// Generic key-value for config persistence (e.g. chunks_per_txhash_index).
	Get(key string) (string, error)
	Put(key, value string) error

	// Close releases all resources.
	Close()
}

// =============================================================================
// TxHash Index Builder
// =============================================================================

// TxHashIndexBuilder builds the RecSplit txhash index for a completed index group.
type TxHashIndexBuilder interface {
	Build(ctx context.Context, indexID uint32) error
}

// =============================================================================
// LFS Writer
// =============================================================================

// LFSWriter writes compressed LCM records to an LFS chunk file.
type LFSWriter interface {
	AppendLedger(lcm xdr.LedgerCloseMeta) (time.Duration, error)
	FsyncAndClose() (time.Duration, error)
	Abort()
	DataBytesWritten() int64
}

// =============================================================================
// Events Writer
// =============================================================================

// EventsWriter writes event data for a chunk to an immutable events cold segment.
// The chunk_writer calls extract_events(lcm) and passes the result here.
type EventsWriter interface {
	AppendEvents(ledgerSeq uint32, events []xdr.ContractEvent) error
	FsyncAndClose() (time.Duration, error)
	Abort()
}

// =============================================================================
// TxHash Entry
// =============================================================================

// TxHashEntry represents a single transaction hash with its ledger sequence.
// On-disk format (36 bytes, no header):
//
//	[txhash:32 bytes][ledgerSeq:4 bytes big-endian]
type TxHashEntry struct {
	TxHash    [32]byte
	LedgerSeq uint32
}
