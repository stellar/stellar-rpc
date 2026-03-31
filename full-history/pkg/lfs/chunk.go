// Package lfs provides utilities for reading from Local Filesystem Ledger Stores.
//
// The LFS format stores ledgers in chunk pack files:
//   - Each chunk contains exactly 10,000 ledgers
//   - Chunk N contains ledger sequences: (N * 10000) + 2 to ((N + 1) * 10000) + 1
//   - Each chunk is a single .pack file using the packfile library
//
// Directory Layout (type-separated with bucket directories):
//
//	{ledgersPath}/{bucketID:05d}/{chunkID:08d}.pack
//
// bucket_id = chunk_id / 1000 (hardcoded), formatted as %05d.
// All chunk IDs use uniform %08d zero-padding.
package lfs

import (
	"fmt"
	"os"
	"path/filepath"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// ChunkSize is the fixed number of ledgers per chunk
	ChunkSize = 10000

	// FirstLedgerSequence is the first ledger in the Stellar blockchain
	FirstLedgerSequence = 2

	// BucketSize is the number of chunks per bucket directory
	BucketSize = 1000
)

// =============================================================================
// Chunk ID / Ledger Sequence Calculations
// =============================================================================

// LedgerToChunkID returns the chunk ID for a given ledger sequence.
func LedgerToChunkID(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedgerSequence) / ChunkSize
}

// ChunkFirstLedger returns the first ledger sequence in a chunk.
func ChunkFirstLedger(chunkID uint32) uint32 {
	return (chunkID * ChunkSize) + FirstLedgerSequence
}

// ChunkLastLedger returns the last ledger sequence in a chunk.
func ChunkLastLedger(chunkID uint32) uint32 {
	return ((chunkID + 1) * ChunkSize) + FirstLedgerSequence - 1
}

// LedgerToLocalIndex returns the local index within a chunk for a given ledger.
func LedgerToLocalIndex(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedgerSequence) % ChunkSize
}

// BucketID returns the bucket directory ID for a chunk.
func BucketID(chunkID uint32) uint32 {
	return chunkID / BucketSize
}

// =============================================================================
// File Path Functions — Type-Separated Layout with Bucket Directories
// =============================================================================

// GetBucketDir returns the bucket directory containing a chunk's .pack file.
//
// Example: GetBucketDir("/mnt/nvme/ledgers", 42) → "/mnt/nvme/ledgers/00000"
func GetBucketDir(ledgersPath string, chunkID uint32) string {
	return filepath.Join(ledgersPath, fmt.Sprintf("%05d", BucketID(chunkID)))
}

// GetPackPath returns the pack file path for a chunk.
//
// Example: GetPackPath("/mnt/nvme/ledgers", 42) → "/mnt/nvme/ledgers/00000/00000042.pack"
func GetPackPath(ledgersPath string, chunkID uint32) string {
	return filepath.Join(ledgersPath, fmt.Sprintf("%05d", BucketID(chunkID)), fmt.Sprintf("%08d.pack", chunkID))
}

// ChunkExists checks if a chunk's .pack file exists.
func ChunkExists(ledgersPath string, chunkID uint32) bool {
	packPath := GetPackPath(ledgersPath, chunkID)
	_, err := os.Stat(packPath)
	return err == nil
}

// =============================================================================
// DEPRECATED — Index-First Layout Functions
// =============================================================================
// These are kept temporarily for any remaining callers during migration.

// GetChunkDir returns the ledgers directory for an index (DEPRECATED).
func GetChunkDir(immutableBase string, indexID uint32) string {
	return filepath.Join(immutableBase, fmt.Sprintf("index-%08d", indexID), "ledgers")
}

// GetDataPath returns the data file path for a chunk (DEPRECATED).
func GetDataPath(immutableBase string, indexID, chunkID uint32) string {
	return filepath.Join(GetChunkDir(immutableBase, indexID), fmt.Sprintf("%08d.data", chunkID))
}

// GetIndexPath returns the index file path for a chunk (DEPRECATED).
func GetIndexPath(immutableBase string, indexID, chunkID uint32) string {
	return filepath.Join(GetChunkDir(immutableBase, indexID), fmt.Sprintf("%08d.index", chunkID))
}
