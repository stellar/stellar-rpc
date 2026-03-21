// Package lfs provides utilities for reading from Local Filesystem Ledger Stores.
//
// The LFS format stores ledgers in chunk files:
//   - Each chunk contains exactly 10,000 ledgers
//   - Chunk N contains ledger sequences: (N * 10000) + 2 to ((N + 1) * 10000) + 1
//   - Each chunk has two files:
//   - NNNNNN.data: Concatenated zstd-compressed LCM records
//   - NNNNNN.index: Byte offsets into the data file
//
// Directory Layout:
//
//	<data_dir>/chunks/XXXX/YYYYYY.data
//	<data_dir>/chunks/XXXX/YYYYYY.index
//	Where: XXXX = chunk_id / 1000, YYYYYY = chunk_id
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

	// IndexHeaderSize is the size of the index file header in bytes
	IndexHeaderSize = 8

	// IndexVersion is the current index file format version
	IndexVersion = 1
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

// =============================================================================
// File Path Functions — Index-First Layout
// =============================================================================
//
// LFS chunk files are stored under index directories:
//
//	{immutableBase}/index-{indexID:08d}/ledgers/{chunkID:08d}.data
//	{immutableBase}/index-{indexID:08d}/ledgers/{chunkID:08d}.index
//
// All IDs use uniform %08d zero-padding.

// GetChunkDir returns the ledgers directory for an index.
//
// Example: GetChunkDir("/data/immutable", 0) → "/data/immutable/index-00000000/ledgers"
func GetChunkDir(immutableBase string, indexID uint32) string {
	return filepath.Join(immutableBase, fmt.Sprintf("index-%08d", indexID), "ledgers")
}

// GetDataPath returns the data file path for a chunk.
//
// Example: GetDataPath("/data/immutable", 0, 42) → "/data/immutable/index-00000000/ledgers/00000042.data"
func GetDataPath(immutableBase string, indexID, chunkID uint32) string {
	return filepath.Join(GetChunkDir(immutableBase, indexID), fmt.Sprintf("%08d.data", chunkID))
}

// GetIndexPath returns the index file path for a chunk.
//
// Example: GetIndexPath("/data/immutable", 0, 42) → "/data/immutable/index-00000000/ledgers/00000042.index"
func GetIndexPath(immutableBase string, indexID, chunkID uint32) string {
	return filepath.Join(GetChunkDir(immutableBase, indexID), fmt.Sprintf("%08d.index", chunkID))
}

// ChunkExists checks if a chunk already exists (both files present).
func ChunkExists(immutableBase string, indexID, chunkID uint32) bool {
	dataPath := GetDataPath(immutableBase, indexID, chunkID)
	indexPath := GetIndexPath(immutableBase, indexID, chunkID)

	_, dataErr := os.Stat(dataPath)
	_, indexErr := os.Stat(indexPath)

	return dataErr == nil && indexErr == nil
}
