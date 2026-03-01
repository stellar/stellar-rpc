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
// File Path Functions
// =============================================================================

// GetChunkDir returns the directory path for a chunk.
func GetChunkDir(dataDir string, chunkID uint32) string {
	parentDir := chunkID / 1000
	return filepath.Join(dataDir, "chunks", fmt.Sprintf("%04d", parentDir))
}

// GetDataPath returns the data file path for a chunk.
func GetDataPath(dataDir string, chunkID uint32) string {
	return filepath.Join(GetChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.data", chunkID))
}

// GetIndexPath returns the index file path for a chunk.
func GetIndexPath(dataDir string, chunkID uint32) string {
	return filepath.Join(GetChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.index", chunkID))
}

// ChunkExists checks if a chunk already exists (both files present).
func ChunkExists(dataDir string, chunkID uint32) bool {
	dataPath := GetDataPath(dataDir, chunkID)
	indexPath := GetIndexPath(dataDir, chunkID)

	_, dataErr := os.Stat(dataPath)
	_, indexErr := os.Stat(indexPath)

	return dataErr == nil && indexErr == nil
}
