// Package lfs provides utilities for reading from Local Filesystem Ledger Stores.
//
// The LFS format stores ledgers in chunk pack files:
//   - Each chunk contains exactly 10,000 ledgers
//   - Chunk N contains ledger sequences: (N * 10000) + 2 to ((N + 1) * 10000) + 1
//   - Each chunk is a single .pack file using the packfile library
//
// Directory Layout (index-first):
//
//	{immutableBase}/index-{indexID:08d}/ledgers/{chunkID:08d}.pack
//
// All IDs use uniform %08d zero-padding.
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
// LFS chunk files are stored as single .pack files under index directories:
//
//	{immutableBase}/index-{indexID:08d}/ledgers/{chunkID:08d}.pack
//
// All IDs use uniform %08d zero-padding.

// GetChunkDir returns the ledgers directory for an index.
//
// Example: GetChunkDir("/data/immutable", 0) → "/data/immutable/index-00000000/ledgers"
func GetChunkDir(immutableBase string, indexID uint32) string {
	return filepath.Join(immutableBase, fmt.Sprintf("index-%08d", indexID), "ledgers")
}

// GetPackPath returns the pack file path for a chunk.
//
// Example: GetPackPath("/data/immutable", 0, 42) → "/data/immutable/index-00000000/ledgers/00000042.pack"
func GetPackPath(immutableBase string, indexID, chunkID uint32) string {
	return filepath.Join(GetChunkDir(immutableBase, indexID), fmt.Sprintf("%08d.pack", chunkID))
}

// GetDataPath returns the data file path for a chunk.
// DEPRECATED: Use GetPackPath instead. Kept for iterator compatibility during migration.
func GetDataPath(immutableBase string, indexID, chunkID uint32) string {
	return filepath.Join(GetChunkDir(immutableBase, indexID), fmt.Sprintf("%08d.data", chunkID))
}

// GetIndexPath returns the index file path for a chunk.
// DEPRECATED: Use GetPackPath instead. Kept for iterator compatibility during migration.
func GetIndexPath(immutableBase string, indexID, chunkID uint32) string {
	return filepath.Join(GetChunkDir(immutableBase, indexID), fmt.Sprintf("%08d.index", chunkID))
}

// ChunkExists checks if a chunk's .pack file exists.
func ChunkExists(immutableBase string, indexID, chunkID uint32) bool {
	packPath := GetPackPath(immutableBase, indexID, chunkID)
	_, err := os.Stat(packPath)
	return err == nil
}
