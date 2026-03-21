package backfill

import (
	"fmt"
	"path/filepath"
)

// =============================================================================
// Path Construction — Index-First Layout
// =============================================================================
//
// All immutable data is organized under index directories:
//
//	{immutableBase}/index-{indexID:08d}/ledgers/{chunkID:08d}.pack
//	{immutableBase}/index-{indexID:08d}/txhash/raw/{chunkID:08d}.bin
//	{immutableBase}/index-{indexID:08d}/txhash/index/cf-{nibble}.idx
//	{immutableBase}/index-{indexID:08d}/events/{chunkID:08d}/events.pack
//
// All IDs use uniform %08d zero-padding.
//
// Pruning an index = rm -rf index-NNNNNNNN/

// IndexDir returns the top-level directory for an index.
//
// Example: IndexDir("/data/immutable", 0) → "/data/immutable/index-00000000"
func IndexDir(immutableBase string, indexID uint32) string {
	return filepath.Join(immutableBase, fmt.Sprintf("index-%08d", indexID))
}

// --- Ledger paths ---

// LedgerPackPath returns the path to a ledger pack file.
//
// Example: LedgerPackPath("/data/immutable", 0, 42) → "/data/immutable/index-00000000/ledgers/00000042.pack"
func LedgerPackPath(immutableBase string, indexID, chunkID uint32) string {
	return filepath.Join(IndexDir(immutableBase, indexID), "ledgers", fmt.Sprintf("%08d.pack", chunkID))
}

// --- TxHash paths ---

// RawTxHashDir returns the directory containing raw .bin files for an index.
//
// Example: RawTxHashDir("/data/immutable", 0) → "/data/immutable/index-00000000/txhash/raw"
func RawTxHashDir(immutableBase string, indexID uint32) string {
	return filepath.Join(IndexDir(immutableBase, indexID), "txhash", "raw")
}

// RawTxHashPath returns the full path to a raw txhash .bin file.
//
// Example: RawTxHashPath("/data/immutable", 0, 42) → "/data/immutable/index-00000000/txhash/raw/00000042.bin"
func RawTxHashPath(immutableBase string, indexID, chunkID uint32) string {
	return filepath.Join(RawTxHashDir(immutableBase, indexID), fmt.Sprintf("%08d.bin", chunkID))
}

// RecSplitTmpDir returns the temporary directory used during RecSplit builds.
//
// Example: RecSplitTmpDir("/data/immutable", 0) → "/data/immutable/index-00000000/txhash/tmp"
func RecSplitTmpDir(immutableBase string, indexID uint32) string {
	return filepath.Join(IndexDir(immutableBase, indexID), "txhash", "tmp")
}

// RecSplitCFTmpDir returns the per-CF temporary directory used during a RecSplit build.
//
// Example: RecSplitCFTmpDir("/data/immutable", 0, "a") → "/data/immutable/index-00000000/txhash/tmp/cf-a"
func RecSplitCFTmpDir(immutableBase string, indexID uint32, cfName string) string {
	return filepath.Join(RecSplitTmpDir(immutableBase, indexID), "cf-"+cfName)
}

// RecSplitIndexDir returns the directory containing RecSplit index files.
//
// Example: RecSplitIndexDir("/data/immutable", 0) → "/data/immutable/index-00000000/txhash/index"
func RecSplitIndexDir(immutableBase string, indexID uint32) string {
	return filepath.Join(IndexDir(immutableBase, indexID), "txhash", "index")
}

// RecSplitIndexPath returns the full path to a RecSplit index file for a specific CF.
//
// Example: RecSplitIndexPath("/data/immutable", 0, "a") → "/data/immutable/index-00000000/txhash/index/cf-a.idx"
func RecSplitIndexPath(immutableBase string, indexID uint32, nibble string) string {
	return filepath.Join(RecSplitIndexDir(immutableBase, indexID), fmt.Sprintf("cf-%s.idx", nibble))
}

// --- Events paths ---

// EventsDir returns the directory for a chunk's events cold segment.
//
// Example: EventsDir("/data/immutable", 0, 42) → "/data/immutable/index-00000000/events/00000042"
func EventsDir(immutableBase string, indexID, chunkID uint32) string {
	return filepath.Join(IndexDir(immutableBase, indexID), "events", fmt.Sprintf("%08d", chunkID))
}
