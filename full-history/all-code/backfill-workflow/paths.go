package backfill

import (
	"fmt"
	"path/filepath"
)

// =============================================================================
// Path Construction — Type-Separated Layout with Bucket Directories
// =============================================================================
//
// Each data type has its own directory tree rooted at its immutable_storage.*.path:
//
//   {ledgers_path}/{bucketID:05d}/{chunkID:08d}.pack
//   {txhash_raw_path}/{bucketID:05d}/{chunkID:08d}.bin
//   {txhash_index_path}/{indexID:08d}/cf-{nibble}.idx
//   {events_path}/{bucketID:05d}/{chunkID:08d}-events.pack
//   {events_path}/{bucketID:05d}/{chunkID:08d}-index.pack
//   {events_path}/{bucketID:05d}/{chunkID:08d}-index.hash
//
// bucket_id = chunk_id / 1000 (hardcoded), formatted as %05d.
// All chunk/index IDs use uniform %08d zero-padding.

// BucketID returns the bucket directory ID for a chunk.
// bucket_id = chunk_id / 1000 (hardcoded filesystem grouping).
func BucketID(chunkID uint32) uint32 {
	return chunkID / 1000
}

// --- Ledger paths ---

// LedgerPackPath returns the path to a ledger pack file.
//
// Example: LedgerPackPath("/mnt/nvme/ledgers", 42) → "/mnt/nvme/ledgers/00000/00000042.pack"
func LedgerPackPath(basePath string, chunkID uint32) string {
	return filepath.Join(basePath, fmt.Sprintf("%05d", BucketID(chunkID)), fmt.Sprintf("%08d.pack", chunkID))
}

// --- TxHash raw paths ---

// RawTxHashDir returns the bucket directory containing raw .bin files.
//
// Example: RawTxHashDir("/mnt/nvme/txhash/raw", 42) → "/mnt/nvme/txhash/raw/00000"
func RawTxHashDir(basePath string, chunkID uint32) string {
	return filepath.Join(basePath, fmt.Sprintf("%05d", BucketID(chunkID)))
}

// RawTxHashPath returns the full path to a raw txhash .bin file.
//
// Example: RawTxHashPath("/mnt/nvme/txhash/raw", 42) → "/mnt/nvme/txhash/raw/00000/00000042.bin"
func RawTxHashPath(basePath string, chunkID uint32) string {
	return filepath.Join(basePath, fmt.Sprintf("%05d", BucketID(chunkID)), fmt.Sprintf("%08d.bin", chunkID))
}

// --- TxHash index paths ---

// RecSplitIndexDir returns the directory containing RecSplit index files for a txhash index.
//
// Example: RecSplitIndexDir("/mnt/nvme/txhash/index", 0) → "/mnt/nvme/txhash/index/00000000"
func RecSplitIndexDir(basePath string, indexID uint32) string {
	return filepath.Join(basePath, fmt.Sprintf("%08d", indexID))
}

// RecSplitTmpDir returns the temporary directory used during RecSplit builds.
//
// Example: RecSplitTmpDir("/mnt/nvme/txhash/index", 0) → "/mnt/nvme/txhash/index/00000000/tmp"
func RecSplitTmpDir(basePath string, indexID uint32) string {
	return filepath.Join(RecSplitIndexDir(basePath, indexID), "tmp")
}

// RecSplitCFTmpDir returns the per-CF temporary directory used during a RecSplit build.
//
// Example: RecSplitCFTmpDir("/mnt/nvme/txhash/index", 0, "a") → "/mnt/nvme/txhash/index/00000000/tmp/cf-a"
func RecSplitCFTmpDir(basePath string, indexID uint32, cfName string) string {
	return filepath.Join(RecSplitTmpDir(basePath, indexID), "cf-"+cfName)
}

// RecSplitIndexPath returns the full path to a RecSplit index file for a specific CF.
//
// Example: RecSplitIndexPath("/mnt/nvme/txhash/index", 0, "a") → "/mnt/nvme/txhash/index/00000000/cf-a.idx"
func RecSplitIndexPath(basePath string, indexID uint32, nibble string) string {
	return filepath.Join(RecSplitIndexDir(basePath, indexID), fmt.Sprintf("cf-%s.idx", nibble))
}

// StreamHashIndexPath returns the path to the single StreamHash txhash index file.
//
// Example: StreamHashIndexPath("/mnt/nvme/txhash/index", 0) → "/mnt/nvme/txhash/index/00000000/txhash.idx"
func StreamHashIndexPath(basePath string, indexID uint32) string {
	return filepath.Join(RecSplitIndexDir(basePath, indexID), "txhash.idx")
}

// --- Events paths ---

// EventsDataPath returns the path to a chunk's events data pack file.
//
// Example: EventsDataPath("/mnt/nvme/events", 42) → "/mnt/nvme/events/00000/00000042-events.pack"
func EventsDataPath(basePath string, chunkID uint32) string {
	return filepath.Join(basePath, fmt.Sprintf("%05d", BucketID(chunkID)), fmt.Sprintf("%08d-events.pack", chunkID))
}

// EventsIndexPath returns the path to a chunk's events bitmap index file.
//
// Example: EventsIndexPath("/mnt/nvme/events", 42) → "/mnt/nvme/events/00000/00000042-index.pack"
func EventsIndexPath(basePath string, chunkID uint32) string {
	return filepath.Join(basePath, fmt.Sprintf("%05d", BucketID(chunkID)), fmt.Sprintf("%08d-index.pack", chunkID))
}

// EventsHashPath returns the path to a chunk's events MPHF hash file.
//
// Example: EventsHashPath("/mnt/nvme/events", 42) → "/mnt/nvme/events/00000/00000042-index.hash"
func EventsHashPath(basePath string, chunkID uint32) string {
	return filepath.Join(basePath, fmt.Sprintf("%05d", BucketID(chunkID)), fmt.Sprintf("%08d-index.hash", chunkID))
}
