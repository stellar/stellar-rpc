package backfill

import (
	"fmt"
	"path/filepath"
)

// =============================================================================
// Path Construction for TxHash Files
// =============================================================================
//
// Raw txhash .bin files and RecSplit index files are stored under:
//
//	{txhashBase}/{rangeID:04d}/raw/{chunkID:06d}.bin     ← raw txhash entries
//	{txhashBase}/{rangeID:04d}/index/cf-{nibble}.idx     ← RecSplit index per CF
//
// The raw/ directory is deleted after all 16 RecSplit indexes are built,
// freeing ~95 GB per range. The index/ directory is permanent.
//
// Directory hierarchy:
//
//	{txhashBase}/
//	├── 0000/
//	│   ├── raw/
//	│   │   ├── 000000.bin
//	│   │   ├── 000001.bin
//	│   │   └── ... (1000 files per range)
//	│   └── index/
//	│       ├── cf-0.idx
//	│       ├── cf-1.idx
//	│       └── ... (16 files per range)
//	├── 0001/
//	│   ├── raw/
//	│   └── index/
//	└── ...

// RawTxHashDir returns the directory containing raw .bin files for a range.
//
// Example: RawTxHashDir("/data/txhash", 0) → "/data/txhash/0000/raw"
func RawTxHashDir(txhashBase string, rangeID uint32) string {
	return filepath.Join(txhashBase, fmt.Sprintf("%04d", rangeID), "raw")
}

// RawTxHashPath returns the full path to a raw txhash .bin file.
//
// Example: RawTxHashPath("/data/txhash", 0, 350) → "/data/txhash/0000/raw/000350.bin"
func RawTxHashPath(txhashBase string, rangeID, chunkID uint32) string {
	return filepath.Join(RawTxHashDir(txhashBase, rangeID), fmt.Sprintf("%06d.bin", chunkID))
}

// RecSplitTmpDir returns the temporary directory used during RecSplit builds for a range.
// Each CF gets a subdirectory (e.g., tmp/cf-0). Cleaned up after all CFs complete.
//
// Example: RecSplitTmpDir("/data/txhash", 0) → "/data/txhash/0000/tmp"
func RecSplitTmpDir(txhashBase string, rangeID uint32) string {
	return filepath.Join(txhashBase, fmt.Sprintf("%04d", rangeID), "tmp")
}

// RecSplitCFTmpDir returns the per-CF temporary directory used during a RecSplit build.
//
// Example: RecSplitCFTmpDir("/data/txhash", 0, "a") → "/data/txhash/0000/tmp/cf-a"
func RecSplitCFTmpDir(txhashBase string, rangeID uint32, cfName string) string {
	return filepath.Join(RecSplitTmpDir(txhashBase, rangeID), "cf-"+cfName)
}

// RecSplitIndexDir returns the directory containing RecSplit index files for a range.
//
// Example: RecSplitIndexDir("/data/txhash", 0) → "/data/txhash/0000/index"
func RecSplitIndexDir(txhashBase string, rangeID uint32) string {
	return filepath.Join(txhashBase, fmt.Sprintf("%04d", rangeID), "index")
}

// RecSplitIndexPath returns the full path to a RecSplit index file for a specific CF.
// The nibble parameter is the lowercase hex string ("0" through "f").
//
// Example: RecSplitIndexPath("/data/txhash", 0, "a") → "/data/txhash/0000/index/cf-a.idx"
func RecSplitIndexPath(txhashBase string, rangeID uint32, nibble string) string {
	return filepath.Join(RecSplitIndexDir(txhashBase, rangeID), fmt.Sprintf("cf-%s.idx", nibble))
}
