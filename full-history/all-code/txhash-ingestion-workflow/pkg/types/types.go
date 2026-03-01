// =============================================================================
// pkg/types/types.go - Core Data Types
// =============================================================================
//
// This package contains pure data types used throughout the txhash-ingestion-workflow.
// These types have no external dependencies beyond the standard library.
//
// =============================================================================

package types

import (
	"time"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// MB is megabytes in bytes
	MB = 1024 * 1024

	// GB is gigabytes in bytes
	GB = 1024 * 1024 * 1024

	// LedgersPerBatch is the number of ledgers processed before checkpointing.
	LedgersPerBatch = 1000

	// DefaultBlockCacheMB is the default block cache size in megabytes.
	DefaultBlockCacheMB = 8192

	// RAMWarningThresholdGB is the RSS threshold that triggers a warning.
	RAMWarningThresholdGB = 100

	// RecSplitBucketSize is the bucket size for RecSplit construction.
	RecSplitBucketSize = 2000

	// RecSplitLeafSize is the leaf size for RecSplit construction.
	RecSplitLeafSize = 8

	// RecSplitDataVersion is the data format version stored in the index.
	RecSplitDataVersion = 1

	// RecSplitLessFalsePositivesEnabled indicates if false positives are enabled.
	RecSplitLessFalsePositivesEnabled = true
)

// =============================================================================
// Phase Enum
// =============================================================================

// Phase represents the current workflow phase.
// Phases progress linearly: INGESTING -> COMPACTING -> BUILDING_RECSPLIT -> VERIFYING_RECSPLIT -> COMPLETE
//
// CRASH RECOVERY BEHAVIOR BY PHASE:
//
//	INGESTING:
//	  - Resume from last_committed_ledger + 1
//	  - Re-ingest up to 999 ledgers (duplicates are fine, compaction dedupes)
//	  - Counts from meta store are accurate for all committed batches
//
//	COMPACTING:
//	  - Restart compaction for ALL 16 CFs
//	  - Compaction is idempotent (re-compacting is safe)
//
//	BUILDING_RECSPLIT:
//	  - Delete all temp and index files
//	  - Rebuild all RecSplit indexes from scratch
//
//	VERIFYING_RECSPLIT:
//	  - Re-run verification for all 16 CFs (parallel, idempotent)
//
//	COMPLETE:
//	  - Log "Already complete" and exit 0
type Phase string

const (
	PhaseIngesting         Phase = "INGESTING"
	PhaseCompacting        Phase = "COMPACTING"
	PhaseBuildingRecsplit  Phase = "BUILDING_RECSPLIT"
	PhaseVerifyingRecsplit Phase = "VERIFYING_RECSPLIT"
	PhaseComplete          Phase = "COMPLETE"
)

// String returns the string representation of the phase.
func (p Phase) String() string {
	return string(p)
}

// =============================================================================
// Entry - Key-Value Pair for Batch Writes
// =============================================================================

// Entry represents a txHash -> ledgerSeq mapping.
//
// KEY FORMAT:
//   - 32 bytes: Transaction hash (raw bytes, not hex encoded)
//   - First byte's high nibble determines column family (0-f)
//
// VALUE FORMAT:
//   - 4 bytes: Ledger sequence number (big-endian uint32)
type Entry struct {
	Key   []byte // 32-byte transaction hash
	Value []byte // 4-byte ledger sequence (big-endian)
}

// =============================================================================
// CFStats - Per-Column Family Statistics
// =============================================================================

// CFLevelStats holds per-level statistics for a column family.
// RocksDB uses levels L0-L6 for its LSM tree structure.
type CFLevelStats struct {
	Level     int   // Level number (0-6)
	FileCount int64 // Number of SST files at this level
	Size      int64 // Total size in bytes at this level
}

// CFStats holds comprehensive statistics for a single column family.
type CFStats struct {
	Name          string         // Column family name ("0" through "f")
	EstimatedKeys int64          // RocksDB's estimate of key count
	TotalSize     int64          // Total size in bytes
	TotalFiles    int64          // Total SST file count
	LevelStats    []CFLevelStats // Per-level breakdown (L0-L6)
}

// =============================================================================
// LatencySummary - Computed Latency Statistics
// =============================================================================

// LatencySummary contains computed latency statistics.
type LatencySummary struct {
	Count  int           // Number of samples
	Min    time.Duration // Minimum latency
	Max    time.Duration // Maximum latency
	Avg    time.Duration // Average (mean) latency
	StdDev time.Duration // Standard deviation
	P50    time.Duration // 50th percentile (median)
	P90    time.Duration // 90th percentile
	P95    time.Duration // 95th percentile
	P99    time.Duration // 99th percentile
}

// =============================================================================
// RocksDBSettings - Tunable RocksDB Parameters
// =============================================================================

// RocksDBSettings contains tunable RocksDB parameters.
type RocksDBSettings struct {
	WriteBufferSizeMB           int
	MaxWriteBufferNumber        int
	MinWriteBufferNumberToMerge int
	TargetFileSizeMB            int
	MaxBackgroundJobs           int
	BloomFilterBitsPerKey       int
	BlockCacheSizeMB            int
	MaxOpenFiles                int

	// ReadOnly opens the database in read-only mode.
	// In read-only mode, write operations (WriteBatch, FlushAll, CompactAll, CompactCF) will panic.
	// Use this mode for tools that only need to read data (e.g., RecSplit building, verification).
	ReadOnly bool

	// DisableWAL disables the Write-Ahead Log for write operations.
	// WARNING: Setting this to true improves write throughput but sacrifices durability.
	// If the process crashes, any data written since the last flush/compaction will be lost.
	// Only use this when you can afford to re-ingest data on crash (e.g., bulk ingestion with checkpoints).
	DisableWAL bool
}

// DefaultRocksDBSettings returns the default RocksDB settings.
func DefaultRocksDBSettings() RocksDBSettings {
	return RocksDBSettings{
		WriteBufferSizeMB:           64,
		MaxWriteBufferNumber:        2,
		MinWriteBufferNumberToMerge: 1,
		TargetFileSizeMB:            256,
		MaxBackgroundJobs:           32,
		BloomFilterBitsPerKey:       12,
		BlockCacheSizeMB:            DefaultBlockCacheMB,
		MaxOpenFiles:                -1,
	}
}

// =============================================================================
// Encoding Utilities
// =============================================================================

// ParseLedgerSeq converts a 4-byte big-endian value to uint32.
func ParseLedgerSeq(value []byte) uint32 {
	if len(value) != 4 {
		return 0
	}
	return uint32(value[0])<<24 | uint32(value[1])<<16 | uint32(value[2])<<8 | uint32(value[3])
}

// EncodeLedgerSeq converts a uint32 to 4-byte big-endian.
func EncodeLedgerSeq(ledgerSeq uint32) []byte {
	return []byte{
		byte(ledgerSeq >> 24),
		byte(ledgerSeq >> 16),
		byte(ledgerSeq >> 8),
		byte(ledgerSeq),
	}
}

// HexToBytes converts a hex string to bytes.
// Returns nil if the string is not valid hex.
func HexToBytes(hexStr string) []byte {
	if len(hexStr)%2 != 0 {
		return nil
	}

	bytes := make([]byte, len(hexStr)/2)
	for i := 0; i < len(hexStr); i += 2 {
		var high, low byte
		if hexStr[i] >= '0' && hexStr[i] <= '9' {
			high = hexStr[i] - '0'
		} else if hexStr[i] >= 'a' && hexStr[i] <= 'f' {
			high = hexStr[i] - 'a' + 10
		} else if hexStr[i] >= 'A' && hexStr[i] <= 'F' {
			high = hexStr[i] - 'A' + 10
		} else {
			return nil
		}

		if hexStr[i+1] >= '0' && hexStr[i+1] <= '9' {
			low = hexStr[i+1] - '0'
		} else if hexStr[i+1] >= 'a' && hexStr[i+1] <= 'f' {
			low = hexStr[i+1] - 'a' + 10
		} else if hexStr[i+1] >= 'A' && hexStr[i+1] <= 'F' {
			low = hexStr[i+1] - 'A' + 10
		} else {
			return nil
		}

		bytes[i/2] = high<<4 | low
	}
	return bytes
}

// BytesToHex converts bytes to a hex string.
func BytesToHex(data []byte) string {
	const hexChars = "0123456789abcdef"
	result := make([]byte, len(data)*2)
	for i, b := range data {
		result[i*2] = hexChars[b>>4]
		result[i*2+1] = hexChars[b&0x0f]
	}
	return string(result)
}
