package helpers

// =============================================================================
// Range Boundary Constants and Formulas
// =============================================================================
//
// These constants and functions define the mapping between ledger sequences,
// 10M-ledger ranges, and 10K-ledger chunks. They are used by both the backfill
// pipeline and future streaming code.
//
// Hierarchy:
//   Range (10M ledgers) → 1000 Chunks (10K ledgers each)
//
// The Stellar blockchain starts at ledger 2 (FirstLedger), so all formulas
// offset by 2 to keep boundaries aligned.
//
// Example ranges:
//   Range 0: ledgers [2, 10_000_001]       → chunks [0, 999]
//   Range 1: ledgers [10_000_002, 20_000_001] → chunks [1000, 1999]
//   Range 2: ledgers [20_000_002, 30_000_001] → chunks [2000, 2999]

const (
	// FirstLedger is the first ledger in the Stellar blockchain.
	// All range and chunk boundary math offsets from this value.
	FirstLedger uint32 = 2

	// RangeSize is the number of ledgers in each range (10 million).
	// Each range contains exactly ChunksPerRange chunks.
	RangeSize uint32 = 10_000_000

	// ChunksPerRange is the number of chunks in each range.
	// Derived from RangeSize / ChunkSize (10K), but declared as a constant
	// to avoid depending on lfs.ChunkSize here (helpers/ must not import lfs/).
	ChunksPerRange uint32 = 1000
)

// LedgerToRangeID returns the range ID for a given ledger sequence.
//
// Example: LedgerToRangeID(2) = 0, LedgerToRangeID(10_000_002) = 1
func LedgerToRangeID(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedger) / RangeSize
}

// RangeFirstLedger returns the first ledger sequence (inclusive) in a range.
//
// Example: RangeFirstLedger(0) = 2, RangeFirstLedger(1) = 10_000_002
func RangeFirstLedger(rangeID uint32) uint32 {
	return (rangeID * RangeSize) + FirstLedger
}

// RangeLastLedger returns the last ledger sequence (inclusive) in a range.
//
// Example: RangeLastLedger(0) = 10_000_001, RangeLastLedger(1) = 20_000_001
func RangeLastLedger(rangeID uint32) uint32 {
	return ((rangeID + 1) * RangeSize) + FirstLedger - 1
}

// RangeFirstChunk returns the first chunk ID in a range.
//
// Example: RangeFirstChunk(0) = 0, RangeFirstChunk(1) = 1000
func RangeFirstChunk(rangeID uint32) uint32 {
	return rangeID * ChunksPerRange
}

// RangeLastChunk returns the last chunk ID (inclusive) in a range.
//
// Example: RangeLastChunk(0) = 999, RangeLastChunk(1) = 1999
func RangeLastChunk(rangeID uint32) uint32 {
	return (rangeID * ChunksPerRange) + ChunksPerRange - 1
}

// ChunkToRangeID returns the range ID that contains a given chunk.
//
// Example: ChunkToRangeID(0) = 0, ChunkToRangeID(999) = 0, ChunkToRangeID(1000) = 1
func ChunkToRangeID(chunkID uint32) uint32 {
	return chunkID / ChunksPerRange
}
