// Package geometry provides range and chunk boundary math for Stellar ledger storage.
//
// These constants and functions define the mapping between ledger sequences,
// 10M-ledger ranges, and 10K-ledger chunks. They are used by both the backfill
// pipeline and streaming code.
//
// Hierarchy:
//
//	Range (10M ledgers) → 1000 Chunks (10K ledgers each)
//
// The Stellar blockchain starts at ledger 2 (FirstLedger), so all formulas
// offset by 2 to keep boundaries aligned.
//
// Example ranges:
//
//	Range 0: ledgers [2, 10_000_001]       → chunks [0, 999]
//	Range 1: ledgers [10_000_002, 20_000_001] → chunks [1000, 1999]
//	Range 2: ledgers [20_000_002, 30_000_001] → chunks [2000, 2999]
package geometry

// =============================================================================
// Range Boundary Constants
// =============================================================================

const (
	// FirstLedger is the first ledger in the Stellar blockchain.
	// All range and chunk boundary math offsets from this value.
	FirstLedger uint32 = 2

	// ChunkSize is the number of ledgers in each chunk (10,000).
	// This is the fundamental unit of data — one LFS file per chunk.
	ChunkSize uint32 = 10_000

	// RangeSize is the number of ledgers in each range (10 million).
	// Each range contains exactly ChunksPerRange chunks.
	RangeSize uint32 = 10_000_000

	// ChunksPerRange is the number of chunks in each range.
	// Derived from RangeSize / ChunkSize (10K), but declared as a constant
	// to avoid depending on lfs.ChunkSize here.
	ChunksPerRange uint32 = 1000
)

// =============================================================================
// Free Functions (use global constants)
// =============================================================================

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

// =============================================================================
// Geometry — Parameterized Range/Chunk Math
// =============================================================================
//
// Geometry encapsulates the range and chunk sizing constants so that production
// code uses the full 10M-range / 10K-chunk layout while tests can substitute
// a smaller geometry (e.g., 100-ledger ranges, 10-ledger chunks) for speed.
//
// All methods mirror the package-level free functions (LedgerToRangeID, etc.)
// but compute boundaries from the Geometry values instead of the global constants.
//
// Production code should pass DefaultGeometry() through the config chain.
// Tests should pass TestGeometry() for fast iteration.

// Geometry holds the chunk/range sizing parameters.
type Geometry struct {
	// RangeSize is the number of ledgers in each range.
	// Production: 10,000,000. Test: 100.
	RangeSize uint32

	// ChunkSize is the number of ledgers in each chunk.
	// Production: 10,000. Test: 10.
	ChunkSize uint32

	// ChunksPerTxHashIndex is the number of chunks in each txhash index group.
	// Controls the cadence of RecSplit index builds.
	// Production: 1,000 (10M ledgers per index). Test: 5 (50 ledgers per index).
	ChunksPerTxHashIndex uint32
}

// DefaultGeometry returns the production geometry:
// 10M-ledger ranges, 10K-ledger chunks, 1000 chunks per index.
func DefaultGeometry() Geometry {
	return Geometry{
		RangeSize:      RangeSize,
		ChunkSize:      10_000,
		ChunksPerTxHashIndex: 1000,
	}
}

// TestGeometry returns a small geometry suitable for fast unit tests:
// 100-ledger ranges, 10-ledger chunks, 5 chunks per index (50 ledgers per index).
func TestGeometry() Geometry {
	return Geometry{
		RangeSize:      100,
		ChunkSize:      10,
		ChunksPerTxHashIndex: 5,
	}
}

// --- Range-level methods ---

// LedgerToRangeID returns the range ID for a given ledger sequence.
func (g Geometry) LedgerToRangeID(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedger) / g.RangeSize
}

// LedgerToIndexID is an alias for LedgerToRangeID. Use in backfill code
// where the concept is called "index" rather than "range".
func (g Geometry) LedgerToIndexID(ledgerSeq uint32) uint32 {
	return g.LedgerToRangeID(ledgerSeq)
}

// RangeFirstLedger returns the first ledger sequence (inclusive) in a range.
func (g Geometry) RangeFirstLedger(rangeID uint32) uint32 {
	return (rangeID * g.RangeSize) + FirstLedger
}

// RangeLastLedger returns the last ledger sequence (inclusive) in a range.
func (g Geometry) RangeLastLedger(rangeID uint32) uint32 {
	return ((rangeID + 1) * g.RangeSize) + FirstLedger - 1
}

// RangeFirstChunk returns the first chunk ID in a range.
func (g Geometry) RangeFirstChunk(rangeID uint32) uint32 {
	return rangeID * g.ChunksPerTxHashIndex
}

// RangeLastChunk returns the last chunk ID (inclusive) in a range.
func (g Geometry) RangeLastChunk(rangeID uint32) uint32 {
	return (rangeID * g.ChunksPerTxHashIndex) + g.ChunksPerTxHashIndex - 1
}

// --- Chunk-level methods ---

// ChunkToRangeID returns the range ID that contains a given chunk.
func (g Geometry) ChunkToRangeID(chunkID uint32) uint32 {
	return chunkID / g.ChunksPerTxHashIndex
}

// ChunkFirstLedger returns the first ledger sequence in a chunk.
func (g Geometry) ChunkFirstLedger(chunkID uint32) uint32 {
	return (chunkID * g.ChunkSize) + FirstLedger
}

// ChunkLastLedger returns the last ledger sequence in a chunk.
func (g Geometry) ChunkLastLedger(chunkID uint32) uint32 {
	return ((chunkID + 1) * g.ChunkSize) + FirstLedger - 1
}

// --- Index-level methods ---

// LedgersPerIndex returns the number of ledgers in one txhash index group.
// Formula: ChunkSize * ChunksPerTxHashIndex.
// Production (ChunkSize=10000, ChunksPerTxHashIndex=1000): 10,000,000
// Test (ChunkSize=10, ChunksPerTxHashIndex=5): 50
func (g Geometry) LedgersPerIndex() uint32 {
	return g.ChunkSize * g.ChunksPerTxHashIndex
}

// IndexID returns the index ID for a given chunk ID.
// Formula: chunkID / ChunksPerTxHashIndex.
// Production: IndexID(1500) = 1 (chunk 1500 is in index 1, which covers chunks 1000-1999)
// Test (ChunksPerTxHashIndex=5): IndexID(7) = 1 (chunk 7 is in index 1, which covers chunks 5-9)
func (g Geometry) IndexID(chunkID uint32) uint32 {
	return chunkID / g.ChunksPerTxHashIndex
}

// IndexFirstChunk returns the first chunk ID in an index group.
// Formula: indexID * ChunksPerTxHashIndex.
// Production: IndexFirstChunk(1) = 1000
// Test (ChunksPerTxHashIndex=5): IndexFirstChunk(1) = 5
func (g Geometry) IndexFirstChunk(indexID uint32) uint32 {
	return indexID * g.ChunksPerTxHashIndex
}

// IndexLastChunk returns the last chunk ID (inclusive) in an index group.
// Formula: (indexID+1)*ChunksPerTxHashIndex - 1.
// Production: IndexLastChunk(1) = 1999
// Test (ChunksPerTxHashIndex=5): IndexLastChunk(1) = 9
func (g Geometry) IndexLastChunk(indexID uint32) uint32 {
	return (indexID+1)*g.ChunksPerTxHashIndex - 1
}

// IsLastChunkInIndex reports whether chunkID is the last chunk in its index group.
// True when (chunkID+1) % ChunksPerTxHashIndex == 0.
// Completing this chunk should trigger build_txhash_index.
// Production: IsLastChunkInIndex(999) = true, IsLastChunkInIndex(1000) = false
// Test (ChunksPerTxHashIndex=5): IsLastChunkInIndex(4) = true, IsLastChunkInIndex(9) = true
func (g Geometry) IsLastChunkInIndex(chunkID uint32) bool {
	return (chunkID+1)%g.ChunksPerTxHashIndex == 0
}

// IndexFirstLedger returns the first ledger sequence in an index group.
// Formula: IndexFirstChunk(indexID) * ChunkSize + FirstLedger.
// Production: IndexFirstLedger(1) = 10,000,002
// Test (ChunkSize=10, ChunksPerTxHashIndex=5): IndexFirstLedger(1) = 52
func (g Geometry) IndexFirstLedger(indexID uint32) uint32 {
	return g.IndexFirstChunk(indexID)*g.ChunkSize + FirstLedger
}

// IndexLastLedger returns the last ledger sequence (inclusive) in an index group.
// Formula: (IndexLastChunk(indexID)+1)*ChunkSize + FirstLedger - 1.
// Production: IndexLastLedger(1) = 20,000,001
// Test (ChunkSize=10, ChunksPerTxHashIndex=5): IndexLastLedger(1) = 101
func (g Geometry) IndexLastLedger(indexID uint32) uint32 {
	return (g.IndexLastChunk(indexID)+1)*g.ChunkSize + FirstLedger - 1
}

// ChunksForIndex returns all chunk IDs in an index group, in ascending order.
// Used by the DAG builder to enumerate process_chunk dependencies.
// Production: ChunksForIndex(1) = [1000, 1001, ..., 1999] (len=1000)
// Test (ChunksPerTxHashIndex=5): ChunksForIndex(1) = [5, 6, 7, 8, 9]
func (g Geometry) ChunksForIndex(indexID uint32) []uint32 {
	first := g.IndexFirstChunk(indexID)
	chunks := make([]uint32, g.ChunksPerTxHashIndex)
	for i := uint32(0); i < g.ChunksPerTxHashIndex; i++ {
		chunks[i] = first + i
	}
	return chunks
}
