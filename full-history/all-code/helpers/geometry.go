package helpers

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

	// ChunksPerRange is the number of chunks in each range.
	// Must equal RangeSize / ChunkSize.
	// Production: 1,000. Test: 10.
	ChunksPerRange uint32
}

// DefaultGeometry returns the production geometry:
// 10M-ledger ranges, 10K-ledger chunks, 1000 chunks per range.
func DefaultGeometry() Geometry {
	return Geometry{
		RangeSize:      RangeSize,
		ChunkSize:      10_000,
		ChunksPerRange: ChunksPerRange,
	}
}

// TestGeometry returns a small geometry suitable for fast unit tests:
// 100-ledger ranges, 10-ledger chunks, 10 chunks per range.
func TestGeometry() Geometry {
	return Geometry{
		RangeSize:      100,
		ChunkSize:      10,
		ChunksPerRange: 10,
	}
}

// --- Range-level methods ---

// LedgerToRangeID returns the range ID for a given ledger sequence.
func (g Geometry) LedgerToRangeID(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedger) / g.RangeSize
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
	return rangeID * g.ChunksPerRange
}

// RangeLastChunk returns the last chunk ID (inclusive) in a range.
func (g Geometry) RangeLastChunk(rangeID uint32) uint32 {
	return (rangeID * g.ChunksPerRange) + g.ChunksPerRange - 1
}

// --- Chunk-level methods ---

// ChunkToRangeID returns the range ID that contains a given chunk.
func (g Geometry) ChunkToRangeID(chunkID uint32) uint32 {
	return chunkID / g.ChunksPerRange
}

// ChunkFirstLedger returns the first ledger sequence in a chunk.
func (g Geometry) ChunkFirstLedger(chunkID uint32) uint32 {
	return (chunkID * g.ChunkSize) + FirstLedger
}

// ChunkLastLedger returns the last ledger sequence in a chunk.
func (g Geometry) ChunkLastLedger(chunkID uint32) uint32 {
	return ((chunkID + 1) * g.ChunkSize) + FirstLedger - 1
}
