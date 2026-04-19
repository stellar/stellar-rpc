// Package geometry holds the chunk and txhash-index boundary math used by
// the backfill pipeline.
//
// Two concepts:
//
//   - Chunk  — 10_000 ledgers. The atomic unit of ingestion + crash recovery.
//   - Index  — ChunksPerTxHashIndex chunks (default 1000, = 10M ledgers).
//     Cadence at which one RecSplit txhash index gets built.
//
// Ledger sequences start at FirstLedger (2), so every boundary formula
// offsets by that anchor.
package geometry

// FirstLedger is the first ledger of the Stellar network.
const FirstLedger uint32 = 2

// ChunkSize is the fixed width of a chunk, in ledgers. Not operator-
// configurable — the directory tree and packfile format both assume it.
const ChunkSize uint32 = 10_000

// Geometry bundles the two sizing knobs that define a backfill layout.
// ChunksPerTxHashIndex is operator-configurable; ChunkSize stays fixed
// in production but tests substitute a smaller value for speed.
type Geometry struct {
	ChunkSize            uint32
	ChunksPerTxHashIndex uint32
}

// DefaultGeometry is the production layout: 10_000-ledger chunks,
// 1_000 chunks per index (10M ledgers per index).
func DefaultGeometry() Geometry {
	return Geometry{ChunkSize: ChunkSize, ChunksPerTxHashIndex: 1000}
}

// TestGeometry is a downscaled layout for unit tests: 10-ledger chunks,
// 5 chunks per index (50 ledgers per index).
func TestGeometry() Geometry {
	return Geometry{ChunkSize: 10, ChunksPerTxHashIndex: 5}
}

// LedgersPerIndex = ChunkSize * ChunksPerTxHashIndex.
func (g Geometry) LedgersPerIndex() uint32 {
	return g.ChunkSize * g.ChunksPerTxHashIndex
}

// --- Chunk-level ---

// LedgerToChunkID returns the chunk ID a ledger falls into.
func (g Geometry) LedgerToChunkID(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedger) / g.ChunkSize
}

// ChunkFirstLedger returns the first ledger (inclusive) in a chunk.
func (g Geometry) ChunkFirstLedger(chunkID uint32) uint32 {
	return (chunkID * g.ChunkSize) + FirstLedger
}

// ChunkLastLedger returns the last ledger (inclusive) in a chunk.
func (g Geometry) ChunkLastLedger(chunkID uint32) uint32 {
	return ((chunkID + 1) * g.ChunkSize) + FirstLedger - 1
}

// --- Index-level ---

// LedgerToIndexID returns the index ID a ledger falls into.
func (g Geometry) LedgerToIndexID(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedger) / g.LedgersPerIndex()
}

// IndexID returns the index ID that contains a given chunk.
func (g Geometry) IndexID(chunkID uint32) uint32 {
	return chunkID / g.ChunksPerTxHashIndex
}

// IndexFirstChunk returns the first chunk ID in an index.
func (g Geometry) IndexFirstChunk(indexID uint32) uint32 {
	return indexID * g.ChunksPerTxHashIndex
}

// IndexLastChunk returns the last chunk ID (inclusive) in an index.
func (g Geometry) IndexLastChunk(indexID uint32) uint32 {
	return (indexID+1)*g.ChunksPerTxHashIndex - 1
}

// IndexFirstLedger returns the first ledger (inclusive) in an index.
func (g Geometry) IndexFirstLedger(indexID uint32) uint32 {
	return g.IndexFirstChunk(indexID)*g.ChunkSize + FirstLedger
}

// IndexLastLedger returns the last ledger (inclusive) in an index.
func (g Geometry) IndexLastLedger(indexID uint32) uint32 {
	return (g.IndexLastChunk(indexID)+1)*g.ChunkSize + FirstLedger - 1
}

// IsLastChunkInIndex reports whether completing chunkID should trigger the
// build_txhash_index task for its index group.
func (g Geometry) IsLastChunkInIndex(chunkID uint32) bool {
	return (chunkID+1)%g.ChunksPerTxHashIndex == 0
}

// ChunksForIndex returns every chunk ID in an index group, ascending.
// Used by the DAG builder (slice #687) to enumerate process_chunk deps.
func (g Geometry) ChunksForIndex(indexID uint32) []uint32 {
	first := g.IndexFirstChunk(indexID)
	out := make([]uint32, g.ChunksPerTxHashIndex)
	for i := range g.ChunksPerTxHashIndex {
		out[i] = first + i
	}
	return out
}
