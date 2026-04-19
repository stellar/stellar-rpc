package geometry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultAndTestGeometry(t *testing.T) {
	d := DefaultGeometry()
	assert.Equal(t, ChunkSize, d.ChunkSize)
	assert.Equal(t, uint32(1000), d.ChunksPerTxHashIndex)
	assert.Equal(t, uint32(10_000_000), d.LedgersPerIndex())

	tg := TestGeometry()
	assert.Equal(t, uint32(10), tg.ChunkSize)
	assert.Equal(t, uint32(5), tg.ChunksPerTxHashIndex)
	assert.Equal(t, uint32(50), tg.LedgersPerIndex())
}

func TestChunkBoundaries(t *testing.T) {
	g := DefaultGeometry()
	// Chunk 0 spans [2, 10001]. Chunk 499 spans [4_990_002, 5_000_001].
	assert.Equal(t, uint32(2), g.ChunkFirstLedger(0))
	assert.Equal(t, uint32(10_001), g.ChunkLastLedger(0))
	assert.Equal(t, uint32(4_990_002), g.ChunkFirstLedger(499))
	assert.Equal(t, uint32(5_000_001), g.ChunkLastLedger(499))
}

func TestLedgerToChunkID(t *testing.T) {
	g := DefaultGeometry()
	cases := []struct {
		ledger  uint32
		wantCID uint32
	}{
		{2, 0},
		{10_001, 0},
		{10_002, 1},
		{5_000_000, 499},
		{5_000_001, 499},
	}
	for _, tc := range cases {
		assert.Equalf(t, tc.wantCID, g.LedgerToChunkID(tc.ledger),
			"LedgerToChunkID(%d)", tc.ledger)
	}
}

func TestLedgerToIndexID(t *testing.T) {
	g := DefaultGeometry()
	// Index 0 spans ledgers [2, 10_000_001]. Index 1 starts at 10_000_002.
	assert.Equal(t, uint32(0), g.LedgerToIndexID(2))
	assert.Equal(t, uint32(0), g.LedgerToIndexID(10_000_001))
	assert.Equal(t, uint32(1), g.LedgerToIndexID(10_000_002))
	assert.Equal(t, uint32(5), g.LedgerToIndexID(56_337_842))
}

func TestIndexID(t *testing.T) {
	g := TestGeometry() // ChunksPerTxHashIndex=5
	// Chunks 0-4 → index 0; chunks 5-9 → index 1.
	assert.Equal(t, uint32(0), g.IndexID(0))
	assert.Equal(t, uint32(0), g.IndexID(4))
	assert.Equal(t, uint32(1), g.IndexID(5))
	assert.Equal(t, uint32(1), g.IndexID(9))
	assert.Equal(t, uint32(2), g.IndexID(10))
}

func TestIndexBoundaries(t *testing.T) {
	g := TestGeometry() // ChunkSize=10, ChunksPerTxHashIndex=5, FirstLedger=2
	// Index 1 = chunks 5-9 = ledgers [52, 101].
	assert.Equal(t, uint32(5), g.IndexFirstChunk(1))
	assert.Equal(t, uint32(9), g.IndexLastChunk(1))
	assert.Equal(t, uint32(52), g.IndexFirstLedger(1))
	assert.Equal(t, uint32(101), g.IndexLastLedger(1))
}

func TestIsLastChunkInIndex(t *testing.T) {
	g := TestGeometry() // ChunksPerTxHashIndex=5
	for _, c := range []uint32{4, 9, 14} {
		assert.Truef(t, g.IsLastChunkInIndex(c), "chunk %d should be last-in-index", c)
	}
	for _, c := range []uint32{0, 3, 5, 8} {
		assert.Falsef(t, g.IsLastChunkInIndex(c), "chunk %d should NOT be last-in-index", c)
	}
}

func TestChunksForIndex(t *testing.T) {
	g := TestGeometry() // ChunksPerTxHashIndex=5
	assert.Equal(t, []uint32{5, 6, 7, 8, 9}, g.ChunksForIndex(1))
}

// TestIndexRoundTrip — every chunk in [0, 30) maps to an index that
// includes it in [IndexFirstChunk, IndexLastChunk].
func TestIndexRoundTrip(t *testing.T) {
	g := TestGeometry()
	for c := range uint32(30) {
		idx := g.IndexID(c)
		assert.GreaterOrEqualf(t, c, g.IndexFirstChunk(idx), "chunk %d below IndexFirstChunk(%d)", c, idx)
		assert.LessOrEqualf(t, c, g.IndexLastChunk(idx), "chunk %d above IndexLastChunk(%d)", c, idx)
	}
}

// TestIndexContiguity — index i's last ledger + 1 == index i+1's first ledger.
func TestIndexContiguity(t *testing.T) {
	g := DefaultGeometry()
	for i := range uint32(5) {
		assert.Equalf(t, g.IndexLastLedger(i)+1, g.IndexFirstLedger(i+1),
			"gap between index %d and %d", i, i+1)
	}
}
