package geometry

import "testing"

func TestLedgerToRangeID(t *testing.T) {
	tests := []struct {
		name      string
		ledgerSeq uint32
		want      uint32
	}{
		{"first ledger", 2, 0},
		{"last ledger in range 0", 10_000_001, 0},
		{"first ledger in range 1", 10_000_002, 1},
		{"last ledger in range 1", 20_000_001, 1},
		{"first ledger in range 2", 20_000_002, 2},
		{"mid-range 0", 5_000_000, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LedgerToRangeID(tt.ledgerSeq)
			if got != tt.want {
				t.Errorf("LedgerToRangeID(%d) = %d, want %d", tt.ledgerSeq, got, tt.want)
			}
		})
	}
}

func TestRangeFirstLedger(t *testing.T) {
	tests := []struct {
		rangeID uint32
		want    uint32
	}{
		{0, 2},
		{1, 10_000_002},
		{2, 20_000_002},
		{3, 30_000_002},
	}
	for _, tt := range tests {
		got := RangeFirstLedger(tt.rangeID)
		if got != tt.want {
			t.Errorf("RangeFirstLedger(%d) = %d, want %d", tt.rangeID, got, tt.want)
		}
	}
}

func TestRangeLastLedger(t *testing.T) {
	tests := []struct {
		rangeID uint32
		want    uint32
	}{
		{0, 10_000_001},
		{1, 20_000_001},
		{2, 30_000_001},
	}
	for _, tt := range tests {
		got := RangeLastLedger(tt.rangeID)
		if got != tt.want {
			t.Errorf("RangeLastLedger(%d) = %d, want %d", tt.rangeID, got, tt.want)
		}
	}
}

func TestRangeFirstChunk(t *testing.T) {
	tests := []struct {
		rangeID uint32
		want    uint32
	}{
		{0, 0},
		{1, 1000},
		{2, 2000},
	}
	for _, tt := range tests {
		got := RangeFirstChunk(tt.rangeID)
		if got != tt.want {
			t.Errorf("RangeFirstChunk(%d) = %d, want %d", tt.rangeID, got, tt.want)
		}
	}
}

func TestRangeLastChunk(t *testing.T) {
	tests := []struct {
		rangeID uint32
		want    uint32
	}{
		{0, 999},
		{1, 1999},
		{2, 2999},
	}
	for _, tt := range tests {
		got := RangeLastChunk(tt.rangeID)
		if got != tt.want {
			t.Errorf("RangeLastChunk(%d) = %d, want %d", tt.rangeID, got, tt.want)
		}
	}
}

func TestChunkToRangeID(t *testing.T) {
	tests := []struct {
		chunkID uint32
		want    uint32
	}{
		{0, 0},
		{999, 0},
		{1000, 1},
		{1999, 1},
		{2000, 2},
	}
	for _, tt := range tests {
		got := ChunkToRangeID(tt.chunkID)
		if got != tt.want {
			t.Errorf("ChunkToRangeID(%d) = %d, want %d", tt.chunkID, got, tt.want)
		}
	}
}

// TestRangeRoundTrips verifies that range boundary formulas are consistent.
func TestRangeRoundTrips(t *testing.T) {
	for rangeID := uint32(0); rangeID < 5; rangeID++ {
		first := RangeFirstLedger(rangeID)
		last := RangeLastLedger(rangeID)

		// First ledger maps back to the same range
		if got := LedgerToRangeID(first); got != rangeID {
			t.Errorf("range %d: LedgerToRangeID(first=%d) = %d", rangeID, first, got)
		}
		// Last ledger maps back to the same range
		if got := LedgerToRangeID(last); got != rangeID {
			t.Errorf("range %d: LedgerToRangeID(last=%d) = %d", rangeID, last, got)
		}
		// Range spans exactly RangeSize ledgers
		if last-first+1 != RangeSize {
			t.Errorf("range %d: span = %d, want %d", rangeID, last-first+1, RangeSize)
		}
		// Chunk round-trip
		firstChunk := RangeFirstChunk(rangeID)
		lastChunk := RangeLastChunk(rangeID)
		if ChunkToRangeID(firstChunk) != rangeID {
			t.Errorf("range %d: ChunkToRangeID(firstChunk=%d) mismatch", rangeID, firstChunk)
		}
		if ChunkToRangeID(lastChunk) != rangeID {
			t.Errorf("range %d: ChunkToRangeID(lastChunk=%d) mismatch", rangeID, lastChunk)
		}
		if lastChunk-firstChunk+1 != ChunksPerRange {
			t.Errorf("range %d: chunk count = %d, want %d", rangeID, lastChunk-firstChunk+1, ChunksPerRange)
		}
	}
}

// TestRangeContiguity verifies there are no gaps between ranges.
func TestRangeContiguity(t *testing.T) {
	for rangeID := uint32(0); rangeID < 5; rangeID++ {
		lastOfCurrent := RangeLastLedger(rangeID)
		firstOfNext := RangeFirstLedger(rangeID + 1)
		if firstOfNext != lastOfCurrent+1 {
			t.Errorf("gap between range %d (last=%d) and range %d (first=%d)",
				rangeID, lastOfCurrent, rangeID+1, firstOfNext)
		}
		lastChunkOfCurrent := RangeLastChunk(rangeID)
		firstChunkOfNext := RangeFirstChunk(rangeID + 1)
		if firstChunkOfNext != lastChunkOfCurrent+1 {
			t.Errorf("gap between range %d (lastChunk=%d) and range %d (firstChunk=%d)",
				rangeID, lastChunkOfCurrent, rangeID+1, firstChunkOfNext)
		}
	}
}

// =============================================================================
// Index-level tests (using TestGeometry: ChunkSize=10, ChunksPerIndex=5)
// =============================================================================

func TestLedgersPerIndex(t *testing.T) {
	g := TestGeometry()
	if got := g.LedgersPerIndex(); got != 50 {
		t.Errorf("TestGeometry().LedgersPerIndex() = %d, want 50 (10*5)", got)
	}

	d := DefaultGeometry()
	if got := d.LedgersPerIndex(); got != 10_000_000 {
		t.Errorf("DefaultGeometry().LedgersPerIndex() = %d, want 10000000 (10000*1000)", got)
	}
}

func TestIndexID(t *testing.T) {
	g := TestGeometry() // ChunksPerIndex=5
	tests := []struct {
		chunkID uint32
		want    uint32
	}{
		{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 0}, // chunks 0-4 → index 0
		{5, 1}, {6, 1}, {7, 1}, {8, 1}, {9, 1}, // chunks 5-9 → index 1
		{10, 2}, // chunk 10 → index 2
	}
	for _, tt := range tests {
		if got := g.IndexID(tt.chunkID); got != tt.want {
			t.Errorf("IndexID(%d) = %d, want %d", tt.chunkID, got, tt.want)
		}
	}
}

func TestIndexBoundaries(t *testing.T) {
	g := TestGeometry() // ChunkSize=10, ChunksPerIndex=5, FirstLedger=2

	// Index 1: chunks 5-9
	if got := g.IndexFirstChunk(1); got != 5 {
		t.Errorf("IndexFirstChunk(1) = %d, want 5", got)
	}
	if got := g.IndexLastChunk(1); got != 9 {
		t.Errorf("IndexLastChunk(1) = %d, want 9", got)
	}

	// Index 1: ledgers: chunk 5 starts at 5*10+2=52, chunk 9 ends at 10*10+2-1=101
	if got := g.IndexFirstLedger(1); got != 52 {
		t.Errorf("IndexFirstLedger(1) = %d, want 52", got)
	}
	if got := g.IndexLastLedger(1); got != 101 {
		t.Errorf("IndexLastLedger(1) = %d, want 101", got)
	}
}

func TestIsLastChunkInIndex(t *testing.T) {
	g := TestGeometry() // ChunksPerIndex=5

	trueChunks := []uint32{4, 9, 14}
	for _, c := range trueChunks {
		if !g.IsLastChunkInIndex(c) {
			t.Errorf("IsLastChunkInIndex(%d) = false, want true", c)
		}
	}

	falseChunks := []uint32{0, 3, 5, 8}
	for _, c := range falseChunks {
		if g.IsLastChunkInIndex(c) {
			t.Errorf("IsLastChunkInIndex(%d) = true, want false", c)
		}
	}
}

func TestChunksForIndex(t *testing.T) {
	g := TestGeometry() // ChunksPerIndex=5
	chunks := g.ChunksForIndex(1)

	want := []uint32{5, 6, 7, 8, 9}
	if len(chunks) != len(want) {
		t.Fatalf("ChunksForIndex(1) len = %d, want %d", len(chunks), len(want))
	}
	for i, c := range chunks {
		if c != want[i] {
			t.Errorf("ChunksForIndex(1)[%d] = %d, want %d", i, c, want[i])
		}
	}

	if uint32(len(chunks)) != g.ChunksPerIndex {
		t.Errorf("len = %d, want ChunksPerIndex=%d", len(chunks), g.ChunksPerIndex)
	}
}

func TestIndexRoundTrip(t *testing.T) {
	g := TestGeometry() // ChunksPerIndex=5

	for c := uint32(0); c < 30; c++ {
		idx := g.IndexID(c)
		first := g.IndexFirstChunk(idx)
		last := g.IndexLastChunk(idx)

		if c < first || c > last {
			t.Errorf("chunk %d: IndexID=%d, but IndexFirstChunk=%d, IndexLastChunk=%d (out of range)",
				c, idx, first, last)
		}
	}
}

func TestIndexCadenceMatchesChunkCount(t *testing.T) {
	g := TestGeometry() // ChunksPerIndex=5

	for i := uint32(0); i < 5; i++ {
		chunks := g.ChunksForIndex(i)
		if uint32(len(chunks)) != g.ChunksPerIndex {
			t.Errorf("ChunksForIndex(%d) len = %d, want %d", i, len(chunks), g.ChunksPerIndex)
		}
	}
}
