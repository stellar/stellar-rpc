package helpers

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
