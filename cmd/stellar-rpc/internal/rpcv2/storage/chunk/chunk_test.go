package chunk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIDFromLedger(t *testing.T) {
	tests := []struct {
		name   string
		ledger uint32
		wantID ID
	}{
		{"genesis ledger maps to chunk 0", 2, 0},
		{"second ledger maps to chunk 0", 3, 0},
		{"last ledger of chunk 0", 10_001, 0},
		{"first ledger of chunk 1", 10_002, 1},
		{"last ledger of chunk 1", 20_001, 1},
		{"first ledger of chunk 2", 20_002, 2},
		{"bucket-boundary chunk 1000", 10_000_002, 1000},
		{"large chunk", 999_990_002, 99_999},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantID, IDFromLedger(tt.ledger))
		})
	}
}

// TestIDFromLedger_PanicsBelowGenesis pins the contract that
// sub-genesis input is a programmer error, not a silent miscalibration.
// Pre-fix, IDFromLedger(0) underflowed uint32 and returned a garbage
// chunk ID.
func TestIDFromLedger_PanicsBelowGenesis(t *testing.T) {
	assert.Panics(t, func() { _ = IDFromLedger(0) })
	assert.Panics(t, func() { _ = IDFromLedger(FirstLedgerSeq - 1) })
}

func TestFirstLastLedger(t *testing.T) {
	tests := []struct {
		id        ID
		wantFirst uint32
		wantLast  uint32
	}{
		{0, 2, 10_001},
		{1, 10_002, 20_001},
		{2, 20_002, 30_001},
		{999, 9_990_002, 10_000_001},
		{1000, 10_000_002, 10_010_001},
		{99_999, 999_990_002, 1_000_000_001},
	}
	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			assert.Equal(t, tt.wantFirst, tt.id.FirstLedger())
			assert.Equal(t, tt.wantLast, tt.id.LastLedger())
		})
	}
}

func TestRoundTripLedgerToChunk(t *testing.T) {
	// For every chunk in a representative range, every ledger in
	// [FirstLedger, LastLedger] must map back to that chunk.
	chunks := []ID{0, 1, 2, 7, 1000, 9_999}
	for _, id := range chunks {
		assert.Equal(t, id, IDFromLedger(id.FirstLedger()),
			"first ledger of chunk %s round-trips", id)
		assert.Equal(t, id, IDFromLedger(id.LastLedger()),
			"last ledger of chunk %s round-trips", id)
		// A mid-range sample.
		mid := id.FirstLedger() + LedgersPerChunk/2
		assert.Equal(t, id, IDFromLedger(mid),
			"mid ledger of chunk %s round-trips", id)
	}
}

func TestChunkBoundariesAreContiguous(t *testing.T) {
	// LastLedger(c) + 1 must equal FirstLedger(c+1) for all c.
	for c := range ID(5) {
		assert.Equal(t, c.LastLedger()+1, (c + 1).FirstLedger())
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		id   ID
		want string
	}{
		{0, "00000000"},
		{1, "00000001"},
		{1000, "00001000"},
		{99_999_999, "99999999"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.id.String())
		})
	}
}

func TestBucketID(t *testing.T) {
	tests := []struct {
		id   ID
		want string
	}{
		{0, "00000"},
		{1, "00000"},
		{999, "00000"},
		{1000, "00001"},
		{1999, "00001"},
		{2000, "00002"},
		{99_999, "00099"},
		{99_999_999, "99999"},
	}
	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			assert.Equal(t, tt.want, tt.id.BucketID())
		})
	}
}
