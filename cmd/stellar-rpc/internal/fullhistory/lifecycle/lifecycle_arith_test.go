package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Arithmetic: geometry.LastCompleteChunkAt, EffectiveRetentionFloor.
// ---------------------------------------------------------------------------

func TestLastCompleteChunkAt(t *testing.T) {
	tests := []struct {
		name   string
		ledger uint32
		want   int64
	}{
		{"below first chunk's last ledger => sentinel -1", chunk.ID(0).LastLedger() - 1, -1},
		{"genesis sentinel (FirstLedgerSeq-1) => -1", chunk.FirstLedgerSeq - 1, -1},
		{"ledger 0 does not underflow => -1", 0, -1},
		{"chunk 0's last ledger => 0", chunk.ID(0).LastLedger(), 0},
		{"chunk 0's last ledger + 1 (into chunk 1) => still 0", chunk.ID(0).LastLedger() + 1, 0},
		{"chunk 5's last ledger => 5", chunk.ID(5).LastLedger(), 5},
		{"the doc's example 10_001 => 0", 10_001, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, geometry.LastCompleteChunkAt(tc.ledger))
		})
	}
}

func TestEffectiveRetentionFloor(t *testing.T) {
	genesis := uint32(chunk.FirstLedgerSeq)
	tests := []struct {
		name            string
		upperBound      uint32
		retentionChunks uint32
		earliest        uint32
		want            uint32
	}{
		{
			name:            "no sliding (retention 0): earliest floor wins",
			upperBound:      chunk.ID(100).LastLedger(),
			retentionChunks: 0,
			earliest:        chunk.ID(10).FirstLedger(),
			want:            chunk.ID(10).FirstLedger(),
		},
		{
			name:            "no sliding, no earliest pin: genesis",
			upperBound:      chunk.ID(100).LastLedger(),
			retentionChunks: 0,
			earliest:        0,
			want:            genesis,
		},
		{
			name:            "sliding floor leads when above earliest",
			upperBound:      chunk.ID(100).LastLedger(), // last complete chunk = 100
			retentionChunks: 10,                         // floor chunk = 100-10+1 = 91
			earliest:        0,
			want:            chunk.ID(91).FirstLedger(),
		},
		{
			name:            "earliest floor leads when above the sliding floor",
			upperBound:      chunk.ID(100).LastLedger(),
			retentionChunks: 10,                         // sliding floor chunk = 91
			earliest:        chunk.ID(95).FirstLedger(), // higher
			want:            chunk.ID(95).FirstLedger(),
		},
		{
			name:            "retention wider than history clamps to chunk 0, never wraps",
			upperBound:      chunk.ID(3).LastLedger(),
			retentionChunks: 1000, // sliding chunk = 3-1000+1 < 0 => clamp to chunk 0
			earliest:        0,
			want:            chunk.ID(0).FirstLedger(),
		},
		{
			name:            "young store (upperBound below first chunk) clamps to chunk 0",
			upperBound:      chunk.FirstLedgerSeq + 5, // no complete chunk yet
			retentionChunks: 5,
			earliest:        0,
			want:            chunk.ID(0).FirstLedger(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, EffectiveRetentionFloor(tc.upperBound, tc.retentionChunks, tc.earliest))
		})
	}
}

