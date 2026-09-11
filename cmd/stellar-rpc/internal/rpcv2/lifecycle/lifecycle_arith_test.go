package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// ---------------------------------------------------------------------------
// Arithmetic: chunk.LastCompleteChunkAt. (The retention floor is tested in the
// geometry package.)
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
			require.Equal(t, tc.want, chunk.LastCompleteChunkAt(tc.ledger))
		})
	}
}
