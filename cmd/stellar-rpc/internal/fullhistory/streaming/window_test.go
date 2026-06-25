package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Tx-hash-index arithmetic.
// ---------------------------------------------------------------------------

func TestNewTxHashIndexLayout_Validation(t *testing.T) {
	_, err := NewTxHashIndexLayout(0)
	require.Error(t, err)

	_, err = NewTxHashIndexLayout(MaxChunksPerTxhashIndex + 1)
	require.Error(t, err)

	w, err := NewTxHashIndexLayout(MaxChunksPerTxhashIndex)
	require.NoError(t, err)
	require.Equal(t, MaxChunksPerTxhashIndex, w.ChunksPerIndex())
}

func TestTxHashIndexArithmetic(t *testing.T) {
	w, err := NewTxHashIndexLayout(1000)
	require.NoError(t, err)

	tests := []struct {
		name              string
		chunkID           chunk.ID
		wantTxHashIndex   TxHashIndexID
		wantFirst, wantHi chunk.ID
	}{
		{"first chunk of index 0", 0, 0, 0, 999},
		{"mid index 0", 500, 0, 0, 999},
		{"last chunk of index 0", 999, 0, 0, 999},
		{"first chunk of index 1", 1000, 1, 1000, 1999},
		{"the doc's example chunk 5350", 5350, 5, 5000, 5999},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantTxHashIndex, w.TxHashIndexID(tc.chunkID))
			require.Equal(t, tc.wantFirst, w.FirstChunk(tc.wantTxHashIndex))
			require.Equal(t, tc.wantHi, w.LastChunk(tc.wantTxHashIndex))
			require.Equal(t, uint32(1000), w.ChunksPerIndex())
		})
	}
}

func TestIsTerminalCoverage(t *testing.T) {
	w, err := NewTxHashIndexLayout(1000)
	require.NoError(t, err)

	// hi == index's last chunk => terminal.
	require.True(t, w.IsTerminalCoverage(TxHashIndexCoverage{Index: 5, Lo: 5100, Hi: 5999}))
	// hi below the last chunk => not terminal (still filling).
	require.False(t, w.IsTerminalCoverage(TxHashIndexCoverage{Index: 5, Lo: 5100, Hi: 5349}))
}
