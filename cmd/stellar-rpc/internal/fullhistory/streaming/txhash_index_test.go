package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
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

// The pinnable cpi ceiling must never exceed what the cold tx-hash index format
// can encode: it stores each ledger as a ColdPayloadSize-byte offset, capping an
// index's ledger span at 2^(8*ColdPayloadSize). Pinning a larger cpi would pass
// validation but make every index build fail, and the pin is immutable. This
// guards the two constants against drifting apart.
func TestMaxChunksPerTxhashIndex_FitsColdPayload(t *testing.T) {
	payloadCapacity := uint64(1) << (8 * txhash.ColdPayloadSize) // 2^24 ledgers
	require.Equal(t, uint32(payloadCapacity/uint64(chunk.LedgersPerChunk)), MaxChunksPerTxhashIndex)

	// The max cpi's whole-window span fits the payload; one more chunk overflows.
	require.LessOrEqual(t, uint64(MaxChunksPerTxhashIndex)*uint64(chunk.LedgersPerChunk), payloadCapacity)
	require.Greater(t, uint64(MaxChunksPerTxhashIndex+1)*uint64(chunk.LedgersPerChunk), payloadCapacity)
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
