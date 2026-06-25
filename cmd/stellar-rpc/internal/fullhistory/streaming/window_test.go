package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// ---------------------------------------------------------------------------
// Window arithmetic.
// ---------------------------------------------------------------------------

func TestNewWindows_Validation(t *testing.T) {
	_, err := NewWindows(0)
	require.Error(t, err)

	_, err = NewWindows(MaxChunksPerTxhashIndex + 1)
	require.Error(t, err)

	w, err := NewWindows(MaxChunksPerTxhashIndex)
	require.NoError(t, err)
	require.Equal(t, MaxChunksPerTxhashIndex, w.ChunksPerIndex())
}

func TestWindowArithmetic(t *testing.T) {
	w, err := NewWindows(1000)
	require.NoError(t, err)

	tests := []struct {
		name              string
		chunkID           chunk.ID
		wantWindow        WindowID
		wantFirst, wantHi chunk.ID
	}{
		{"first chunk of window 0", 0, 0, 0, 999},
		{"mid window 0", 500, 0, 0, 999},
		{"last chunk of window 0", 999, 0, 0, 999},
		{"first chunk of window 1", 1000, 1, 1000, 1999},
		{"the doc's example chunk 5350", 5350, 5, 5000, 5999},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantWindow, w.WindowID(tc.chunkID))
			require.Equal(t, tc.wantFirst, w.FirstChunk(tc.wantWindow))
			require.Equal(t, tc.wantHi, w.LastChunk(tc.wantWindow))
			require.Equal(t, uint32(1000), w.ChunksPerIndex())
		})
	}
}

func TestIsTerminalCoverage(t *testing.T) {
	w, err := NewWindows(1000)
	require.NoError(t, err)

	// hi == window's last chunk => terminal.
	require.True(t, w.IsTerminalCoverage(IndexCoverage{Window: 5, Lo: 5100, Hi: 5999}))
	// hi below the last chunk => not terminal (still filling).
	require.False(t, w.IsTerminalCoverage(IndexCoverage{Window: 5, Lo: 5100, Hi: 5349}))
}
