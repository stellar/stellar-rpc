package geometry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

func TestRetention_FloorAt(t *testing.T) {
	tests := []struct {
		name          string
		frontier      int64
		size          uint32
		earliestChunk chunk.ID
		want          chunk.ID
	}{
		{"sliding floor leads when above earliest", 100, 10, 0, 91},
		{"earliest floor leads when above the sliding floor", 100, 10, 95, 95},
		{"full history (size 0) pins at earliest", 100, 0, 10, 10},
		{"full history (size 0), earliest genesis chunk", 100, 0, 0, 0},
		{"retention wider than history clamps to earliest", 3, 1000, 0, 0},
		{"young store (-1, nothing complete) clamps to earliest", -1, 5, 0, 0},
		{"young store (-1) with an earliest pin returns the earliest", -1, 5, 3, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRetention(tc.size, tc.earliestChunk)
			require.Equal(t, tc.want, r.FloorAt(tc.frontier))
		})
	}
}

// Full history does not move with the highest chunk (no sliding window).
func TestRetention_FullHistoryDoesNotSlide(t *testing.T) {
	r := NewRetention(0, chunk.ID(50))
	require.Equal(t, chunk.ID(50), r.FloorAt(100))
	require.Equal(t, chunk.ID(50), r.FloorAt(1000))
}

// Shortening retention raises the floor immediately, with no per-chunk state.
func TestRetention_ShorteningRaisesFloor(t *testing.T) {
	require.Equal(t, chunk.ID(51), NewRetention(50, 0).FloorAt(100))
	require.Equal(t, chunk.ID(91), NewRetention(10, 0).FloorAt(100))
}

// A whole tx-hash index is below the floor exactly when its last chunk is, so
// callers compare layout.LastChunk(id) against the floor — no index-specific method.
func TestRetention_FloorVsIndexLastChunk(t *testing.T) {
	layout, err := NewTxHashIndexLayout(4) // indexes: 0=[0,3], 1=[4,7], 2=[8,11]
	require.NoError(t, err)
	floor := NewRetention(4, 0).FloorAt(11) // retain 4 back from chunk 11 ⇒ chunk 8
	require.Equal(t, chunk.ID(8), floor)
	assert.Less(t, layout.LastChunk(0), floor, "index 0 wholly below the floor")
	assert.Less(t, layout.LastChunk(1), floor, "index 1 wholly below the floor")
	assert.GreaterOrEqual(t, layout.LastChunk(2), floor, "index 2 straddles the floor")
}
