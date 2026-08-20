package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// viewWithLatest builds a ReadView with floor chunk 5 (oldest ledger 50002) and
// the given latest ledger — bounds tests read only those two fields, no catalog or
// snapshot needed.
func viewWithLatest(latest uint32) *ReadView {
	return &ReadView{floor: 5, latestLedger: latest}
}

func TestOldestLedger(t *testing.T) {
	a := viewWithLatest(60000)
	assert.Equal(t, chunk.ID(5).FirstLedger(), a.OldestLedger()) // 50002
}

// floor chunk 5 → oldest 50002; latest 60000. View range [50002, 60000].
func TestClampRange_Ascending(t *testing.T) {
	a := viewWithLatest(60000)
	const oldest, latest = 50002, 60000

	t.Run("leading edge below floor is rejected with the available range", func(t *testing.T) {
		_, _, err := a.ClampRange(Ascending, 40000, 55000)
		var re *RangeError
		require.ErrorAs(t, err, &re)
		assert.Equal(t, uint32(40000), re.Requested)
		assert.Equal(t, uint32(oldest), re.Oldest)
		assert.Equal(t, uint32(latest), re.Latest)
	})

	t.Run("trailing edge beyond latest is truncated", func(t *testing.T) {
		lo, hi, err := a.ClampRange(Ascending, oldest, 70000)
		require.NoError(t, err)
		assert.Equal(t, uint32(oldest), lo)
		assert.Equal(t, uint32(latest), hi)
	})

	t.Run("in-range request is unchanged", func(t *testing.T) {
		lo, hi, err := a.ClampRange(Ascending, 55000, 59000)
		require.NoError(t, err)
		assert.Equal(t, uint32(55000), lo)
		assert.Equal(t, uint32(59000), hi)
	})

	t.Run("start beyond latest yields an empty range (nothing to serve yet)", func(t *testing.T) {
		lo, hi, err := a.ClampRange(Ascending, 65000, 70000)
		require.NoError(t, err) // not below-floor, so not an error — just future
		assert.Greater(t, lo, hi, "lo > hi signals empty")
	})

	t.Run("inverted input is rejected, not mislabeled as empty", func(t *testing.T) {
		_, _, err := a.ClampRange(Ascending, 59000, 51000) // in-range but lo > hi
		require.ErrorIs(t, err, ErrInvertedRange)
	})
}

func TestClampRange_Descending(t *testing.T) {
	a := viewWithLatest(60000)
	const oldest, latest = 50002, 60000

	t.Run("leading (high) edge below floor is rejected", func(t *testing.T) {
		_, _, err := a.ClampRange(Descending, 40000, 45000)
		var re *RangeError
		require.ErrorAs(t, err, &re)
		assert.Equal(t, uint32(45000), re.Requested, "the high edge is the leading edge descending")
	})

	t.Run("high edge beyond latest yields an empty range (wait)", func(t *testing.T) {
		// Never truncated: a descending scan cannot revisit a ledger, so
		// serving below a top this view lacks would skip it forever.
		lo, hi, err := a.ClampRange(Descending, 55000, 70000)
		require.NoError(t, err)
		assert.Greater(t, lo, hi, "lo > hi signals wait")
	})

	t.Run("low edge below floor terminates at the floor", func(t *testing.T) {
		lo, hi, err := a.ClampRange(Descending, 40000, 59000)
		require.NoError(t, err) // leading (high) edge 59000 is in range
		assert.Equal(t, uint32(oldest), lo, "scan terminates at the floor")
		assert.Equal(t, uint32(59000), hi)
	})
}

func TestChunksBetween(t *testing.T) {
	t.Run("ascending spans the chunks in order", func(t *testing.T) {
		assert.Equal(t, []chunk.ID{5, 6, 7}, chunksBetween(5, 7, Ascending))
	})

	t.Run("descending reverses the traversal", func(t *testing.T) {
		assert.Equal(t, []chunk.ID{7, 6, 5}, chunksBetween(5, 7, Descending))
	})

	t.Run("single chunk", func(t *testing.T) {
		assert.Equal(t, []chunk.ID{6}, chunksBetween(6, 6, Ascending))
	})
}
