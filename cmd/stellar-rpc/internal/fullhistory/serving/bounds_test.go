package serving

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// admittedRange builds an Admission with floor chunk 5 (oldest ledger 50002) and
// the given watermark — bounds tests read only those two fields, no catalog or
// snapshot needed.
func admittedRange(latest uint32) *Admission {
	return &Admission{floor: 5, latest: latest}
}

func TestOldestLedger(t *testing.T) {
	a := admittedRange(60000)
	assert.Equal(t, chunk.ID(5).FirstLedger(), a.OldestLedger()) // 50002
}

// floor chunk 5 → oldest 50002; watermark 60000. Admitted range [50002, 60000].
func TestClampRange_Ascending(t *testing.T) {
	a := admittedRange(60000)
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
	a := admittedRange(60000)
	const oldest, latest = 50002, 60000

	t.Run("leading (high) edge below floor is rejected", func(t *testing.T) {
		_, _, err := a.ClampRange(Descending, 40000, 45000)
		var re *RangeError
		require.ErrorAs(t, err, &re)
		assert.Equal(t, uint32(45000), re.Requested, "the high edge is the leading edge descending")
	})

	t.Run("high edge beyond latest is truncated", func(t *testing.T) {
		lo, hi, err := a.ClampRange(Descending, 55000, 70000)
		require.NoError(t, err)
		assert.Equal(t, uint32(55000), lo)
		assert.Equal(t, uint32(latest), hi)
	})

	t.Run("low edge below floor terminates at the floor", func(t *testing.T) {
		lo, hi, err := a.ClampRange(Descending, 40000, 59000)
		require.NoError(t, err) // leading (high) edge 59000 is in range
		assert.Equal(t, uint32(oldest), lo, "scan terminates at the floor")
		assert.Equal(t, uint32(59000), hi)
	})
}

// floor chunk 5 (oldest 50002), latest chunk 7 mid (70500 → chunk 7). Chunks 5..7
// overlap the admitted range.
func TestChunksForRange(t *testing.T) {
	a := admittedRange(70500) // latest in chunk 7

	t.Run("ascending spans the overlapping chunks in order", func(t *testing.T) {
		chunks, err := a.ChunksForRange(Ascending, chunk.ID(5).FirstLedger(), 70500)
		require.NoError(t, err)
		assert.Equal(t, []chunk.ID{5, 6, 7}, chunks)
	})

	t.Run("descending reverses the traversal", func(t *testing.T) {
		chunks, err := a.ChunksForRange(Descending, chunk.ID(5).FirstLedger(), 70500)
		require.NoError(t, err)
		assert.Equal(t, []chunk.ID{7, 6, 5}, chunks)
	})

	t.Run("clamps then traverses: high edge beyond latest stops at latest's chunk", func(t *testing.T) {
		// hi 999999 truncates to latest (chunk 7); lo in chunk 6.
		chunks, err := a.ChunksForRange(Ascending, chunk.ID(6).FirstLedger(), 999999)
		require.NoError(t, err)
		assert.Equal(t, []chunk.ID{6, 7}, chunks)
	})

	t.Run("single chunk", func(t *testing.T) {
		chunks, err := a.ChunksForRange(Ascending, 60005, 60050)
		require.NoError(t, err)
		assert.Equal(t, []chunk.ID{6}, chunks)
	})

	t.Run("leading edge below floor is rejected", func(t *testing.T) {
		_, err := a.ChunksForRange(Ascending, 1000, 60000)
		var re *RangeError
		require.ErrorAs(t, err, &re)
	})

	t.Run("request beyond latest yields no chunks", func(t *testing.T) {
		chunks, err := a.ChunksForRange(Ascending, 80000, 90000)
		require.NoError(t, err)
		assert.Empty(t, chunks)
	})
}
