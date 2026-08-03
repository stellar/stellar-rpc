package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeAll(t *testing.T, val []byte) map[TermKey][]uint32 {
	t.Helper()
	out := map[TermKey][]uint32{}
	require.NoError(t, DecodePackedRow(val, func(term TermKey, ids []uint32) {
		out[term] = append([]uint32(nil), ids...)
	}))
	return out
}

func TestPackedIndexRow_RoundTrip(t *testing.T) {
	var t1, t2 TermKey
	t1[0] = 0x01
	t2[0] = 0x02
	in := map[TermKey][]uint32{
		t1: {0, 1, 2, 500},
		// Non-contiguous, large absolute IDs exercise multi-byte varints.
		t2: {7, 1 << 20, 1<<20 + 1},
	}
	assert.Equal(t, in, decodeAll(t, AppendPackedRow(nil, in)))
}

func TestPackedIndexRow_RejectsCorruption(t *testing.T) {
	var term TermKey
	term[0] = 0xaa
	good := AppendPackedRow(nil, map[TermKey][]uint32{term: {3, 4}})

	nop := func(TermKey, []uint32) {}
	// Truncated term header.
	require.ErrorContains(t, DecodePackedRow(good[:10], nop), "trailing bytes")
	// id count exceeding remaining bytes (truncate the ID area).
	require.ErrorContains(t, DecodePackedRow(good[:len(good)-1], nop), "exceeds")
	// Zero delta = duplicate ID within a term.
	dup := make([]byte, 0, len(term)+3)
	dup = append(dup, term[:]...)
	dup = append(dup, 2 /* count */, 3 /* first id */, 0 /* zero delta */)
	require.ErrorContains(t, DecodePackedRow(dup, nop), "zero delta")
}
