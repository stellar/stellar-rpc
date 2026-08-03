package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
)

func decodeAll(t *testing.T, val []byte) map[events.TermKey][]uint32 {
	t.Helper()
	out := map[events.TermKey][]uint32{}
	require.NoError(t, events.DecodePackedRow(val, func(term events.TermKey, ids []uint32) {
		out[term] = append([]uint32(nil), ids...)
	}))
	return out
}

func TestPackedIndexRow_RoundTrip(t *testing.T) {
	var t1, t2 events.TermKey
	t1[0] = 0x01
	t2[0] = 0x02
	in := map[events.TermKey][]uint32{
		t1: {0, 1, 2, 500},
		// Non-contiguous, large absolute IDs exercise multi-byte varints.
		t2: {7, 1 << 20, 1<<20 + 1},
	}
	assert.Equal(t, in, decodeAll(t, events.AppendPackedRow(nil, in)))
}

func TestPackedIndexRow_RejectsCorruption(t *testing.T) {
	var term events.TermKey
	term[0] = 0xaa
	good := events.AppendPackedRow(nil, map[events.TermKey][]uint32{term: {3, 4}})

	nop := func(events.TermKey, []uint32) {}
	// Truncated term header.
	require.ErrorContains(t, events.DecodePackedRow(good[:10], nop), "trailing bytes")
	// id count exceeding remaining bytes (truncate the ID area).
	require.ErrorContains(t, events.DecodePackedRow(good[:len(good)-1], nop), "exceeds")
	// Zero delta = duplicate ID within a term.
	dup := make([]byte, 0, len(term)+3)
	dup = append(dup, term[:]...)
	dup = append(dup, 2 /* count */, 3 /* first id */, 0 /* zero delta */)
	require.ErrorContains(t, events.DecodePackedRow(dup, nop), "zero delta")
}
