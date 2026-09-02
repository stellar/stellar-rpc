package event

import (
	"encoding/binary"
	"math"
	"slices"
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

// TestAppendRecordIDs_ShortTail pins the guard the sealed-run probe's walk
// leans on: a fragment shorter than the 16-byte term key is an error, not an
// over-read of the window buffer.
func TestAppendRecordIDs_ShortTail(t *testing.T) {
	var term TermKey
	term[0] = 0xaa
	rec := AppendTermPostings(nil, term, []uint32{3, 4})
	for n := range 16 {
		ids, adv, err := appendRecordIDs(nil, rec[:n])
		require.ErrorContains(t, err, "want 16-byte term", "%d-byte fragment", n)
		assert.Zero(t, adv)
		assert.Nil(t, ids)
	}
	ids, adv, err := appendRecordIDs(nil, rec)
	require.NoError(t, err)
	assert.Equal(t, []uint32{3, 4}, ids)
	assert.Equal(t, len(rec), adv)
}

// FuzzAppendRecordIDs is the pin that lets a SECOND decoder exist beside the
// shared codec (codec.go's one-definition-site rule: the near-2^64 wrap fix
// once had to be applied to both). Over arbitrary bytes, appendRecordIDs and
// decodePostings — which delegates to runspill.DecodeAscendingIDs, THE
// validation core — must agree on accept-vs-reject, on the ids produced, and
// on how many bytes the record spans. That is a stronger guarantee over the
// input space than shared code plus table tests.
func FuzzAppendRecordIDs(f *testing.F) {
	var term TermKey
	term[0] = 0xaa
	rec := func(tail ...byte) []byte {
		return append(append([]byte{}, term[:]...), tail...)
	}
	seq := make([]uint32, 300)
	for i := range seq {
		seq[i] = uint32(i) * 3
	}
	// Real records, built by the package's own encoder.
	for _, ids := range [][]uint32{
		{0},
		{7},
		{0, 1, 2, 500},
		{3, 4, 8},
		{1 << 20, 1<<20 + 1, 1 << 21},
		{0, math.MaxUint32},
		{math.MaxUint32},
		seq,
	} {
		f.Add(AppendTermPostings(nil, term, ids))
	}
	// Two records back to back: only the first may be consumed.
	f.Add(AppendTermPostings(AppendTermPostings(nil, term, []uint32{1, 2}), term, []uint32{3}))
	// Structural corruption: empty, short tails, a count of zero, a count
	// past the remaining bytes, a zero delta, and a delta near 2^64 (the
	// wrap the raw-varint reject exists for).
	f.Add([]byte{})
	f.Add(make([]byte, 15))
	f.Add(make([]byte, 16))
	f.Add(rec(0x00))
	f.Add(rec(0x7f, 0x01))
	f.Add(rec(0x02, 0x03, 0x00))
	f.Add(rec(append([]byte{0x01}, binary.AppendUvarint(nil, math.MaxUint64)...)...))
	f.Add(rec(append([]byte{0x02, 0x01}, binary.AppendUvarint(nil, math.MaxUint64-1)...)...))
	f.Add(rec(append([]byte{0x02, 0x01}, binary.AppendUvarint(nil, math.MaxUint32)...)...))

	f.Fuzz(func(t *testing.T, in []byte) {
		got, n, err := appendRecordIDs(nil, in)
		if len(in) < 16 {
			require.Error(t, err, "a fragment shorter than a term key must be rejected")
			return
		}
		want, rest, wantErr := decodePostings(in[16:], nil)
		if wantErr != nil {
			require.Error(t, err, "the shared codec rejected what the fused decoder accepted")
			return
		}
		require.NoError(t, err, "the fused decoder rejected what the shared codec accepted")
		require.Equal(t, want, got, "ids differ from the shared codec's")
		require.Equal(t, len(in)-len(rest), n, "record length differs from the bytes the codec consumed")
		require.Equal(t, PackedRecordLen(in), n, "record length differs from PackedRecordLen's walk")

		// The accumulator contract: ids land after whatever dst already
		// holds, and nothing before them moves.
		pre := []uint32{9, 9, 9}
		out, n2, err2 := appendRecordIDs(slices.Clone(pre), in)
		require.NoError(t, err2)
		require.Equal(t, n, n2)
		require.Equal(t, append(slices.Clone(pre), want...), out)
	})
}
