package events

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostingsCodec_RoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		ids  []uint32
	}{
		{"one posting", []uint32{0}},
		{"contiguous", []uint32{0, 1, 2, 3}},
		// Non-contiguous, large absolute IDs exercise multi-byte varints.
		{"scattered", []uint32{7, 1 << 20, 1<<20 + 1}},
		{"extremes", []uint32{0, math.MaxUint32}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodePostings(AppendPostings(nil, tc.ids))
			require.NoError(t, err)
			assert.Equal(t, tc.ids, got)
		})
	}
}

// TestPostingsCodec_Appends pins that AppendPostings appends rather than
// replaces: index.pack's writer puts the codec byte down first and encodes
// straight after it.
func TestPostingsCodec_Appends(t *testing.T) {
	buf := AppendPostings([]byte{0xff}, []uint32{3, 4})
	require.Equal(t, byte(0xff), buf[0])
	got, err := DecodePostings(buf[1:])
	require.NoError(t, err)
	assert.Equal(t, []uint32{3, 4}, got)
}

func TestPostingsCodec_RejectsCorruption(t *testing.T) {
	good := AppendPostings(nil, []uint32{3, 4})

	// Empty body: no count uvarint at all.
	require.ErrorContains(t, decodeErr(t, nil), "bad id-count uvarint")
	// A zero count cannot describe a term: a stored term always holds at
	// least one posting.
	require.ErrorContains(t, decodeErr(t, []byte{0}), "exceeds")
	// id count exceeding the remaining bytes (truncate the ID area).
	require.ErrorContains(t, decodeErr(t, good[:len(good)-1]), "exceeds")
	// Zero delta = duplicate ID within a term.
	require.ErrorContains(t, decodeErr(t, []byte{2 /* count */, 3 /* first id */, 0}), "zero delta")
	// Trailing bytes mean the record was not written by this codec.
	require.ErrorContains(t, decodeErr(t, append(good, 0x00)), "trailing bytes")

	// A delta that overflows uint32 must be rejected as a raw varint, before
	// it is added to the running absolute: a value near 2^64 would otherwise
	// wrap back under MaxUint32 and smuggle in a non-ascending ID.
	wrap := binary.AppendUvarint([]byte{2 /* count */, 1 /* first id */}, math.MaxUint64)
	require.ErrorContains(t, decodeErr(t, wrap), "overflows uint32")

	// An in-range delta whose running absolute leaves uint32 is rejected too.
	over := binary.AppendUvarint([]byte{2 /* count */}, math.MaxUint32)
	over = binary.AppendUvarint(over, 1)
	require.ErrorContains(t, decodeErr(t, over), "overflows uint32")
}

func decodeErr(t *testing.T, b []byte) error {
	t.Helper()
	_, err := DecodePostings(b)
	require.Error(t, err)
	return err
}
