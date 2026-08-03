package txhash

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeRow_RoundTripSorted(t *testing.T) {
	rng := testRNG(0x11)
	hashes := make([][32]byte, 500)
	for i := range hashes {
		hashes[i] = randHash(rng)
	}
	row, err := EncodeRow(hashes)
	require.NoError(t, err)
	require.Len(t, row, 500*rowHashLen)

	n, err := validateRow(row)
	require.NoError(t, err)
	assert.Equal(t, 500, n)
	for i := 1; i < n; i++ {
		prev, cur := row[(i-1)*rowHashLen:i*rowHashLen], row[i*rowHashLen:(i+1)*rowHashLen]
		require.Negative(t, bytes.Compare(prev, cur), "row not strictly ascending at %d", i)
	}
	for _, h := range hashes {
		require.True(t, rowContains(row, h), "hash %x lost by the codec", h)
	}
	assert.False(t, rowContains(row, randHash(rng)))
}

func TestEncodeRow_EmptyLedger(t *testing.T) {
	row, err := EncodeRow(nil)
	require.NoError(t, err)
	assert.Empty(t, row)
	n, err := validateRow(row)
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestEncodeRow_RejectsIntraLedgerDuplicate(t *testing.T) {
	h1, h2 := [32]byte{1}, [32]byte{2}
	_, err := EncodeRow([][32]byte{h1, h2, h1})
	require.ErrorContains(t, err, "duplicate")
}

func TestEncodeRow_CountBounds(t *testing.T) {
	over := make([][32]byte, maxRowHashes+1)
	for i := range over {
		binary.BigEndian.PutUint32(over[i][:4], uint32(i))
	}
	_, err := EncodeRow(over)
	require.ErrorContains(t, err, "max")

	row, err := EncodeRow(over[:maxRowHashes])
	require.NoError(t, err, "exactly maxRowHashes is legal")
	n, err := validateRow(row)
	require.NoError(t, err)
	assert.Equal(t, maxRowHashes, n)
}

func TestValidateRow_RejectsMalformedRows(t *testing.T) {
	a, b := [32]byte{1}, [32]byte{2}

	_, err := validateRow(make([]byte, rowHashLen+1))
	require.ErrorContains(t, err, "multiple")

	_, err = validateRow(slices.Concat(b[:], a[:]))
	require.ErrorContains(t, err, "ascending")

	_, err = validateRow(slices.Concat(a[:], a[:]))
	require.ErrorContains(t, err, "ascending", "an intra-row duplicate is an ordering violation")
}
