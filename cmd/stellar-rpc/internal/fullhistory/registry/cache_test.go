package registry

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

type fakeReader struct{ id chunk.ID }

// newFakeCache wires a readerCache over fake readers, recording retirements.
func newFakeCache(capacity int) (*readerCache[*fakeReader], *[]chunk.ID) {
	retired := &[]chunk.ID{}
	c := newReaderCache(capacity,
		func(id chunk.ID) (*fakeReader, error) { return &fakeReader{id: id}, nil },
		func(r *fakeReader) { *retired = append(*retired, r.id) },
	)
	return c, retired
}

func TestReaderCache_HitReturnsSameReader(t *testing.T) {
	c, retired := newFakeCache(2)
	r0, err := c.acquire(0)
	require.NoError(t, err)
	again, err := c.acquire(0)
	require.NoError(t, err)
	require.Same(t, r0, again)
	require.Empty(t, *retired)
	require.Equal(t, 1, c.len())
}

func TestReaderCache_EvictionRetiresLRUNeverInline(t *testing.T) {
	c, retired := newFakeCache(2)
	_, err := c.acquire(0)
	require.NoError(t, err)
	_, err = c.acquire(1)
	require.NoError(t, err)
	_, err = c.acquire(0) // refresh 0's recency: 1 is now the LRU
	require.NoError(t, err)

	_, err = c.acquire(2)
	require.NoError(t, err)
	require.Equal(t, []chunk.ID{1}, *retired, "inserting over capacity retires the least recently used")
	require.Equal(t, 2, c.len())

	again, err := c.acquire(1) // 1 was evicted: this is a fresh open, evicting 0 (LRU after 2's insert)
	require.NoError(t, err)
	require.Equal(t, chunk.ID(1), again.id)
	require.Equal(t, []chunk.ID{1, 0}, *retired)
}

func TestReaderCache_DropRetiresOnlyThatChunk(t *testing.T) {
	c, retired := newFakeCache(4)
	for id := range chunk.ID(3) {
		_, err := c.acquire(id)
		require.NoError(t, err)
	}
	c.drop(1)
	require.Equal(t, []chunk.ID{1}, *retired)
	require.Equal(t, 2, c.len())
	c.drop(1) // absent: no-op
	require.Equal(t, []chunk.ID{1}, *retired)
}

func TestReaderCache_DropBelowRetiresBelowFloorOnly(t *testing.T) {
	c, retired := newFakeCache(8)
	for id := range chunk.ID(5) {
		_, err := c.acquire(id)
		require.NoError(t, err)
	}
	c.dropBelow(3)
	require.ElementsMatch(t, []chunk.ID{0, 1, 2}, *retired)
	require.Equal(t, 2, c.len())
}

func TestReaderCache_DropAllEmptiesTheCache(t *testing.T) {
	c, retired := newFakeCache(8)
	for id := range chunk.ID(3) {
		_, err := c.acquire(id)
		require.NoError(t, err)
	}
	c.dropAll()
	require.ElementsMatch(t, []chunk.ID{0, 1, 2}, *retired)
	require.Zero(t, c.len())
}

func TestReaderCache_OpenErrorIsNotCached(t *testing.T) {
	boom := errors.New("boom")
	fail := true
	c := newReaderCache(2,
		func(id chunk.ID) (*fakeReader, error) {
			if fail {
				return nil, boom
			}
			return &fakeReader{id: id}, nil
		},
		func(*fakeReader) {},
	)
	_, err := c.acquire(7)
	require.ErrorIs(t, err, boom)
	require.Zero(t, c.len())

	fail = false
	r, err := c.acquire(7)
	require.NoError(t, err)
	require.Equal(t, chunk.ID(7), r.id)
}
