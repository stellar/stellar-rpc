package events

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemBitmaps_AddToAndGet(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("transfer"), FieldTopic0)

	require.NoError(t, s.AddTo(key, 0))
	require.NoError(t, s.AddTo(key, 1))
	require.NoError(t, s.AddTo(key, 2))

	bm, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(3), bm.GetCardinality())
	assert.True(t, bm.Contains(0))
	assert.True(t, bm.Contains(1))
	assert.True(t, bm.Contains(2))
}

func TestMemBitmaps_GetMissing(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("missing"), FieldTopic0)
	bm, err := s.Get(key)
	require.NoError(t, err)
	assert.Nil(t, bm)
}

func TestMemBitmaps_ListMode(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("sparse"), FieldTopic0)

	for i := range uint32(promotionThreshold - 1) {
		require.NoError(t, s.AddTo(key, i))
	}

	te := s.terms[key]
	require.NotNil(t, te)
	assert.Nil(t, te.bm)
	assert.Len(t, te.ids, promotionThreshold-1)

	bm, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(promotionThreshold-1), bm.GetCardinality())

	// Get should not have promoted list.
	assert.Nil(t, te.bm)
}

func TestMemBitmaps_Promotion(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("dense"), FieldTopic0)

	for i := range uint32(promotionThreshold) {
		require.NoError(t, s.AddTo(key, i))
	}

	te := s.terms[key]
	require.NotNil(t, te)
	assert.NotNil(t, te.bm)
	assert.Nil(t, te.ids)
	assert.Equal(t, uint64(promotionThreshold), te.bm.GetCardinality())
}

func TestMemBitmaps_AddAfterPromotion(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("dense"), FieldTopic0)

	for i := range uint32(promotionThreshold) {
		require.NoError(t, s.AddTo(key, i))
	}
	require.NoError(t, s.AddTo(key, 1000))
	require.NoError(t, s.AddTo(key, 2000))

	bm, err := s.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(promotionThreshold+2), bm.GetCardinality())
	assert.True(t, bm.Contains(1000))
	assert.True(t, bm.Contains(2000))
}

func TestMemBitmaps_Len(t *testing.T) {
	s := newMemBitmaps()
	assert.Equal(t, int64(0), s.Len())

	keyA := ComputeTermKey([]byte("a"), FieldTopic0)
	keyB := ComputeTermKey([]byte("b"), FieldTopic1)

	require.NoError(t, s.AddTo(keyA, 0))
	assert.Equal(t, int64(1), s.Len())

	require.NoError(t, s.AddTo(keyA, 1)) // same term
	assert.Equal(t, int64(1), s.Len())

	require.NoError(t, s.AddTo(keyB, 2))
	assert.Equal(t, int64(2), s.Len())
}

func TestMemBitmaps_Iterate(t *testing.T) {
	s := newMemBitmaps()

	keyA := ComputeTermKey([]byte("a"), FieldTopic0)
	keyB := ComputeTermKey([]byte("b"), FieldTopic1)

	require.NoError(t, s.AddTo(keyA, 0))
	require.NoError(t, s.AddTo(keyA, 1))
	require.NoError(t, s.AddTo(keyB, 2))

	visited := make(map[TermKey]uint64)
	for key, bm := range s.All() {
		visited[key] = bm.GetCardinality()
	}

	assert.Len(t, visited, 2)
	assert.Equal(t, uint64(2), visited[keyA])
	assert.Equal(t, uint64(1), visited[keyB])
}

func TestMemBitmaps_IterateMixed(t *testing.T) {
	s := newMemBitmaps()

	sparseKey := ComputeTermKey([]byte("sparse"), FieldTopic0)
	denseKey := ComputeTermKey([]byte("dense"), FieldTopic0)

	// Sparse: stays in list mode.
	require.NoError(t, s.AddTo(sparseKey, 0))
	require.NoError(t, s.AddTo(sparseKey, 1))

	// Dense: promoted to bitmap mode.
	for i := range uint32(promotionThreshold + 10) {
		require.NoError(t, s.AddTo(denseKey, 100+i))
	}

	visited := make(map[TermKey]uint64)
	for key, bm := range s.All() {
		visited[key] = bm.GetCardinality()
	}

	assert.Len(t, visited, 2)
	assert.Equal(t, uint64(2), visited[sparseKey])
	assert.Equal(t, uint64(promotionThreshold+10), visited[denseKey])
}

func TestMemBitmaps_IterateEarlyStop(t *testing.T) {
	s := newMemBitmaps()

	for i := range 10 {
		key := ComputeTermKey([]byte{byte(i)}, FieldTopic0)
		require.NoError(t, s.AddTo(key, uint32(i)))
	}

	var count int
	for range s.All() {
		count++
		if count >= 3 {
			break
		}
	}

	assert.Equal(t, 3, count)

	// Early stop must leave the store in a usable read state.
	bm, err := s.Get(ComputeTermKey([]byte{0}, FieldTopic0))
	require.NoError(t, err)
	require.NotNil(t, bm)
}

func TestMemBitmaps_BatchAddTo(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("batch"), FieldTopic0)

	require.NoError(t, s.AddTo(key, 0, 1, 2, 3, 4))

	bm, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(5), bm.GetCardinality())
	assert.True(t, bm.Contains(0))
	assert.True(t, bm.Contains(4))
}

func TestMemBitmaps_BatchAddToPromotion(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("batch-promote"), FieldTopic0)

	// Single batch call that crosses threshold.
	ids := make([]uint32, promotionThreshold+10)
	for i := range ids {
		ids[i] = uint32(i)
	}
	require.NoError(t, s.AddTo(key, ids...))

	te := s.terms[key]
	require.NotNil(t, te)
	assert.NotNil(t, te.bm)
	assert.Nil(t, te.ids)
	assert.Equal(t, uint64(promotionThreshold+10), te.bm.GetCardinality())
}

func TestMemBitmaps_GetReturnsClone(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("clone"), FieldTopic0)

	// Promote to bitmap mode.
	for i := range uint32(promotionThreshold) {
		require.NoError(t, s.AddTo(key, i))
	}

	// Mutating the returned bitmap should not affect the store.
	bm1, err := s.Get(key)
	require.NoError(t, err)
	bm1.Add(99999)

	bm2, err := s.Get(key)
	require.NoError(t, err)
	assert.False(t, bm2.Contains(99999))
	assert.Equal(t, uint64(promotionThreshold), bm2.GetCardinality())
}

// TestMemBitmaps_GetReturnsCloneInSparseMode pins that Get clones
// even for sparse (list-mode) terms — the sparse path builds a fresh
// bitmap from the stored id slice, so by construction it can't alias
// the live store. Locks that contract.
func TestMemBitmaps_GetReturnsCloneInSparseMode(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("sparse"), FieldTopic0)

	// Stay below promotion threshold so the term lives as []uint32.
	require.NoError(t, s.AddTo(key, 0, 1, 2))

	bm1, err := s.Get(key)
	require.NoError(t, err)
	bm1.Add(99999)

	bm2, err := s.Get(key)
	require.NoError(t, err)
	assert.False(t, bm2.Contains(99999))
	assert.Equal(t, uint64(3), bm2.GetCardinality())
}

// TestMemBitmaps_All_ConcurrentGetIsSafe runs many concurrent Get
// callers against the same store while one goroutine iterates via
// All. All holds an RLock for its iteration body; Get also takes
// RLock and clones. Both can run concurrently. The test fails under
// race detector if either path returns shared mutable state.
func TestMemBitmaps_All_ConcurrentGetIsSafe(t *testing.T) {
	s := newMemBitmaps()
	const nTerms = 200
	keys := make([]TermKey, nTerms)
	for i := range nTerms {
		k := ComputeTermKey([]byte{byte(i / 256), byte(i % 256)}, FieldTopic0)
		keys[i] = k
		// Promote some terms to bitmap mode; leave others sparse.
		idCount := uint32(promotionThreshold + 1)
		if i%2 == 0 {
			idCount = 3
		}
		ids := make([]uint32, idCount)
		for j := range ids {
			ids[j] = uint32(j)
		}
		require.NoError(t, s.AddTo(k, ids...))
	}

	const numReaders = 8
	var wg sync.WaitGroup
	for r := range numReaders {
		wg.Go(func() {
			for it := range 50 {
				if it%2 == 0 {
					// Iterate via All; mutate clones, not yielded
					// live pointers.
					for _, bm := range s.All() {
						bmClone := bm.Clone()
						bmClone.Add(uint32(999_000 + r))
					}
					continue
				}
				// Get every key; mutate the returned bitmap.
				for _, k := range keys {
					bm, err := s.Get(k)
					require.NoError(t, err)
					require.NotNil(t, bm)
					bm.Add(uint32(999_000 + r))
				}
			}
		})
	}
	wg.Wait()

	// Sanity check: the live store is unchanged by all that mutation.
	for _, k := range keys {
		bm, err := s.Get(k)
		require.NoError(t, err)
		assert.False(t, bm.Contains(999_000),
			"mutation of a Get-returned bitmap must not leak into the store")
	}
}

func TestMemBitmaps_ConcurrentReadWrite(t *testing.T) {
	s := newMemBitmaps()

	const numTerms = 100
	const numEvents = 10_000
	const numReaders = 4

	keys := make([]TermKey, numTerms)
	for i := range keys {
		keys[i] = ComputeTermKey([]byte{byte(i)}, FieldTopic0)
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range uint32(numEvents) {
			require.NoError(t, s.AddTo(keys[i%numTerms], i))
		}
	})

	for range numReaders {
		wg.Go(func() {
			for i := range numEvents {
				_, _ = s.Get(keys[i%numTerms])
				_ = s.Len()
			}
		})
	}

	wg.Wait()

	assert.Equal(t, int64(numTerms), s.Len())
	for _, key := range keys {
		bm, err := s.Get(key)
		require.NoError(t, err)
		require.NotNil(t, bm)
		assert.Equal(t, uint64(numEvents/numTerms), bm.GetCardinality())
	}
}
