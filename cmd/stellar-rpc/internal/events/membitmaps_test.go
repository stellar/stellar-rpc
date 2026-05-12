package events

import (
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
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

func TestMemBitmaps_Put(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("loaded"), FieldTopic0)

	bm := roaring.BitmapOf(10, 20, 30)
	require.NoError(t, s.Put(key, bm))

	result, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint64(3), result.GetCardinality())
	assert.True(t, result.Contains(10))
	assert.Equal(t, int64(1), s.Len())
}

func TestMemBitmaps_PutOverwrite(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("term"), FieldTopic0)

	require.NoError(t, s.Put(key, roaring.BitmapOf(1, 2)))
	require.NoError(t, s.Put(key, roaring.BitmapOf(3, 4, 5)))

	assert.Equal(t, int64(1), s.Len())
	bm, err := s.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), bm.GetCardinality())
}

func TestMemBitmaps_Delete(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("term"), FieldTopic0)

	require.NoError(t, s.AddTo(key, 0))
	assert.Equal(t, int64(1), s.Len())

	require.NoError(t, s.Delete(key))
	bm, err := s.Get(key)
	require.NoError(t, err)
	assert.Nil(t, bm)
	assert.Equal(t, int64(0), s.Len())
}

func TestMemBitmaps_DeleteMissing(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("missing"), FieldTopic0)
	require.NoError(t, s.Delete(key))
	assert.Equal(t, int64(0), s.Len())
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

	// Verify the lock was released — Get should not deadlock.
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

func TestMemBitmaps_GetAfterCloseSkipsClone(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("frozen"), FieldTopic0)

	// Promote to bitmap mode.
	for i := range uint32(promotionThreshold) {
		require.NoError(t, s.AddTo(key, i))
	}

	require.NoError(t, s.Close())

	// After close, Get returns the live pointer (same instance each call).
	bm1, err := s.Get(key)
	require.NoError(t, err)
	bm2, err := s.Get(key)
	require.NoError(t, err)
	assert.Same(t, bm1, bm2)
}

func TestMemBitmaps_Close_RejectsWrites(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("transfer"), FieldTopic0)

	// Writes succeed before close.
	require.NoError(t, s.AddTo(key, 1, 2, 3))

	require.NoError(t, s.Close())

	// All mutating methods return ErrClosed.
	require.ErrorIs(t, s.AddTo(key, 4), ErrClosed)
	require.ErrorIs(t, s.Put(key, roaring.New()), ErrClosed)
	require.ErrorIs(t, s.Delete(key), ErrClosed)

	// Reads still work.
	bm, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(3), bm.GetCardinality())
}

func TestMemBitmaps_Close_AllStillWorks(t *testing.T) {
	s := newMemBitmaps()
	for i := range 10 {
		key := ComputeTermKey([]byte{byte(i)}, FieldTopic0)
		require.NoError(t, s.AddTo(key, uint32(i)))
	}

	require.NoError(t, s.Close())

	var count int
	for range s.All() {
		count++
	}
	assert.Equal(t, 10, count)
}

func TestMemBitmaps_Close_Idempotent(t *testing.T) {
	s := newMemBitmaps()
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

func TestMemBitmaps_Close_AllowsConcurrentReads(t *testing.T) {
	// After Close, All() doesn't acquire the lock, so multiple readers
	// can iterate without contention.
	s := newMemBitmaps()
	for i := range 100 {
		key := ComputeTermKey([]byte{byte(i)}, FieldTopic0)
		require.NoError(t, s.AddTo(key, uint32(i)))
	}
	require.NoError(t, s.Close())

	const numReaders = 8
	var wg sync.WaitGroup
	for range numReaders {
		wg.Go(func() {
			for range 100 {
				var count int
				for range s.All() {
					count++
				}
				assert.Equal(t, 100, count)
			}
		})
	}
	wg.Wait()
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
