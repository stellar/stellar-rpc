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

	s.AddTo(key, 0)
	s.AddTo(key, 1)
	s.AddTo(key, 2)

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
		s.AddTo(key, i)
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
		s.AddTo(key, i)
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
		s.AddTo(key, i)
	}
	s.AddTo(key, 1000)
	s.AddTo(key, 2000)

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

	s.AddTo(keyA, 0)
	assert.Equal(t, int64(1), s.Len())

	s.AddTo(keyA, 1) // same term
	assert.Equal(t, int64(1), s.Len())

	s.AddTo(keyB, 2)
	assert.Equal(t, int64(2), s.Len())
}

func TestMemBitmaps_Iterate(t *testing.T) {
	s := newMemBitmaps()

	keyA := ComputeTermKey([]byte("a"), FieldTopic0)
	keyB := ComputeTermKey([]byte("b"), FieldTopic1)

	s.AddTo(keyA, 0)
	s.AddTo(keyA, 1)
	s.AddTo(keyB, 2)

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
	s.AddTo(sparseKey, 0)
	s.AddTo(sparseKey, 1)

	// Dense: promoted to bitmap mode.
	for i := range uint32(promotionThreshold + 10) {
		s.AddTo(denseKey, 100+i)
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
		s.AddTo(key, uint32(i))
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

	s.AddTo(key, 0, 1, 2, 3, 4)

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
	s.AddTo(key, ids...)

	te := s.terms[key]
	require.NotNil(t, te)
	assert.NotNil(t, te.bm)
	assert.Nil(t, te.ids)
	assert.Equal(t, uint64(promotionThreshold+10), te.bm.GetCardinality())
}

// TestMemBitmaps_DenseGetReturnsLiveReference pins that promoted
// (dense-mode) terms return the borrowed mirror pointer with no
// defensive clone. Callers MUST NOT mutate the returned bitmap (see
// BitmapIndex.Get); this test deliberately violates the contract to
// document the borrow semantics — a future regression that
// re-introduces a Clone would fail loudly here.
func TestMemBitmaps_DenseGetReturnsLiveReference(t *testing.T) {
	s := newMemBitmaps()
	key := ComputeTermKey([]byte("borrow"), FieldTopic0)

	// Promote to bitmap mode.
	for i := range uint32(promotionThreshold) {
		s.AddTo(key, i)
	}

	bm1, err := s.Get(key)
	require.NoError(t, err)
	bm1.Add(99999)

	bm2, err := s.Get(key)
	require.NoError(t, err)
	assert.True(t, bm2.Contains(99999),
		"dense Get must return the live mirror reference; mutation bleeds through (caller obligation: do not mutate)")
}

// TestMemBitmaps_ConcurrentReadOnlyAccessIsSafe runs many concurrent
// Get + All callers against the same store. Both paths take RLock,
// so they can run in parallel; Get returns borrowed pointers (per
// the BitmapIndex.Get contract). This test stays strictly read-only
// — concurrent AddTo with Get is intentionally NOT exercised
// because the borrow contract requires callers to serialise
// writes against reads externally (in the hot store, IngestLedger
// events does this via HotStore.mu). Run under -race to catch any
// data race in the map traversal or bitmap-internal state.
func TestMemBitmaps_ConcurrentReadOnlyAccessIsSafe(t *testing.T) {
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
		s.AddTo(k, ids...)
	}

	const numReaders = 8
	var wg sync.WaitGroup
	for range numReaders {
		wg.Go(func() {
			for it := range 50 {
				if it%2 == 0 {
					// Iterate via All; read-only inspection of yielded
					// pointers (cardinality only — no mutation).
					for _, bm := range s.All() {
						_ = bm.GetCardinality()
					}
					continue
				}
				// Get every key; inspect read-only.
				for _, k := range keys {
					bm, err := s.Get(k)
					require.NoError(t, err)
					require.NotNil(t, bm)
					_ = bm.Contains(0)
				}
			}
		})
	}
	wg.Wait()

	// Sanity check: the store is untouched by all that reading.
	for _, k := range keys {
		bm, err := s.Get(k)
		require.NoError(t, err)
		require.NotNil(t, bm)
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
			s.AddTo(keys[i%numTerms], i)
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

// TestMemBitmaps_AddToIsIdempotent pins the dedup contract: AddTo
// can be called multiple times with the same eventID for the same
// key and the result is the same as adding it once. Covers both
// list mode (uses the sorted-prefix >= check) and bitmap mode
// (uses roaring's set semantics).
func TestMemBitmaps_AddToIsIdempotent(t *testing.T) {
	t.Run("list mode", func(t *testing.T) {
		s := newMemBitmaps()
		key := ComputeTermKey([]byte("sparse"), FieldTopic0)

		// Add a few in order.
		s.AddTo(key, 0)
		s.AddTo(key, 1)
		s.AddTo(key, 2)

		// Replay (simulates a phase-3 retry after partial failure).
		s.AddTo(key, 0)
		s.AddTo(key, 1)
		s.AddTo(key, 2)
		// Also replay multiple at once.
		s.AddTo(key, 1, 2)
		// And add a new one — must still go through.
		s.AddTo(key, 3)

		bm, err := s.Get(key)
		require.NoError(t, err)
		require.NotNil(t, bm)
		assert.Equal(t, uint64(4), bm.GetCardinality())
		for _, id := range []uint32{0, 1, 2, 3} {
			assert.True(t, bm.Contains(id))
		}
	})

	t.Run("bitmap mode", func(t *testing.T) {
		s := newMemBitmaps()
		key := ComputeTermKey([]byte("dense"), FieldTopic0)

		// Force bitmap mode by exceeding the threshold.
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}
		te := s.terms[key]
		require.NotNil(t, te.bm, "must have promoted to bitmap mode")

		// Replay — bitmap.AddMany is set-semantic, so no cardinality change.
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}

		bm, err := s.Get(key)
		require.NoError(t, err)
		assert.Equal(t, uint64(promotionThreshold), bm.GetCardinality())
	})
}
