package events

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestConcurrentBitmaps builds an empty ConcurrentBitmaps via the
// only remaining constructor (production always converts from a
// warmup/backfill-built Bitmaps).
func newTestConcurrentBitmaps() *ConcurrentBitmaps {
	return NewConcurrentBitmapsFromBitmaps(NewBitmaps())
}

func TestConcurrentBitmaps_AddToAndGet(t *testing.T) {
	s := newTestConcurrentBitmaps()
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

func TestConcurrentBitmaps_GetMissing(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("missing"), FieldTopic0)
	bm, err := s.Get(key)
	require.NoError(t, err)
	assert.Nil(t, bm)
}

func TestConcurrentBitmaps_ListMode(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("sparse"), FieldTopic0)

	for i := range uint32(promotionThreshold - 1) {
		s.AddTo(key, i)
	}

	st := s.terms[key]
	require.NotNil(t, st)
	assert.Nil(t, st.bm, "must still be in list mode")
	assert.Len(t, st.ids, promotionThreshold-1)

	bm, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(promotionThreshold-1), bm.GetCardinality())

	// Get must not have promoted.
	assert.Nil(t, s.terms[key].bm)
}

func TestConcurrentBitmaps_Promotion(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("dense"), FieldTopic0)

	for i := range uint32(promotionThreshold) {
		s.AddTo(key, i)
	}

	st := s.terms[key]
	require.NotNil(t, st)
	require.NotNil(t, st.bm)
	assert.Empty(t, st.ids, "sparse ids cleared after promotion")
	assert.Equal(t, uint64(promotionThreshold), st.bm.GetCardinality())
}

func TestConcurrentBitmaps_AddAfterPromotion(t *testing.T) {
	s := newTestConcurrentBitmaps()
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

func TestConcurrentBitmaps_BatchAddTo(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("batch"), FieldTopic0)

	s.AddTo(key, 0, 1, 2, 3, 4)

	bm, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(5), bm.GetCardinality())
	assert.True(t, bm.Contains(0))
	assert.True(t, bm.Contains(4))
}

func TestConcurrentBitmaps_BatchAddToPromotion(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("batch-promote"), FieldTopic0)

	// Single batch call that crosses threshold.
	ids := make([]uint32, promotionThreshold+10)
	for i := range ids {
		ids[i] = uint32(i)
	}
	s.AddTo(key, ids...)

	st := s.terms[key]
	require.NotNil(t, st)
	require.NotNil(t, st.bm, "single-batch over threshold must promote immediately")
	assert.Equal(t, uint64(promotionThreshold+10), st.bm.GetCardinality())
}

// TestConcurrentBitmaps_GetReturnsImmutableSnapshot pins the
// clone-on-read contract: a bitmap returned by Get is an owned
// point-in-time copy, so a subsequent AddTo (which mutates the
// mirror's bitmap in place) does NOT change the previously-returned
// clone. This is the key invariant readers can rely on.
func TestConcurrentBitmaps_GetReturnsImmutableSnapshot(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("borrow"), FieldTopic0)

	// Promote to bitmap mode.
	for i := range uint32(promotionThreshold) {
		s.AddTo(key, i)
	}

	before, err := s.Get(key)
	require.NoError(t, err)
	beforeCard := before.GetCardinality()

	// A later AddTo mutates the mirror in place.
	s.AddTo(key, 9_999_999)

	// before is an independent clone; it keeps the pre-AddTo cardinality.
	assert.Equal(t, beforeCard, before.GetCardinality(),
		"the clone returned by Get must be unaffected by a later AddTo")

	after, err := s.Get(key)
	require.NoError(t, err)
	assert.True(t, after.Contains(9_999_999),
		"a fresh Get must observe the in-place AddTo")
}

// TestConcurrentBitmaps_ConcurrentGetIsSafe runs many concurrent
// Get callers against the same store. Get holds the RLock and
// returns an owned clone, so concurrent reads should not race. Run
// under -race.
func TestConcurrentBitmaps_ConcurrentGetIsSafe(t *testing.T) {
	s := newTestConcurrentBitmaps()
	const nTerms = 200
	keys := make([]TermKey, nTerms)
	for i := range nTerms {
		k := ComputeTermKey([]byte{byte(i / 256), byte(i % 256)}, FieldTopic0)
		keys[i] = k
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
			for range 50 {
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

// TestConcurrentBitmaps_ConcurrentReadWrite exercises the RWMutex
// contract under a single writer and many readers. Readers take the
// RLock and operate on their own clones while the writer mutates the
// mirror in place under the write lock. Under -race no data races
// should be reported.
func TestConcurrentBitmaps_ConcurrentReadWrite(t *testing.T) {
	s := newTestConcurrentBitmaps()

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
			}
		})
	}

	wg.Wait()

	for _, key := range keys {
		bm, err := s.Get(key)
		require.NoError(t, err)
		require.NotNil(t, bm)
		assert.Equal(t, uint64(numEvents/numTerms), bm.GetCardinality())
	}
}

// TestConcurrentBitmaps_GetDuringPromotionNeverReturnsNil pins
// concurrent-reader safety across the sparse→dense promotion
// transition. The RWMutex serializes the writer's in-place promotion
// (ids → bm) against every reader, so a reader can never observe a
// half-promoted (bm=nil, ids=empty) state. The test still has value
// as a -race probe: many readers calling Get while a writer drives
// terms across the promotion boundary should never produce a nil
// bitmap and should never trip the race detector.
func TestConcurrentBitmaps_GetDuringPromotionNeverReturnsNil(t *testing.T) {
	s := newTestConcurrentBitmaps()
	const numKeys = 200

	keys := make([]TermKey, numKeys)
	for i := range numKeys {
		keys[i] = ComputeTermKey([]byte{byte(i / 256), byte(i % 256)}, FieldTopic0)
	}

	// Seed each term with promotionThreshold-1 ids: sparse mode,
	// one event away from promotion.
	for _, k := range keys {
		ids := make([]uint32, promotionThreshold-1)
		for j := range ids {
			ids[j] = uint32(j)
		}
		s.AddTo(k, ids...)
	}

	var wg sync.WaitGroup
	const numReaders = 8

	stop := make(chan struct{})

	// Writer goroutine: trigger promotion on each key by appending
	// one more event each cycle. After all keys promote it stops.
	wg.Go(func() {
		defer close(stop)
		for i, k := range keys {
			s.AddTo(k, uint32(promotionThreshold-1+i))
		}
	})

	for range numReaders {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, k := range keys {
					bm, err := s.Get(k)
					require.NoError(t, err)
					// The term was seeded with promotionThreshold-1
					// ids and the writer only appends — Get must
					// always observe a non-nil bitmap.
					require.NotNil(t, bm, "Get returned nil during promotion window")
				}
			}
		})
	}
	wg.Wait()
}

// TestConcurrentBitmaps_AddToIsIdempotent pins the dedup contract:
// AddTo can be called multiple times with the same eventID for the
// same key and the result is the same as adding it once. Covers
// both list mode (sorted-prefix check) and bitmap mode (roaring's
// set semantics).
func TestConcurrentBitmaps_AddToIsIdempotent(t *testing.T) {
	t.Run("list mode", func(t *testing.T) {
		s := newTestConcurrentBitmaps()
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
		s := newTestConcurrentBitmaps()
		key := ComputeTermKey([]byte("dense"), FieldTopic0)

		// Force bitmap mode by exceeding the threshold.
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}
		require.NotNil(t, s.terms[key].bm, "must have promoted to bitmap mode")

		// Replay — bitmap.AddMany is set-semantic, so no cardinality change.
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}

		bm, err := s.Get(key)
		require.NoError(t, err)
		assert.Equal(t, uint64(promotionThreshold), bm.GetCardinality())
	})
}

// TestConcurrentBitmaps_DenseAddToIsInPlace pins the new write
// contract: on a dense term, AddTo mutates the stored bitmap in place
// (same pointer, grown cardinality) rather than cloning-and-replacing
// it, and copy-on-write stays off. It also checks that a bitmap a
// reader took via an earlier Get is an independent clone, untouched by
// the later in-place write.
func TestConcurrentBitmaps_DenseAddToIsInPlace(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("inplace"), FieldTopic0)
	for i := range uint32(promotionThreshold) {
		s.AddTo(key, i)
	}

	st := s.terms[key]
	require.NotNil(t, st)
	require.NotNil(t, st.bm)
	bmPtr := st.bm
	cardBefore := bmPtr.GetCardinality()
	assert.False(t, bmPtr.GetCopyOnWrite(),
		"stored dense bitmap must have CopyOnWrite disabled")

	// A clone taken now must stay independent of later writes.
	earlier, err := s.Get(key)
	require.NoError(t, err)
	earlierCard := earlier.GetCardinality()

	s.AddTo(key, 10_000)

	// Same underlying bitmap, mutated in place.
	assert.Same(t, bmPtr, s.terms[key].bm,
		"AddTo must mutate the dense bitmap in place, not replace it")
	assert.Equal(t, cardBefore+1, s.terms[key].bm.GetCardinality())
	assert.False(t, s.terms[key].bm.GetCopyOnWrite(),
		"in-place AddTo must not flip CopyOnWrite on")

	// The earlier Get result is an independent clone.
	assert.Equal(t, earlierCard, earlier.GetCardinality(),
		"a bitmap returned by an earlier Get must be unaffected by a later AddTo")
	assert.False(t, earlier.Contains(10_000))
}

// TestNewConcurrentBitmapsFromBitmaps_DirectlyPinsContract verifies
// the warmup-side conversion constructor:
//   - input bitmaps survive in the result with their cardinality
//     intact;
//   - copy-on-write stays off, which is what clone-on-read requires;
//   - nil bitmaps in the input map are skipped (not panicked);
//   - subsequent AddTo on the result adds to the stored bitmap in
//     place (no clone-and-replace on the popular-term ingest path).
func TestNewConcurrentBitmapsFromBitmaps_DirectlyPinsContract(t *testing.T) {
	src := NewBitmaps()
	keyA := ComputeTermKey([]byte("a"), FieldTopic0)
	keyB := ComputeTermKey([]byte("b"), FieldTopic1)
	keyNil := ComputeTermKey([]byte("nil"), FieldTopic2)

	src.AddTo(keyA, 0, 1, 2, 3, 4)
	src.AddTo(keyB, 100, 101)
	src[keyNil] = nil // simulate a nil entry that the constructor must skip

	cb := NewConcurrentBitmapsFromBitmaps(src)

	bmA, err := cb.Get(keyA)
	require.NoError(t, err)
	require.NotNil(t, bmA)
	assert.Equal(t, uint64(5), bmA.GetCardinality())
	assert.False(t, bmA.GetCopyOnWrite(),
		"FromBitmaps must keep CopyOnWrite off so clone-on-read is race-free")

	bmB, err := cb.Get(keyB)
	require.NoError(t, err)
	require.NotNil(t, bmB)
	assert.Equal(t, uint64(2), bmB.GetCardinality())
	assert.False(t, bmB.GetCopyOnWrite())

	bmNil, err := cb.Get(keyNil)
	require.NoError(t, err)
	assert.Nil(t, bmNil, "nil source entries must be skipped, not panicked")

	// Subsequent AddTo on the converted index grows the stored bitmap
	// in place, with CopyOnWrite still off.
	cb.AddTo(keyA, 5)
	post := cb.terms[keyA].bm
	require.NotNil(t, post)
	assert.False(t, post.GetCopyOnWrite())
	assert.Equal(t, uint64(6), post.GetCardinality())
}
