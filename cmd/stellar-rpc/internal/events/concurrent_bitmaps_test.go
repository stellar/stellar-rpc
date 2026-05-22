package events

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentBitmaps_AddToAndGet(t *testing.T) {
	s := NewConcurrentBitmaps()
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
	s := NewConcurrentBitmaps()
	key := ComputeTermKey([]byte("missing"), FieldTopic0)
	bm, err := s.Get(key)
	require.NoError(t, err)
	assert.Nil(t, bm)
}

func TestConcurrentBitmaps_ListMode(t *testing.T) {
	s := NewConcurrentBitmaps()
	key := ComputeTermKey([]byte("sparse"), FieldTopic0)

	for i := range uint32(promotionThreshold - 1) {
		s.AddTo(key, i)
	}

	te := s.terms[key]
	require.NotNil(t, te)
	assert.Nil(t, te.bm.Load(), "must still be in list mode")
	ids := te.ids.Load()
	require.NotNil(t, ids)
	assert.Len(t, *ids, promotionThreshold-1)

	bm, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(promotionThreshold-1), bm.GetCardinality())

	// Get must not have promoted.
	assert.Nil(t, te.bm.Load())
}

func TestConcurrentBitmaps_Promotion(t *testing.T) {
	s := NewConcurrentBitmaps()
	key := ComputeTermKey([]byte("dense"), FieldTopic0)

	for i := range uint32(promotionThreshold) {
		s.AddTo(key, i)
	}

	te := s.terms[key]
	require.NotNil(t, te)
	bm := te.bm.Load()
	require.NotNil(t, bm)
	ids := te.ids.Load()
	require.NotNil(t, ids)
	assert.Empty(t, *ids, "sparse ids cleared after promotion")
	assert.Equal(t, uint64(promotionThreshold), bm.GetCardinality())
}

func TestConcurrentBitmaps_AddAfterPromotion(t *testing.T) {
	s := NewConcurrentBitmaps()
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

func TestConcurrentBitmaps_Len(t *testing.T) {
	s := NewConcurrentBitmaps()
	assert.Equal(t, 0, s.Len())

	keyA := ComputeTermKey([]byte("a"), FieldTopic0)
	keyB := ComputeTermKey([]byte("b"), FieldTopic1)

	s.AddTo(keyA, 0)
	assert.Equal(t, 1, s.Len())

	s.AddTo(keyA, 1) // same term
	assert.Equal(t, 1, s.Len())

	s.AddTo(keyB, 2)
	assert.Equal(t, 2, s.Len())
}

// TestConcurrentBitmaps_SnapshotIsUniquelyOwned pins that Snapshot
// returns a Bitmaps whose bitmaps are independent of the source
// ConcurrentBitmaps. Mutating a snapshot bitmap must not affect a
// subsequent Snapshot of the same source.
func TestConcurrentBitmaps_SnapshotIsUniquelyOwned(t *testing.T) {
	s := NewConcurrentBitmaps()
	key := ComputeTermKey([]byte("dense"), FieldTopic0)
	for i := range uint32(promotionThreshold) {
		s.AddTo(key, i)
	}

	snap1 := s.Snapshot()
	bm1 := snap1[key]
	require.NotNil(t, bm1)
	bm1.Add(999_999)

	snap2 := s.Snapshot()
	bm2 := snap2[key]
	require.NotNil(t, bm2)
	assert.False(t, bm2.Contains(999_999),
		"mutating a snapshot bitmap must not bleed into the source ConcurrentBitmaps")

	// And the original index is still clean.
	live, err := s.Get(key)
	require.NoError(t, err)
	assert.False(t, live.Contains(999_999),
		"snapshot mutation must not affect the live ConcurrentBitmaps")
}

func TestConcurrentBitmaps_SnapshotIncludesSparseAndDense(t *testing.T) {
	s := NewConcurrentBitmaps()

	sparseKey := ComputeTermKey([]byte("sparse"), FieldTopic0)
	denseKey := ComputeTermKey([]byte("dense"), FieldTopic0)

	// Sparse: stays in list mode.
	s.AddTo(sparseKey, 0)
	s.AddTo(sparseKey, 1)

	// Dense: promoted to bitmap mode.
	for i := range uint32(promotionThreshold + 10) {
		s.AddTo(denseKey, 100+i)
	}

	snap := s.Snapshot()
	assert.Len(t, snap, 2)
	require.NotNil(t, snap[sparseKey])
	require.NotNil(t, snap[denseKey])
	assert.Equal(t, uint64(2), snap[sparseKey].GetCardinality())
	assert.Equal(t, uint64(promotionThreshold+10), snap[denseKey].GetCardinality())
}

func TestConcurrentBitmaps_BatchAddTo(t *testing.T) {
	s := NewConcurrentBitmaps()
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
	s := NewConcurrentBitmaps()
	key := ComputeTermKey([]byte("batch-promote"), FieldTopic0)

	// Single batch call that crosses threshold.
	ids := make([]uint32, promotionThreshold+10)
	for i := range ids {
		ids[i] = uint32(i)
	}
	s.AddTo(key, ids...)

	te := s.terms[key]
	require.NotNil(t, te)
	bm := te.bm.Load()
	require.NotNil(t, bm, "single-batch over threshold must promote immediately")
	assert.Equal(t, uint64(promotionThreshold+10), bm.GetCardinality())
}

// TestConcurrentBitmaps_GetReturnsImmutableSnapshot pins the
// COW-on-write contract: a bitmap returned by Get is an immutable
// snapshot, so a subsequent AddTo (which produces a new snapshot
// via atomic.Store) does NOT mutate the previously-returned
// pointer. This is the key invariant readers can rely on across the
// borrow.
func TestConcurrentBitmaps_GetReturnsImmutableSnapshot(t *testing.T) {
	s := NewConcurrentBitmaps()
	key := ComputeTermKey([]byte("borrow"), FieldTopic0)

	// Promote to bitmap mode.
	for i := range uint32(promotionThreshold) {
		s.AddTo(key, i)
	}

	before, err := s.Get(key)
	require.NoError(t, err)
	beforeCard := before.GetCardinality()

	// New AddTo publishes a new snapshot via atomic.Store.
	s.AddTo(key, 9_999_999)

	// before still observes the pre-AddTo cardinality.
	assert.Equal(t, beforeCard, before.GetCardinality(),
		"AddTo published a new snapshot; the borrowed pointer must remain unchanged")

	after, err := s.Get(key)
	require.NoError(t, err)
	assert.True(t, after.Contains(9_999_999),
		"subsequent Get must observe the new snapshot")
}

// TestConcurrentBitmaps_ConcurrentReadOnlyAccessIsSafe runs many
// concurrent Get + Snapshot callers against the same store. Both
// paths take RLock briefly and then return immutable snapshots. Run
// under -race to catch any data race in map traversal or
// bitmap-internal state.
func TestConcurrentBitmaps_ConcurrentReadOnlyAccessIsSafe(t *testing.T) {
	s := NewConcurrentBitmaps()
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
			for it := range 50 {
				if it%2 == 0 {
					for _, bm := range s.Snapshot() {
						_ = bm.GetCardinality()
					}
					continue
				}
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

// TestConcurrentBitmaps_ConcurrentReadWrite exercises the COW
// contract under a single writer and many readers. Readers atomic-
// Load the current snapshot and operate on it independently while
// the writer publishes new snapshots; no clones or locks span the
// borrow. Under -race no data races should be reported.
func TestConcurrentBitmaps_ConcurrentReadWrite(t *testing.T) {
	s := NewConcurrentBitmaps()

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

	assert.Equal(t, numTerms, s.Len())
	for _, key := range keys {
		bm, err := s.Get(key)
		require.NoError(t, err)
		require.NotNil(t, bm)
		assert.Equal(t, uint64(numEvents/numTerms), bm.GetCardinality())
	}
}

// TestConcurrentBitmaps_PromotionWindowGetNeverReturnsNil pins the
// fix for a visibility bug: during promotion, AddTo Stores bm and
// then Stores empty ids. A reader's two Loads can straddle both
// writes, observing (bm=nil, ids=empty) for a term that is in fact
// populated. Get must re-Load bm in that case so the result is
// never a spurious nil.
//
// The test runs many promotion cycles in a writer goroutine while
// many readers call Get; every successful Get must return a
// non-nil bitmap. Run under -race to also catch any data race in
// the atomic state transition.
func TestConcurrentBitmaps_PromotionWindowGetNeverReturnsNil(t *testing.T) {
	s := NewConcurrentBitmaps()
	const numKeys = 200

	keys := make([]TermKey, numKeys)
	for i := range numKeys {
		keys[i] = ComputeTermKey([]byte{byte(i / 256), byte(i % 256)}, FieldTopic0)
	}

	// Seed each term with promotionThreshold-1 ids: sparse mode,
	// one event away from promotion.
	for i, k := range keys {
		ids := make([]uint32, promotionThreshold-1)
		for j := range ids {
			ids[j] = uint32(j)
		}
		s.AddTo(k, ids...)
		_ = i
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
		s := NewConcurrentBitmaps()
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
		s := NewConcurrentBitmaps()
		key := ComputeTermKey([]byte("dense"), FieldTopic0)

		// Force bitmap mode by exceeding the threshold.
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}
		te := s.terms[key]
		require.NotNil(t, te.bm.Load(), "must have promoted to bitmap mode")

		// Replay — bitmap.AddMany is set-semantic, so no cardinality change.
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}

		bm, err := s.Get(key)
		require.NoError(t, err)
		assert.Equal(t, uint64(promotionThreshold), bm.GetCardinality())
	})
}
