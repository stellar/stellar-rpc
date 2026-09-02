package event

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

	post, err := s.Get(key)
	require.NoError(t, err)
	require.True(t, post.Present())
	assert.Equal(t, uint64(3), post.Cardinality())
	assert.True(t, post.Contains(0))
	assert.True(t, post.Contains(1))
	assert.True(t, post.Contains(2))
}

func TestConcurrentBitmaps_GetMissing(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("missing"), FieldTopic0)
	post, err := s.Get(key)
	require.NoError(t, err)
	assert.False(t, post.Present())
}

func TestConcurrentBitmaps_ListMode(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("sparse"), FieldTopic0)

	for i := range uint32(promotionThreshold - 1) {
		s.AddTo(key, i)
	}

	p := s.terms[key]
	require.NotNil(t, p)
	st := p.Load()
	require.NotNil(t, st)
	assert.Nil(t, st.bm, "must still be in list mode")
	assert.Len(t, st.ids, promotionThreshold-1)

	post, err := s.Get(key)
	require.NoError(t, err)
	require.True(t, post.Present())
	assert.Equal(t, uint64(promotionThreshold-1), post.Cardinality())

	// The form follows storage: a sparse term is never materialized, and what
	// comes back is the store's own slice rather than a copy.
	require.NotNil(t, post.IDs(), "a sparse term must not be materialized")
	assert.Same(t, &st.ids[0], &post.IDs()[0])

	// Get must not have promoted.
	assert.Nil(t, p.Load().bm)
}

func TestConcurrentBitmaps_Promotion(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("dense"), FieldTopic0)

	for i := range uint32(promotionThreshold) {
		s.AddTo(key, i)
	}

	p := s.terms[key]
	require.NotNil(t, p)
	st := p.Load()
	require.NotNil(t, st.bm)
	assert.Empty(t, st.ids, "sparse ids cleared after promotion")
	assert.Equal(t, uint64(promotionThreshold), st.bm.GetCardinality())

	post, err := s.Get(key)
	require.NoError(t, err)
	require.True(t, post.Present())
	assert.Nil(t, post.IDs(), "a promoted term comes back as a bitmap")
}

func TestConcurrentBitmaps_AddAfterPromotion(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("dense"), FieldTopic0)

	for i := range uint32(promotionThreshold) {
		s.AddTo(key, i)
	}
	s.AddTo(key, 1000)
	s.AddTo(key, 2000)

	post, err := s.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(promotionThreshold+2), post.Cardinality())
	assert.True(t, post.Contains(1000))
	assert.True(t, post.Contains(2000))
}

func TestConcurrentBitmaps_BatchAddTo(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("batch"), FieldTopic0)

	s.AddTo(key, 0, 1, 2, 3, 4)

	post, err := s.Get(key)
	require.NoError(t, err)
	require.True(t, post.Present())
	assert.Equal(t, uint64(5), post.Cardinality())
	assert.True(t, post.Contains(0))
	assert.True(t, post.Contains(4))
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

	p := s.terms[key]
	require.NotNil(t, p)
	st := p.Load()
	require.NotNil(t, st.bm, "single-batch over threshold must promote immediately")
	assert.Equal(t, uint64(promotionThreshold+10), st.bm.GetCardinality())
}

// TestConcurrentBitmaps_GetReturnsImmutableSnapshot pins the
// COW-on-write contract: a bitmap returned by Get is an immutable
// snapshot, so a subsequent AddTo (which produces a new snapshot
// via atomic.Store) does NOT mutate the previously-returned
// pointer. This is the key invariant readers can rely on across the
// borrow.
func TestConcurrentBitmaps_GetReturnsImmutableSnapshot(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("borrow"), FieldTopic0)

	// Promote to bitmap mode.
	for i := range uint32(promotionThreshold) {
		s.AddTo(key, i)
	}

	before, err := s.Get(key)
	require.NoError(t, err)
	beforeCard := before.Cardinality()

	// New AddTo publishes a new snapshot via atomic.Store.
	s.AddTo(key, 9_999_999)

	// before still observes the pre-AddTo cardinality.
	assert.Equal(t, beforeCard, before.Cardinality(),
		"AddTo published a new snapshot; the borrowed pointer must remain unchanged")

	after, err := s.Get(key)
	require.NoError(t, err)
	assert.True(t, after.Contains(9_999_999),
		"subsequent Get must observe the new snapshot")
}

// TestConcurrentBitmaps_ConcurrentGetIsSafe runs many concurrent
// Get callers against the same store. Get is lock-free past the
// brief map-lookup RLock and returns an immutable snapshot, so
// concurrent reads should not race. Run under -race.
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
					post, err := s.Get(k)
					require.NoError(t, err)
					require.True(t, post.Present())
					_ = post.Contains(0)
				}
			}
		})
	}
	wg.Wait()

	// Sanity check: the store is untouched by all that reading.
	for _, k := range keys {
		post, err := s.Get(k)
		require.NoError(t, err)
		require.True(t, post.Present())
	}
}

// TestConcurrentBitmaps_ConcurrentReadWrite exercises the COW
// contract under a single writer and many readers. Readers atomic-
// Load the current snapshot and operate on it independently while
// the writer publishes new snapshots; no clones or locks span the
// borrow. Under -race no data races should be reported.
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
		post, err := s.Get(key)
		require.NoError(t, err)
		require.True(t, post.Present())
		assert.Equal(t, uint64(numEvents/numTerms), post.Cardinality())
	}
}

// TestConcurrentBitmaps_GetDuringPromotionNeverReturnsAbsent pins
// concurrent-reader safety across the sparse→dense promotion
// transition. The current termState design publishes the whole
// (ids, bm) pair via a single atomic.Store, so the
// observability bug it was originally added to catch (a reader's
// two Loads of separate ids/bm atomic.Pointers straddling two
// separate Stores and seeing (bm=nil, ids=empty)) is structurally
// impossible. The test still has value as a -race probe: many
// readers calling Get while a writer drives terms across the
// promotion boundary should never produce an absent result and
// should never trip the race detector.
func TestConcurrentBitmaps_GetDuringPromotionNeverReturnsAbsent(t *testing.T) {
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
					post, err := s.Get(k)
					require.NoError(t, err)
					// The term was seeded with promotionThreshold-1
					// ids and the writer only appends — Get must
					// always observe a present term.
					require.True(t, post.Present(), "Get returned absent during promotion window")
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

		post, err := s.Get(key)
		require.NoError(t, err)
		require.True(t, post.Present())
		assert.Equal(t, uint64(4), post.Cardinality())
		for _, id := range []uint32{0, 1, 2, 3} {
			assert.True(t, post.Contains(id))
		}
	})

	t.Run("bitmap mode", func(t *testing.T) {
		s := newTestConcurrentBitmaps()
		key := ComputeTermKey([]byte("dense"), FieldTopic0)

		// Force bitmap mode by exceeding the threshold.
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}
		p := s.terms[key]
		require.NotNil(t, p.Load().bm, "must have promoted to bitmap mode")

		// Replay — bitmap.AddMany is set-semantic, so no cardinality change.
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}

		post, err := s.Get(key)
		require.NoError(t, err)
		assert.Equal(t, uint64(promotionThreshold), post.Cardinality())
	})
}

// TestConcurrentBitmaps_DenseAddToSetsCopyOnWrite pins that the
// dense path on AddTo (both the promotion transition in AddTo
// itself and newTermState's over-threshold initial batch) sets
// CopyOnWrite on the published bitmap. The perf design relies on
// this: a regression that drops SetCopyOnWrite would silently make
// every subsequent AddTo's Clone deep-copy the whole bitmap
// (+40% hot-ingest wall, observed empirically).
func TestConcurrentBitmaps_DenseAddToSetsCopyOnWrite(t *testing.T) {
	t.Run("via promotion in AddTo", func(t *testing.T) {
		s := newTestConcurrentBitmaps()
		key := ComputeTermKey([]byte("promote"), FieldTopic0)
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}
		bm := s.terms[key].Load().bm
		require.NotNil(t, bm)
		assert.True(t, bm.GetCopyOnWrite(),
			"dense bitmap after promotion must have CopyOnWrite enabled")
	})

	t.Run("via newTermState over-threshold initial batch", func(t *testing.T) {
		s := newTestConcurrentBitmaps()
		key := ComputeTermKey([]byte("initial"), FieldTopic0)
		ids := make([]uint32, promotionThreshold+10)
		for i := range ids {
			ids[i] = uint32(i)
		}
		s.AddTo(key, ids...)
		bm := s.terms[key].Load().bm
		require.NotNil(t, bm)
		assert.True(t, bm.GetCopyOnWrite(),
			"dense bitmap from over-threshold initial AddTo must have CopyOnWrite enabled")
	})

	t.Run("subsequent AddTo preserves CopyOnWrite via Clone", func(t *testing.T) {
		s := newTestConcurrentBitmaps()
		key := ComputeTermKey([]byte("evolve"), FieldTopic0)
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}
		s.AddTo(key, 10_000, 10_001, 10_002)
		bm := s.terms[key].Load().bm
		require.NotNil(t, bm)
		assert.True(t, bm.GetCopyOnWrite(),
			"dense bitmap after additional AddTos must keep CopyOnWrite (inherited via Clone)")
	})
}

// TestNewConcurrentBitmapsFromBitmaps_DirectlyPinsContract verifies
// the warmup-side conversion constructor:
//   - input bitmaps survive in the result with their cardinality
//     intact;
//   - each non-nil bitmap has CopyOnWrite enabled after conversion;
//   - nil bitmaps in the input map are skipped (not panicked);
//   - subsequent AddTo on the result Clones via the COW fast path
//     (no full deep copy on the popular-term ingest path).
func TestNewConcurrentBitmapsFromBitmaps_DirectlyPinsContract(t *testing.T) {
	src := NewBitmaps()
	keyA := ComputeTermKey([]byte("a"), FieldTopic0)
	keyB := ComputeTermKey([]byte("b"), FieldTopic1)
	keyNil := ComputeTermKey([]byte("nil"), FieldTopic2)

	src.AddTo(keyA, 0, 1, 2, 3, 4)
	src.AddTo(keyB, 100, 101)
	src[keyNil] = nil // simulate a nil entry that the constructor must skip

	cb := NewConcurrentBitmapsFromBitmaps(src)

	postA, err := cb.Get(keyA)
	require.NoError(t, err)
	require.True(t, postA.Present())
	assert.Equal(t, uint64(5), postA.Cardinality())
	assert.True(t, postA.Bitmap().GetCopyOnWrite(),
		"FromBitmaps must enable CopyOnWrite so the live-ingest AddTo path Clones via the fast shallow path")

	postB, err := cb.Get(keyB)
	require.NoError(t, err)
	require.True(t, postB.Present())
	assert.Equal(t, uint64(2), postB.Cardinality())
	assert.True(t, postB.Bitmap().GetCopyOnWrite())

	postNil, err := cb.Get(keyNil)
	require.NoError(t, err)
	assert.False(t, postNil.Present(), "nil source entries must be skipped, not panicked")

	// Subsequent AddTo on the converted index goes down the tail-delta
	// path: the BASE bitmap is reused untouched (same pointer, same COW
	// flag, same cardinality) and the new ID rides in the tail; Get sees
	// the union.
	cb.AddTo(keyA, 5)
	st := cb.terms[keyA].Load()
	require.NotNil(t, st.bm)
	assert.Same(t, postA.Bitmap(), st.bm, "tail-delta AddTo must reuse the base, not clone it")
	assert.Equal(t, []uint32{5}, st.tail)
	got, err := cb.Get(keyA)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), got.Cardinality())

	// A merge (tail crossing tailMergeThreshold) Clones the base; the COW
	// flag must survive the merge so later merges stay shallow.
	bulk := make([]uint32, tailMergeThreshold)
	for i := range bulk {
		bulk[i] = uint32(6 + i)
	}
	cb.AddTo(keyA, bulk...)
	merged := cb.terms[keyA].Load()
	require.NotNil(t, merged.bm)
	assert.Empty(t, merged.tail)
	assert.NotSame(t, postA.Bitmap(), merged.bm)
	assert.True(t, merged.bm.GetCopyOnWrite(),
		"merge must preserve CopyOnWrite so subsequent merges Clone via the fast shallow path")
	assert.Equal(t, uint64(6+tailMergeThreshold), merged.bm.GetCardinality())
}
