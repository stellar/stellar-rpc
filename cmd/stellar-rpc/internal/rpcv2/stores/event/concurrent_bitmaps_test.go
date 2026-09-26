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

	bm, err := s.Get(key)
	require.NoError(t, err)
	require.True(t, bm.Present())
	assert.Equal(t, uint64(3), bm.Cardinality())
	assert.True(t, bm.Contains(0))
	assert.True(t, bm.Contains(1))
	assert.True(t, bm.Contains(2))
}

func TestConcurrentBitmaps_GetMissing(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("missing"), FieldTopic0)
	bm, err := s.Get(key)
	require.NoError(t, err)
	assert.False(t, bm.Present())
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
	assert.Nil(t, st.dense, "must still be in list mode")
	assert.Len(t, st.ids, promotionThreshold-1)

	post, err := s.Get(key)
	require.NoError(t, err)
	require.True(t, post.Present())
	assert.Equal(t, uint64(promotionThreshold-1), post.Cardinality())

	// A sparse term is handed back UN-MATERIALIZED and ZERO-COPY: the ids
	// slice is the store's own published one, not a bitmap built for the
	// caller. Intersect drives straight off it, which is the whole point of
	// keeping small terms as delta postings.
	require.NotNil(t, post.IDs(), "a sparse term's postings must stay ID-backed")
	assert.Same(t, &st.ids[0], &post.IDs()[0],
		"a sparse Get must alias the published ids, not copy them")

	// Get must not have promoted.
	assert.Nil(t, p.Load().dense)
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
	require.NotNil(t, st.dense)
	assert.Empty(t, st.ids, "sparse ids cleared after promotion")
	post, err := s.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(promotionThreshold), post.Cardinality())
	assert.Nil(t, post.IDs(), "a dense term's postings are bitmap-backed, never ID-backed")
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
	assert.Equal(t, uint64(promotionThreshold+2), bm.Cardinality())
	assert.True(t, bm.Contains(1000))
	assert.True(t, bm.Contains(2000))
}

func TestConcurrentBitmaps_BatchAddTo(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("batch"), FieldTopic0)

	s.AddTo(key, 0, 1, 2, 3, 4)

	bm, err := s.Get(key)
	require.NoError(t, err)
	require.True(t, bm.Present())
	assert.Equal(t, uint64(5), bm.Cardinality())
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

	p := s.terms[key]
	require.NotNil(t, p)
	st := p.Load()
	require.NotNil(t, st.dense, "single-batch over threshold must promote immediately")
	bm, err := s.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(promotionThreshold+10), bm.Cardinality())
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
					bm, err := s.Get(k)
					require.NoError(t, err)
					require.True(t, bm.Present())
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
		require.True(t, bm.Present())
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
		bm, err := s.Get(key)
		require.NoError(t, err)
		require.True(t, bm.Present())
		assert.Equal(t, uint64(numEvents/numTerms), bm.Cardinality())
	}
}

// TestConcurrentBitmaps_GetDuringPromotionNeverReturnsNil pins
// concurrent-reader safety across the sparse→dense promotion
// transition. The current termState design publishes the whole
// (ids, bm) pair via a single atomic.Store, so the
// observability bug it was originally added to catch (a reader's
// two Loads of separate ids/bm atomic.Pointers straddling two
// separate Stores and seeing (bm=nil, ids=empty)) is structurally
// impossible. The test still has value as a -race probe: many
// readers calling Get while a writer drives terms across the
// promotion boundary should never produce a nil bitmap and
// should never trip the race detector.
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
					require.True(t, bm.Present(), "Get returned nil during promotion window")
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
		require.True(t, bm.Present())
		assert.Equal(t, uint64(4), bm.Cardinality())
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
		p := s.terms[key]
		require.NotNil(t, p.Load().dense, "must have promoted to bitmap mode")

		// Replay — bitmap.AddMany is set-semantic, so no cardinality change.
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}

		bm, err := s.Get(key)
		require.NoError(t, err)
		assert.Equal(t, uint64(promotionThreshold), bm.Cardinality())
	})
}

// assertDenseCOW checks that a term is dense and that CopyOnWrite is
// enabled on both halves of its state: the writer-private bitmap
// (where it makes the reader-side Clone shallow) and the snapshot
// handed to readers (which inherits the flag through that Clone).
func assertDenseCOW(t *testing.T, s *ConcurrentBitmaps, key TermKey, msg string) {
	t.Helper()
	p := s.terms[key]
	require.NotNil(t, p)
	d := p.Load().dense
	require.NotNil(t, d, "term must be dense")
	assert.True(t, d.wbm.GetCopyOnWrite(), msg+" (writer bitmap)")
	bm, err := s.Get(key)
	require.NoError(t, err)
	require.True(t, bm.Present())
	assert.True(t, bm.Bitmap().GetCopyOnWrite(), msg+" (published snapshot)")
}

// TestConcurrentBitmaps_DenseAddToSetsCopyOnWrite pins that the
// dense path on AddTo (both the promotion transition in AddTo
// itself and newTermState's over-threshold initial batch) sets
// CopyOnWrite. The perf design relies on this: a regression that
// drops SetCopyOnWrite would silently make every reader-side Clone
// deep-copy the whole bitmap
// (+40% hot-ingest wall, observed empirically).
func TestConcurrentBitmaps_DenseAddToSetsCopyOnWrite(t *testing.T) {
	t.Run("via promotion in AddTo", func(t *testing.T) {
		s := newTestConcurrentBitmaps()
		key := ComputeTermKey([]byte("promote"), FieldTopic0)
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}
		assertDenseCOW(t, s, key,
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
		assertDenseCOW(t, s, key,
			"dense bitmap from over-threshold initial AddTo must have CopyOnWrite enabled")
	})

	t.Run("subsequent AddTo preserves CopyOnWrite via Clone", func(t *testing.T) {
		s := newTestConcurrentBitmaps()
		key := ComputeTermKey([]byte("evolve"), FieldTopic0)
		for i := range uint32(promotionThreshold) {
			s.AddTo(key, i)
		}
		s.AddTo(key, 10_000, 10_001, 10_002)
		assertDenseCOW(t, s, key,
			"dense bitmap after additional AddTos must keep CopyOnWrite (inherited via Clone)")
	})
}

// TestNewConcurrentBitmapsFromBitmaps_DirectlyPinsContract verifies
// the warmup-side conversion constructor:
//   - input bitmaps survive in the result with their cardinality
//     intact;
//   - terms below promotionThreshold become sparse lists, terms at or
//     above it become dense and keep CopyOnWrite;
//   - nil bitmaps in the input map are skipped;
//   - a subsequent AddTo on a converted dense term is visible to the
//     next Get.
func TestNewConcurrentBitmapsFromBitmaps_DirectlyPinsContract(t *testing.T) {
	src := NewBitmaps()
	keySparse := ComputeTermKey([]byte("a"), FieldTopic0)
	keyDense := ComputeTermKey([]byte("b"), FieldTopic1)
	keyNil := ComputeTermKey([]byte("nil"), FieldTopic2)

	src.AddTo(keySparse, 0, 1, 2, 3, 4)
	dense := make([]uint32, promotionThreshold)
	for i := range dense {
		dense[i] = uint32(i) * 1_000
	}
	src.AddTo(keyDense, dense...)
	src[keyNil] = nil

	cb := NewConcurrentBitmapsFromBitmaps(src)

	bmSparse, err := cb.Get(keySparse)
	require.NoError(t, err)
	require.True(t, bmSparse.Present())
	assert.Equal(t, uint64(5), bmSparse.Cardinality())
	assert.Nil(t, cb.terms[keySparse].Load().dense, "a sub-threshold warmup term must be sparse")

	bmDense, err := cb.Get(keyDense)
	require.NoError(t, err)
	require.True(t, bmDense.Present())
	assert.Equal(t, uint64(len(dense)), bmDense.Cardinality())
	assert.True(t, bmDense.Bitmap().GetCopyOnWrite())
	assert.NotNil(t, cb.terms[keyDense].Load().dense, "a warmup term at the threshold must be dense")

	bmNil, err := cb.Get(keyNil)
	require.NoError(t, err)
	assert.False(t, bmNil.Present(), "nil source entries must be skipped, not panicked")

	cb.AddTo(keyDense, 999_999)
	post, err := cb.Get(keyDense)
	require.NoError(t, err)
	require.True(t, post.Present())
	assert.True(t, post.Bitmap().GetCopyOnWrite())
	assert.Equal(t, uint64(len(dense)+1), post.Cardinality())
	assert.Equal(t, uint64(len(dense)), bmDense.Cardinality(), "earlier snapshot stays immutable")
}

// TestConcurrentBitmaps_ContentEquivalenceAcrossTransitions drives one term
// through every representation transition — sparse, promotion, and a long
// run of dense appends — and checks Get's exact contents against a reference
// set at each step. The batch widths straddle promotionThreshold and the id
// stride leaves gaps, so a range-shaped off-by-one cannot hide behind a
// contiguous run, and the assertion is on the ids themselves rather than on
// a cardinality.
func TestConcurrentBitmaps_ContentEquivalenceAcrossTransitions(t *testing.T) {
	cb := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("equivalence"), FieldTopic0)
	var want []uint32

	next := uint32(0)
	for _, batch := range []int{1, 63, 100, 4000, 4200, 8192, 9000, 17} {
		ids := make([]uint32, batch)
		for i := range ids {
			ids[i] = next
			next += 2 // gaps, so ranges don't mask off-by-ones
		}
		cb.AddTo(key, ids...)
		want = append(want, ids...)

		post, err := cb.Get(key)
		require.NoError(t, err)
		require.True(t, post.Present())
		assert.Equal(t, want, post.Bitmap().ToArray(), "after batch of %d", batch)
	}
}
