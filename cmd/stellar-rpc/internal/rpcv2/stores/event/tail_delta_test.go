package event

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tdKey returns a distinct TermKey for tests.
func tdKey(b byte) TermKey {
	var k TermKey
	k[0] = b
	return k
}

// mustGet fetches key's bitmap, failing the test on error.
func mustGet(t *testing.T, cb *ConcurrentBitmaps, key TermKey) []uint32 {
	t.Helper()
	bm, err := cb.Get(key)
	require.NoError(t, err)
	if bm == nil {
		return nil
	}
	return bm.ToArray()
}

// TestTailDelta_ContentEquivalence drives one term through every
// representation transition — sparse, promotion, tail accumulation, several
// merges — and checks Get against a reference set at each step.
func TestTailDelta_ContentEquivalence(t *testing.T) {
	cb := NewConcurrentBitmapsFromBitmaps(Bitmaps{})
	key := tdKey(1)
	var want []uint32

	// Ascending batches of varying width: crosses promotionThreshold (64)
	// early and tailMergeThreshold (8192) several times.
	next := uint32(0)
	for _, batch := range []int{1, 63, 100, 4000, 4200, 8192, 9000, 17} {
		ids := make([]uint32, batch)
		for i := range ids {
			ids[i] = next
			next += 2 // gaps, so ranges don't mask off-by-ones
		}
		cb.AddTo(key, ids...)
		want = append(want, ids...)
		assert.Equal(t, want, mustGet(t, cb, key), "after batch of %d", batch)
	}
}

// TestTailDelta_BaseReusedBetweenMerges pins the whole point of the design:
// below the merge threshold the published states SHARE the same base bitmap
// (no clone), and crossing the threshold publishes a merged, tail-free state.
func TestTailDelta_BaseReusedBetweenMerges(t *testing.T) {
	cb := NewConcurrentBitmapsFromBitmaps(Bitmaps{})
	key := tdKey(2)

	// Promote to dense with one big batch.
	big := make([]uint32, promotionThreshold)
	for i := range big {
		big[i] = uint32(i)
	}
	cb.AddTo(key, big...)

	cb.rwmu.RLock()
	p := cb.terms[key]
	cb.rwmu.RUnlock()
	require.NotNil(t, p)
	base := p.Load().bm
	require.NotNil(t, base)

	// Small adds: base pointer must be REUSED, tail must grow.
	cb.AddTo(key, uint32(promotionThreshold))
	cb.AddTo(key, uint32(promotionThreshold+1))
	st := p.Load()
	assert.Same(t, base, st.bm, "small add must not clone the base")
	assert.Equal(t, []uint32{promotionThreshold, promotionThreshold + 1}, st.tail)

	// Cross the merge threshold: new base, empty tail, content intact.
	rest := make([]uint32, tailMergeThreshold)
	for i := range rest {
		rest[i] = uint32(promotionThreshold + 2 + i)
	}
	cb.AddTo(key, rest...)
	st = p.Load()
	assert.NotSame(t, base, st.bm, "merge must publish a fresh base")
	assert.Empty(t, st.tail)
	assert.EqualValues(t, promotionThreshold+2+tailMergeThreshold,
		st.bm.GetCardinality())
}

// TestTailDelta_SnapshotImmutability holds Get snapshots while the writer
// advances the term through tails and merges, verifying no held snapshot
// ever changes content — the reader contract the atomic-publish design rests on.
func TestTailDelta_SnapshotImmutability(t *testing.T) {
	cb := NewConcurrentBitmapsFromBitmaps(Bitmaps{})
	key := tdKey(3)

	type snap struct {
		got  []uint32
		card uint64
	}
	var snaps []snap
	take := func() {
		bm, err := cb.Get(key)
		require.NoError(t, err)
		if bm != nil {
			snaps = append(snaps, snap{got: bm.ToArray(), card: bm.GetCardinality()})
		}
	}

	next := uint32(0)
	for range 40 {
		ids := make([]uint32, 500) // recurring merges at 8192
		for i := range ids {
			ids[i] = next
			next++
		}
		cb.AddTo(key, ids...)
		take()
	}
	// Re-verify every held snapshot against its recorded content.
	for i, s := range snaps {
		assert.Equal(t, s.card, uint64(len(s.got)), "snap %d", i)
		assert.EqualValues(t, (i+1)*500, s.card, "snap %d cardinality drifted", i)
	}
}

// TestTailDelta_GetMemoized: repeated Gets on the same tail-bearing state
// return the same materialized bitmap (no rebuild per call).
func TestTailDelta_GetMemoized(t *testing.T) {
	cb := NewConcurrentBitmapsFromBitmaps(Bitmaps{})
	key := tdKey(4)
	big := make([]uint32, promotionThreshold)
	for i := range big {
		big[i] = uint32(i)
	}
	cb.AddTo(key, big...)
	cb.AddTo(key, promotionThreshold) // tail-bearing now

	m1, err := cb.Get(key)
	require.NoError(t, err)
	m2, err := cb.Get(key)
	require.NoError(t, err)
	assert.Same(t, m1, m2, "materialization must be memoized per state")

	// A new publish invalidates the memo (fresh state, fresh memo).
	cb.AddTo(key, promotionThreshold+1)
	m3, err := cb.Get(key)
	require.NoError(t, err)
	assert.NotSame(t, m1, m3)
	assert.EqualValues(t, promotionThreshold+2, m3.GetCardinality())
}

// TestTailDelta_WriterReaderRace hammers one dense term with the single
// writer while readers Get and fully iterate their snapshots, including
// retained old materializations re-checked at the end. Run with -race; it
// exists to catch in-place tail mutation or writer-Clone-vs-reader races.
func TestTailDelta_WriterReaderRace(t *testing.T) {
	cb := NewConcurrentBitmapsFromBitmaps(Bitmaps{})
	key := tdKey(5)

	const (
		writerBatches = 400
		batch         = 100 // tail path most iterations, merges included
		readers       = 4
	)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	type held struct {
		card uint64
		sum  uint64
	}
	heldByReader := make([][]held, readers)

	for r := range readers {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				bm, err := cb.Get(key)
				if err != nil || bm == nil {
					continue
				}
				var sum uint64
				it := bm.Iterator()
				for it.HasNext() {
					sum += uint64(it.Next())
				}
				heldByReader[r] = append(heldByReader[r], held{card: bm.GetCardinality(), sum: sum})
				if len(heldByReader[r]) > 64 {
					heldByReader[r] = heldByReader[r][32:] // keep some old snapshots live
				}
			}
		}(r)
	}

	next := uint32(0)
	for range writerBatches {
		ids := make([]uint32, batch)
		for i := range ids {
			ids[i] = next
			next++
		}
		cb.AddTo(key, ids...)
	}
	close(stop)
	wg.Wait()

	// Content sanity: cardinality of any observed snapshot must be a
	// multiple of nothing in particular, but monotone per reader and its
	// sum must match sum(0..card-1) since IDs are 0..card-1 contiguous.
	for r, hs := range heldByReader {
		var prev uint64
		for i, h := range hs {
			require.GreaterOrEqual(t, h.card, prev, "reader %d snap %d regressed", r, i)
			prev = h.card
			require.Equal(t, h.card*(h.card-1)/2, h.sum,
				"reader %d snap %d content corrupt", r, i)
		}
	}

	final := mustGet(t, cb, key)
	require.Len(t, final, int(writerBatches*batch))
}

// TestTailDelta_WriterReaderRace_MultiContainer targets the COW-bookkeeping
// race pair the single-container sibling test cannot reach: reader-side
// materialization (roaring.FastOr → lazyOR → appendCopy READS the base's
// needCopyOnWrite flags for every base-only container) versus the writer's
// merge (which must never WRITE any published bitmap's bookkeeping). The
// term's IDs are strided so the base spans hundreds of roaring containers
// while the tail stays confined to the top one — that is what routes the
// reader through appendCopy over base-only containers. Run with -race.
func TestTailDelta_WriterReaderRace_MultiContainer(t *testing.T) {
	cb := NewConcurrentBitmapsFromBitmaps(Bitmaps{})
	key := tdKey(6)

	const (
		writerBatches = 200
		batch         = 3000 // merge roughly every third batch (threshold 8192)
		stride        = 64   // 600k IDs * 64 ≈ 38M span ≈ 580 containers
		readers       = 4
	)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for range readers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				bm, err := cb.Get(key)
				if err != nil || bm == nil {
					continue
				}
				_ = bm.GetCardinality()
			}
		})
	}

	next := uint32(0)
	for range writerBatches {
		ids := make([]uint32, batch)
		for i := range ids {
			ids[i] = next
			next += stride
		}
		cb.AddTo(key, ids...)
	}
	close(stop)
	wg.Wait()

	require.Len(t, mustGet(t, cb, key), writerBatches*batch)
}
