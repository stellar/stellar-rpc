package event

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentBitmaps_SnapshotSurvivesMultiContainerWrites: a
// snapshot handed to a reader is never mutated afterwards. The ids
// span ~16 containers so the writer both deep-copies shared
// containers and inserts new ones.
func TestConcurrentBitmaps_SnapshotSurvivesMultiContainerWrites(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("multi-container"), FieldTopic0)

	// Promote and seed one container.
	seed := make([]uint32, promotionThreshold)
	for i := range seed {
		seed[i] = uint32(i)
	}
	s.AddTo(key, seed...)

	held, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, held)
	heldCard := held.GetCardinality()
	require.Equal(t, uint64(promotionThreshold), heldCard)
	heldCopy := held.ToArray()

	// 1_000 more ids spread over 16 fresh containers, written one
	// small batch per "ledger" so the writer touches an already-shared
	// container repeatedly.
	added := make([]uint32, 0, 100*10)
	for batch := range 100 {
		ids := make([]uint32, 10)
		for j := range ids {
			ids[j] = uint32(batch%16)*65_536 + 1_000 + uint32(batch)*10 + uint32(j)
		}
		s.AddTo(key, ids...)
		added = append(added, ids...)
	}

	assert.Equal(t, heldCard, held.GetCardinality(),
		"a snapshot returned by Get must not be mutated by later AddTo calls")
	assert.Equal(t, heldCopy, held.ToArray(),
		"a snapshot returned by Get must keep exactly its original ids")
	for _, id := range added {
		assert.False(t, held.Contains(id),
			"the held snapshot must not observe ids added after it was taken")
	}

	fresh, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, fresh)
	assert.Equal(t, heldCard+uint64(len(added)), fresh.GetCardinality(),
		"a Get after AddTo must observe every write")
	for _, id := range added {
		require.True(t, fresh.Contains(id), "fresh snapshot missing id %d", id)
	}
}

// TestConcurrentBitmaps_GetAfterAddToIsFresh pins the freshness
// contract call by call: every single AddTo must be visible to the
// very next Get. The published snapshot is allowed to lag, but only
// until someone reads.
func TestConcurrentBitmaps_GetAfterAddToIsFresh(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("freshness"), FieldTopic0)

	for i := range uint32(promotionThreshold + 500) {
		s.AddTo(key, i)
		bm, err := s.Get(key)
		require.NoError(t, err)
		require.NotNil(t, bm)
		require.Equal(t, uint64(i+1), bm.GetCardinality(),
			"Get after AddTo(%d) must see every id added so far", i)
		require.True(t, bm.Contains(i))
	}
}

// TestConcurrentBitmaps_WriterReaderRace runs one writer against
// eight readers that both snapshot and read through the snapshot
// (Contains / GetCardinality / Iterator). Under -race this pins that
// the writer's in-place AddMany and the readers' Clone never overlap
// on the writer-private bitmap, and that nothing mutates a snapshot
// while another goroutine reads it. The writer starts only once every
// reader is in its loop, and each reader keeps going for minReads
// iterations past the writer's end, so the test cannot pass without
// reader work overlapping writes.
func TestConcurrentBitmaps_WriterReaderRace(t *testing.T) {
	s := newTestConcurrentBitmaps()

	const numTerms = 32
	const numLedgers = 400
	const perLedger = 8
	const numReaders = 8
	const minReads = 64

	keys := make([]TermKey, numTerms)
	for i := range keys {
		keys[i] = ComputeTermKey([]byte{0xAA, byte(i)}, FieldTopic1)
	}

	var done atomic.Bool
	var reads atomic.Uint64
	var ready, wg sync.WaitGroup
	ready.Add(numReaders)

	wg.Go(func() {
		defer done.Store(true)
		ready.Wait()
		var next uint32
		for range numLedgers {
			for _, key := range keys {
				ids := make([]uint32, perLedger)
				for j := range ids {
					ids[j] = next
					next++
				}
				s.AddTo(key, ids...)
			}
		}
	})

	for r := range numReaders {
		wg.Go(func() {
			ready.Done()
			for i, n := 0, 0; !done.Load() || n < minReads; i++ {
				key := keys[(i+r)%numTerms]
				bm, err := s.Get(key)
				if err != nil || bm == nil {
					continue
				}
				n++
				reads.Add(1)
				_ = bm.GetCardinality()
				_ = bm.Contains(uint32(i))
				it := bm.Iterator()
				for n := 0; n < 64 && it.HasNext(); n++ {
					_ = it.Next()
				}
			}
		})
	}

	wg.Wait()
	assert.GreaterOrEqual(t, reads.Load(), uint64(numReaders*minReads),
		"every reader must have done real reads")

	want := uint64(numLedgers * perLedger)
	for _, key := range keys {
		bm, err := s.Get(key)
		require.NoError(t, err)
		require.NotNil(t, bm)
		assert.Equal(t, want, bm.GetCardinality())
	}
}

// TestConcurrentBitmaps_PromotionMidStreamUnderReaders drives the
// sparse→dense transition while readers are live, then keeps writing
// past it. Readers must never see a nil bitmap, never see a
// cardinality that goes backwards, and must see the full set at the
// end.
func TestConcurrentBitmaps_PromotionMidStreamUnderReaders(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("promote-midstream"), FieldTopic2)

	const total = promotionThreshold * 8
	const numReaders = 4
	const minReads = 64

	var writes atomic.Uint32
	var done atomic.Bool
	var reads atomic.Uint64
	var ready, wg sync.WaitGroup
	ready.Add(numReaders)

	wg.Go(func() {
		defer done.Store(true)
		ready.Wait()
		for i := range uint32(total) {
			s.AddTo(key, i)
			writes.Store(i + 1)
		}
	})

	for range numReaders {
		wg.Go(func() {
			ready.Done()
			var last uint64
			for n := 0; !done.Load() || n < minReads; {
				bm, err := s.Get(key)
				if err != nil || bm == nil {
					continue
				}
				n++
				reads.Add(1)
				card := bm.GetCardinality()
				assert.GreaterOrEqual(t, card, last,
					"a term's observed cardinality must never go backwards")
				assert.LessOrEqual(t, card, uint64(writes.Load())+1,
					"a reader must not observe more ids than were written")
				last = card
			}
		})
	}

	wg.Wait()
	assert.GreaterOrEqual(t, reads.Load(), uint64(numReaders*minReads),
		"every reader must have done real reads")

	bm, err := s.Get(key)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(total), bm.GetCardinality())
}

// TestConcurrentBitmaps_WarmupThenAddToThenGet covers the warmup
// entry path: NewConcurrentBitmapsFromBitmaps takes ownership of the
// built bitmaps without publishing a snapshot, so the first Get has
// to materialize one. A write before that first read must still be
// visible.
func TestConcurrentBitmaps_WarmupThenAddToThenGet(t *testing.T) {
	src := NewBitmaps()
	keyRead := ComputeTermKey([]byte("warm-read-first"), FieldTopic0)
	keyWrite := ComputeTermKey([]byte("warm-write-first"), FieldTopic0)

	seed := make([]uint32, 200)
	for i := range seed {
		seed[i] = uint32(i) * 1_000 // spans several containers
	}
	src.AddTo(keyRead, seed...)
	src.AddTo(keyWrite, seed...)

	cb := NewConcurrentBitmapsFromBitmaps(src)

	// Read first, then write, then read again.
	first, err := cb.Get(keyRead)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, uint64(len(seed)), first.GetCardinality())
	cb.AddTo(keyRead, 999_999)
	assert.Equal(t, uint64(len(seed)), first.GetCardinality(),
		"the warmup snapshot handed out earlier must stay immutable")
	second, err := cb.Get(keyRead)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(seed)+1), second.GetCardinality())
	assert.True(t, second.Contains(999_999))

	// Write before the first read ever happens.
	cb.AddTo(keyWrite, 888_888)
	only, err := cb.Get(keyWrite)
	require.NoError(t, err)
	require.NotNil(t, only)
	assert.Equal(t, uint64(len(seed)+1), only.GetCardinality())
	assert.True(t, only.Contains(888_888))
	assert.True(t, only.GetCopyOnWrite(),
		"warmup snapshots must keep CopyOnWrite so republishing stays shallow")
}

// TestConcurrentBitmaps_UnreadTermNeverClones pins the allocation
// property the fix exists for: a dense term that nobody reads
// publishes no snapshot at all, so neither the entry's termState nor
// the term's published bitmap moves across thousands of AddTo calls.
// A regression that re-published per AddTo would move one of them.
func TestConcurrentBitmaps_UnreadTermNeverClones(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("unread"), FieldTopic0)

	seed := make([]uint32, promotionThreshold)
	for i := range seed {
		seed[i] = uint32(i)
	}
	s.AddTo(key, seed...)

	entry := s.terms[key].Load()
	d := entry.dense
	require.NotNil(t, d)
	require.Nil(t, d.pub.Load(), "an unread dense term must publish nothing at promotion")

	for i := range uint32(5_000) {
		s.AddTo(key, 100_000+i)
	}
	assert.Same(t, entry, s.terms[key].Load(),
		"AddTo on a dense term must not publish a new termState")
	assert.Nil(t, d.pub.Load(),
		"AddTo on a dense term must not publish a snapshot")

	bm, err := s.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(promotionThreshold+5_000), bm.GetCardinality())
	assert.Same(t, entry, s.terms[key].Load(),
		"a dense term's termState is published once and never replaced")
	assert.Same(t, bm, d.pub.Load(),
		"the first Get after writes must publish the snapshot it returns")

	// A second Get with no intervening write returns the same pointer.
	again, err := s.Get(key)
	require.NoError(t, err)
	assert.Same(t, bm, again,
		"Get must return the same pointer when nothing changed")
}

// TestConcurrentBitmaps_DenseStateAfterAddToAndGet checks the state
// of a dense term at each step: no snapshot after promotion and after
// a write, the returned snapshot in pub after a read. Earlier
// snapshots stay frozen.
func TestConcurrentBitmaps_DenseStateAfterAddToAndGet(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("publish-order"), FieldTopic0)

	seed := make([]uint32, promotionThreshold)
	for i := range seed {
		seed[i] = uint32(i)
	}
	s.AddTo(key, seed...)

	d := s.terms[key].Load().dense
	require.NotNil(t, d)
	assert.Nil(t, d.pub.Load(), "a freshly promoted term has no snapshot until first read")

	first, err := s.Get(key)
	require.NoError(t, err)
	assert.Same(t, first, d.pub.Load(),
		"the publishing reader stores the snapshot it returns")

	s.AddTo(key, 12_345)
	assert.Nil(t, d.pub.Load(), "AddTo clears the stale snapshot before returning")
	assert.False(t, first.Contains(12_345), "an earlier snapshot stays frozen")

	second, err := s.Get(key)
	require.NoError(t, err)
	assert.True(t, second.Contains(12_345), "the next Get observes the write")
	assert.Same(t, second, d.pub.Load())
	assert.False(t, first.Contains(12_345), "the earlier snapshot is still frozen")
}

// freshnessWriterCounters is the shared state between the freshness
// stress writer and its readers.
type freshnessWriterCounters struct {
	committed *atomic.Uint32 // highest ID whose AddTo has returned
	observed  *atomic.Uint32 // highest committed ID a reader has seen in a snapshot
	batches   *atomic.Uint32 // batches the writer has finished
}

// freshnessWriter adds numBatches three-ID batches under key, handing
// off to the readers after each one: it does not start the next batch
// until some reader has returned a snapshot holding this batch's last
// ID. That snapshot must be a clone made after the AddTo, so every
// batch forces at least one republish, and the readers race each
// other to make it. Returns early once the test has failed so a
// reader that gave up cannot hang the writer.
func freshnessWriter(
	t *testing.T, s *ConcurrentBitmaps, key TermKey, c freshnessWriterCounters,
	numBatches, firstID, idStride uint32,
) {
	next := firstID
	for i := range numBatches {
		batch := []uint32{next, next + 977, next + 65_536}
		s.AddTo(key, batch...)
		last := batch[len(batch)-1]
		c.committed.Store(last)
		c.batches.Store(i + 1)
		next += idStride
		for c.observed.Load() < last {
			if t.Failed() {
				return
			}
			runtime.Gosched()
		}
	}
}

// storeMax raises v to want unless v is already at least want.
func storeMax(v *atomic.Uint32, want uint32) {
	for {
		seen := v.Load()
		if seen >= want || v.CompareAndSwap(seen, want) {
			return
		}
	}
}

// TestConcurrentBitmaps_FreshnessUnderConcurrentPublishers: the
// writer publishes the highest ID it has finished adding; each reader
// loads that ID, calls Get, and asserts the snapshot contains it. The
// batch count is fixed so the ids stay below MaxUint32 and never
// repeat an id an older snapshot already holds.
func TestConcurrentBitmaps_FreshnessUnderConcurrentPublishers(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("freshness-stress"), FieldTopic0)

	// Seed ~200 containers so a republish Clone is long enough for a
	// concurrent reader to interleave with it.
	seed := make([]uint32, 0, 200*8)
	for c := range uint32(200) {
		for j := range uint32(8) {
			seed = append(seed, c*65_536+j)
		}
	}
	s.AddTo(key, seed...)

	numReaders := max(16, 2*runtime.GOMAXPROCS(0))
	// firstID + numBatches*idStride + 65_536 stays below MaxUint32.
	const (
		numBatches = 1_000
		firstID    = uint32(20_000_000)
		idStride   = uint32(131_072)
	)

	var committed, observed, batches atomic.Uint32
	var done atomic.Bool
	var reads atomic.Uint64
	var wg sync.WaitGroup

	wg.Go(func() {
		defer done.Store(true)
		freshnessWriter(t, s, key, freshnessWriterCounters{
			committed: &committed, observed: &observed, batches: &batches,
		}, numBatches, firstID, idStride)
	})

	for range numReaders {
		wg.Go(func() {
			for !done.Load() {
				want := committed.Load()
				if want == 0 {
					runtime.Gosched()
					continue
				}
				bm, err := s.Get(key)
				if err != nil || bm == nil {
					continue
				}
				reads.Add(1)
				// Yield: every read after a write takes the term
				// mutex, and unyielding readers starve the writer.
				runtime.Gosched()
				// Get started after the AddTo that committed `want`
				// returned, so the snapshot must contain it.
				if !bm.Contains(want) {
					t.Errorf("Get returned a snapshot missing id %d, "+
						"committed before this Get started (cardinality %d)",
						want, bm.GetCardinality())
					return
				}
				storeMax(&observed, want)
			}
		})
	}

	wg.Wait()
	t.Logf("freshness stress: %d reads, %d batches",
		reads.Load(), batches.Load())
	assert.Equal(t, uint32(numBatches), batches.Load(), "the writer must finish every batch")
	assert.GreaterOrEqual(t, observed.Load(), committed.Load(),
		"a reader must observe the final committed batch")
	require.Positive(t, reads.Load(), "the stress loop must have done real reads")
}

// TestConcurrentBitmaps_WarmupSubThresholdTermStaysSparse pins the
// warmup representation of a small term: it is a sparse list, Get
// returns its ids, later AddTo calls stay in sparse mode, and the term
// promotes at the threshold like a term built by AddTo.
func TestConcurrentBitmaps_WarmupSubThresholdTermStaysSparse(t *testing.T) {
	src := NewBitmaps()
	key := ComputeTermKey([]byte("warm-small"), FieldTopic0)
	seed := make([]uint32, promotionThreshold-2)
	for i := range seed {
		seed[i] = uint32(i) * 7
	}
	src.AddTo(key, seed...)

	cb := NewConcurrentBitmapsFromBitmaps(src)
	st := cb.terms[key].Load()
	require.Nil(t, st.dense)
	assert.Equal(t, seed, st.ids)

	bm, err := cb.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(seed)), bm.GetCardinality())

	next := seed[len(seed)-1] + 1
	cb.AddTo(key, next)
	require.Nil(t, cb.terms[key].Load().dense, "one below the threshold stays sparse")

	cb.AddTo(key, next+1)
	require.NotNil(t, cb.terms[key].Load().dense, "reaching the threshold promotes")
	bm, err = cb.Get(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(seed)+2), bm.GetCardinality())
	assert.True(t, bm.Contains(next+1))
}
