package event

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// This file pins ONE property of the no-materialize read seam
// (lookupPostings, postings.bitmap, postings.estimate): a read that
// starts after an AddTo returns observes that AddTo's ids.
//
// It needs its own tests because the match-path tests cannot see the
// bug it guards. They build their sources up front and then only read,
// so a postings accessor that returned a stale dense snapshot — the raw
// pub pointer, nil-or-behind after every write — would agree with them
// on every case. Only write-then-read-through-the-accessor separates
// the two.

// postingsIDs drains a term's postings through the accessor the query
// engine uses, so a test asserting on it exercises the same path a
// real read takes rather than the representation underneath.
func postingsIDs(t *testing.T, s *ConcurrentBitmaps, key TermKey) []uint32 {
	t.Helper()
	p := s.lookupPostings(key)
	require.True(t, p.present(), "the term must be present in the index")
	return postingIDs(p)
}

// denseOf returns the term's denseState, failing the test when the
// term has not promoted. Tests use it only to observe pub, the field
// whose nil-ness is what makes a read take the un-snapshotted path.
func denseOf(t *testing.T, s *ConcurrentBitmaps, key TermKey) *denseState {
	t.Helper()
	st := s.terms[key].Load()
	require.NotNil(t, st.dense, "term must be dense")
	return st.dense
}

// ascending is the id list a term holds after n writes of stride ids.
func ascending(n, stride uint32) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(i) * stride
	}
	return out
}

// TestPostings_SparseLookupSeesTheWriteJustMade: on a sparse term,
// every accessor reads the termState the last AddTo published — the
// ids, the count, and the cursor all include the write that just
// returned.
func TestPostings_SparseLookupSeesTheWriteJustMade(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("sparse-freshness"), FieldTopic0)

	want := make([]uint32, 0, promotionThreshold-1)
	for i := range uint32(promotionThreshold - 1) {
		id := i * 3
		s.AddTo(key, id)
		want = append(want, id)

		p := s.lookupPostings(key)
		require.True(t, p.present(), "a term written to is present")
		require.Nil(t, p.bitmap(), "a sub-threshold term stays sparse")
		assert.Equal(t, want, postingIDs(p),
			"the cursor must yield every id added so far, the last one included")
		assert.Equal(t, uint64(len(want)), p.estimate(),
			"estimate must count the write that just returned")
	}
}

// TestPostings_PromotionIsVisibleThroughLookupPostings: the AddTo that
// crosses promotionThreshold rebuilds the term as a bitmap, and a
// lookup right after it sees the whole term — the promoting batch
// included — through the dense representation.
func TestPostings_PromotionIsVisibleThroughLookupPostings(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("promotion-freshness"), FieldTopic0)

	const stride = 5
	// One short of the threshold: still a list, no bitmap.
	below := ascending(promotionThreshold-1, stride)
	s.AddTo(key, below...)
	require.Nil(t, s.terms[key].Load().dense, "one below the threshold stays sparse")
	require.Nil(t, s.lookupPostings(key).bitmap(), "a sparse term has no bitmap")
	require.Equal(t, uint64(len(below)), s.lookupPostings(key).estimate())

	// The id that promotes. Read the accessors before anything else
	// touches the term, so a promotion the lookup missed shows up as a
	// short drain rather than being papered over by a later snapshot.
	promoting := uint32(promotionThreshold-1) * stride
	s.AddTo(key, promoting)
	want := append(append([]uint32{}, below...), promoting)

	p := s.lookupPostings(key)
	require.True(t, p.present())
	require.NotNil(t, p.bitmap(), "crossing the threshold promotes to a bitmap")
	assert.True(t, p.bitmap().Contains(promoting),
		"the promoting id must be in the bitmap the promotion built")
	assert.Equal(t, want, postingIDs(p),
		"a lookup straight after promotion yields every id the term holds")
	assert.Equal(t, uint64(len(want)), p.estimate(),
		"estimate must count the promoting write")

	// And the writes after promotion, which take the in-place dense path.
	next := promoting + stride
	s.AddTo(key, next)
	want = append(want, next)
	assert.Equal(t, want, postingsIDs(t, s, key),
		"the first dense-mode write must be visible to the next lookup")
	assert.Equal(t, uint64(len(want)), s.lookupPostings(key).estimate())
}

// TestPostings_DenseLookupSeesWritesSinceLastSnapshot: the case a
// stale read would pass every differential on. A reader publishes a
// snapshot, the writer invalidates it, and the next lookup must clone
// again rather than hand back the pub pointer it finds nil.
func TestPostings_DenseLookupSeesWritesSinceLastSnapshot(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("dense-freshness"), FieldTopic0)

	seed := ascending(promotionThreshold, 4)
	s.AddTo(key, seed...)
	d := denseOf(t, s, key)

	// A first read publishes the snapshot every later read would
	// wrongly reuse.
	held := s.lookupPostings(key).bitmap()
	require.NotNil(t, held)
	require.Same(t, held, d.pub.Load(), "the first read publishes what it returns")
	heldCard := held.GetCardinality()

	// Writes spread over fresh containers, so a stale read is wrong by
	// more than a bit inside a container the snapshot already shares.
	fresh := []uint32{1_000_000, 1_000_000 + 65_536, 1_000_000 + 3*65_536}
	s.AddTo(key, fresh...)
	require.Nil(t, d.pub.Load(),
		"AddTo drops the snapshot, so the next read takes the un-snapshotted path")

	p := s.lookupPostings(key)
	bm := p.bitmap()
	require.NotNil(t, bm)
	for _, id := range fresh {
		assert.True(t, bm.Contains(id),
			"a lookup after AddTo must observe id %d", id)
		assert.False(t, held.Contains(id),
			"the snapshot taken before the write stays frozen")
	}
	assert.Equal(t, heldCard+uint64(len(fresh)), bm.GetCardinality())
	assert.Equal(t, heldCard+uint64(len(fresh)), p.estimate(),
		"estimate must count the writes made since the last snapshot")
	assert.Subset(t, postingIDs(p), fresh,
		"the cursor the query engine walks must yield the fresh ids too")
}

// TestPostings_EstimateCountsUnsnapshottedWritesWithoutCloning pins
// both halves of the planner's cost rule at once: weighing a dense
// term written since its last read reports the CURRENT count, and
// does it without publishing a snapshot — the clone only a read that
// actually walks the term should pay.
func TestPostings_EstimateCountsUnsnapshottedWritesWithoutCloning(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("estimate-freshness"), FieldTopic0)

	seed := ascending(promotionThreshold, 4)
	s.AddTo(key, seed...)
	d := denseOf(t, s, key)

	// Never read: pub is nil from promotion onwards.
	require.Nil(t, d.pub.Load())
	assert.Equal(t, uint64(len(seed)), s.lookupPostings(key).estimate(),
		"a term nobody has read yet weighs what it holds")
	assert.Nil(t, d.pub.Load(), "estimate must not publish a snapshot")

	// Read once to publish, then invalidate and weigh again.
	require.NotNil(t, s.lookupPostings(key).bitmap())
	require.NotNil(t, d.pub.Load())

	s.AddTo(key, 2_000_000, 2_000_000+65_536)
	require.Nil(t, d.pub.Load())
	assert.Equal(t, uint64(len(seed)+2), s.lookupPostings(key).estimate(),
		"estimate reads through to the writer's bitmap, never a dropped snapshot")
	assert.Nil(t, d.pub.Load(),
		"weighing a written-since term must still not clone it")
}

// TestPostings_FreshnessUnderConcurrentPublishers is
// TestConcurrentBitmaps_FreshnessUnderConcurrentPublishers aimed at
// the accessors #968 added: readers race the writer through
// lookupPostings instead of Get, and each read must observe the id
// whose AddTo returned before the read began. Run with -race.
func TestPostings_FreshnessUnderConcurrentPublishers(t *testing.T) {
	s := newTestConcurrentBitmaps()
	key := ComputeTermKey([]byte("postings-freshness-stress"), FieldTopic0)

	// ~200 containers, so a republish Clone is long enough for a
	// concurrent reader to interleave with it.
	seed := make([]uint32, 0, 200*8)
	for c := range uint32(200) {
		for j := range uint32(8) {
			seed = append(seed, c*65_536+j)
		}
	}
	s.AddTo(key, seed...)
	seedCard := uint64(len(seed))

	numReaders := max(16, 2*runtime.GOMAXPROCS(0))
	// firstID + numBatches*idStride + 65_536 stays below MaxUint32.
	const (
		numBatches = 500
		firstID    = uint32(20_000_000)
		idStride   = uint32(131_072)
		perBatch   = 3 // freshnessWriter adds three ids per batch
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
				// Sample the batch count BEFORE the read: whatever it
				// says is already durable in the writer's bitmap, so
				// the read cannot legally weigh less.
				finished := batches.Load()
				want := committed.Load()
				if want == 0 {
					runtime.Gosched()
					continue
				}
				p := s.lookupPostings(key)
				if !p.present() {
					t.Errorf("lookupPostings lost a term that has been written")
					return
				}
				bm := p.bitmap()
				if bm == nil {
					t.Errorf("lookupPostings returned no bitmap for a dense term")
					return
				}
				reads.Add(1)
				// Yield: a read after a write takes the term mutex,
				// and unyielding readers starve the writer.
				runtime.Gosched()
				if !bm.Contains(want) {
					t.Errorf("lookupPostings returned a bitmap missing id %d, "+
						"committed before the lookup started (cardinality %d)",
						want, bm.GetCardinality())
					return
				}
				// estimate runs after bitmap, so it can only have
				// grown: a stale read of the dropped snapshot would
				// come back short.
				if est := p.estimate(); est < seedCard+uint64(finished)*perBatch {
					t.Errorf("estimate = %d, below the %d ids committed before the read",
						est, seedCard+uint64(finished)*perBatch)
					return
				} else if est < bm.GetCardinality() {
					t.Errorf("estimate = %d, below the %d of the bitmap it just handed out",
						est, bm.GetCardinality())
					return
				}
				storeMax(&observed, want)
			}
		})
	}

	wg.Wait()
	t.Logf("postings freshness stress: %d reads, %d batches", reads.Load(), batches.Load())
	assert.Equal(t, uint32(numBatches), batches.Load(), "the writer must finish every batch")
	assert.GreaterOrEqual(t, observed.Load(), committed.Load(),
		"a reader must observe the final committed batch")
	require.Positive(t, reads.Load(), "the stress loop must have done real reads")
}

// TestHotStore_LookupPostingsSeesTheWriteJustMade carries the same
// property one layer up, through the seam the query planner actually
// calls: HotStore.lookupPostings must reflect an applyLedger that has
// returned, for a sparse term and for a dense one alike.
func TestHotStore_LookupPostingsSeesTheWriteJustMade(t *testing.T) {
	h := openHotStoreForTest(t, chunk.ID(0)).store
	// index() is the store's documented test-only write hook, which is
	// what lets this drive the mirror without an ingest whose term
	// derivation would decide the representations for us.
	mirror := h.index()
	sparseKey := ComputeTermKey([]byte("hot-sparse"), FieldTopic0)
	denseKey := ComputeTermKey([]byte("hot-dense"), FieldTopic0)

	mirror.AddTo(denseKey, ascending(promotionThreshold, 4)...)
	mirror.AddTo(sparseKey, 1, 2, 3)

	// Publish snapshots, then invalidate the dense one.
	first, err := h.lookupPostings(t.Context(), []TermKey{sparseKey, denseKey})
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.NotNil(t, first[1].bitmap())
	require.NotNil(t, denseOf(t, mirror, denseKey).pub.Load())

	mirror.AddTo(sparseKey, 4)
	mirror.AddTo(denseKey, 3_000_000)
	require.Nil(t, denseOf(t, mirror, denseKey).pub.Load())

	got, err := h.lookupPostings(t.Context(), []TermKey{sparseKey, denseKey})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []uint32{1, 2, 3, 4}, postingIDs(got[0]),
		"the sparse term must carry the id added since the last lookup")
	assert.Equal(t, uint64(4), got[0].estimate())
	assert.True(t, got[1].bitmap().Contains(uint32(3_000_000)),
		"the dense term must carry the id added since its snapshot was dropped")
	assert.Equal(t, uint64(promotionThreshold+1), got[1].estimate())
}
