package event

import (
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
)

// promotionThreshold is the number of event IDs a term holds in a
// sorted list before it is promoted to a roaring bitmap. Observed
// mean cardinality is ~14.5–16.3 events per term (production chunks
// 005901–005908; see BenchmarkEventIndex_10M), so most terms stay
// in list mode.
const promotionThreshold = 64

// denseState is the state of one dense term. One is allocated at
// promotion and lives for the term's lifetime.
//
//   - wbm is the writer's private bitmap. AddTo mutates it in place.
//     It is never handed to a reader.
//   - pub is the last immutable snapshot handed to readers. nil means
//     wbm holds writes that no snapshot has: nil at promotion, nil
//     again after every write, non-nil only once a reader has cloned
//     wbm since the last write.
//   - mu serializes AddMany on wbm against Clone of wbm. Both write
//     wbm's copy-on-write flags. Every store to pub happens under mu.
type denseState struct {
	mu  sync.Mutex
	wbm *roaring.Bitmap
	pub atomic.Pointer[roaring.Bitmap]
}

// termState is the immutable per-term entry. Exactly one field is set.
//   - Sparse: ids is the sorted event-ID list. AddTo publishes a new
//     termState on every write.
//   - Dense: dense is the term's denseState. Published once, at
//     promotion; later snapshots go into dense.pub.
type termState struct {
	ids   []uint32
	dense *denseState
}

// ConcurrentBitmaps is the in-memory event index for live ingest:
// one writer, many readers.
//
// rwmu protects only the terms map. Per-term state is reached through
// an atomic pointer.
//
// AddTo is single-writer: the orchestrator ingests from one goroutine
// per chunk. Get is safe to call concurrently with AddTo. Get takes
// the map's read lock for the lookup and no other lock, unless the
// term is dense and was written since its last snapshot; then one
// reader clones the writer's bitmap under the per-term mutex. AddTo
// on a dense term waits for a reader that is cloning that term.
type ConcurrentBitmaps struct {
	rwmu  sync.RWMutex
	terms map[TermKey]*atomic.Pointer[termState]
}

// NewConcurrentBitmapsFromBitmaps takes ownership of b. The input must not
// be used afterwards.
//
// It is the only constructor, and production's only caller hands it an EMPTY
// Bitmaps: the hot index's dense overlay starts empty and self-fills via
// promotion, and a warmed-up chunk rebuilds the overlay by replaying its rows
// and sealed runs, never by handing a built Bitmaps over. The conversion path
// below is what the tests that pin the ownership contract drive.
//
// Terms below promotionThreshold become sparse lists, the same
// representation AddTo gives them. Terms at or above it keep their
// bitmap as the writer-private wbm. No snapshot is published here;
// the first Get on a dense term clones it.
func NewConcurrentBitmapsFromBitmaps(b Bitmaps) *ConcurrentBitmaps {
	cb := &ConcurrentBitmaps{terms: make(map[TermKey]*atomic.Pointer[termState], len(b))}
	for k, bm := range b {
		if bm == nil {
			continue
		}
		p := &atomic.Pointer[termState]{}
		if bm.GetCardinality() < promotionThreshold {
			p.Store(termStateFromIDs(bm.ToArray()))
		} else {
			p.Store(&termState{dense: newDenseState(bm)})
		}
		cb.terms[k] = p
	}
	return cb
}

// Get returns the postings for key, or the zero Postings when key is not
// indexed. The result is read-only: a dense term hands back the snapshot
// every concurrent reader of that term shares, and a sparse term hands back
// the store's own published id slice. The index mutates neither.
//
// A sparse entry is returned as its id slice, un-materialized, since that is
// the form it is stored in and the form Intersect can drive from. AddTo
// never writes into a published slice — it builds a fresh one per publish —
// so holding it is safe for as long as the caller wants it.
//
// Forbidden on a returned bitmap — these mutate internal state a
// concurrent reader or the writer may also be touching:
//   - Clone, CloneCopyOnWriteContainers (COW is on: Clone writes its
//     source's copy-on-write flags, so two readers cloning race)
//   - RunOptimize, AddRange, RemoveRange, FlipInt
//   - Add, AddMany, Remove, CheckedAdd, CheckedRemove, AddInt
//   - SetCopyOnWrite
//   - single-input roaring.FastAnd / roaring.FastOr (roaring takes a
//     Clone-the-input shortcut when there is only one input)
//
// The same rule applies to a returned id slice: read it, never write to it.
//
// Safe: any non-mutating read (Contains, GetCardinality, Iterator,
// ToArray, IsEmpty, Minimum, Maximum) plus roaring.And / FastAnd /
// FastOr with 2+ inputs (Intersect and Union guard their single-input
// cases before calling the aggregators).
//
// A Get that starts after an AddTo returns sees that AddTo's IDs, and
// what it returns stays valid for as long as the caller holds it.
func (s *ConcurrentBitmaps) Get(key TermKey) (Postings, error) {
	s.rwmu.RLock()
	p := s.terms[key]
	s.rwmu.RUnlock()
	if p == nil {
		return Postings{}, nil
	}
	st := p.Load()
	if st.dense != nil {
		return BitmapPostings(st.dense.snapshot()), nil
	}
	return IDPostings(st.ids), nil
}

// Has reports whether key is tracked, without materializing anything — the
// hot index's per-ledger dense-overlay membership probe.
func (s *ConcurrentBitmaps) Has(key TermKey) bool {
	s.rwmu.RLock()
	_, ok := s.terms[key]
	s.rwmu.RUnlock()
	return ok
}

// snapshot returns the term's current immutable bitmap. If a write
// landed since the last snapshot it clones wbm once and publishes
// the clone.
func (d *denseState) snapshot() *roaring.Bitmap {
	if bm := d.pub.Load(); bm != nil {
		return bm
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	bm := d.pub.Load()
	if bm == nil {
		bm = d.wbm.Clone()
		d.pub.Store(bm)
	}
	return bm
}

// AddTo records each eventID under key. Callers feed events in
// event-ID order relative to the chunk, so a duplicate is a retry of an
// already-added prefix and is skipped.
func (s *ConcurrentBitmaps) AddTo(key TermKey, eventIDs ...uint32) {
	if len(eventIDs) == 0 {
		return
	}

	s.rwmu.RLock()
	p, ok := s.terms[key]
	s.rwmu.RUnlock()

	if !ok {
		next := newTermState(eventIDs)
		p = &atomic.Pointer[termState]{}
		p.Store(next)
		s.rwmu.Lock()
		s.terms[key] = p
		s.rwmu.Unlock()
		return
	}

	old := p.Load()
	if d := old.dense; d != nil {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.wbm.AddMany(eventIDs)
		d.pub.Store(nil)
		return
	}

	// Sparse mode: new list, then promote if it crossed the threshold.
	ids := make([]uint32, 0, len(old.ids)+len(eventIDs))
	ids = append(ids, old.ids...)
	p.Store(termStateFromIDs(appendSorted(ids, eventIDs)))
}

// appendSorted appends the ids in src that are greater than dst's
// last element.
func appendSorted(dst, src []uint32) []uint32 {
	for _, id := range src {
		if len(dst) > 0 && dst[len(dst)-1] >= id {
			continue
		}
		dst = append(dst, id)
	}
	return dst
}

// termStateFromIDs builds the termState for an ascending, unique id
// list, applying promotionThreshold.
func termStateFromIDs(ids []uint32) *termState {
	if len(ids) >= promotionThreshold {
		return promote(ids)
	}
	return &termState{ids: ids}
}

// newDenseState wraps a writer-owned bitmap as a dense term. The
// bitmap is marked CopyOnWrite so snapshot's Clone is shallow. No
// snapshot is published; the first Get clones.
func newDenseState(bm *roaring.Bitmap) *denseState {
	bm.SetCopyOnWrite(true)
	return &denseState{wbm: bm}
}

// promote builds the dense termState for a term from its sorted ids.
func promote(ids []uint32) *termState {
	bm := roaring.New()
	bm.AddMany(ids)
	return &termState{dense: newDenseState(bm)}
}

// newTermState builds the first termState for a new key. A batch at
// or above the threshold is promoted without building a list.
func newTermState(eventIDs []uint32) *termState {
	if len(eventIDs) >= promotionThreshold {
		return promote(eventIDs)
	}
	return termStateFromIDs(appendSorted(make([]uint32, 0, len(eventIDs)), eventIDs))
}
