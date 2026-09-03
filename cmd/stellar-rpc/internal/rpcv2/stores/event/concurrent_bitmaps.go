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
//   - pub is the last immutable snapshot handed to readers. nil until
//     a read; set back to nil by every write.
//   - dirty is true when wbm holds writes that pub does not.
//   - mu serializes AddMany on wbm against Clone of wbm. Both write
//     wbm's copy-on-write flags.
//
// Invariant: pub == nil implies dirty == true, except for the window
// inside AddTo between clearing pub and setting dirty, which is held
// under mu.
type denseState struct {
	mu    sync.Mutex
	wbm   *roaring.Bitmap
	pub   atomic.Pointer[roaring.Bitmap]
	dirty atomic.Bool
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
// per chunk. Get is safe to call concurrently with AddTo. Get is
// lock-free unless the term is dense and was written since the last
// snapshot; then one reader clones the writer's bitmap under the
// per-term mutex. AddTo on a dense term waits for a reader that is
// cloning that term.
type ConcurrentBitmaps struct {
	rwmu  sync.RWMutex
	terms map[TermKey]*atomic.Pointer[termState]
}

// NewConcurrentBitmapsFromBitmaps takes ownership of a Bitmaps built
// by warmup or backfill. The input must not be used afterwards.
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
			// ToArray is ascending and unique, so it is already a
			// valid sparse list.
			p.Store(termStateFromIDs(bm.ToArray()))
		} else {
			p.Store(&termState{dense: newDenseState(bm)})
		}
		cb.terms[k] = p
	}
	return cb
}

// Get returns the bitmap for key, or (nil, nil) when key is not
// indexed. The result is an immutable snapshot that no one mutates
// afterwards, as long as callers do not mutate it either.
//
// The snapshot is shared by every concurrent reader of the term.
// These methods write its container index or copy-on-write flags and
// would race with another reader: Clone, CloneCopyOnWriteContainers,
// RunOptimize, AddRange, RemoveRange, FlipInt, Add, AddMany, Remove,
// CheckedAdd, CheckedRemove, AddInt, SetCopyOnWrite, and any
// *Writable* accessor.
//
// Safe: non-mutating reads (Contains, GetCardinality, Iterator,
// ToArray, IsEmpty, Minimum, Maximum) and roaring.And, roaring.FastAnd
// and roaring.FastOr with two or more inputs. With one input FastAnd
// and FastOr may Clone it; callers must guard that case.
//
// A Get that starts after an AddTo returns sees that AddTo's IDs.
// Callers may hold the pointer indefinitely.
func (s *ConcurrentBitmaps) Get(key TermKey) (*roaring.Bitmap, error) {
	s.rwmu.RLock()
	p := s.terms[key]
	s.rwmu.RUnlock()
	if p == nil {
		return nil, nil //nolint:nilnil // not-found is signaled by nil bitmap, no error
	}
	st := p.Load()
	if st.dense != nil {
		return st.dense.snapshot(), nil
	}
	bm := roaring.New()
	bm.AddMany(st.ids)
	return bm, nil
}

// snapshot returns the term's current immutable bitmap. If a write
// landed since the last snapshot it clones wbm once and publishes
// the clone.
//
// Ordering: dirty is read before pub, and a publisher stores pub
// before it clears dirty. AddTo sets dirty under mu before it
// returns. So a Get that starts after AddTo returns either loads
// dirty == true and takes the slow path, or loads dirty == false
// and then loads a pub that was stored after that write. Loading pub
// first could return a stale snapshot when another reader publishes
// and clears dirty between the two loads.
//
// pub == nil with dirty == false is possible for the window inside
// AddTo; the fast path falls through to the slow path and clones.
//
// The clone is shallow: wbm has CopyOnWrite set. This relies on two
// roaring properties (v2.18.2): Clone marks every container of the
// source copy-on-write, and AddMany deep-copies a flagged container
// before writing to it. So the writer's next AddMany copies only the
// containers it touches, and never a published snapshot.
func (d *denseState) snapshot() *roaring.Bitmap {
	if !d.dirty.Load() {
		if bm := d.pub.Load(); bm != nil {
			return bm
		}
	}
	d.mu.Lock()
	bm := d.pub.Load()
	if d.dirty.Load() || bm == nil {
		bm = d.wbm.Clone()
		d.pub.Store(bm)
		d.dirty.Store(false)
	}
	d.mu.Unlock()
	return bm
}

// AddTo records each eventID under key. Callers feed events in
// chunk-relative event-ID order, so a duplicate is a retry of an
// already-added prefix and is skipped. AddTo must not run
// concurrently with itself.
//
// Dense terms: AddMany on wbm under mu, then pub = nil and
// dirty = true. No clone and no publish. The only allocations are
// container growth and, when a reader has snapshotted the term since
// the last write, roaring's deep copy of each touched container.
// Cost is O(len(eventIDs)), not O(containers).
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
		d.wbm.AddMany(eventIDs)
		// Drop the stale snapshot so the index stops holding the
		// containers roaring just deep-copied.
		d.pub.Store(nil)
		d.dirty.Store(true)
		d.mu.Unlock()
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
// snapshot is published; dirty starts true.
func newDenseState(bm *roaring.Bitmap) *denseState {
	bm.SetCopyOnWrite(true)
	d := &denseState{wbm: bm}
	d.dirty.Store(true)
	return d
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
