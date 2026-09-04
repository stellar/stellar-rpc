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
			p.Store(termStateFromIDs(bm.ToArray()))
		} else {
			p.Store(&termState{dense: newDenseState(bm)})
		}
		cb.terms[k] = p
	}
	return cb
}

// Get returns the bitmap for key, or (nil, nil) when key is not
// indexed. The result is read-only: dense terms share one bitmap
// across all concurrent readers, and the index never mutates it.
// Read-only includes Clone: with copy-on-write on, roaring's Clone
// writes flags on its source, so two readers cloning one snapshot
// race. A Get that starts after an AddTo returns sees that AddTo's
// IDs, and the pointer stays valid for as long as the caller holds it.
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
