package events

import (
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
)

// promotionThreshold is the number of event IDs stored in a list
// before promoting to a roaring bitmap. Most terms are sparse and
// a list is more memory-efficient than a roaring bitmap for small sets.
//
// Value of 64 chosen to comfortably exceed the observed mean
// cardinality (~14.5–16.3 events per term across production chunks
// 005901–005908; see BenchmarkEventIndex_10M for the modeled
// distribution). Terms below the threshold stay in list mode
// (≈256 B per slice); only long-tail dense terms promote to roaring.
const promotionThreshold = 64

// termState holds a single term's event IDs in one of two modes. At
// most one of ids / bm is non-nil at any moment:
//   - Sparse mode: ids holds the sorted []uint32 of event IDs.
//   - Dense mode: bm holds the roaring bitmap.
//
// ConcurrentBitmaps.mu guards every access, so the writer mutates a
// *termState in place (AddMany onto bm, append onto ids) rather than
// publishing a new value.
type termState struct {
	ids []uint32        // sparse mode (nil once dense)
	bm  *roaring.Bitmap // dense mode (nil while sparse)
}

// ConcurrentBitmaps is the in-memory event index for live ingest:
// one writer, many concurrent readers, all serialized by a single
// RWMutex that guards both the map and every per-term state.
//
// The write path inverts the usual clone-on-write. AddTo takes the
// write lock and mutates the term's bitmap in place. Because event
// IDs are assigned monotonically, AddMany only ever touches the tail
// container, so a per-ledger apply costs O(that ledger's events) and
// allocates almost nothing — independent of how large a hot term has
// grown.
//
// Get takes the read lock and returns an owned deep clone (dense) or
// a fresh bitmap built from the ids (sparse). Queries touch few
// terms, so one clone per queried term is cheap, and the caller may
// read or mutate the result freely.
//
// Copy-on-write is off on every stored bitmap, and that is what makes
// clone-on-read safe: with COW disabled roaring's Clone is a pure
// read-only deep copy of the source, so many readers cloning the same
// term under a shared RLock never touch shared mutable state. (With
// COW on, Clone rewrites the source's needCopyOnWrite slice as a side
// effect, and concurrent clones would race.)
type ConcurrentBitmaps struct {
	mu    sync.RWMutex
	terms map[TermKey]*termState
}

// NewConcurrentBitmapsFromBitmaps takes ownership of a single-threaded
// Bitmaps (typically built via warmup or backfill) and wraps it as a
// ConcurrentBitmaps. The input bitmaps already have copy-on-write off,
// which is exactly what the clone-on-read path requires.
//
// The input Bitmaps must not be used after this call returns.
func NewConcurrentBitmapsFromBitmaps(b Bitmaps) *ConcurrentBitmaps {
	cb := &ConcurrentBitmaps{terms: make(map[TermKey]*termState, len(b))}
	for k, bm := range b {
		if bm == nil {
			continue
		}
		cb.terms[k] = &termState{bm: bm}
	}
	return cb
}

// Get returns the bitmap for the given term key, or (nil, nil) when
// the key is not indexed. The returned bitmap is an owned copy — a
// deep clone for a dense term, a fresh bitmap for a sparse one — so
// it is a point-in-time snapshot: a later AddTo won't affect it, and
// mutating it won't affect the mirror. The error is always nil; it is
// kept for API compatibility.
func (s *ConcurrentBitmaps) Get(key TermKey) (*roaring.Bitmap, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st := s.terms[key]
	if st == nil {
		return nil, nil //nolint:nilnil // not-found is signaled by nil bitmap, no error
	}
	if st.bm != nil {
		return st.bm.Clone(), nil
	}
	bm := roaring.New()
	bm.AddMany(st.ids)
	return bm, nil
}

// AddTo records each eventID under key, in place under the write
// lock. Idempotent: callers (HotStore.applyLedger via the post-commit
// hook, warmup) feed events in chunk-relative event-ID order, so any
// duplicate is a retry of the already-added sorted prefix — skipped
// in sparse mode, absorbed by roaring's set semantics in dense mode.
//
// The write lock serializes writers; ingest still drives AddTo from a
// single goroutine per chunk.
func (s *ConcurrentBitmaps) AddTo(key TermKey, eventIDs ...uint32) {
	if len(eventIDs) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	st := s.terms[key]
	if st == nil {
		s.terms[key] = newTermState(eventIDs)
		return
	}

	if st.bm != nil {
		// Dense mode: monotonic IDs mean AddMany only touches the
		// tail container.
		st.bm.AddMany(eventIDs)
		return
	}

	// Sparse mode: grow the id list (dedup against the monotonic
	// prefix), promoting to dense if we cross the threshold.
	for _, id := range eventIDs {
		if len(st.ids) > 0 && st.ids[len(st.ids)-1] >= id {
			continue
		}
		st.ids = append(st.ids, id)
	}
	if len(st.ids) >= promotionThreshold {
		bm := roaring.New()
		bm.AddMany(st.ids)
		st.bm = bm
		st.ids = nil
	}
}

// newTermState builds a fresh termState seeded with the given initial
// eventIDs, used by AddTo on the new-key path. Promotes to dense mode
// immediately if the initial batch already meets the threshold.
func newTermState(eventIDs []uint32) *termState {
	if len(eventIDs) >= promotionThreshold {
		bm := roaring.New()
		bm.AddMany(eventIDs)
		return &termState{bm: bm}
	}
	ids := make([]uint32, 0, len(eventIDs))
	for _, id := range eventIDs {
		if len(ids) > 0 && ids[len(ids)-1] >= id {
			continue
		}
		ids = append(ids, id)
	}
	return &termState{ids: ids}
}
