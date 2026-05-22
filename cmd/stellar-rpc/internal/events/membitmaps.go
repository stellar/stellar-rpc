package events

import (
	"iter"
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

// termEntry holds event IDs for a single term, either as a compact list
// or a roaring bitmap.
type termEntry struct {
	ids []uint32
	bm  *roaring.Bitmap
}

// memBitmaps is an in-memory EventIndex. Internally, it uses a compact
// list for sparse terms (≤64 events) and roaring bitmaps for dense terms,
// but always returns *roaring.Bitmap via Get.
//
// Thread-safety: Get and All both take RLock around map access. The
// returned bitmap from Get is the live mirror state in dense mode
// (no defensive clone) — callers MUST NOT mutate it and MUST NOT
// hold the pointer across a write that touches the same term. See
// the BitmapIndex contract on Get for the full caller obligation.
type memBitmaps struct {
	mu    sync.RWMutex
	terms map[TermKey]*termEntry
}

var _ BitmapIndex = (*memBitmaps)(nil)

func newMemBitmaps() *memBitmaps {
	return &memBitmaps{terms: make(map[TermKey]*termEntry)}
}

func NewMemBitmaps() BitmapIndex {
	return newMemBitmaps()
}

// Get returns the bitmap for the given term key, or (nil, nil) when
// the key is not indexed. The returned bitmap is BORROWED in dense
// mode (a direct pointer to the mirror's stored bitmap); callers
// MUST NOT mutate it — doing so would corrupt the index for every
// other reader. Sparse-mode terms still allocate a fresh bitmap on
// each call (built from the stored id list); the contract is the
// same — treat the result as read-only regardless.
//
// Concurrency: holds an RLock around map access. The borrowed
// bitmap pointer is valid as long as no concurrent AddTo touches
// the same key. Callers needing to hold the pointer across a
// potential write must serialize externally (in practice: ingest
// to a chunk's hot store is single-threaded and gated by
// HotStore.mu; reads of that chunk's mirror that happen between
// IngestLedgerEvents calls observe a stable bitmap).
//
// This contract eliminates a per-Lookup Clone — for high-cardinality
// terms (popular contracts, common verbs) Clone was the dominant
// cost in HotStore.LookupKeys.
func (s *memBitmaps) Get(key TermKey) (*roaring.Bitmap, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	te := s.terms[key]
	if te == nil {
		return nil, nil //nolint:nilnil // not-found is signaled by nil bitmap, no error
	}
	if te.bm != nil {
		return te.bm, nil
	}
	bm := roaring.New()
	bm.AddMany(te.ids)
	return bm, nil
}

// AddTo records each eventID under key. It is idempotent and
// requires eventIDs to be added in monotonically increasing order
// per term: callers (IngestLedgerEvents, warmup) feed events in
// chunk-relative event-ID order, so any duplicate is a retry of
// the already-added sorted prefix and is skipped silently. The
// same (key, eventID) pair has the same effect added once or many
// times.
//
// Bitmap mode is idempotent via roaring's set semantics; list mode
// dedupes against the last entry (>= comparison — not just ==,
// because a partial retry can replay multiple already-added IDs).
func (s *memBitmaps) AddTo(key TermKey, eventIDs ...uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	te, ok := s.terms[key]
	if !ok {
		te = &termEntry{}
		s.terms[key] = te
	}

	if te.bm != nil {
		te.bm.AddMany(eventIDs)
		return
	}
	for _, id := range eventIDs {
		if len(te.ids) > 0 && te.ids[len(te.ids)-1] >= id {
			// Already in the sorted prefix — retry duplicate, skip.
			continue
		}
		te.ids = append(te.ids, id)
	}
	if len(te.ids) >= promotionThreshold {
		te.bm = roaring.New()
		te.bm.AddMany(te.ids)
		te.ids = nil
	}
}

// All returns an iterator over all terms. For sparse terms still in
// list mode, a temporary bitmap is built per iteration step.
//
// The RLock is held for the duration of the iteration body. The
// yielded *roaring.Bitmap is the live pointer inside the store —
// valid only inside the iteration body. Callers wanting to retain
// the pointer past the iteration must Clone it. Concurrent AddTo
// (writer) blocks until the iteration completes; concurrent Get
// (reader) runs in parallel and only clones.
func (s *memBitmaps) All() iter.Seq2[TermKey, *roaring.Bitmap] {
	return func(yield func(TermKey, *roaring.Bitmap) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for key, te := range s.terms {
			var bm *roaring.Bitmap
			if te.bm != nil {
				bm = te.bm
			} else {
				bm = roaring.New()
				bm.AddMany(te.ids)
			}
			if !yield(key, bm) {
				return
			}
		}
	}
}

func (s *memBitmaps) Len() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.terms))
}
