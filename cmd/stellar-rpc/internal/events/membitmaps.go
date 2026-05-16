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
// Thread-safe. There is no closed/frozen state: Get always returns a
// clone, All holds an RLock for the iteration body. See BitmapIndex
// for the concurrency contract.
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

// Get returns a clone of the bitmap for the given term key. Callers
// may mutate the returned bitmap without affecting the index or
// other concurrent readers. Sparse-mode terms produce a fresh
// bitmap built from the stored id list; promoted terms produce a
// clone of the stored bitmap.
func (s *memBitmaps) Get(key TermKey) (*roaring.Bitmap, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	te := s.terms[key]
	if te == nil {
		return nil, nil //nolint:nilnil // not-found is signaled by nil bitmap, no error
	}
	if te.bm != nil {
		return te.bm.Clone(), nil
	}
	bm := roaring.New()
	bm.AddMany(te.ids)
	return bm, nil
}

func (s *memBitmaps) AddTo(key TermKey, eventIDs ...uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	te, ok := s.terms[key]
	if !ok {
		te = &termEntry{}
		s.terms[key] = te
	}

	if te.bm != nil {
		te.bm.AddMany(eventIDs)
	} else {
		te.ids = append(te.ids, eventIDs...)
		if len(te.ids) >= promotionThreshold {
			te.bm = roaring.New()
			te.bm.AddMany(te.ids)
			te.ids = nil
		}
	}
	return nil
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
