package events

import (
	"iter"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
)

// promotionThreshold is the number of event IDs stored in a list
// before promoting to a roaring bitmap. Most terms are sparse and
// a list is more memory-efficient than a roaring bitmap for small sets.
const promotionThreshold = 64

// termEntry holds event IDs for a single term, either as a compact list
// or a roaring bitmap.
type termEntry struct {
	ids []uint32
	bm  *roaring.Bitmap
}

// memBitmaps is an in-memory BitmapStore. Internally, it uses a compact
// list for sparse terms (≤64 events) and roaring bitmaps for dense terms,
// but always returns *roaring.Bitmap via Get.
// Thread-safe.
type memBitmaps struct {
	mu    sync.RWMutex
	terms map[TermKey]*termEntry
}

func newMemBitmaps() *memBitmaps {
	return &memBitmaps{
		terms: make(map[TermKey]*termEntry),
	}
}

// Get returns a clone of the bitmap so callers can safely use it
// while the writer concurrently modifies the original via AddTo.
func (s *memBitmaps) Get(key TermKey) (*roaring.Bitmap, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	te := s.terms[key]
	if te == nil {
		return nil, nil
	}
	if te.bm != nil {
		return te.bm.Clone(), nil
	}
	bm := roaring.New()
	bm.AddMany(te.ids)
	return bm, nil
}

func (s *memBitmaps) Put(key TermKey, bm *roaring.Bitmap) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terms[key] = &termEntry{bm: bm}
	return nil
}

func (s *memBitmaps) Delete(key TermKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.terms, key)
	return nil
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
// list mode, a temporary bitmap is built per iteration step. Holds
// a read lock for the duration of iteration.
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

func (s *memBitmaps) Optimize() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, te := range s.terms {
		if te.bm != nil {
			te.bm.RunOptimize()
		}
	}
}

func (s *memBitmaps) Len() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.terms))
}

func (s *memBitmaps) Close() error { return nil }
