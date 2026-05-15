package events

import (
	"errors"
	"iter"
	"sync"
	"sync/atomic"

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

// ErrClosed is returned by mutating methods on a memBitmaps that has been closed.
var ErrClosed = errors.New("memBitmaps is closed")

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
// Thread-safe. After Close() is called, the contents are immutable:
// mutating methods return ErrClosed and All() iterates without acquiring
// the read lock (no contention with writers, since no writes can happen).
type memBitmaps struct {
	mu     sync.RWMutex
	terms  map[TermKey]*termEntry
	closed atomic.Bool
}

var _ BitmapIndex = (*memBitmaps)(nil)

func newMemBitmaps() *memBitmaps {
	return &memBitmaps{terms: make(map[TermKey]*termEntry)}
}

func NewMemBitmaps() BitmapIndex {
	return newMemBitmaps()
}

// Get returns the bitmap for the given term key.
//
// If the store is open, returns a clone so callers can safely use it
// while the writer concurrently modifies the original via AddTo.
// If the store is closed, returns the live bitmap pointer (no clone),
// since no writes can happen. Callers must not mutate the returned bitmap.
func (s *memBitmaps) Get(key TermKey) (*roaring.Bitmap, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	te := s.terms[key]
	if te == nil {
		return nil, nil //nolint:nilnil // not-found is signaled by nil bitmap, no error
	}
	if te.bm != nil {
		if s.closed.Load() {
			return te.bm, nil
		}
		return te.bm.Clone(), nil
	}
	bm := roaring.New()
	bm.AddMany(te.ids)
	return bm, nil
}

func (s *memBitmaps) AddTo(key TermKey, eventIDs ...uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
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
// REQUIRES: the store has been Close()'d before All is called.
// Panics otherwise. The lifecycle is intentionally explicit:
//
//   - Open state — AddTo allowed, Get/Len allowed (Get clones).
//   - Closed state — AddTo rejected, Get/Len allowed (no clone),
//     All allowed (yields live pointers without copying).
//
// Yielded *roaring.Bitmap pointers are live references into the
// store. They are safe to retain past the iteration because the
// store is immutable once closed; concurrent AddTo is rejected.
func (s *memBitmaps) All() iter.Seq2[TermKey, *roaring.Bitmap] {
	return func(yield func(TermKey, *roaring.Bitmap) bool) {
		if !s.closed.Load() {
			panic("memBitmaps: All called on open store — caller must Close before iterating")
		}
		// No lock needed: closed means no concurrent writers (AddTo
		// would have been rejected) and no concurrent Close (idempotent).
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

// Close marks the store as immutable. Subsequent calls to mutating
// methods (AddTo, Put, Delete) return ErrClosed. Iteration via All()
// after Close skips lock acquisition since no concurrent writes are
// possible. Idempotent.
func (s *memBitmaps) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed.Store(true)
	return nil
}
