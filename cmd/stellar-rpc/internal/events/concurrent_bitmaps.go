package events

import (
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

// concurrentTermEntry holds event IDs for a single term, either as a
// compact list (sparse) or a roaring bitmap (dense). Both fields are
// atomic.Pointers so writers publish copy-on-write snapshots and
// readers atomic.Load the current state without holding any lock past
// the map lookup.
//
// At most one of ids / bm is non-nil at any moment:
//   - Sparse mode: ids holds the sorted []uint32 of event IDs.
//   - Dense mode: bm holds the roaring.Bitmap.
//
// The transition from sparse to dense happens inside AddTo via an
// atomic.Store on bm and an atomic.Store of an empty slice on ids.
type concurrentTermEntry struct {
	ids atomic.Pointer[[]uint32]
	bm  atomic.Pointer[roaring.Bitmap]
}

// ConcurrentBitmaps is the in-memory event index for live ingest:
// one writer, many concurrent readers. Each per-term entry is a pair
// of atomic.Pointers (sparse list and dense bitmap), and AddTo
// publishes new copies rather than mutating in place — so readers
// atomic.Load and operate on an immutable snapshot for as long as
// they hold the pointer. No lock is required across the borrowed
// pointer's lifetime.
//
// The struct-level RWMutex protects only the map's structure (the
// terms map insert when a new key arrives). Once an entry exists,
// all subsequent AddTo and Get operations bypass the lock entirely —
// they go through atomic.Load/Store on the entry's pointers.
//
// Single-writer contract: AddTo must be called from a single
// goroutine (the orchestrator's ingest goroutine for the owning
// chunk). The atomic-pointer COW pattern relies on the writer
// observing the previous value via atomic.Load and producing the
// next via atomic.Store; two concurrent writers would lose updates.
//
// To freeze the index to disk, call Snapshot to get a uniquely-owned
// Bitmaps the caller can mutate (e.g., RunOptimize before
// MarshalBinary).
type ConcurrentBitmaps struct {
	rwmu  sync.RWMutex
	terms map[TermKey]*concurrentTermEntry
}

// NewConcurrentBitmaps returns an empty ConcurrentBitmaps ready for
// single-writer + many-reader use.
func NewConcurrentBitmaps() *ConcurrentBitmaps {
	return &ConcurrentBitmaps{terms: make(map[TermKey]*concurrentTermEntry)}
}

// Get returns the bitmap for the given term key, or (nil, nil) when
// the key is not indexed. The returned bitmap is an immutable
// snapshot: writers publish new copies via atomic.Store, so the
// pointer this method returns will never be mutated by anyone.
//
// Callers may hold the pointer arbitrarily long. The only constraint
// is: do not mutate it yourself. A subsequent Get on the same key
// may return either this same pointer (no AddTo happened in between)
// or a newer snapshot — both are valid; the older pointer remains
// usable until the caller drops it.
//
// Concurrency: the RLock is held only for the map lookup. Once the
// per-entry pointer is captured, the lock is released; the atomic
// load on the entry happens lock-free.
func (s *ConcurrentBitmaps) Get(key TermKey) (*roaring.Bitmap, error) {
	s.rwmu.RLock()
	te := s.terms[key]
	s.rwmu.RUnlock()
	if te == nil {
		return nil, nil //nolint:nilnil // not-found is signaled by nil bitmap, no error
	}
	if bm := te.bm.Load(); bm != nil {
		return bm, nil
	}
	ids := te.ids.Load()
	if ids != nil && len(*ids) > 0 {
		bm := roaring.New()
		bm.AddMany(*ids)
		return bm, nil
	}
	// Either genuinely empty OR mid-promotion: the writer Stored bm
	// then Stored empty ids, and this reader's two Loads straddled
	// both writes. Re-Load bm. An entry that exists in the map must
	// have either a non-nil bm or non-empty ids — newConcurrentTermEntry
	// never leaves both empty.
	if bm := te.bm.Load(); bm != nil {
		return bm, nil
	}
	return nil, nil //nolint:nilnil
}

// AddTo records each eventID under key. Idempotent: callers
// (IngestLedgerEvents, warmup) feed events in chunk-relative event-ID
// order, so any duplicate is a retry of the already-added sorted
// prefix and is skipped. The same (key, eventID) pair has the same
// effect added once or many times.
//
// Single-writer contract: AddTo must not run concurrently with
// itself. The orchestrator drives ingest from one goroutine per
// chunk.
//
// COW behavior: for an existing term, AddTo clones the current
// per-mode state (slice or bitmap), appends the new IDs, and
// atomic.Stores the new pointer. Concurrent Get callers atomic.Load
// either the previous snapshot or the new one — both are immutable.
func (s *ConcurrentBitmaps) AddTo(key TermKey, eventIDs ...uint32) {
	if len(eventIDs) == 0 {
		return
	}

	s.rwmu.RLock()
	te, ok := s.terms[key]
	s.rwmu.RUnlock()

	if !ok {
		// New key. Construct a fully-initialized entry, then take
		// the write lock briefly to insert into the map.
		te = newConcurrentTermEntry(eventIDs)
		s.rwmu.Lock()
		s.terms[key] = te
		s.rwmu.Unlock()
		return
	}

	if bm := te.bm.Load(); bm != nil {
		next := bm.Clone()
		next.AddMany(eventIDs)
		te.bm.Store(next)
		return
	}

	old := te.ids.Load()
	next := make([]uint32, 0, len(*old)+len(eventIDs))
	next = append(next, (*old)...)
	for _, id := range eventIDs {
		if len(next) > 0 && next[len(next)-1] >= id {
			// Already in the sorted prefix — retry duplicate, skip.
			continue
		}
		next = append(next, id)
	}
	if len(next) >= promotionThreshold {
		bm := roaring.New()
		bm.AddMany(next)
		te.bm.Store(bm)
		// Clear ids only after bm is published — Get checks bm
		// first, so a reader between Store(bm) and Store(empty ids)
		// returns the dense form.
		var empty []uint32
		te.ids.Store(&empty)
		return
	}
	te.ids.Store(&next)
}

// newConcurrentTermEntry builds a fresh entry seeded with the given
// initial eventIDs. Used by AddTo on the new-key path. Promotes to
// dense mode immediately if the initial batch already exceeds the
// threshold.
func newConcurrentTermEntry(eventIDs []uint32) *concurrentTermEntry {
	te := &concurrentTermEntry{}
	if len(eventIDs) >= promotionThreshold {
		bm := roaring.New()
		bm.AddMany(eventIDs)
		te.bm.Store(bm)
		var empty []uint32
		te.ids.Store(&empty)
		return te
	}
	ids := make([]uint32, 0, len(eventIDs))
	for _, id := range eventIDs {
		if len(ids) > 0 && ids[len(ids)-1] >= id {
			continue
		}
		ids = append(ids, id)
	}
	te.ids.Store(&ids)
	return te
}

// Len returns the number of distinct terms currently indexed.
func (s *ConcurrentBitmaps) Len() int {
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()
	return len(s.terms)
}

// Snapshot materializes the index into a Bitmaps that the caller
// uniquely owns. Each per-term bitmap is a fresh Clone (for
// dense-mode entries) or a freshly-built bitmap from the sparse-mode
// id list, so the returned Bitmaps shares no state with the
// ConcurrentBitmaps. The caller may freely mutate the bitmaps
// (e.g., RunOptimize before MarshalBinary in WriteColdIndex).
//
// Cost: one Clone per dense term + one materialize per sparse term.
// The RWMutex is held in read mode for the entire iteration,
// blocking only new-key inserts (AddTo on existing keys does not
// take the write lock).
func (s *ConcurrentBitmaps) Snapshot() Bitmaps {
	s.rwmu.RLock()
	defer s.rwmu.RUnlock()
	snap := make(Bitmaps, len(s.terms))
	for k, te := range s.terms {
		if bm := te.bm.Load(); bm != nil {
			snap[k] = bm.Clone()
			continue
		}
		ids := te.ids.Load()
		if ids != nil && len(*ids) > 0 {
			bm := roaring.New()
			bm.AddMany(*ids)
			snap[k] = bm
			continue
		}
		// Mid-promotion (writer Stored bm then Stored empty ids,
		// reader's two Loads straddled both writes). Re-Load bm.
		// See Get for the full reasoning.
		if bm := te.bm.Load(); bm != nil {
			snap[k] = bm.Clone()
		}
	}
	return snap
}
