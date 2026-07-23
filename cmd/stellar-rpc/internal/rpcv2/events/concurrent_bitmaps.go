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

// termState is an immutable snapshot of a single term's event IDs.
// At most one of ids / bm is non-nil at any moment:
//   - Sparse mode: ids holds the sorted []uint32 of event IDs.
//   - Dense mode: bm holds the roaring.Bitmap (with COW enabled so
//     subsequent Clones in AddTo are O(1) shallow copies).
//
// A new termState is allocated and atomically published on every
// AddTo. Readers atomic.Load the pointer and operate on the
// resulting struct — by construction the (ids, bm) pair is always
// observed consistently, so the previous (bm.Load, ids.Load) split
// and its promotion-window re-Load workaround are gone.
type termState struct {
	ids []uint32
	bm  *roaring.Bitmap
}

// ConcurrentBitmaps is the in-memory event index for live ingest:
// one writer, many concurrent readers. Each per-term entry is a
// single atomic.Pointer[termState]. AddTo publishes a new
// termState via a single atomic.Store, so readers atomic.Load and
// operate on an immutable snapshot for as long as they hold the
// pointer. No lock is required across the borrowed pointer's
// lifetime.
//
// The struct-level RWMutex protects only the map's structure (the
// terms map insert when a new key arrives). Once an entry exists,
// all subsequent AddTo and Get operations bypass the lock entirely.
//
// Concurrency contracts:
//
//   - AddTo: single-writer. The orchestrator drives ingest from one
//     goroutine per chunk. The COW Clone inside AddTo (on the dense
//     path) mutates the source bitmap's needCopyOnWrite slice as a
//     side effect of roaring's clone implementation; two concurrent
//     AddTo calls on the same key would race on that.
//
//   - Get / LookupKeys: many-reader. No Clone, just atomic.Load.
//     Safe to call concurrently with AddTo.
type ConcurrentBitmaps struct {
	rwmu  sync.RWMutex
	terms map[TermKey]*atomic.Pointer[termState]
}

// NewConcurrentBitmapsFromBitmaps takes ownership of a single-threaded
// Bitmaps (typically built via warmup or backfill) and wraps it as a
// ConcurrentBitmaps. Each per-term bitmap is marked CopyOnWrite so
// subsequent AddTo calls share containers via lazy COW.
//
// The input Bitmaps must not be used after this call returns.
func NewConcurrentBitmapsFromBitmaps(b Bitmaps) *ConcurrentBitmaps {
	cb := &ConcurrentBitmaps{terms: make(map[TermKey]*atomic.Pointer[termState], len(b))}
	for k, bm := range b {
		if bm == nil {
			continue
		}
		bm.SetCopyOnWrite(true)
		p := &atomic.Pointer[termState]{}
		p.Store(&termState{bm: bm})
		cb.terms[k] = p
	}
	return cb
}

// Get returns the bitmap for the given term key, or (nil, nil) when
// the key is not indexed. The returned bitmap is an immutable
// snapshot: writers publish new termStates via atomic.Store, so the
// pointer this method returns will never be mutated by anyone — but
// only if callers respect the "read-only" half of the contract.
//
// Forbidden caller-side methods on the returned bitmap (these have
// side effects on the bitmap's internal needCopyOnWrite[] array,
// which the writer's COW Clone also writes to; concurrent calls
// from two goroutines would race):
//
//   - Clone, CloneCopyOnWriteContainers
//   - RunOptimize, AddRange, RemoveRange, FlipInt
//   - Add, AddMany, Remove, CheckedAdd, CheckedRemove, AddInt
//   - SetCopyOnWrite
//   - Any *Writable* accessor on the underlying roaringArray
//
// Safe caller-side methods (used by event.Query today): any
// non-mutating read — Contains, GetCardinality, Iterator,
// ToArray, IsEmpty, Minimum, Maximum — plus the non-mutating
// aggregation entry points roaring.And, roaring.FastAnd (≥2
// inputs), roaring.FastOr (≥2 inputs), which produce fresh
// result bitmaps without writing through their inputs. Note the
// ≥2-input qualifier on FastAnd/FastOr: with a single input the
// roaring library has historically taken a Clone-the-input
// shortcut, so callers MUST avoid passing a singleton slice to
// those aggregators (the event store's Query guards its single-input
// cases before calling FastAnd/FastOr).
//
// Callers may hold the pointer arbitrarily long. A subsequent Get
// on the same key may return either this same pointer (no AddTo
// happened in between) or a newer snapshot — both are valid; the
// older pointer remains usable until the caller drops it.
//
// Concurrency: the RLock is held only for the map lookup. Once the
// per-entry pointer is captured, the lock is released; the atomic
// load on the entry happens lock-free.
func (s *ConcurrentBitmaps) Get(key TermKey) (*roaring.Bitmap, error) {
	s.rwmu.RLock()
	p := s.terms[key]
	s.rwmu.RUnlock()
	if p == nil {
		return nil, nil //nolint:nilnil // not-found is signaled by nil bitmap, no error
	}
	// The pointer is always Stored with a non-nil termState holding a
	// non-nil bm or non-empty ids before it is published to the map.
	st := p.Load()
	if st.bm != nil {
		return st.bm, nil
	}
	bm := roaring.New()
	bm.AddMany(st.ids)
	return bm, nil
}

// AddTo records each eventID under key. Idempotent: callers
// (HotStore.applyLedger via the post-commit hook, warmup) feed
// events in chunk-relative event-ID order, so any duplicate is a
// retry of the already-added sorted prefix and is skipped. The same (key, eventID) pair has
// the same effect added once or many times.
//
// Single-writer contract: AddTo must not run concurrently with
// itself. The orchestrator drives ingest from one goroutine per
// chunk.
//
// COW behavior: for an existing dense term, AddTo Clones the
// current bitmap (O(1) shallow copy because the source has
// CopyOnWrite enabled), AddManys the new IDs (which COW-clones only
// the touched containers), then atomic.Stores a new termState
// pointing at the resulting bitmap. The old termState's bitmap is
// untouched — concurrent readers holding it see the previous
// snapshot.
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
	if old.bm != nil {
		// Dense mode: COW Clone + AddMany + atomic publish.
		next := old.bm.Clone()
		next.AddMany(eventIDs)
		p.Store(&termState{bm: next})
		return
	}

	// Sparse mode: build a new id list (dedup against monotonic
	// prefix); promote to dense if we cross the threshold.
	ids := make([]uint32, 0, len(old.ids)+len(eventIDs))
	ids = append(ids, old.ids...)
	for _, id := range eventIDs {
		if len(ids) > 0 && ids[len(ids)-1] >= id {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) >= promotionThreshold {
		bm := roaring.New()
		bm.AddMany(ids)
		// Enable lazy container-level copy-on-write. Subsequent
		// AddTo calls clone bm via roaring.Clone (O(1) shallow:
		// shares container pointers and marks both bitmaps as
		// needing COW), then AddMany routes through
		// getWritableContainerAtIndex which deep-copies only the
		// touched containers. This turns each ingest call's
		// Clone+AddMany into per-touched-container work instead of
		// per-bitmap work — ~40× speedup for popular dense terms.
		bm.SetCopyOnWrite(true)
		p.Store(&termState{bm: bm})
		return
	}
	p.Store(&termState{ids: ids})
}

// newTermState builds a fresh termState seeded with the given
// initial eventIDs. Used by AddTo on the new-key path. Promotes to
// dense mode immediately if the initial batch already exceeds the
// threshold.
func newTermState(eventIDs []uint32) *termState {
	if len(eventIDs) >= promotionThreshold {
		bm := roaring.New()
		bm.AddMany(eventIDs)
		bm.SetCopyOnWrite(true) // see AddTo for rationale
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
