package event

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

// tailMergeThreshold caps a dense term's pending tail before AddTo folds it
// into the base bitmap. Sized so the long tail of medium-rate dense terms
// (~6 IDs/ledger) merges once per ~1,300 ledgers instead of cloning per
// ledger — the clone population was the apply phase's chunk-fill growth term
// and ~20% of hot ingestion's allocations — while a firehose term receiving
// every event still merges about once per ledger (bounded reader-side tail
// work). Memory bound: ≤ ~32 KB of tail per dense term.
const tailMergeThreshold = 8192

// termState is an immutable snapshot of a single term's event IDs.
// Exactly one representation is active:
//   - Sparse mode: ids holds the sorted []uint32 of event IDs.
//   - Dense mode: bm holds the published base roaring.Bitmap, and tail
//     optionally holds sorted IDs newer than the base — appended by AddTo
//     without touching bm, and folded into the base only when
//     tailMergeThreshold is reached. bm and tail are IMMUTABLE once
//     published: AddTo builds a FRESH tail slice every publish (never an
//     in-place append — readers may still be iterating the old one), and bm
//     is NEVER written again after publication — not even its COW
//     bookkeeping (see wbm below).
//
// A new termState is allocated and atomically published on every AddTo.
// Readers atomic.Load the pointer and operate on the resulting struct — by
// construction the representation is always observed consistently.
//
// wbm is the writer's PRIVATE mutable twin of bm: same content at publish
// time, sharing containers with bm COW-style. AddTo's merge path folds the
// tail into wbm (AddMany deep-copies only the touched, COW-marked
// containers) and publishes bm = wbm.Clone(). wbm is both the Clone
// RECEIVER and the mutation target, so roaring's bookkeeping writes (Clone
// marks the receiver's needCopyOnWrite slice) land only on this
// never-published object. That is what makes reader-side aggregation
// (roaring.FastOr/FastAnd, whose appendCopy READS needCopyOnWrite) safe on
// published bitmaps. Readers must never touch wbm; only the single writer
// does.
//
// mat is the one reader-mutable field: a write-once memo of base∪tail built
// by the first Get that needs it (see Get). Both racing builders derive it
// from the same immutable (bm, tail) pair, so the CAS loser discards an
// identical duplicate — the race can waste work, never diverge state.
type termState struct {
	ids  []uint32
	bm   *roaring.Bitmap
	tail []uint32
	mat  atomic.Pointer[roaring.Bitmap]
	wbm  *roaring.Bitmap
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
//     goroutine per chunk. All bitmap mutation — the merge-path
//     AddMany and the COW Clone that snapshots it — happens on the
//     writer-private wbm twin (see termState); published bitmaps are
//     never written again, bookkeeping included. Two concurrent
//     AddTo calls on the same key would race on wbm.
//
//   - Get / LookupKeys: many-reader. No Clone, just atomic.Load.
//     Safe to call concurrently with AddTo precisely because AddTo
//     never writes a published bitmap: reader-side FastOr/FastAnd
//     READ a bitmap's COW bookkeeping (appendCopy), which would race
//     a writer-side Clone of that same bitmap.
type ConcurrentBitmaps struct {
	rwmu  sync.RWMutex
	terms map[TermKey]*atomic.Pointer[termState]
}

// NewConcurrentBitmapsFromBitmaps takes ownership of a single-threaded
// Bitmaps and wraps it as a ConcurrentBitmaps. Production only ever passes
// an empty Bitmaps today (the hot index's dense overlay starts empty and
// self-fills via promotion); the conversion path is kept for tests that pin
// its ownership contract. Each per-term bitmap is marked CopyOnWrite so
// merge snapshots share containers via lazy COW.
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
		// The input bitmap becomes the writer-private wbm; what readers see
		// is a COW snapshot of it (shallow: shares containers). See termState.
		p.Store(&termState{bm: bm.Clone(), wbm: bm})
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
// which other concurrent readers READ via FastOr/FastAnd's
// appendCopy; concurrent calls from two goroutines would race):
//
//   - Clone, CloneCopyOnWriteContainers
//   - RunOptimize, AddRange, RemoveRange, FlipInt
//   - Add, AddMany, Remove, CheckedAdd, CheckedRemove, AddInt
//   - SetCopyOnWrite
//   - Any *Writable* accessor on the underlying roaringArray
//
// Safe caller-side methods (used by event.Matches today): any
// non-mutating read — Contains, GetCardinality, Iterator,
// ToArray, IsEmpty, Minimum, Maximum — plus the non-mutating
// aggregation entry points roaring.And, roaring.FastAnd (≥2
// inputs), roaring.FastOr (≥2 inputs), which produce fresh
// result bitmaps without writing through their inputs. Note the
// ≥2-input qualifier on FastAnd/FastOr: with a single input the
// roaring library has historically taken a Clone-the-input
// shortcut, so callers MUST avoid passing a singleton slice to
// those aggregators (the event store's Matches guards its single-input
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
	if st.bm != nil && len(st.tail) == 0 {
		return st.bm, nil
	}
	// Tail-bearing dense state or sparse state: materialize base∪tail (or
	// the id list) once per published state, memoized in st.mat. Racing
	// readers build content-identical bitmaps from the same immutable
	// inputs; the CAS loser discards a duplicate — wasted work, never
	// divergent state. Construction uses only reader-safe operations:
	// roaring.FastOr with two inputs, which READS st.bm's COW bookkeeping
	// but writes nothing through it — safe because published bitmaps are
	// never written after publication (the writer Clones only its private
	// wbm twin; see termState). NEVER base.Clone here — Clone WRITES the
	// receiver's COW bookkeeping and would race every other reader.
	if m := st.mat.Load(); m != nil {
		return m, nil
	}
	var m *roaring.Bitmap
	if st.bm != nil {
		tb := roaring.New()
		tb.AddMany(st.tail)
		m = roaring.FastOr(st.bm, tb)
	} else {
		m = roaring.New()
		m.AddMany(st.ids)
	}
	st.mat.CompareAndSwap(nil, m)
	return st.mat.Load(), nil
}

// Has reports whether key is tracked, without materializing anything — the
// hot index's per-ledger dense-overlay membership probe.
func (s *ConcurrentBitmaps) Has(key TermKey) bool {
	s.rwmu.RLock()
	_, ok := s.terms[key]
	s.rwmu.RUnlock()
	return ok
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
// Dense-path behavior (tail-delta): AddTo normally just publishes a new
// termState with the SAME base bitmap and a freshly-copied tail slice —
// no bitmap work at all. Only when the tail reaches tailMergeThreshold
// does it fold the tail into the writer-private wbm twin (AddMany
// COW-copies only touched containers) and publish a merged, tail-free
// state whose base is a fresh O(1) shallow snapshot of wbm. Old
// termStates stay fully immutable — bookkeeping included — so concurrent
// readers holding one see the previous snapshot.
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
		// Dense mode: append to the pending tail — the base bitmap is NOT
		// touched, so the per-ledger COW Clone this path used to pay (the
		// apply phase's chunk-fill growth term and ~20% of hot ingestion's
		// allocations) happens only at merge, amortized over
		// tailMergeThreshold IDs.
		if len(old.tail)+len(eventIDs) >= tailMergeThreshold {
			// Merge: fold tail+new IDs into the writer-private wbm, then
			// publish a fresh COW snapshot of it — ONE shallow Clone per
			// threshold-full of IDs, with AddMany deep-copying only the
			// touched containers (all COW-marked at publish time). wbm is
			// both the AddMany target and the Clone RECEIVER, so no
			// published bitmap's COW bookkeeping is ever written after
			// publication — that is the invariant reader-side FastOr
			// materialization and query-side aggregation depend on.
			// Readers holding the old state keep an untouched (bm, tail)
			// pair.
			wbm := old.wbm
			wbm.AddMany(old.tail)
			wbm.AddMany(eventIDs)
			p.Store(&termState{bm: wbm.Clone(), wbm: wbm})
			return
		}
		// FRESH slice every publish — never append(old.tail, ...): readers
		// may be iterating old.tail's backing array right now, and an
		// in-place append into spare capacity would race them.
		tail := make([]uint32, 0, len(old.tail)+len(eventIDs))
		tail = append(tail, old.tail...)
		for _, id := range eventIDs {
			// Same monotonic dedup as the sparse path: IDs arrive ascending
			// (single-writer, chunk-relative order), so a duplicate is a
			// retry of an already-appended prefix. Duplicates vs the BASE
			// are absorbed by set semantics at merge/materialize time.
			if len(tail) > 0 && tail[len(tail)-1] >= id {
				continue
			}
			tail = append(tail, id)
		}
		p.Store(&termState{bm: old.bm, wbm: old.wbm, tail: tail})
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
		// Enable lazy container-level copy-on-write: publish snapshots are
		// O(1) shallow Clones sharing container pointers, and the writer's
		// merge AddMany routes through getWritableContainerAtIndex which
		// deep-copies only the touched containers — per-touched-container
		// work instead of per-bitmap work, ~40× for popular dense terms.
		// The freshly-built bitmap becomes the writer-private wbm; readers
		// get a COW snapshot of it (see termState — published bitmaps must
		// never be Clone receivers).
		bm.SetCopyOnWrite(true)
		p.Store(&termState{bm: bm.Clone(), wbm: bm})
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
		return &termState{bm: bm.Clone(), wbm: bm}
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
