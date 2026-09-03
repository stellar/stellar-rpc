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

// denseState is the whole state of one dense term. Exactly one is
// allocated per promoted term and it lives for the term's lifetime,
// so a dense term allocates nothing per AddTo and nothing per
// published snapshot beyond the snapshot itself.
//
//   - wbm is the writer's private bitmap. AddTo mutates it in place;
//     it is NEVER handed to a reader.
//   - pub is the last immutable snapshot published to readers. nil
//     until the first read after promotion or warmup.
//   - dirty says wbm holds writes that pub does not. Set by AddTo,
//     cleared by the reader that republishes.
//   - mu serializes the writer's in-place AddMany against a reader's
//     wbm.Clone(). Both mutate wbm's needCopyOnWrite slice
//     (roaringarray.go:258-288 markAllAsNeedingCopyOnWrite on the
//     clone source; roaringarray.go:348-353
//     getWritableContainerAtIndex on the AddMany path), so they must
//     not overlap.
//
// Invariant: pub == nil implies dirty is true. Every constructor of a
// denseState sets dirty before the entry becomes reachable, and the
// only code that clears dirty stores a non-nil pub first.
type denseState struct {
	mu    sync.Mutex
	wbm   *roaring.Bitmap
	pub   atomic.Pointer[roaring.Bitmap]
	dirty atomic.Bool
}

// termState is the immutable per-term entry value. Exactly one of
// ids / dense is set:
//   - Sparse mode: ids holds the sorted []uint32 of event IDs. A new
//     termState is published on every AddTo.
//   - Dense mode: dense points at the term's denseState. This
//     termState is published once, at promotion, and is never
//     replaced for the rest of the term's life — fresher snapshots
//     are published inside denseState.pub instead.
//
// Readers atomic.Load the pointer and operate on the resulting
// struct, so the (ids, dense) pair is always observed consistently.
type termState struct {
	ids   []uint32
	dense *denseState
}

// ConcurrentBitmaps is the in-memory event index for live ingest:
// one writer, many concurrent readers. Each per-term entry is a
// single atomic.Pointer[termState].
//
// The struct-level RWMutex protects only the map's structure (the
// terms map insert when a new key arrives). Once an entry exists,
// all subsequent AddTo and Get operations bypass the lock entirely.
//
// Dense terms are writer-owned: AddTo mutates a private bitmap in
// place and only flags the term dirty, and the next reader clones
// that bitmap once to publish a fresh immutable snapshot. This keeps
// AddTo's cost proportional to the IDs it adds instead of to the
// term's container count, which is what made apply latency grow as a
// chunk filled.
//
// Concurrency contracts:
//
//   - AddTo: single-writer. The orchestrator drives ingest from one
//     goroutine per chunk. Two concurrent AddTo calls on the same
//     key would race on the sparse-mode read-modify-write and on
//     the dense bitmap's container array.
//
//   - Get / LookupKeys: many-reader. Lock-free unless the term is
//     dense and a write landed since the last snapshot, in which
//     case exactly one reader pays a Clone under the per-term mutex.
//     Safe to call concurrently with AddTo.
type ConcurrentBitmaps struct {
	rwmu  sync.RWMutex
	terms map[TermKey]*atomic.Pointer[termState]
}

// NewConcurrentBitmapsFromBitmaps takes ownership of a single-threaded
// Bitmaps (typically built via warmup or backfill) and wraps it as a
// ConcurrentBitmaps. Each per-term bitmap becomes that term's
// writer-private bitmap, marked CopyOnWrite so the first reader
// snapshot is a shallow clone.
//
// No snapshot is published here: entries start dirty and the first
// Get materializes them. Warmup builds millions of terms and most are
// never read, so cloning every one up front would be pure waste.
//
// The input Bitmaps must not be used after this call returns.
func NewConcurrentBitmapsFromBitmaps(b Bitmaps) *ConcurrentBitmaps {
	cb := &ConcurrentBitmaps{terms: make(map[TermKey]*atomic.Pointer[termState], len(b))}
	for k, bm := range b {
		if bm == nil {
			continue
		}
		p := &atomic.Pointer[termState]{}
		p.Store(&termState{dense: newDenseState(bm)})
		cb.terms[k] = p
	}
	return cb
}

// Get returns the bitmap for the given term key, or (nil, nil) when
// the key is not indexed. The returned bitmap is an immutable
// snapshot: the writer never mutates a published snapshot, and a
// fresher one is published by replacing the pointer — so the
// pointer this method returns will never be mutated by anyone, but
// only if callers respect the "read-only" half of the contract.
//
// Forbidden caller-side methods on the returned bitmap (these have
// side effects on the bitmap's internal needCopyOnWrite[] array,
// which a concurrent reader taking a fresh snapshot may also write
// to; concurrent calls from two goroutines would race):
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
// per-entry pointer is captured, the lock is released.
func (s *ConcurrentBitmaps) Get(key TermKey) (*roaring.Bitmap, error) {
	s.rwmu.RLock()
	p := s.terms[key]
	s.rwmu.RUnlock()
	if p == nil {
		return nil, nil //nolint:nilnil // not-found is signaled by nil bitmap, no error
	}
	// The pointer is always Stored with a non-nil termState before it
	// is published to the map.
	st := p.Load()
	if st.dense != nil {
		return st.dense.snapshot(), nil
	}
	bm := roaring.New()
	bm.AddMany(st.ids)
	return bm, nil
}

// snapshot returns the term's current immutable bitmap, republishing
// it from the writer's private bitmap first if a write landed since
// the last snapshot.
//
// Freshness argument — a Get that starts after an AddTo returns must
// observe that AddTo's IDs. The flag is read BEFORE the pointer, and
// the publisher stores the pointer BEFORE clearing the flag:
//
//   - AddTo completes its AddMany and sets dirty=true, both under mu,
//     before returning. So a Get that starts afterwards loads dirty
//     no earlier than that store.
//   - If it loads dirty==true it takes the slow path and either
//     clones wbm itself or finds a snapshot another reader published
//     under mu after the write. Either way the result includes the
//     write.
//   - If it loads dirty==false, some publisher had already cleared
//     the flag, and that publisher stored pub before clearing. The
//     pub load that follows therefore returns that snapshot or a
//     later one — never an older one.
//
// Reading the pointer first would break this: a reader could load a
// stale pub, then have a concurrent publisher store a fresh pub and
// clear dirty, then load dirty==false and return the stale snapshot.
func (d *denseState) snapshot() *roaring.Bitmap {
	if !d.dirty.Load() {
		if bm := d.pub.Load(); bm != nil {
			return bm
		}
	}
	// Republish: clone the writer's bitmap once. With CopyOnWrite
	// enabled on wbm the clone is shallow (it shares containers and
	// marks both sides as needing COW — roaringarray.go:258-288), and
	// the writer's next AddMany deep-copies only the containers it
	// touches (roaringarray.go:348-353, reached from Bitmap.addwithptr
	// at roaring.go:1177). The snapshot itself is never mutated
	// afterwards by anyone.
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
// Dense-mode behavior: AddMany mutates the term's writer-private
// bitmap in place under the per-term mutex, then flags it dirty.
// Nothing is allocated or published, so the call costs
// O(len(eventIDs)) rather than O(containers) — the term whose growth
// with chunk fill dominated the apply phase. Readers holding an
// earlier snapshot are unaffected; the next Get republishes.
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
		// Dense mode: in-place add, no clone, no publish. dirty is set
		// before the unlock so a Get starting after this call returns
		// cannot miss these IDs (see denseState.snapshot).
		d.mu.Lock()
		d.wbm.AddMany(eventIDs)
		d.dirty.Store(true)
		d.mu.Unlock()
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
		p.Store(promote(ids))
		return
	}
	p.Store(&termState{ids: ids})
}

// newDenseState wraps a writer-owned bitmap as a dense term. The
// bitmap is marked CopyOnWrite so the reader-side Clone in snapshot
// is O(containers) of slice copy rather than a deep copy of every
// container, and so the writer's following AddMany deep-copies only
// the containers it touches.
//
// No snapshot is published: dirty starts true, which the pub == nil
// invariant requires, and the first reader materializes one. A dense
// term that is written but never read therefore never clones.
func newDenseState(bm *roaring.Bitmap) *denseState {
	bm.SetCopyOnWrite(true)
	d := &denseState{wbm: bm}
	d.dirty.Store(true)
	return d
}

// promote builds the dense representation of a term from its sorted
// ids. The returned termState is the last one this term ever
// publishes; later snapshots go into denseState.pub.
func promote(ids []uint32) *termState {
	bm := roaring.New()
	bm.AddMany(ids)
	return &termState{dense: newDenseState(bm)}
}

// newTermState builds a fresh termState seeded with the given
// initial eventIDs. Used by AddTo on the new-key path. Promotes to
// dense mode immediately if the initial batch already exceeds the
// threshold.
func newTermState(eventIDs []uint32) *termState {
	if len(eventIDs) >= promotionThreshold {
		return promote(eventIDs)
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
