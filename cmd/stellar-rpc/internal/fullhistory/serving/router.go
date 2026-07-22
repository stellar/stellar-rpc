// Package serving is the query read side: it routes each requested chunk to its
// serving store (frozen cold files or a ready hot database) against a consistent
// snapshot of the catalog taken when the request is admitted. See
// design-docs/query-routing-design.md.
package serving

import (
	"sync"
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
)

// Router owns the two pieces of serving state that cannot live in the catalog:
// the latest watermark, which advances every ledger, and the open hot-database
// handles, which are live objects. Everything else a query needs is read from
// the catalog through an admission snapshot.
//
// The Router does not own the catalog; the daemon constructs and closes it.
type Router struct {
	catalog   *catalog.Catalog
	retention geometry.Retention

	// latest is the newest fully ingested ledger visible to queries. The ingest
	// loop advances it as the final step of each per-ledger cycle.
	latest atomic.Uint32

	// handles is the copy-on-write set of open hot-database handles, published
	// atomically. A query loads it once at admission.
	handles atomic.Pointer[HandleSet]

	// mu serializes handle-set updates (publish/discard) so a lost update cannot
	// drop a concurrently published handle.
	mu sync.Mutex
}

// HandleSet is an immutable set of open hot-database handles keyed by chunk,
// replaced wholesale on every publish or discard so a query that loaded one keeps
// reading it.
type HandleSet struct {
	hot map[chunk.ID]*hotchunk.DB
}

// clone returns a deep copy of the map so a copy-on-write update never mutates a
// set a query is already reading.
func (h *HandleSet) clone() *HandleSet {
	m := make(map[chunk.ID]*hotchunk.DB, len(h.hot))
	for c, db := range h.hot {
		m[c] = db
	}
	return &HandleSet{hot: m}
}

// NewRouter binds a Router to the catalog and retention policy, starting with an
// empty handle set and a zero watermark.
func NewRouter(cat *catalog.Catalog, retention geometry.Retention) *Router {
	r := &Router{catalog: cat, retention: retention}
	r.handles.Store(&HandleSet{hot: map[chunk.ID]*hotchunk.DB{}})
	return r
}

func (r *Router) SetLatest(seq uint32) { r.latest.Store(seq) }

func (r *Router) Latest() uint32 { return r.latest.Load() }

// PublishHandle adds or replaces the hot database for chunk c and publishes the
// new set atomically.
func (r *Router) PublishHandle(c chunk.ID, db *hotchunk.DB) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := r.handles.Load().clone()
	next.hot[c] = db
	r.handles.Store(next)
}

// DiscardHandle removes the hot database for chunk c and publishes the new set
// atomically. It only unpublishes the handle; deferred deletion closes it later.
func (r *Router) DiscardHandle(c chunk.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := r.handles.Load().clone()
	delete(next.hot, c)
	r.handles.Store(next)
}

// Admission is one query's consistent view of serving state, held for the
// request's lifetime and released when it completes. It carries the admitted
// watermark and retention floor, the handle set loaded at admission, and the
// catalog snapshot the routing reads run against.
type Admission struct {
	latest  uint32
	floor   chunk.ID
	handles *HandleSet
	snap    *rocksdb.Snapshot
	catalog *catalog.Catalog
}

// Admit captures a query's view of serving state with three loads, in this order:
// latest first, the handle set second, the catalog snapshot last. The order makes
// the snapshot's metadata the newest of the three, so any skew between the handle
// set and the snapshot resolves safely (see the design's Admission section).
//
// The caller MUST call Release when the request completes, including on error
// paths.
func (r *Router) Admit() (*Admission, error) {
	latest := r.latest.Load()
	handles := r.handles.Load()
	snap, err := r.catalog.NewSnapshot()
	if err != nil {
		return nil, err
	}
	frontier, err := hotFrontier(r.catalog, snap)
	if err != nil {
		r.catalog.ReleaseSnapshot(snap)
		return nil, err
	}
	return &Admission{
		latest:  latest,
		floor:   r.retention.FloorAt(frontier),
		handles: handles,
		snap:    snap,
		catalog: r.catalog,
	}, nil
}

func (a *Admission) Latest() uint32 { return a.latest }

func (a *Admission) Floor() chunk.ID { return a.floor }

// Release releases the admission snapshot back to the catalog.
func (a *Admission) Release() { a.catalog.ReleaseSnapshot(a.snap) }

// hotFrontier is the last-complete-chunk anchor the floor is derived from: the
// highest ready hot chunk in the snapshot minus one (the highest ready chunk is
// the live, still-ingesting chunk, so the one below it is the last complete one).
// Returns -1 when no hot chunk is ready, meaning nothing is complete yet — the
// signed convention Retention.FloorAt expects.
func hotFrontier(cat *catalog.Catalog, snap *rocksdb.Snapshot) (int64, error) {
	ready, err := cat.ReadyHotChunkKeysAsOf(snap)
	if err != nil {
		return 0, err
	}
	if len(ready) == 0 {
		return -1, nil
	}
	return int64(ready[len(ready)-1]) - 1, nil
}
