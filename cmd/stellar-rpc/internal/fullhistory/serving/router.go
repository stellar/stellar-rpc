// Package serving is the query read side: it routes each requested chunk to its
// serving store (frozen cold files or a ready hot database) against a consistent
// snapshot of the catalog taken when the request is admitted. See
// design-docs/query-routing-design.md.
package serving

import (
	"fmt"
	"sync"
	"sync/atomic"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

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

	// watermark is the newest fully ingested ledger visible to queries. The ingest
	// loop advances it as the final step of each per-ledger cycle. Queries read a
	// frozen copy at admission (Admission.Latest), never this live value.
	watermark atomic.Uint32

	// handles is the copy-on-write map of open hot-database handles, published
	// atomically. A query loads it once at admission.
	handles atomic.Pointer[hotHandles]

	// mu serializes handle updates (publish/discard) so a lost update cannot drop
	// a concurrently published handle.
	mu sync.Mutex
}

// hotHandles is an immutable map of open hot-database handles keyed by chunk,
// replaced wholesale on every publish or discard so a query that loaded one keeps
// reading it.
type hotHandles struct {
	byChunk map[chunk.ID]*hotchunk.DB
}

// clone returns a deep copy so a copy-on-write update never mutates a map a query
// is already reading.
func (h *hotHandles) clone() *hotHandles {
	m := make(map[chunk.ID]*hotchunk.DB, len(h.byChunk))
	for c, db := range h.byChunk {
		m[c] = db
	}
	return &hotHandles{byChunk: m}
}

// NewRouter binds a Router to the catalog and retention policy, starting with an
// empty handle map and a zero watermark.
func NewRouter(cat *catalog.Catalog, retention geometry.Retention) *Router {
	r := &Router{catalog: cat, retention: retention}
	r.handles.Store(&hotHandles{byChunk: map[chunk.ID]*hotchunk.DB{}})
	return r
}

// PublishReadyHandles opens and publishes a handle for every ready hot chunk
// except liveChunk, which the ingestion loop opens and publishes itself. These are
// completed chunks a prior run left ready (not yet discarded); queries read them
// hot until the freeze covers them cold. They are opened read-write so the events
// facade is warmed (a read-only open is ledgers-only), and the router closes them
// at discard. Runs at startup before any query is admitted.
func (r *Router) PublishReadyHandles(liveChunk chunk.ID, logger *supportlog.Entry) error {
	ready, err := r.catalog.ReadyHotChunkKeys()
	if err != nil {
		return fmt.Errorf("bootstrap: read ready hot chunks: %w", err)
	}
	for _, c := range ready {
		if c == liveChunk {
			continue
		}
		db, err := hotchunk.OpenReadyWrite(geometry.HotReady, r.catalog.Layout().HotChunkPath(c), c, logger)
		if err != nil {
			return fmt.Errorf("bootstrap: open hot chunk %s: %w", c, err)
		}
		r.PublishHandle(c, db)
	}
	return nil
}

// SetWatermark publishes the newest fully ingested ledger; the ingest loop calls
// it as the final step of each per-ledger cycle.
func (r *Router) SetWatermark(seq uint32) { r.watermark.Store(seq) }

// Watermark returns the live watermark. Queries do not call this — they read the
// frozen Admission.Latest captured at admission (see the watermark field).
func (r *Router) Watermark() uint32 { return r.watermark.Load() }

// Handle returns the currently published hot database for chunk c, if any. The
// freeze source reads a completed chunk through this shared handle rather than
// opening a second reader against the still-open writer.
func (r *Router) Handle(c chunk.ID) (*hotchunk.DB, bool) {
	db, ok := r.handles.Load().byChunk[c]
	return db, ok
}

// PublishHandle adds or replaces the hot database for chunk c and publishes the
// new set atomically.
func (r *Router) PublishHandle(c chunk.ID, db *hotchunk.DB) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := r.handles.Load().clone()
	next.byChunk[c] = db
	r.handles.Store(next)
}

// DiscardHandle removes the hot database for chunk c and publishes the new set
// atomically, returning the removed handle (nil if absent). It only unpublishes;
// the caller (deferred deletion) closes the returned handle later.
func (r *Router) DiscardHandle(c chunk.ID) *hotchunk.DB {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur := r.handles.Load()
	db := cur.byChunk[c]
	next := cur.clone()
	delete(next.byChunk, c)
	r.handles.Store(next)
	return db
}

// Close closes every published hot handle and clears the set, flushing each DB on
// the way out. Called once on clean shutdown, after ingestion and lifecycle have
// stopped, so nothing races it; handle Close is idempotent, so the live chunk
// (also closed by the ingestion loop) double-closes harmlessly. The catalog is
// caller-owned and is not closed here.
func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, db := range r.handles.Load().byChunk {
		_ = db.Close()
	}
	r.handles.Store(&hotHandles{byChunk: map[chunk.ID]*hotchunk.DB{}})
}

// Admission is one query's consistent view of serving state, held for the
// request's lifetime and released when it completes. It carries the admitted
// watermark and retention floor, the handle set loaded at admission, and the
// catalog snapshot the routing reads run against.
type Admission struct {
	latest  uint32
	floor   chunk.ID
	handles *hotHandles
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
	latest := r.watermark.Load()
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
