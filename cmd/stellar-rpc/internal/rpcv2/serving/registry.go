// Package serving is the query read side: it routes each requested chunk to its
// serving store (frozen cold files or a ready hot database) against a consistent
// snapshot of the catalog taken when the read view is acquired. See
// design-docs/query-routing-design.md.
package serving

import (
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// Registry owns the two pieces of serving state that cannot live in the catalog:
// the latest ledger, which advances every commit, and the open hot-database
// handles, which are live objects. Everything else a query needs is read from
// the catalog through the read view's snapshot.
//
// The Registry does not own the catalog; the daemon constructs and closes it.
type Registry struct {
	catalog   *catalog.Catalog
	retention geometry.Retention

	// latestLedger is the newest fully ingested ledger visible to queries. The
	// ingest loop advances it as the final step of each per-ledger cycle. Queries
	// read a frozen copy (ReadView.LatestLedger), never this live value.
	latestLedger atomic.Uint32

	// handles is the copy-on-write map of open hot-database handles, published
	// atomically. A read view loads it once at acquisition.
	handles atomic.Pointer[hotHandles]

	// mu serializes handle updates (publish/discard/close) so a lost update cannot
	// drop a concurrently published handle. Also guards closing.
	mu sync.Mutex

	// closing holds handles unpublished by DiscardHandle but not yet closed because
	// a reader was still in flight. CloseDiscarded retries them across lifecycle
	// runs until they drain; Registry.Close drains them at shutdown. Keeping the
	// handle here (not just its chunk id) is what lets the close actually retry —
	// once unpublished, it is the only remaining reference. Guarded by mu.
	closing map[chunk.ID]*hotchunk.DB

	// newSnapshot is the snapshot constructor NewReadView uses — a test seam
	// defaulting to catalog.NewSnapshot. The load-order test hooks it to mutate
	// the registry from inside the snapshot call, pinning that the latest ledger
	// and the handle set are loaded BEFORE the snapshot (the ordering the
	// design's skew argument depends on, otherwise unobservable).
	newSnapshot func() (*rocksdb.Snapshot, error)
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
	maps.Copy(m, h.byChunk)
	return &hotHandles{byChunk: m}
}

// NewRegistry binds a Registry to the catalog and retention policy, starting with
// an empty handle map and latest ledger zero.
func NewRegistry(cat *catalog.Catalog, retention geometry.Retention) *Registry {
	r := &Registry{
		catalog:     cat,
		retention:   retention,
		closing:     map[chunk.ID]*hotchunk.DB{},
		newSnapshot: cat.NewSnapshot,
	}
	r.handles.Store(&hotHandles{byChunk: map[chunk.ID]*hotchunk.DB{}})
	return r
}

// PublishReadyHandles opens and publishes a handle for every ready hot chunk
// except liveChunk, which the ingestion loop opens and publishes itself. These are
// completed chunks a prior run left ready (not yet discarded); queries read them
// hot until the freeze covers them cold. They are opened read-write so the events
// facade is warmed (a read-only open is ledgers-only), and the registry closes them
// at discard. Runs at startup before any read view is acquired.
func (r *Registry) PublishReadyHandles(liveChunk chunk.ID, logger *supportlog.Entry) error {
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

// SetLatestLedger publishes the newest fully ingested ledger; the ingest loop calls
// it as the final step of each per-ledger cycle.
func (r *Registry) SetLatestLedger(seq uint32) { r.latestLedger.Store(seq) }

// LatestLedger returns the live latest ledger. Queries do not call this — they
// read the frozen ReadView.LatestLedger captured at acquisition (see the
// latestLedger field).
func (r *Registry) LatestLedger() uint32 { return r.latestLedger.Load() }

// Handle returns the currently published hot database for chunk c, if any. The
// freeze source reads a completed chunk through this shared handle rather than
// opening a second reader against the still-open writer.
func (r *Registry) Handle(c chunk.ID) (*hotchunk.DB, bool) {
	db, ok := r.handles.Load().byChunk[c]
	return db, ok
}

// PublishHandle adds or replaces the hot database for chunk c and publishes the
// new set atomically.
func (r *Registry) PublishHandle(c chunk.ID, db *hotchunk.DB) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := r.handles.Load().clone()
	next.byChunk[c] = db
	r.handles.Store(next)
}

// DiscardHandle unpublishes chunk c's handle so new read views stop routing to it,
// moving it to the closing set for CloseDiscarded to close once idle. Idempotent:
// a repeat call (the retry re-collecting the transient key) is a no-op, since the
// handle is no longer published.
func (r *Registry) DiscardHandle(c chunk.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur := r.handles.Load()
	db, ok := cur.byChunk[c]
	if !ok {
		return // already discarded (in closing, or already closed)
	}
	next := cur.clone()
	delete(next.byChunk, c)
	r.handles.Store(next)
	r.closing[c] = db
}

// CloseDiscarded closes chunk c's discarded handle when no operation is in flight,
// returning true once it is closed (or there was none pending). False means a
// reader is still in flight; the caller leaves the chunk's transient key so a
// later run re-collects it and retries — the handle stays in closing until it
// drains. Deferred deletion calls this after the grace period, before unlinking
// the chunk's files.
//
// The close runs under mu, so a successful close's memtable flush briefly blocks a
// concurrent boundary PublishHandle. Read-view acquisition is unaffected (it loads
// handles without mu), and the stall is rare (a discard overlapping a boundary)
// and bounded (one memtable), so it is not worth closing outside the lock.
func (r *Registry) CloseDiscarded(c chunk.ID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	db, ok := r.closing[c]
	if !ok {
		return true // nothing pending
	}
	if closed, _ := db.CloseIfIdle(); !closed {
		return false
	}
	delete(r.closing, c)
	return true
}

// Close closes every hot handle — both published and awaiting-close — and clears
// the sets, flushing each DB on the way out. Called once on clean shutdown, after
// ingestion and lifecycle have stopped, so nothing races it; handle Close is
// idempotent, so the live chunk (also closed by the ingestion loop) double-closes
// harmlessly. The catalog is caller-owned and is not closed here.
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, db := range r.handles.Load().byChunk {
		_ = db.Close()
	}
	r.handles.Store(&hotHandles{byChunk: map[chunk.ID]*hotchunk.DB{}})
	for c, db := range r.closing {
		_ = db.Close()
		delete(r.closing, c)
	}
}

// ReadView is one query's consistent view of serving state, held for the
// request's lifetime and released when it completes. It carries the latest ledger
// and retention floor as of acquisition, the handle set loaded then, and the
// catalog snapshot the routing reads run against.
type ReadView struct {
	latestLedger uint32
	floor        chunk.ID
	handles      *hotHandles
	snap         *rocksdb.Snapshot
	catalog      *catalog.Catalog

	// closers releases every cold reader this view opened (hot facades are
	// registry-owned and never appear here). Appended by the resolve methods,
	// drained by Release — a view's resources live exactly as long as the view.
	// A ReadView serves one request on one goroutine; no locking.
	closers []func() error
}

// NewReadView captures a query's view of serving state with three loads, in
// this order: the latest ledger first, the handle set second, the catalog snapshot
// last. The order makes the snapshot's metadata the newest of the three, so any
// skew between the handle set and the snapshot resolves safely (see the design's
// Admission section).
//
// The caller MUST call Release when the request completes, including on error
// paths.
func (r *Registry) NewReadView() (*ReadView, error) {
	latest := r.latestLedger.Load()
	handles := r.handles.Load()
	snap, err := r.newSnapshot()
	if err != nil {
		return nil, err
	}
	lastComplete, err := lastCompleteChunkAsOf(r.catalog, snap)
	if err != nil {
		r.catalog.ReleaseSnapshot(snap)
		return nil, err
	}
	return &ReadView{
		latestLedger: latest,
		floor:        r.retention.FloorAt(lastComplete),
		handles:      handles,
		snap:         snap,
		catalog:      r.catalog,
	}, nil
}

func (a *ReadView) LatestLedger() uint32 { return a.latestLedger }

func (a *ReadView) FloorChunk() chunk.ID { return a.floor }

// Release closes every reader the view opened, then releases the snapshot back
// to the catalog. Close failures are logged, not returned: a close error on a
// read-only reader is not actionable by the caller.
func (a *ReadView) Release() {
	for _, c := range a.closers {
		if err := c(); err != nil {
			a.catalog.Logger().WithError(err).Warn("serving: close view-owned reader")
		}
	}
	a.closers = nil
	a.catalog.ReleaseSnapshot(a.snap)
}

// errNoReadyHotChunk means a snapshot held no ready hot chunk at all. That cannot
// happen in a working daemon — the live chunk's key is created before serving
// starts and is never demoted — so it marks a broken catalog. Failing the
// acquisition is safer than the alternative, which would derive the widest
// possible floor from broken state. TODO(#772): count these on an alarm metric.
var errNoReadyHotChunk = errors.New("serving: no ready hot chunk in snapshot (broken catalog)")

// lastCompleteChunkAsOf returns the anchor the floor is derived from: the highest
// ready hot chunk in the snapshot minus one (the highest ready chunk is the live,
// still-ingesting chunk, so the one below it is the last complete one). A young
// store — only chunk 0 ready, nothing complete — correctly yields -1, the signed
// convention Retention.FloorAt expects. An EMPTY scan is errNoReadyHotChunk.
func lastCompleteChunkAsOf(cat *catalog.Catalog, snap *rocksdb.Snapshot) (int64, error) {
	ready, err := cat.ReadyHotChunkKeysAsOf(snap)
	if err != nil {
		return 0, err
	}
	if len(ready) == 0 {
		return 0, errNoReadyHotChunk
	}
	return int64(ready[len(ready)-1]) - 1, nil
}
