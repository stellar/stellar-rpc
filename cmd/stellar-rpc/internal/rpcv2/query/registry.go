// Package query is the read side of the daemon: it routes each requested chunk to its
// serving store (frozen cold files or a ready hot database) against a consistent
// snapshot of the catalog taken when the read view is acquired. See
// design-docs/query-routing-design.md.
package query

import (
	"fmt"
	"maps"
	"sync"
	"sync/atomic"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
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

	// maxScanLedgers caps the ledgers one event page may scan. Zero means
	// defaultMaxScanLedgers, resolved by QueryEvents. A test-only shrink:
	// production always runs the per-request scan bound (see
	// chunk.LedgersPerChunk) — the bound is an invariant, not configuration.
	// Held here rather than in a package var so a test shrinking the window
	// cannot leak into another test's pages.
	maxScanLedgers uint32

	// latest is the newest fully ingested ledger visible to queries, paired with
	// its close time so both publish atomically. The ingest loop advances it as
	// the final step of each per-ledger cycle. Queries read a frozen copy
	// (ReadView.LatestLedger / LatestCloseTime), never this live value.
	latest atomic.Pointer[ledgerStamp]

	// oldest is a read-through cache of the retention floor's first ledger and
	// its close time. Adapters populate it after a fallback point read
	// (RecordOldestCloseTime); readers trust it only while its seq still equals
	// the view's floor first ledger, so a moved floor invalidates it by
	// construction — no explicit invalidation.
	oldest atomic.Pointer[ledgerStamp]

	// handles is the copy-on-write map of open hot-database handles, published
	// atomically. A read view loads it once at acquisition.
	handles atomic.Pointer[handleSet]

	// mu serializes handle updates (publish/discard/close) so a lost update cannot
	// drop a concurrently published handle. Also guards closing.
	mu sync.Mutex

	// closing holds handles unpublished by DiscardHandle but not yet closed because
	// a reader was still in flight. TryCloseHandle retries them across lifecycle
	// runs until they drain; Registry.Close drains them at shutdown. Keeping the
	// handle here (not just its chunk id) is what lets the close actually retry —
	// once unpublished, it is the only remaining reference. Guarded by mu.
	closing map[chunk.ID]*hotchunk.DB

	// newSnapshot and loadHandles are the seams the load-order tests hook —
	// defaulting to catalog.NewSnapshot and handles.Load. Together they pin the
	// full three-load order the design's skew argument depends on (latest ledger,
	// then handles, then snapshot), which is otherwise unobservable: the
	// newSnapshot hook catches either load drifting after the snapshot, and the
	// loadHandles hook catches the latest ledger drifting after the handle set.
	newSnapshot func() (*catalog.Snapshot, error)
	loadHandles func() *handleSet
}

// ledgerStamp pairs a ledger sequence with its close time (unix seconds).
// closeTime 0 means "not known yet": OpenRegistry seeds the latest stamp from
// the catalog (which has no close times), and the oldest cache starts empty.
// Consumers fall back to a point read when the close time is 0.
type ledgerStamp struct {
	seq       uint32
	closeTime int64
}

// handleSet is an immutable map of open hot-database handles keyed by chunk,
// replaced wholesale on every publish or discard so a query that loaded one keeps
// reading it.
type handleSet struct {
	byChunk map[chunk.ID]*hotchunk.DB
}

// clone returns a deep copy so a copy-on-write update never mutates a map a query
// is already reading.
func (h *handleSet) clone() *handleSet {
	m := make(map[chunk.ID]*hotchunk.DB, len(h.byChunk))
	maps.Copy(m, h.byChunk)
	return &handleSet{byChunk: m}
}

// OpenRegistry constructs a serving-ready registry in one call: it opens and
// publishes a handle for every ready hot chunk below the live one, publishes the
// caller's live handle, and seeds the latest ledger — so a half-initialized
// registry is never representable in the daemon and the startup ordering cannot
// be gotten wrong. The live chunk comes from live.ChunkID() (passing it
// separately could disagree with the handle) and the logger from the catalog.
// The live DB stays the caller's to open: creating a chunk and flipping its
// catalog key is ingestion's transition, not the read side's. lastCommitted is
// passed because the live DB is empty on a fresh boundary; the value comes from
// the caller's whole-catalog derivation. On error, every handle the call opened
// is closed.
func OpenRegistry(
	cat *catalog.Catalog, retention geometry.Retention, live *hotchunk.DB, lastCommitted uint32,
) (*Registry, error) {
	r := NewRegistry(cat, retention)
	if err := r.publishReadyHandles(live.ChunkID(), cat.Logger()); err != nil {
		r.Close()
		return nil, err
	}
	r.PublishHandle(live.ChunkID(), live)
	// lastCommitted comes from the catalog, which has no close times, so the
	// stamp starts with close time 0 (unknown): adapters fall back to one point
	// read until the first ingested ledger stamps a real value.
	r.SetLatestLedger(lastCommitted, 0)
	return r, nil
}

// NewRegistry binds a bare Registry to the catalog and retention policy: an empty
// handle map and latest ledger zero. The daemon uses OpenRegistry; this is the
// seam for tests, which publish their own state (the bench publishes into a
// closingSink, not a Registry).
func NewRegistry(cat *catalog.Catalog, retention geometry.Retention) *Registry {
	r := &Registry{
		catalog:     cat,
		retention:   retention,
		closing:     map[chunk.ID]*hotchunk.DB{},
		newSnapshot: cat.NewSnapshot,
	}
	r.loadHandles = r.handles.Load
	r.handles.Store(&handleSet{byChunk: map[chunk.ID]*hotchunk.DB{}})
	r.latest.Store(&ledgerStamp{})
	return r
}

// SetLatestLedger publishes the newest fully ingested ledger together with its
// close time (unix seconds; 0 = unknown, consumers fall back to a point read);
// the ingest loop calls it as the final step of each per-ledger cycle.
func (r *Registry) SetLatestLedger(seq uint32, closeTimeUnix int64) {
	r.latest.Store(&ledgerStamp{seq: seq, closeTime: closeTimeUnix})
}

// LatestLedger returns the live latest ledger. Queries do not call this — they
// read the frozen ReadView.LatestLedger captured at acquisition (see the
// latest field).
func (r *Registry) LatestLedger() uint32 { return r.latest.Load().seq }

// RecordOldestCloseTime caches the retention floor's first ledger and its close
// time so getLedgerRange stops paying a point read (a cold packfile open in the
// common case) per request. A plain Store is enough: concurrent writers can only
// race about the same immutable close time (or a newer floor's, which the seq
// check on the read side handles), so last-write-wins is always correct.
func (r *Registry) RecordOldestCloseTime(seq uint32, closeTimeUnix int64) {
	r.oldest.Store(&ledgerStamp{seq: seq, closeTime: closeTimeUnix})
}

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
// moving it to the closing set for TryCloseHandle to close once idle. Idempotent:
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

// TryCloseHandle closes chunk c's handle when no operation is in flight,
// returning whether it is closed (true also when nothing was pending). It only
// ever closes a handle DiscardHandle already unpublished — never a published one.
// False means a reader is still in flight; the caller leaves the chunk's
// transient key so a later run re-collects it and retries — the handle stays in
// closing until it drains. Deferred deletion calls this after the grace period,
// before unlinking the chunk's files.
//
// The close runs under mu, so a successful close's memtable flush briefly blocks a
// concurrent boundary PublishHandle. Read-view acquisition is unaffected (it loads
// handles without mu), and the stall is rare (a discard overlapping a boundary)
// and bounded (one memtable), so it is not worth closing outside the lock.
func (r *Registry) TryCloseHandle(c chunk.ID) bool {
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
	r.handles.Store(&handleSet{byChunk: map[chunk.ID]*hotchunk.DB{}})
	for c, db := range r.closing {
		_ = db.Close()
		delete(r.closing, c)
	}
}

// ReadView is one query's consistent view of serving state, held for the
// request's lifetime and released when it completes. It carries the latest
// ledger stamp and retention floor as of acquisition, the handle set loaded
// then, and the catalog snapshot the routing reads run against.
type ReadView struct {
	latest ledgerStamp
	// oldestStamp is the oldest-close-time cache entry captured at acquisition.
	// It is trusted only while its seq equals the view's OldestLedger (see
	// OldestCloseTime).
	oldestStamp ledgerStamp
	floor       chunk.ID
	handles     *handleSet
	snap        *catalog.Snapshot
	catalog     *catalog.Catalog

	// maxScanLedgers is the registry's window, copied at acquisition so one
	// page's bound cannot change mid-request. Zero means defaultMaxScanLedgers,
	// so a view built without it still bounds its pages.
	maxScanLedgers uint32

	// closers releases every cold reader this view opened (hot facades are
	// registry-owned and never appear here). Appended by the resolve methods and
	// by ScanLedgers' walk backstop, drained by Release — a view's resources live
	// exactly as long as the view. A ReadView serves one request on one
	// goroutine; no locking.
	closers []func() error
}

// NewReadView captures a query's view of serving state with three loads, in
// this order: the latest ledger stamp first, the handle set second, the catalog
// snapshot last. The order makes the snapshot's metadata the newest of the
// three, so the servable window derived from it is never staler than the
// handles. The ordering alone does NOT make handle/snapshot skew safe — at a
// chunk boundary the hot key can flip ready before the handle publishes, and a
// prune can retire a chunk whose handle this view already loaded. What keeps
// skewed reads correct are the gates downstream: every lookup is checked
// against the view's window (the adapters' inWindow / windowGatedIndex), and a
// chunk without a serving store resolves to ErrUnavailable. A read path that
// skips those gates cannot lean on this ordering. (See the design's Read views
// section.)
//
// The caller MUST call Release when the request completes, including on error
// paths.
func (r *Registry) NewReadView() (*ReadView, error) {
	latest := r.latest.Load()
	handles := r.loadHandles()
	snap, err := r.newSnapshot()
	if err != nil {
		return nil, err
	}
	lastComplete, err := snap.LastCompleteChunk()
	if err != nil {
		snap.Release()
		return nil, err
	}
	view := &ReadView{
		latest:         *latest,
		maxScanLedgers: r.maxScanLedgers,
		floor:          r.retention.FloorAt(lastComplete),
		handles:        handles,
		snap:           snap,
		catalog:        r.catalog,
	}
	// The oldest-close-time cache rides along outside the three-load order: it
	// is a pure optimization whose staleness the seq check in OldestCloseTime
	// already handles, so it cannot participate in any skew argument.
	if s := r.oldest.Load(); s != nil {
		view.oldestStamp = *s
	}
	return view, nil
}

// publishReadyHandles opens and publishes a handle for every ready hot chunk
// except liveChunk, whose handle OpenRegistry publishes from the caller's open.
// These are completed chunks a prior run left ready (not yet discarded); queries
// read them hot until the freeze covers them cold. They are opened read-write so
// the events facade is warmed (a read-only open is ledgers-only), and the
// registry closes them at discard. Runs at startup before any read view is
// acquired.
func (r *Registry) publishReadyHandles(liveChunk chunk.ID, logger *supportlog.Entry) error {
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

func (a *ReadView) LatestLedger() uint32 { return a.latest.seq }

// LatestCloseTime returns the latest ledger's close time captured at
// acquisition. ok is false when the stamp predates any real commit (OpenRegistry
// seeds close time 0 from the catalog) — the caller point-reads instead.
func (a *ReadView) LatestCloseTime() (int64, bool) {
	return a.latest.closeTime, a.latest.closeTime != 0
}

// OldestCloseTime returns the cached close time of the view's oldest servable
// ledger. ok is false when nothing was recorded yet or the recorded seq no
// longer equals the view's OldestLedger (the retention floor moved since the
// cache was written) — the caller point-reads and re-records.
func (a *ReadView) OldestCloseTime() (int64, bool) {
	if a.oldestStamp.closeTime == 0 || a.oldestStamp.seq != a.OldestLedger() {
		return 0, false
	}
	return a.oldestStamp.closeTime, true
}

func (a *ReadView) FloorChunk() chunk.ID { return a.floor }

// Release closes every reader the view opened, then releases the snapshot back
// to the catalog. Close failures are logged, not returned: a close error on a
// read-only reader is not actionable by the caller. Idempotent: a second call
// is a no-op — releasing the RocksDB snapshot twice would be a C-side
// double-free, and guarding here is cheaper than auditing every caller forever.
func (a *ReadView) Release() {
	if a.snap == nil {
		return // already released
	}
	for _, c := range a.closers {
		if err := c(); err != nil {
			a.catalog.Logger().WithError(err).Warn("query: close view-owned reader")
		}
	}
	a.closers = nil
	a.snap.Release()
	a.snap = nil
}
