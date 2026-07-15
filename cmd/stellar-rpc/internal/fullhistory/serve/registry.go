// Package serve is the read-side registry/View for the full-history query POC.
// It projects the durable catalog into an in-memory serving map that queries
// admit against, following design-docs/query-routing-design.md (Registry /
// View / Admission / View-update-points), simplified for the POC.
//
// A query admits by loading `latest` then the current View (order is
// load-bearing — see Admit). It then routes each chunk to a hot handle or a
// per-request cold reader against that one immutable View for its whole
// lifetime. Storage-side hooks (LedgerCommitted, HotOpened, ChunkClosed,
// TickCompleted) publish new Views as the serving map changes.
//
// The package must not import the fullhistory root (the root imports serve).
package serve

import (
	"cmp"
	"errors"
	"iter"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
)

// ErrUnavailable is returned by the View resolvers when a chunk has no serving
// home (neither a cold flag nor a hot handle).
var ErrUnavailable = errors.New("no serving home for chunk")

// Kind names a per-chunk serving data type. Defined for the downstream query
// handlers (Tasks 3-7); the resolvers below expose one method per kind.
type Kind int

const (
	KindLedgers Kind = iota
	KindEvents
)

// ColdFlags records which cold artifacts are frozen and servable for a chunk.
// It holds flags, not open readers — cold readers are opened per request (see
// the View resolvers).
type ColdFlags struct{ Ledgers, Events bool }

// TxIndexCoverage is one window's frozen tx-hash index reader and the chunk
// range it covers. The slice on a View is ascending by Lo.
type TxIndexCoverage struct {
	Lo, Hi chunk.ID
	Reader *txhash.ColdReader
}

// View is one immutable snapshot of the serving map. A query keeps its admitted
// View pointer for its whole lifetime; publishers clone-and-replace rather than
// mutate in place, so a live query never observes a half-applied change.
type View struct {
	Floor    chunk.ID                  // chunk-aligned retention floor
	Earliest uint32                    // catalog earliest_ledger (genesis clamp)
	Hot      map[chunk.ID]*hotchunk.DB // live chunk: shared write handle; closed ready chunks: OpenReadView handle
	Cold     map[chunk.ID]ColdFlags
	TxIdx    []TxIndexCoverage
}

func emptyView() *View {
	return &View{
		Hot:  map[chunk.ID]*hotchunk.DB{},
		Cold: map[chunk.ID]ColdFlags{},
	}
}

// Registry owns the serving map: the current immutable View plus the `latest`
// watermark, rebuilt from the catalog at startup and updated by the hooks. All
// hooks are safe on a nil *Registry, so the no-serve path needs no guards.
type Registry struct {
	mu        sync.Mutex
	current   atomic.Pointer[View]
	latest    atomic.Uint32
	cat       *catalog.Catalog
	retention geometry.Retention
	logger    *supportlog.Entry
}

// NewRegistry returns a registry seeded with an empty View (so Admit before
// BuildInitial is safe and publish never dereferences nil).
func NewRegistry(cat *catalog.Catalog, retention geometry.Retention, logger *supportlog.Entry) *Registry {
	r := &Registry{cat: cat, retention: retention, logger: logger}
	r.current.Store(emptyView())
	return r
}

// Admit performs the two atomic loads a query needs, latest FIRST. The order is
// load-bearing (design §Admission): reading latest before the View guarantees
// every ledger <= the admitted latest has a serving home in the loaded View. A
// chunk boundary between the loads could otherwise let latest point into a
// chunk the older View does not serve.
func (r *Registry) Admit() (uint32, *View) {
	latest := r.latest.Load()
	v := r.current.Load()
	return latest, v
}

// LedgerCommitted advances `latest`. Called only after hotService.Ingest
// returned, so latest advances last (design: "latest advances last").
func (r *Registry) LedgerCommitted(seq uint32) {
	if r == nil {
		return
	}
	r.latest.Store(seq)
}

// HotOpened publishes a chunk's shared write handle — the hotloop/startup hook
// for openHotDBForChunk flipping the chunk "ready".
func (r *Registry) HotOpened(c chunk.ID, db *hotchunk.DB) {
	if r == nil {
		return
	}
	r.publish(func(v *View) { v.Hot[c] = db })
}

// ChunkClosed reopens a boundary-fenced chunk as a read-only full-facade view
// and republishes it, replacing the (now write-fenced) handle. On open failure
// it removes the entry — the next freeze/tick covers the chunk from cold. The
// old write handle is owned and closed by ingestion, never here.
func (r *Registry) ChunkClosed(c chunk.ID) {
	if r == nil {
		return
	}
	db, err := hotchunk.OpenReadView(geometry.HotReady, r.cat.Layout().HotChunkPath(c), c, r.logger)
	if err != nil {
		r.logger.WithError(err).WithField("chunk", c).Warn("serve: reopen closed chunk as read view failed; removing")
		r.publish(func(v *View) { delete(v.Hot, c) })
		return
	}
	r.publish(func(v *View) { v.Hot[c] = db })
}

// BuildInitial rebuilds the whole View from a fresh catalog scan at startup,
// before ServeReads begins (design: startup serveReads row). It seeds `latest`,
// the retention floor, cold flags, frozen tx-hash readers, and read views for
// every ready hot chunk EXCEPT the resume (highest) chunk — that one is
// published by HotOpened once ingestion opens its write handle.
func (r *Registry) BuildInitial(lastCommitted uint32) error {
	if r == nil {
		return nil
	}
	r.latest.Store(lastCommitted)

	v := emptyView()
	v.Floor = r.retention.FloorAt(geometry.LastCompleteChunkAt(lastCommitted))
	earliest, pinned, err := r.cat.EarliestLedger()
	if err != nil {
		return err
	}
	if pinned {
		v.Earliest = earliest
	}
	if v.Cold, err = r.scanCold(); err != nil {
		return err
	}
	txidx, err := r.rescanTxIdx(nil)
	if err != nil {
		return err
	}
	v.TxIdx = txidx
	if err := r.openInitialHot(v); err != nil {
		return err
	}

	r.mu.Lock()
	r.current.Store(v)
	r.mu.Unlock()
	return nil
}

// TickCompleted rescans the catalog and republishes: it recomputes Cold, TxIdx,
// and Floor from durable state, keeps existing hot handles, closes handles for
// chunks that vanished (discarded) and swaps in fresh tx-hash readers for
// superseded coverages. Cold/TxIdx rescans run before the lock; the hot-key scan
// and the clone-and-store run UNDER mu (so a chunk going live via HotOpened can
// never be misread as discarded — see the in-body note). On any scan error it
// logs and leaves the current View in place (lifecycle re-runs the next tick).
func (r *Registry) TickCompleted() {
	if r == nil {
		return
	}
	floor := r.retention.FloorAt(geometry.LastCompleteChunkAt(r.latest.Load()))

	cold, err := r.scanCold()
	if err != nil {
		r.logger.WithError(err).Error("serve: tick cold rescan failed")
		return
	}

	txidx, err := r.rescanTxIdx(r.current.Load().TxIdx)
	if err != nil {
		r.logger.WithError(err).Error("serve: tick tx-hash rescan failed")
		return
	}

	// The hot-key scan, the delete-set, and the clone-and-store MUST run together
	// under mu. openHotDBForChunk creates a chunk's hot:chunk key BEFORE HotOpened
	// publishes its live write handle, and HotOpened serializes on this same mu.
	// So a scan taken under the lock can never miss a chunk whose handle is already
	// in the View: either HotOpened has run (handle in Hot AND key in the scan) or
	// it has not (neither). Scanning outside the lock reopened that gap — a chunk
	// that went live between an unlocked scan and the publish would be absent from
	// the stale scan set and get deleted-and-closed while live, ErrStoreClosed-ing
	// the next Ingest.
	var toClose []*hotchunk.DB
	r.mu.Lock()
	hotKeys, err := r.cat.HotChunkKeys()
	if err != nil {
		r.mu.Unlock()
		r.logger.WithError(err).Error("serve: tick hot-key scan failed")
		return
	}
	present := make(map[chunk.ID]struct{}, len(hotKeys))
	for _, c := range hotKeys {
		present[c] = struct{}{}
	}
	next := r.current.Load().clone()
	next.Floor = floor
	next.Cold = cold
	next.TxIdx = txidx
	for c, db := range next.Hot {
		if _, ok := present[c]; ok {
			continue
		}
		// Discarded: remove from the serving map, then close the handle.
		// rocksdb Store.Close is lifecycle-guarded — an in-flight read on an
		// older View gets stores.ErrStoreClosed, memory-safe.
		delete(next.Hot, c)
		toClose = append(toClose, db)
	}
	r.current.Store(next)
	r.mu.Unlock()

	for _, db := range toClose {
		if err := db.Close(); err != nil {
			r.logger.WithError(err).Warn("serve: closing discarded hot handle")
		}
	}
}

// scanCold builds the Cold map from the frozen per-chunk ledger/events artifact
// keys. The transient txhash .bin kind is never a cold serving artifact.
func (r *Registry) scanCold() (map[chunk.ID]ColdFlags, error) {
	refs, err := r.cat.ChunkArtifactKeys()
	if err != nil {
		return nil, err
	}
	cold := map[chunk.ID]ColdFlags{}
	for _, ref := range refs {
		if ref.State != geometry.StateFrozen {
			continue
		}
		f := cold[ref.Chunk]
		switch ref.Kind {
		case geometry.KindLedgers:
			f.Ledgers = true
		case geometry.KindEvents:
			f.Events = true
		case geometry.KindTxHash:
			// txhash .bin is a transient index artifact, never a cold serving
			// artifact — see the doc comment above.
		}
		cold[ref.Chunk] = f
	}
	return cold, nil
}

// rescanTxIdx builds the ascending TxIndexCoverage slice from the frozen
// coverage keys, reusing a reader from prev when the exact [Lo, Hi] coverage is
// unchanged (so a steady tick opens nothing new).
//
// POC: superseded .idx readers leak (bounded by coverage swaps per run) — a
// dropped coverage's reader is never closed, because streamhash Close unmaps and
// a racing read on an older View would fault; reaper at productionization.
func (r *Registry) rescanTxIdx(prev []TxIndexCoverage) ([]TxIndexCoverage, error) {
	covs, err := r.cat.AllTxHashIndexKeys()
	if err != nil {
		return nil, err
	}
	reuse := make(map[[2]chunk.ID]*txhash.ColdReader, len(prev))
	for _, p := range prev {
		reuse[[2]chunk.ID{p.Lo, p.Hi}] = p.Reader
	}
	layout := r.cat.Layout()
	var out []TxIndexCoverage
	for _, cov := range covs {
		if cov.State != geometry.StateFrozen {
			continue
		}
		reader, ok := reuse[[2]chunk.ID{cov.Lo, cov.Hi}]
		if !ok {
			reader, err = txhash.OpenColdReader(layout.TxHashIndexFilePath(cov))
			if err != nil {
				return nil, err
			}
		}
		out = append(out, TxIndexCoverage{Lo: cov.Lo, Hi: cov.Hi, Reader: reader})
	}
	slices.SortFunc(out, func(a, b TxIndexCoverage) int { return cmp.Compare(a.Lo, b.Lo) })
	return out, nil
}

// openInitialHot opens a read view for every ready hot chunk except the highest
// (the resume chunk, published by HotOpened).
func (r *Registry) openInitialHot(v *View) error {
	ready, err := r.cat.ReadyHotChunkKeys()
	if err != nil {
		return err
	}
	layout := r.cat.Layout()
	for i, c := range ready {
		if i == len(ready)-1 {
			continue // resume chunk: ingestion publishes it via HotOpened
		}
		db, err := hotchunk.OpenReadView(geometry.HotReady, layout.HotChunkPath(c), c, r.logger)
		if err != nil {
			return err
		}
		v.Hot[c] = db
	}
	return nil
}

// publish serializes View updates: clone the current View, apply mutate, and
// atomically store the result (design §Publishing View updates).
func (r *Registry) publish(mutate func(*View)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := r.current.Load().clone()
	mutate(next)
	r.current.Store(next)
}

// LedgerChunk is a per-chunk ledger handle the caller must Close(). Hot-backed
// handles have a no-op Close; cold-backed ones own and close the cold reader.
type LedgerChunk struct {
	src     ledgerSource
	closeFn func() error
}

// ledgerSource is the read surface shared by ledger.HotStore and
// ledger.ColdReader — the two things a LedgerChunk can front.
type ledgerSource interface {
	GetLedgerRaw(seq uint32) ([]byte, error)
	IterateLedgers(start, end uint32) iter.Seq2[ledger.Entry, error]
}

func (lc LedgerChunk) Get(seq uint32) ([]byte, error) { return lc.src.GetLedgerRaw(seq) }

func (lc LedgerChunk) Iterate(start, end uint32) iter.Seq2[ledger.Entry, error] {
	return lc.src.IterateLedgers(start, end)
}

func (lc LedgerChunk) Close() error {
	if lc.closeFn == nil {
		return nil
	}
	return lc.closeFn()
}

// EventsChunk is a per-chunk events handle the caller must Close(). Same Close
// contract as LedgerChunk.
type EventsChunk struct {
	reader  eventstore.Reader
	closeFn func() error
}

func (ec EventsChunk) Reader() eventstore.Reader { return ec.reader }

func (ec EventsChunk) Close() error {
	if ec.closeFn == nil {
		return nil
	}
	return ec.closeFn()
}

// ResolveLedgers routes chunk c to its ledger store: cold wins over hot (design
// §Chunk resolution), else the hot handle, else ErrUnavailable.
//
// POC: no LRU — cold readers are opened per request and closed by the caller.
func (v *View) ResolveLedgers(c chunk.ID, layout geometry.Layout) (LedgerChunk, error) {
	if v.Cold[c].Ledgers {
		cr, err := ledger.OpenColdReader(layout.LedgerPackPath(c))
		if err != nil {
			return LedgerChunk{}, err
		}
		return LedgerChunk{src: cr, closeFn: cr.Close}, nil
	}
	if db, ok := v.Hot[c]; ok {
		return LedgerChunk{src: db.Ledgers()}, nil
	}
	return LedgerChunk{}, ErrUnavailable
}

// ResolveEvents routes chunk c to its events store: cold wins over hot, else the
// hot handle, else ErrUnavailable.
//
// POC: no LRU — cold readers are opened per request and closed by the caller.
func (v *View) ResolveEvents(c chunk.ID, layout geometry.Layout) (EventsChunk, error) {
	if v.Cold[c].Events {
		cr, err := eventstore.OpenColdReader(c, layout.EventsBucketDir(c), eventstore.ColdReaderOptions{})
		if err != nil {
			return EventsChunk{}, err
		}
		return EventsChunk{reader: cr, closeFn: cr.Close}, nil
	}
	if db, ok := v.Hot[c]; ok {
		return EventsChunk{reader: db.Events()}, nil
	}
	return EventsChunk{}, ErrUnavailable
}

// clone shallow-copies the maps and slice so a publisher can mutate the copy
// without disturbing the View live queries still hold.
func (v *View) clone() *View {
	return &View{
		Floor:    v.Floor,
		Earliest: v.Earliest,
		Hot:      maps.Clone(v.Hot),
		Cold:     maps.Clone(v.Cold),
		TxIdx:    slices.Clone(v.TxIdx),
	}
}
