package registry

import (
	"cmp"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
)

// Registry owns the serving map. It publishes immutable Views to queries,
// maintains the latest watermark, and routes every retired resource through
// its Reaper. Build one with BuildFromCatalog; it lives for one daemon run
// (the supervised restart loop rebuilds it) and dies with Close.
type Registry struct {
	mu      sync.Mutex           // serializes View publishes
	current atomic.Pointer[View] // the View new queries admit
	latest  atomic.Uint32        // newest fully ingested ledger; advances outside the View
	reaper  *Reaper

	layout      geometry.Layout
	logger      *supportlog.Entry
	ledgerCache *readerCache[*ledger.ColdReader]
	eventCache  *readerCache[*eventstore.ColdReader]

	closeOnce sync.Once
}

// Snapshot is one admission: the latest watermark and the View it was read
// with. Queries clamp their range against [View.FloorLedger(), Latest] and
// resolve every chunk against View for their entire lifetime.
type Snapshot struct {
	Latest uint32
	View   *View
}

// LedgerStoreHandle is the per-chunk ledger read surface the serving adapters
// consume — the method set ledger.ColdReader and ledger.HotStore share.
//
// Range semantics differ by tier: the cold reader rejects any [start, end]
// not fully inside its pack's coverage (stores.ErrOutOfRange), while the hot
// store returns whatever committed keys fall inside the range. Callers clip
// requests to the chunk's ledger window (and, on the live chunk, to the
// admitted latest) before iterating.
type LedgerStoreHandle interface {
	GetLedgerRaw(seq uint32) ([]byte, error)
	IterateLedgers(start, end uint32) iter.Seq2[ledger.Entry, error]
}

// newRegistry assembles a registry with defaulted options and an empty View.
// BuildFromCatalog is the public constructor.
func newRegistry(layout geometry.Layout, logger *supportlog.Entry, opts Options) *Registry {
	grace := opts.Grace
	if grace <= 0 {
		grace = DefaultGrace
	}
	ledgerCap := opts.LedgerCacheCap
	if ledgerCap <= 0 {
		ledgerCap = DefaultLedgerCacheCap
	}
	eventCap := opts.EventCacheCap
	if eventCap <= 0 {
		eventCap = DefaultEventCacheCap
	}

	r := &Registry{
		reaper: NewReaper(grace, logger),
		layout: layout,
		logger: logger,
	}
	retire := func(rd interface{ Close() error }) { r.reaper.Schedule(rd.Close) }
	r.ledgerCache = newReaderCache(ledgerCap,
		func(c chunk.ID) (*ledger.ColdReader, error) {
			return ledger.OpenColdReader(layout.LedgerPackPath(c))
		},
		func(rd *ledger.ColdReader) { retire(rd) },
	)
	r.eventCache = newReaderCache(eventCap,
		func(c chunk.ID) (*eventstore.ColdReader, error) {
			return eventstore.OpenColdReader(c, layout.EventsBucketDir(c), eventstore.ColdReaderOptions{})
		},
		func(rd *eventstore.ColdReader) { retire(rd) },
	)
	r.current.Store(emptyView(0))
	return r
}

// Admit is a query's admission: it loads the latest watermark, then the
// current View. The order is load-bearing (spec §Admission): the write side
// publishes a chunk's serving home before advancing latest into it, so
// reading latest FIRST guarantees the subsequently loaded View can serve
// every ledger up to it. Reversed, a chunk boundary between the two loads
// could hand back a latest the View cannot serve.
func (r *Registry) Admit() Snapshot {
	latest := r.latest.Load()
	view := r.current.Load()
	return Snapshot{Latest: latest, View: view}
}

// Reaper exposes the registry's reaper so the lifecycle can defer physical
// destruction (unlink + catalog key delete) behind the same grace period its
// unpublished resources wait out.
func (r *Registry) Reaper() *Reaper { return r.reaper }

// PublishHot registers chunk c's shared hot handle. The write side calls it
// after the chunk's hot key flips "ready" and before the chunk's first ledger
// commits, so the watermark can never enter a chunk the current View does not
// serve. Ownership of db transfers to the registry: it stays open, serving
// both ingestion and queries, until UnpublishHot (discard) or Close retires
// it.
func (r *Registry) PublishHot(c chunk.ID, db *hotchunk.DB) {
	if db == nil {
		r.logger.WithField("chunk", c.String()).Error("registry: PublishHot called with a nil handle; ignored")
		return
	}
	r.publish(func(next *View) []func() error {
		old, existed := next.hot[c]
		next.hot[c] = db
		if existed && old != db {
			// Never happens in the write protocol (one open per chunk); if a
			// caller re-publishes anyway, retire the displaced handle rather
			// than leak it.
			r.logger.WithField("chunk", c.String()).
				Warn("registry: PublishHot replaced an existing hot handle; retiring the old one")
			return []func() error{old.Close}
		}
		return nil
	})
}

// AdvanceLatest publishes seq as the newest fully ingested ledger. The write
// side calls it as the FINAL step of the per-ledger cycle, after the atomic
// commit and the in-memory event applies — queries admitted afterward may
// observe the ledger.
func (r *Registry) AdvanceLatest(seq uint32) {
	r.latest.Store(seq)
}

// PublishFrozen flags chunk c's kinds as cold-servable. The write side calls
// it after the FlipChunkFrozen commit (R1: only durable, serving-ready
// artifacts enter a View). KindTxHash is accepted and skipped — the .bin is
// an index-build input, never chunk-served — so callers can pass exactly the
// kinds they froze.
func (r *Registry) PublishFrozen(c chunk.ID, kinds ...geometry.Kind) {
	flags := ColdChunk{}
	for _, k := range kinds {
		switch k {
		case geometry.KindLedgers:
			flags.Ledgers = true
		case geometry.KindEvents:
			flags.Events = true
		case geometry.KindTxHash:
			// index-build input, never served from the chunk tier
		default:
			r.logger.WithField("chunk", c.String()).WithField("kind", string(k)).
				Warn("registry: PublishFrozen called with an unknown kind; skipped")
		}
	}
	if flags == (ColdChunk{}) {
		return
	}
	r.publish(func(next *View) []func() error {
		cc := next.cold[c]
		cc.Ledgers = cc.Ledgers || flags.Ledgers
		cc.Events = cc.Events || flags.Events
		next.cold[c] = cc
		return nil
	})
}

// SwapTxIndex replaces (or installs) window cov.Index's coverage with cov,
// opening the new .idx reader and retiring the predecessor's through the
// reaper. The write side calls it after the CommitTxHashIndex atomic write.
// On error the View is unchanged and still serves the predecessor.
func (r *Registry) SwapTxIndex(cov geometry.TxHashIndexCoverage) error {
	if cov.State != geometry.StateFrozen {
		// R1: a transient coverage must never enter a View. Only reachable
		// through caller error — the hook contract is post-commit.
		err := fmt.Errorf("registry: SwapTxIndex requires a %q coverage, got %q for index %s",
			geometry.StateFrozen, cov.State, cov.Index)
		r.logger.WithError(err).Error("registry: refusing tx-index swap")
		return err
	}
	reader, err := txhash.OpenColdReader(r.layout.TxHashIndexFilePath(cov))
	if err != nil {
		return fmt.Errorf("registry: open replacement tx index for window %s: %w", cov.Index, err)
	}
	r.publish(func(next *View) []func() error {
		entry := IndexCoverage{Window: cov.Index, Lo: cov.Lo, Hi: cov.Hi, Idx: reader}
		for i := range next.indexes {
			if next.indexes[i].Window == cov.Index {
				old := next.indexes[i].Idx
				next.indexes[i] = entry
				return []func() error{old.Close}
			}
		}
		next.indexes = append(next.indexes, entry)
		slices.SortFunc(next.indexes, func(a, b IndexCoverage) int {
			return cmp.Compare(a.Window, b.Window)
		})
		return nil
	})
	return nil
}

// UnpublishHot removes chunk c's hot handle from the View (discard). After
// the grace period the reaper closes the handle and then runs destroy — the
// physical cleanup (dir removal + catalog key delete) the lifecycle hands in.
// The chunk's cold artifacts keep serving throughout: discard only runs once
// cold coverage exists. A nil destroy just closes the handle.
func (r *Registry) UnpublishHot(c chunk.ID, destroy func() error) {
	r.publish(func(next *View) []func() error {
		db, existed := next.hot[c]
		if existed {
			delete(next.hot, c)
		}
		return []func() error{func() error {
			var errs []error
			if existed {
				// Close before destroy: destroy removes the RocksDB dir out
				// from under the handle otherwise. A close failure does not
				// stop the destroy — its debris is re-swept by the catalog
				// scans either way.
				if err := db.Close(); err != nil {
					errs = append(errs, fmt.Errorf("close hot chunk %s: %w", c, err))
				}
			}
			if destroy != nil {
				if err := destroy(); err != nil {
					errs = append(errs, err)
				}
			}
			return errors.Join(errs...)
		}}
	})
}

// AdvanceFloor publishes floor as the new retention floor and drops every
// below-floor entry — hot handles, cold flags, index coverages, and cached
// cold readers — retiring their resources through the reaper. The lifecycle
// calls it FIRST in a prune run: gate, unpublish, demote, then destroy. The
// floor never moves backward; a regressing call is ignored with a warning.
func (r *Registry) AdvanceFloor(floor chunk.ID) {
	r.publish(func(next *View) []func() error {
		if floor <= next.floor {
			if floor < next.floor {
				r.logger.WithField("floor", floor.String()).WithField("current", next.floor.String()).
					Warn("registry: AdvanceFloor would move the floor backward; ignored")
			}
			return nil
		}
		var retireList []func() error
		next.floor = floor
		for c, db := range next.hot {
			if c < floor {
				delete(next.hot, c)
				retireList = append(retireList, db.Close)
			}
		}
		for c := range next.cold {
			if c < floor {
				delete(next.cold, c)
			}
		}
		kept := next.indexes[:0]
		for _, ic := range next.indexes {
			if ic.Hi < floor {
				retireList = append(retireList, ic.Idx.Close)
				continue
			}
			kept = append(kept, ic)
		}
		next.indexes = kept
		return retireList
	})
	// Retire cached readers for dropped chunks. A query admitted on an older
	// View can still re-open one afterward; that re-inserted reader is
	// bounded by the LRU and closes on eviction or teardown.
	r.ledgerCache.dropBelow(floor)
	r.eventCache.dropBelow(floor)
}

// Close tears the registry down on run() exit: it swaps in an empty View,
// retires every resource (hot handles, index readers, cached cold readers),
// and closes the reaper — which runs every pending destruction immediately.
// No grace period applies: the caller has already stopped query admission and
// the write-side loops, and the supervised restart must find every RocksDB
// LOCK released. Idempotent.
func (r *Registry) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		v := r.current.Load()
		r.current.Store(emptyView(v.floor))
		r.mu.Unlock()

		for _, db := range v.hot {
			r.reaper.Schedule(db.Close)
		}
		for _, ic := range v.indexes {
			r.reaper.Schedule(ic.Idx.Close)
		}
		r.ledgerCache.dropAll()
		r.eventCache.dropAll()
		r.reaper.Close()
	})
}

// LedgerReaderFor resolves chunk c's ledger store under View v: the cold pack
// (through the LRU cache) when frozen, else the hot handle's ledger facade,
// else ErrUnavailable.
func (r *Registry) LedgerReaderFor(v *View, c chunk.ID) (LedgerStoreHandle, error) {
	t, err := v.resolve(c, geometry.KindLedgers)
	if err != nil {
		return nil, err
	}
	if t.cold {
		return r.ledgerCache.acquire(c)
	}
	return t.hot.Ledgers(), nil
}

// EventReaderFor resolves chunk c's event store under View v: the cold
// segment reader (through the LRU cache) when frozen, else the hot handle's
// warmed events facade, else ErrUnavailable. Registry-owned hot handles are
// always read-write opens, so Events() is always composed.
func (r *Registry) EventReaderFor(v *View, c chunk.ID) (eventstore.Reader, error) {
	t, err := v.resolve(c, geometry.KindEvents)
	if err != nil {
		return nil, err
	}
	if t.cold {
		return r.eventCache.acquire(c)
	}
	return t.hot.Events(), nil
}

// publish is the one View-update primitive (spec §Publishing View updates):
// serialize on mu, clone the current View, apply mutate, atomically store the
// clone, then hand whatever mutate retired to the reaper. Queries admitted
// before the store keep their old View untouched.
func (r *Registry) publish(mutate func(next *View) (retire []func() error)) {
	r.mu.Lock()
	next := r.current.Load().clone()
	retire := mutate(next)
	r.current.Store(next)
	r.mu.Unlock()

	for _, destroy := range retire {
		r.reaper.Schedule(destroy)
	}
}

// Grace and cache-capacity defaults, used when Options leaves them zero. The
// service assembly (stage 6) feeds real values: grace = max request duration
// + margin; caps from the [serving] config.
const (
	DefaultGrace          = 30 * time.Second
	DefaultLedgerCacheCap = 128
	DefaultEventCacheCap  = 32
)
