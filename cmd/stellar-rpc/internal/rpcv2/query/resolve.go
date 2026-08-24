package query

import (
	"errors"
	"iter"
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// ErrUnavailable means a chunk has no serving store for the requested kind in the
// read view's snapshot: neither a frozen cold artifact nor a ready hot database. It
// is R1 in effect — a freezing, pruning, or transient resource is invisible to
// routing regardless of what is on disk.
//
// Within the serving model this is unreachable for an in-window read: coverage
// is published before the old store is discarded, routing prefers cold, and
// the window gates keep below-floor and above-latest reads out. Every
// occurrence is therefore counted and logged (see unavailable) so a violated
// invariant is visible to operators instead of silent.
var ErrUnavailable = errors.New("query: chunk has no serving store")

// unavailableResolves counts reads that found no serving store for a chunk.
// Process-wide by design — the metrics exporter reads it via
// UnavailableResolves.
//
//nolint:gochecknoglobals // one tally across all views; read-only outside this file
var unavailableResolves atomic.Uint64

// UnavailableResolves returns the process-wide count of reads that found no
// serving store. See ErrUnavailable.
func UnavailableResolves() uint64 { return unavailableResolves.Load() }

// unavailable counts and logs one no-serving-store read, then returns
// ErrUnavailable. The chunk id goes in the log, not a metric label.
func (a *ReadView) unavailable(c chunk.ID, k geometry.Kind) error {
	unavailableResolves.Add(1)
	a.catalog.Logger().WithField("chunk", c).WithField("kind", k).
		Warn("query: chunk has no serving store (unreachable within the serving model)")
	return ErrUnavailable
}

// LedgerReader is the per-chunk ledger read surface the range queries consume,
// satisfied by both the hot store and the cold pack reader. It deliberately omits
// LastSeq (the two tiers' signatures differ) since routing reads within an
// already-known chunk range.
type LedgerReader interface {
	GetLedgerRaw(seq uint32) ([]byte, error)
	IterateLedgers(start, end uint32) iter.Seq2[ledger.Entry, error]
}

// tier is which storage serves a chunk for a kind under the read view's snapshot.
type tier int

const (
	tierNone tier = iota // no serving home (R1: not finished, or no handle)
	tierCold             // a frozen cold artifact
	tierHot              // a ready hot database with a published handle
)

// resolveTier is the single routing-decision site: for chunk c and kind k, read
// the artifact and hot states through the read view's snapshot and apply the
// serving rules once. A frozen artifact wins (cold), even when the chunk is also
// hot (cold-wins during the freeze-to-discard overlap); otherwise a ready hot
// database whose handle the read view loaded serves it (hot); otherwise none.
// States other than "frozen"/"ready" are never served (R1). The hot DB is
// returned only for tierHot.
func (a *ReadView) resolveTier(c chunk.ID, k geometry.Kind) (tier, *hotchunk.DB, error) {
	st, err := a.snap.State(c, k)
	if err != nil {
		return tierNone, nil, err
	}
	if st == geometry.StateFrozen {
		return tierCold, nil, nil
	}
	hst, err := a.snap.HotState(c)
	if err != nil {
		return tierNone, nil, err
	}
	if hst == geometry.HotReady {
		if db, ok := a.handles.byChunk[c]; ok {
			return tierHot, db, nil
		}
	}
	return tierNone, nil, nil
}

// Ledgers resolves chunk c's ledger store for this request. A cold reader is
// view-owned — Release closes it; the hot facade is registry-owned. Returns
// ErrUnavailable when c has no serving home.
func (a *ReadView) Ledgers(c chunk.ID) (LedgerReader, error) {
	r, closeFn, err := a.resolveLedgers(c)
	if err != nil {
		return nil, err
	}
	if closeFn != nil {
		a.closers = append(a.closers, closeFn)
	}
	return r, nil
}

// resolveLedgers is Ledgers without the view registration: the returned close is
// the CALLER's to run (nil for the registry-owned hot facade). ScanLedgers uses
// it to close each cold reader as the walk passes its chunk instead of holding
// every reader until Release.
func (a *ReadView) resolveLedgers(c chunk.ID) (LedgerReader, func() error, error) {
	t, db, err := a.resolveTier(c, geometry.KindLedgers)
	if err != nil {
		return nil, nil, err
	}
	switch t {
	case tierCold:
		cr, err := ledger.OpenColdReader(a.catalog.Layout().LedgerPackPath(c))
		if err != nil {
			return nil, nil, err
		}
		return cr, cr.Close, nil
	case tierHot:
		return db.Ledgers(), nil, nil
	default:
		return nil, nil, a.unavailable(c, geometry.KindLedgers)
	}
}

// Events resolves chunk c's event store as the common event.Reader the
// query engine consumes, uniform across tiers. A cold reader is view-owned —
// Release closes it; the hot facade is registry-owned. Returns ErrUnavailable
// when c has no serving home. The hot facade is safe here because the registry
// holds read-write handles, whose events store is warmed (a read-only open
// would have none).
func (a *ReadView) Events(c chunk.ID) (event.Reader, error) {
	t, db, err := a.resolveTier(c, geometry.KindEvents)
	if err != nil {
		return nil, err
	}
	switch t {
	case tierCold:
		// TODO(events adapter / #772): thread read concurrency
		// (ColdReaderOptions.Concurrency → the packfile ReadItems concurrency) here;
		// decide whether it is config-driven or caller-supplied. Default for now.
		cr, err := event.OpenColdReader(c, a.catalog.Layout().EventsBucketDir(c), event.ColdReaderOptions{})
		if err != nil {
			return nil, err
		}
		a.closers = append(a.closers, cr.Close)
		return cr, nil
	case tierHot:
		return db.Events(), nil
	default:
		return nil, a.unavailable(c, geometry.KindEvents)
	}
}
