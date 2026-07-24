package serving

import (
	"errors"
	"iter"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
)

// ErrUnavailable means a chunk has no serving store for the requested kind in the
// admitted snapshot: neither a frozen cold artifact nor a ready hot database. It
// is R1 in effect — a freezing, pruning, or transient resource is invisible to
// routing regardless of what is on disk.
var ErrUnavailable = errors.New("serving: chunk has no serving store")

// LedgerReader is the per-chunk ledger read surface the range queries consume,
// satisfied by both the hot store and the cold pack reader. It deliberately omits
// LastSeq (the two tiers' signatures differ) since routing reads within an
// already-known chunk range.
type LedgerReader interface {
	GetLedgerRaw(seq uint32) ([]byte, error)
	IterateLedgers(start, end uint32) iter.Seq2[ledger.Entry, error]
}

// tier is which storage serves a chunk for a kind under the admitted snapshot.
type tier int

const (
	tierNone tier = iota // no serving home (R1: not finished, or no handle)
	tierCold             // a frozen cold artifact
	tierHot              // a ready hot database with a published handle
)

// resolveTier is the single routing-decision site: for chunk c and kind k, read
// the artifact and hot states through the admission snapshot and apply the serving
// rules once. A frozen artifact wins (cold), even when the chunk is also hot
// (cold-wins during the freeze-to-discard overlap); otherwise a ready hot database
// whose handle the admission loaded serves it (hot); otherwise none. States other
// than "frozen"/"ready" are never served (R1). The hot DB is returned only for
// tierHot.
func (a *Admission) resolveTier(c chunk.ID, k geometry.Kind) (tier, *hotchunk.DB, error) {
	st, err := a.catalog.StateAsOf(a.snap, c, k)
	if err != nil {
		return tierNone, nil, err
	}
	if st == geometry.StateFrozen {
		return tierCold, nil, nil
	}
	hst, err := a.catalog.HotStateAsOf(a.snap, c)
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

// LedgerReader resolves chunk c's ledger store for this request. The returned
// close releases a cold reader; for the hot tier it is a no-op (the router owns
// the handle). Returns ErrUnavailable when c has no serving home.
func (a *Admission) LedgerReader(c chunk.ID) (LedgerReader, func() error, error) {
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
		return db.Ledgers(), noClose, nil
	default:
		return nil, nil, ErrUnavailable
	}
}

// EventReader resolves chunk c's event store as the common eventstore.Reader the
// query engine consumes, uniform across tiers. The returned close releases a cold
// reader; the hot tier is a no-op. Returns ErrUnavailable when c has no serving
// home. The hot facade is safe here because the router holds read-write handles,
// whose events store is warmed (a read-only open would have none).
func (a *Admission) EventReader(c chunk.ID) (eventstore.Reader, func() error, error) {
	t, db, err := a.resolveTier(c, geometry.KindEvents)
	if err != nil {
		return nil, nil, err
	}
	switch t {
	case tierCold:
		// TODO(events adapter / #772): thread read concurrency
		// (ColdReaderOptions.Concurrency → the packfile ReadItems concurrency) here;
		// decide whether it is config-driven or caller-supplied. Default for now.
		cr, err := eventstore.OpenColdReader(c, a.catalog.Layout().EventsBucketDir(c), eventstore.ColdReaderOptions{})
		if err != nil {
			return nil, nil, err
		}
		return cr, cr.Close, nil
	case tierHot:
		return db.Events(), noClose, nil
	default:
		return nil, nil, ErrUnavailable
	}
}

func noClose() error { return nil }
