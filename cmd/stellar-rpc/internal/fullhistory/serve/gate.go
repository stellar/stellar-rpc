package serve

import (
	"context"
	"sync/atomic"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

// errBackfillInProgress is THE error every gated method returns while no
// serving state is published (startup backfill still running, or the daemon's
// run loop is between a failure and its supervised restart — which re-runs
// backfill too). One shared value keeps the error shape identical across
// every endpoint.
var errBackfillInProgress = &jrpc2.Error{
	Code:    jrpc2.InternalError,
	Message: "backfill in progress; query serving not started",
}

// state is one daemon run's serving face: the db.* readers built over that
// run's registry. It is published as a unit so a request never mixes readers
// from two runs.
type state struct {
	ledgers      db.LedgerReader
	transactions db.TransactionReader
	events       db.EventReader
}

// gate is the backfill gate: nil = no serving state (every gated method errors
// with errBackfillInProgress), non-nil = the current run's readers. The HTTP
// server and its handler map live for the whole daemon and are built exactly
// once; the gate is the one mutable cell they dereference per request, so a
// supervised restart swaps serving state without touching the server.
type gate struct {
	p atomic.Pointer[state]
}

func (g *gate) load() *state { return g.p.Load() }

// publish makes st the serving state and returns an undo that clears it —
// but only if st is still current, so a slow undo from a dead run can never
// clobber the next run's published state.
func (g *gate) publish(st *state) (undo func()) {
	g.p.Store(st)
	return func() { g.p.CompareAndSwap(st, nil) }
}

// ---------------------------------------------------------------------------
// Gated readers: the db.* faces the method handlers are built against. The
// handler map is assembled once at daemon startup, but each run's readers only
// exist once that run's registry is built — so these wrappers re-resolve the
// current state on every call. A call with no state published returns
// errBackfillInProgress; the method-level gate wrapper normally rejects such
// requests before they reach a reader, so this path only fires for a request
// already in flight when the state is torn down.
// ---------------------------------------------------------------------------

type gatedLedgerReader struct{ g *gate }

var _ db.LedgerReader = gatedLedgerReader{}

func (r gatedLedgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	st := r.g.load()
	if st == nil {
		return xdr.LedgerCloseMeta{}, false, errBackfillInProgress
	}
	return st.ledgers.GetLedger(ctx, sequence)
}

func (r gatedLedgerReader) StreamAllLedgers(ctx context.Context, f db.StreamLedgerFn) error {
	st := r.g.load()
	if st == nil {
		return errBackfillInProgress
	}
	return st.ledgers.StreamAllLedgers(ctx, f)
}

func (r gatedLedgerReader) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	st := r.g.load()
	if st == nil {
		return ledgerbucketwindow.LedgerRange{}, errBackfillInProgress
	}
	return st.ledgers.GetLedgerRange(ctx)
}

func (r gatedLedgerReader) GetLedgerCountInRange(
	ctx context.Context, start, end uint32,
) (uint32, uint32, uint32, error) {
	st := r.g.load()
	if st == nil {
		return 0, 0, 0, errBackfillInProgress
	}
	return st.ledgers.GetLedgerCountInRange(ctx, start, end)
}

func (r gatedLedgerReader) StreamLedgerRange(
	ctx context.Context, startLedger, endLedger uint32, f db.StreamLedgerFn,
) error {
	st := r.g.load()
	if st == nil {
		return errBackfillInProgress
	}
	return st.ledgers.StreamLedgerRange(ctx, startLedger, endLedger, f)
}

func (r gatedLedgerReader) NewTx(ctx context.Context) (db.LedgerReaderTx, error) {
	st := r.g.load()
	if st == nil {
		return nil, errBackfillInProgress
	}
	return st.ledgers.NewTx(ctx)
}

func (r gatedLedgerReader) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	st := r.g.load()
	if st == nil {
		return 0, errBackfillInProgress
	}
	return st.ledgers.GetLatestLedgerSequence(ctx)
}

type gatedTransactionReader struct{ g *gate }

var _ db.TransactionReader = gatedTransactionReader{}

func (r gatedTransactionReader) GetTransaction(ctx context.Context, hash xdr.Hash) (db.Transaction, error) {
	st := r.g.load()
	if st == nil {
		return db.Transaction{}, errBackfillInProgress
	}
	return st.transactions.GetTransaction(ctx, hash)
}

type gatedEventReader struct{ g *gate }

var _ db.EventReader = gatedEventReader{}

func (r gatedEventReader) GetEvents(
	ctx context.Context,
	cursorRange protocol.CursorRange,
	contractIDs [][]byte,
	topics db.TopicFilters,
	eventTypes []int,
	f db.ScanFunction,
) error {
	st := r.g.load()
	if st == nil {
		return errBackfillInProgress
	}
	return st.events.GetEvents(ctx, cursorRange, contractIDs, topics, eventTypes, f)
}
