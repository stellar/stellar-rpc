package query

import (
	"iter"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// The range helpers the endpoint adapters build on. Both clamp the raw request
// range themselves (RangeError / ErrInvertedRange surface here) and resolve the
// overlapping chunks. EventParts opens everything up front: getEvents' 10,000-
// ledger protocol cap makes its chunk count structural. ScanLedgers cannot lean
// on that — getTransactions' scan shape is (cursor, latestLedger), which on a
// deep-history node spans thousands of chunks — so it must be cheap at any
// count, not just correct.

// ScanLedgers returns a flat ascending iterator over the raw ledgers in
// [lo, hi] clamped to the view's range. The per-chunk intersect lives here, so
// a caller cannot read past the view's latestLedger or below its floor; the
// intersect also satisfies the cold reader's coverage requirement, since a
// frozen pack covers its whole chunk. Entry.Bytes follows the hot store's
// borrow contract: valid only until the next iteration step.
//
// The walk holds at most TWO readers open regardless of range: chunk i's reader
// plus a one-ahead open of chunk i+1 (so the border chunk's open overlaps the
// current chunk's streaming; both stores defer their heavy validation to first
// use), and each cold reader is closed as the walk passes its chunk. Release
// stays the backstop for whatever is open at an early break. The first two
// chunks resolve at call time, so their failures surface here; a later chunk's
// failure surfaces mid-stream at its own position.
func (a *ReadView) ScanLedgers(lo, hi uint32) (iter.Seq2[ledger.Entry, error], error) {
	lo, hi, err := a.ClampRange(Ascending, lo, hi)
	if err != nil {
		return nil, err
	}
	if lo > hi {
		return func(func(ledger.Entry, error) bool) {}, nil // beyond latest: empty
	}
	chunks := chunksBetween(chunk.IDFromLedger(lo), chunk.IDFromLedger(hi), Ascending)

	w := &ledgerWalk{view: a, chunks: chunks}
	// Release's backstop for an early break; the walk itself closes each cold
	// reader as it passes the chunk (closeChunk pops, so double-close is a no-op).
	a.closers = append(a.closers, w.closeAll)
	// Open the walk's first window (chunks 0 and 1) now, so their failures
	// surface to the caller rather than mid-stream.
	if err := w.open(0); err != nil {
		return nil, err
	}
	if err := w.open(1); err != nil {
		return nil, err
	}

	return func(yield func(ledger.Entry, error) bool) {
		for i, c := range chunks {
			r, ok := w.readers[i]
			if !ok {
				if oerr := w.open(i); oerr != nil {
					yield(ledger.Entry{}, oerr)
					return
				}
				r = w.readers[i]
			}
			// One-ahead: start the next chunk's open while this one streams. An
			// error here is deliberately dropped — the walk retries when it
			// reaches the chunk, surfacing the failure at its own position.
			_ = w.open(i + 1)

			from := max(lo, c.FirstLedger())
			to := min(hi, c.LastLedger())
			for e, ierr := range r.IterateLedgers(from, to) {
				if !yield(e, ierr) || ierr != nil {
					return
				}
			}
			w.closeChunk(i)
		}
	}, nil
}

// ledgerWalk is ScanLedgers' reader window: the open readers by chunk index and
// the cold closers the walk still owes. A ReadView serves one request on one
// goroutine, so the walk needs no locking.
type ledgerWalk struct {
	view    *ReadView
	chunks  []chunk.ID
	readers map[int]LedgerReader
	closers map[int]func() error
}

// open resolves chunk index i and caches its reader; already-open and
// out-of-range indexes are no-ops. Cold closers are the walk's to run.
func (w *ledgerWalk) open(i int) error {
	if i >= len(w.chunks) {
		return nil
	}
	if w.readers == nil {
		w.readers = map[int]LedgerReader{}
		w.closers = map[int]func() error{}
	}
	if _, ok := w.readers[i]; ok {
		return nil
	}
	r, closeFn, err := w.view.resolveLedgers(w.chunks[i])
	if err != nil {
		return err
	}
	w.readers[i] = r
	if closeFn != nil {
		w.closers[i] = closeFn
	}
	return nil
}

// closeChunk closes chunk index i's cold reader (a no-op for hot or already
// closed) as the walk passes it.
func (w *ledgerWalk) closeChunk(i int) {
	delete(w.readers, i)
	if closeFn, ok := w.closers[i]; ok {
		delete(w.closers, i)
		if err := closeFn(); err != nil {
			w.view.catalog.Logger().WithError(err).Warn("query: close scanned chunk reader")
		}
	}
}

// closeAll is the Release backstop: close whatever the walk still holds.
func (w *ledgerWalk) closeAll() error {
	for i := range w.closers {
		w.closeChunk(i)
	}
	return nil
}

// EventPart is one chunk's slice of a clamped range: the reader plus the
// intersected ledger bounds — the input shape the events query engine consumes.
type EventPart struct {
	Chunk    chunk.ID
	Reader   event.Reader
	From, To uint32
}

// EventParts resolves the clamped [lo, hi] into per-chunk parts in scan order
// (one or two under the page limits). An empty result means the request lies
// beyond latest.
func (a *ReadView) EventParts(dir Direction, lo, hi uint32) ([]EventPart, error) {
	lo, hi, err := a.ClampRange(dir, lo, hi)
	if err != nil {
		return nil, err
	}
	if lo > hi {
		return nil, nil // beyond latest: nothing to serve yet
	}
	chunks := chunksBetween(chunk.IDFromLedger(lo), chunk.IDFromLedger(hi), dir)
	parts := make([]EventPart, 0, len(chunks))
	for _, c := range chunks {
		r, err := a.Events(c)
		if err != nil {
			return nil, err
		}
		parts = append(parts, EventPart{
			Chunk: c, Reader: r,
			From: max(lo, c.FirstLedger()), To: min(hi, c.LastLedger()),
		})
	}
	return parts, nil
}
