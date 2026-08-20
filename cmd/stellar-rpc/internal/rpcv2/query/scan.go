package query

import (
	"iter"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// The range helpers the endpoint adapters build on. They clamp the raw
// request range themselves (RangeError / ErrInvertedRange surface here) and
// resolve the overlapping chunks lazily: a cursor-driven scan can span
// thousands of chunks on a deep-history node, so a reader opens only when the
// walk reaches its chunk. ScanLedgers holds at most two readers; the events
// pager's walkChunks resolves one chunk at a time.

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
	lo, hi, outcome, err := a.ClampRange(Ascending, lo, hi)
	if err != nil {
		return nil, err
	}
	if outcome != RangeServe {
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
