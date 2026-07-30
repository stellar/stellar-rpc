package query

import (
	"iter"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// The range helpers the endpoint adapters build on. Both clamp the raw request
// range themselves (RangeError / ErrInvertedRange surface here), resolve the
// overlapping chunks, and open every chunk's reader up front: the readers are
// view-owned (closed by Release), the border chunk's open overlaps the first
// chunk's streaming, and since both stores defer validation to first use, an
// open failure surfaces at its own position. Page limits cap every request at
// two chunks; the code handles any count.

// ScanLedgers returns a flat ascending iterator over the raw ledgers in
// [lo, hi] clamped to the view's range. The per-chunk intersect lives here, so
// a caller cannot read past the admitted latest or below the floor; the
// intersect also satisfies the cold reader's coverage requirement, since a
// frozen pack covers its whole chunk. Entry.Bytes follows the hot store's
// borrow contract: valid only until the next iteration step.
func (a *ReadView) ScanLedgers(lo, hi uint32) (iter.Seq2[ledger.Entry, error], error) {
	lo, hi, err := a.ClampRange(Ascending, lo, hi)
	if err != nil {
		return nil, err
	}
	if lo > hi {
		return func(func(ledger.Entry, error) bool) {}, nil // beyond latest: empty
	}
	chunks := chunksBetween(chunk.IDFromLedger(lo), chunk.IDFromLedger(hi), Ascending)
	readers := make([]LedgerReader, len(chunks))
	for i, c := range chunks {
		if readers[i], err = a.Ledgers(c); err != nil {
			return nil, err
		}
	}
	return func(yield func(ledger.Entry, error) bool) {
		for i, c := range chunks {
			from := max(lo, c.FirstLedger())
			to := min(hi, c.LastLedger())
			for e, ierr := range readers[i].IterateLedgers(from, to) {
				if !yield(e, ierr) || ierr != nil {
					return
				}
			}
		}
	}, nil
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
