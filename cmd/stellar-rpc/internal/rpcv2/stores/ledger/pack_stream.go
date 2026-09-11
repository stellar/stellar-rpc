package ledger

import (
	"context"
	"fmt"
	"iter"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// packStream is a ledgerbackend.LedgerStream backed by a single cold packfile.
// Like the SDK's NewBufferedStorageStream it owns its lifecycle: each RawLedgers
// call opens the pack's ColdReader, yields each ledger's wire-format
// xdr.LedgerCloseMeta bytes for the requested range, and closes the reader when
// iteration ends.
type packStream struct {
	path string
}

// NewPackStream returns a ledgerbackend.LedgerStream over a single cold packfile
// at path (one .pack per chunk). It is the LOCAL, already-frozen ledger source:
// the backfill freeze protocol re-derives a chunk's other artifacts from it when
// the ledgers are already on disk. Because it speaks the same streaming currency
// as the external backends (BufferedStorageStream, CaptiveCoreStream), the cold
// materializer never learns which source it drank from.
func NewPackStream(path string) ledgerbackend.LedgerStream {
	return &packStream{path: path}
}

var _ ledgerbackend.LedgerStream = (*packStream)(nil)

// RawLedgers streams the pack's raw ledger bytes by opening a ColdReader and
// delegating to IterateLedgers, which yields packfile borrows directly. Each
// yielded slice is valid only until the next iteration step; the ingest driver
// consumes each ledger fully before the next yield, and every ingester copies the
// bytes it retains. ctx is observed between ledgers (yielding its error once
// canceled), so a drain loop can rely on the stream for cancellation. An unbounded
// range streams through the pack's LastSeq.
func (p *packStream) RawLedgers(
	ctx context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		cr, err := OpenColdReader(p.path)
		if err != nil {
			yield(nil, fmt.Errorf("OpenColdReader %s: %w", p.path, err))
			return
		}
		defer func() { _ = cr.Close() }()

		to := r.To()
		if !r.Bounded() {
			last, lerr := cr.LastSeq()
			if lerr != nil {
				yield(nil, lerr)
				return
			}
			to = last
		}
		for entry, ierr := range cr.IterateLedgers(r.From(), to) {
			if cerr := ctx.Err(); cerr != nil {
				yield(nil, cerr)
				return
			}
			if ierr != nil {
				yield(nil, ierr)
				return
			}
			if !yield(entry.Bytes, nil) {
				return
			}
		}
	}
}
