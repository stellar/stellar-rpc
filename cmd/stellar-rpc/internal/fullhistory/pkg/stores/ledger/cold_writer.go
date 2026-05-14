package ledger

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
)

// formatLedgerCold is the packfile trailer Format identifying a
// cold-ledger pack.
// Bumped on any breaking change to record contents or AppData
// layout.
const formatLedgerCold packfile.Format = 1

// appDataSize is the byte size of the cold-ledger AppData payload:
// firstSeq as a 4-byte big-endian uint32.
const appDataSize = 4

// ColdWriter builds a single packfile of zstd-compressed ledgers
// keyed by a contiguous sequence range.
// firstSeq is fixed at construction; each AppendLedger must pass
// the next sequence in order.
// Finalize writes the trailer (carrying firstSeq in AppData so
// OpenColdStore can recover it).
// Close before Finalize removes the partial .pack; Close after
// Finalize is a no-op.
type ColdWriter struct {
	pw       *packfile.Writer
	firstSeq uint32
	nextSeq  uint32
	closed   atomic.Bool
}

// NewColdWriter validates inputs and returns a writer ready to
// accept AppendLedger calls in [firstSeq, firstSeq+N) order.
//
// newEncoder is called once per packfile worker.
// Production wires it as
// `func() packfile.RecordEncoder { return zstd.NewCompressor() }`.
func NewColdWriter(
	path string,
	firstSeq uint32,
	newEncoder func() packfile.RecordEncoder,
	logger *supportlog.Entry,
) (*ColdWriter, error) {
	if path == "" {
		return nil, rocksdb.ErrInvalidConfig
	}
	if newEncoder == nil {
		return nil, rocksdb.ErrInvalidConfig
	}
	if logger == nil {
		return nil, rocksdb.ErrInvalidConfig
	}
	pw, err := packfile.Create(path, packfile.WriterOptions{
		Format:           formatLedgerCold,
		NewRecordEncoder: newEncoder,
		ContentHash:      true,
	})
	if err != nil {
		return nil, fmt.Errorf("cold: create packfile %q: %w", path, err)
	}
	return &ColdWriter{
		pw:       pw,
		firstSeq: firstSeq,
		nextSeq:  firstSeq,
	}, nil
}

// AppendLedger writes one ledger.
// seq must equal firstSeq + N where N is the count of prior
// successful appends.
// A gap or out-of-order seq returns an error and does not advance
// internal state.
func (w *ColdWriter) AppendLedger(seq uint32, ledgerBytes []byte) error {
	if w.closed.Load() {
		return stores.ErrStoreClosed
	}
	if seq != w.nextSeq {
		return fmt.Errorf("cold: expected seq %d, got %d", w.nextSeq, seq)
	}
	if err := w.pw.AppendItem(ledgerBytes); err != nil {
		return err
	}
	w.nextSeq++
	return nil
}

// Finalize flushes any partial record, writes the index + AppData
// (firstSeq big-endian uint32) + trailer, fsyncs, and closes the
// underlying file.
// After Finalize, Close is a no-op.
func (w *ColdWriter) Finalize() error {
	if w.closed.Load() {
		return stores.ErrStoreClosed
	}
	var ad [appDataSize]byte
	binary.BigEndian.PutUint32(ad[:], w.firstSeq)
	if err := w.pw.Finish(ad[:]); err != nil {
		return err
	}
	w.closed.Store(true)
	return nil
}

// Close releases the writer.
// Called before Finalize, it removes the partial .pack file.
// Called after Finalize (or twice), it is a no-op.
func (w *ColdWriter) Close() error {
	if w.closed.Swap(true) {
		return nil
	}
	return w.pw.Close()
}
