package ledger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// formatLedgerCold is the packfile trailer Format identifying a
// cold-ledger pack written by this package.
// Bumped on any breaking change to per-item encoding (codec, item
// layout) or AppData layout.
const formatLedgerCold packfile.Format = 1

// appDataSize is the byte size of the cold-ledger AppData payload:
// firstSeq as a 4-byte big-endian uint32.
const appDataSize = 4

// ColdWriter builds a single packfile of zstd-compressed ledgers
// keyed by a contiguous sequence range.
// One packfile item = one zstd-compressed ledger (ItemsPerRecord=1
// so every ledger has its own record + offset, enabling direct
// random-access reads on the read side).
// firstSeq is fixed at construction; each AppendLedger must pass
// the next sequence in order.
// Finalize writes the trailer (carrying firstSeq in AppData so
// OpenColdStore can recover it) and closes the file.
// Close before Finalize removes the partial .pack; Close after
// Finalize is a no-op.
type ColdWriter struct {
	pw       *packfile.Writer
	enc      *zstd.Compressor
	encBuf   []byte
	firstSeq uint32
	nextSeq  uint32
	closed   atomic.Bool
}

// NewColdWriter validates inputs and returns a writer ready to
// accept AppendLedger calls in [firstSeq, firstSeq+N) order.
// The writer owns its own *zstd.Compressor (zstd compressors are
// not concurrent-safe, so one-per-writer is the right shape).
func NewColdWriter(
	path string,
	firstSeq uint32,
	logger *supportlog.Entry,
) (*ColdWriter, error) {
	if path == "" {
		return nil, rocksdb.ErrInvalidConfig
	}
	if logger == nil {
		return nil, rocksdb.ErrInvalidConfig
	}
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord: 1,
		Format:         formatLedgerCold,
		ContentHash:    true,
	})
	if err != nil {
		return nil, fmt.Errorf("cold: create packfile %q: %w", path, err)
	}
	return &ColdWriter{
		pw:       pw,
		enc:      zstd.NewCompressor(),
		firstSeq: firstSeq,
		nextSeq:  firstSeq,
	}, nil
}

// AppendLedger compresses ledgerBytes with zstd and writes it as
// the next item in the packfile.
// seq must equal firstSeq + N where N is the count of prior
// successful appends; a gap or out-of-order seq returns an error
// and does not advance internal state.
func (w *ColdWriter) AppendLedger(seq uint32, ledgerBytes []byte) error {
	if w.closed.Load() {
		return stores.ErrStoreClosed
	}
	if seq != w.nextSeq {
		return fmt.Errorf("cold: expected seq %d, got %d", w.nextSeq, seq)
	}
	compressed, err := w.enc.Encode(w.encBuf[:0], ledgerBytes)
	if err != nil {
		return fmt.Errorf("cold: compress seq %d: %w", seq, err)
	}
	if err := w.pw.AppendItem(compressed); err != nil {
		return err
	}
	// packfile.AppendItem copies its argument, so we can hold onto
	// the underlying array for the next Encode call.
	w.encBuf = compressed
	w.nextSeq++
	return nil
}

// Finalize flushes the packfile index, writes AppData (firstSeq
// big-endian uint32) + trailer, fsyncs, and closes the underlying
// file.
// After Finalize, AppendLedger and re-Finalize return
// stores.ErrStoreClosed; Close is a no-op.
func (w *ColdWriter) Finalize() error {
	if w.closed.Load() {
		return stores.ErrStoreClosed
	}
	var ad [appDataSize]byte
	binary.BigEndian.PutUint32(ad[:], w.firstSeq)
	if err := w.pw.Finish(ad[:]); err != nil {
		return err
	}
	_ = w.enc.Close()
	w.closed.Store(true)
	return nil
}

// Close releases the writer.
// Called before Finalize, it removes the partial .pack file and
// closes the compressor.
// Called after Finalize (or twice), it is a no-op.
func (w *ColdWriter) Close() error {
	if w.closed.Swap(true) {
		return nil
	}
	return errors.Join(w.enc.Close(), w.pw.Close())
}
