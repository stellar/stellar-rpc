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

const formatLedgerCold packfile.Format = 1

// appDataSize is the byte size of the cold-ledger AppData trailer
// payload: firstSeq, big-endian uint32.
const appDataSize = 4

type ColdWriter struct {
	pw       *packfile.Writer
	enc      *zstd.Compressor
	encBuf   []byte
	firstSeq uint32
	nextSeq  uint32
	closed   atomic.Bool
}

// NewColdWriter truncates any pre-existing file at path so a
// crashed prior attempt can be retried at the same path.
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
		Overwrite:      true,
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

// AppendLedger rejects a gap or out-of-order seq without advancing
// internal state.
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

// Close before Finalize removes the partial .pack file.
func (w *ColdWriter) Close() error {
	if w.closed.Swap(true) {
		return nil
	}
	return errors.Join(w.enc.Close(), w.pw.Close())
}
