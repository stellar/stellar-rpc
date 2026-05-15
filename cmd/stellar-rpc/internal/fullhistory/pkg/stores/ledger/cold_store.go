package ledger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
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

// ColdStoreWriter is two-phase because writes can fail mid-flow.
// Commit finalizes (trailer + fsync); Close removes the partial
// pack if Commit hasn't run. Two functions because a single
// "Close" can't tell success from failure — the deferred-Close
// pattern needs Commit as the explicit success signal:
//
//	w, _ := NewColdStoreWriter(path, firstSeq, log)
//	defer w.Close()        // cleans up partial on any early return
//	for seq, b := range src {
//	    if err := w.AppendLedger(seq, b); err != nil {
//	        return err     // partial pack auto-removed by deferred Close
//	    }
//	}
//	return w.Commit()      // success — deferred Close becomes a no-op
type ColdStoreWriter struct {
	pw       *packfile.Writer
	enc      *zstd.Compressor
	encBuf   []byte
	firstSeq uint32
	nextSeq  uint32
	closed   atomic.Bool
}

// NewColdStoreWriter truncates any pre-existing file at path so a
// crashed prior attempt can be retried at the same path.
func NewColdStoreWriter(
	path string,
	firstSeq uint32,
	logger *supportlog.Entry,
) (*ColdStoreWriter, error) {
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
	return &ColdStoreWriter{
		pw:       pw,
		enc:      zstd.NewCompressor(),
		firstSeq: firstSeq,
		nextSeq:  firstSeq,
	}, nil
}

// AppendLedger rejects a gap or out-of-order seq without advancing
// internal state.
func (w *ColdStoreWriter) AppendLedger(seq uint32, ledgerBytes []byte) error {
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

func (w *ColdStoreWriter) Commit() error {
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

func (w *ColdStoreWriter) Close() error {
	if w.closed.Swap(true) {
		return nil
	}
	return errors.Join(w.enc.Close(), w.pw.Close())
}

type ColdStoreReader struct {
	reader   *packfile.Reader
	decoder  *zstd.Decompressor
	firstSeq uint32
	lastSeq  uint32
	closed   atomic.Bool
}

// NewColdStoreReader takes a caller-owned decoder, typically a
// single *zstd.Decompressor shared across all ColdStoreReaders in
// the process. ColdStoreReader.Close does not touch it.
func NewColdStoreReader(
	path string,
	decoder *zstd.Decompressor,
	logger *supportlog.Entry,
) (*ColdStoreReader, error) {
	if path == "" {
		return nil, rocksdb.ErrInvalidConfig
	}
	if decoder == nil {
		return nil, rocksdb.ErrInvalidConfig
	}
	if logger == nil {
		return nil, rocksdb.ErrInvalidConfig
	}
	r := packfile.Open(path, packfile.ReaderOptions{})
	tr, err := r.Trailer()
	if err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("cold: open %q: %w", path, err)
	}
	if tr.Format != formatLedgerCold {
		_ = r.Close()
		return nil, fmt.Errorf("cold: expected format %d, got %d", formatLedgerCold, tr.Format)
	}
	if tr.TotalItems == 0 {
		_ = r.Close()
		return nil, fmt.Errorf("cold: pack %q contains no items", path)
	}
	ad, err := r.AppData()
	if err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("cold: read AppData %q: %w", path, err)
	}
	if len(ad) != appDataSize {
		_ = r.Close()
		return nil, fmt.Errorf("cold: expected %d-byte AppData (firstSeq), got %d", appDataSize, len(ad))
	}
	firstSeq := binary.BigEndian.Uint32(ad)
	lastSeq := firstSeq + tr.TotalItems - 1
	return &ColdStoreReader{
		reader:   r,
		decoder:  decoder,
		firstSeq: firstSeq,
		lastSeq:  lastSeq,
	}, nil
}

func (c *ColdStoreReader) FirstSeq() uint32 { return c.firstSeq }
func (c *ColdStoreReader) LastSeq() uint32  { return c.lastSeq }

// GetLedgerRaw returns stores.ErrNotFound if seq is outside
// [FirstSeq, LastSeq].
func (c *ColdStoreReader) GetLedgerRaw(seq uint32) ([]byte, error) {
	if c.closed.Load() {
		return nil, stores.ErrStoreClosed
	}
	if seq < c.firstSeq || seq > c.lastSeq {
		return nil, stores.ErrNotFound
	}
	pos := int(seq - c.firstSeq)
	var out []byte
	err := c.reader.ReadItem(pos, func(b []byte) error {
		var derr error
		out, derr = c.decoder.Decode(nil, b)
		return derr
	})
	if err != nil {
		if errors.Is(err, packfile.ErrPositionOutOfRange) {
			return nil, stores.ErrNotFound
		}
		return nil, err
	}
	return out, nil
}

// IterateLedgers clamps [start, end] to [FirstSeq, LastSeq]; an
// out-of-window range is a no-op, and a closed store yields
// stores.ErrStoreClosed once.
func (c *ColdStoreReader) IterateLedgers(start, end uint32) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		if c.closed.Load() {
			yield(Entry{}, stores.ErrStoreClosed)
			return
		}
		// Short-circuit before subtracting below — a caller-passed
		// start < firstSeq would underflow uint32 otherwise.
		if start > end || end < c.firstSeq || start > c.lastSeq {
			return
		}
		if start < c.firstSeq {
			start = c.firstSeq
		}
		if end > c.lastSeq {
			end = c.lastSeq
		}
		startPos := int(start - c.firstSeq)
		count := int(end-start) + 1

		seq := start
		for item, err := range c.reader.ReadRange(startPos, count) {
			if err != nil {
				yield(Entry{}, err)
				return
			}
			decompressed, derr := c.decoder.Decode(nil, item)
			if derr != nil {
				yield(Entry{}, derr)
				return
			}
			if !yield(Entry{Seq: seq, Bytes: decompressed}, nil) {
				return
			}
			seq++
		}
	}
}

// Close does not close the caller-owned decoder.
func (c *ColdStoreReader) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.reader.Close()
}
