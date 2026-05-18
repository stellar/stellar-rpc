package ledger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"
	"os"
	"sync"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

const formatLedgerCold packfile.Format = 1

// appDataSize — firstSeq (4 BE). lastSeq is derived from
// trailer.TotalItems at open.
const appDataSize = 4

// ColdWriterOptions configures the underlying packfile writer.
// The zero value is a sensible default (serial encoding, no
// background writeback).
type ColdWriterOptions struct {
	// Concurrency sets parallel record-encoder workers. 0 means 1
	// (serial). Bump for large backfills where zstd encoding is
	// CPU-bound; pick a value <= NumCPU.
	Concurrency int

	// BytesPerSync triggers background dirty-page writeback every
	// N bytes (Linux: sync_file_range, non-blocking). Spreads I/O
	// across the write phase so the final fsync in Commit has less
	// to flush. 0 disables.
	BytesPerSync int
}

// ColdStoreWriter is two-phase: Commit finalizes; Close cleans up
// a partial pack when Commit hasn't run. A ColdStoreWriter must be
// used by a single goroutine — AppendLedger, Commit, and Close are
// not safe for concurrent invocation. Idiomatic use:
//
//	w, _ := NewColdStoreWriter(path, firstSeq, ledger.ColdWriterOptions{})
//	defer w.Close()
//	for seq, b := range src {
//	    if err := w.AppendLedger(seq, b); err != nil {
//	        return err
//	    }
//	}
//	return w.Commit()
type ColdStoreWriter struct {
	pw       *packfile.Writer
	firstSeq uint32
	nextSeq  uint32
	path     string
}

// NewColdStoreWriter truncates any pre-existing file at path so a
// crashed prior attempt can be retried at the same path.
func NewColdStoreWriter(path string, firstSeq uint32, opts ColdWriterOptions) (*ColdStoreWriter, error) {
	if path == "" {
		return nil, stores.ErrInvalidConfig
	}
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord:   1,
		Format:           formatLedgerCold,
		Overwrite:        true,
		NewRecordEncoder: func() packfile.RecordEncoder { return zstd.NewCompressor() },
		Concurrency:      opts.Concurrency,
		BytesPerSync:     opts.BytesPerSync,
	})
	if err != nil {
		return nil, fmt.Errorf("cold: create packfile %q: %w", path, err)
	}
	return &ColdStoreWriter{
		pw:       pw,
		firstSeq: firstSeq,
		nextSeq:  firstSeq,
		path:     path,
	}, nil
}

// AppendLedger appends one ledger. seq must equal the writer's
// current nextSeq; a gap or out-of-order seq returns an error
// without advancing internal state.
func (w *ColdStoreWriter) AppendLedger(seq uint32, ledgerBytes []byte) error {
	if seq != w.nextSeq {
		return fmt.Errorf("cold %q: expected seq %d, got %d", w.path, w.nextSeq, seq)
	}
	if err := w.pw.AppendItem(ledgerBytes); err != nil {
		return translateWriterErr(err)
	}
	w.nextSeq++
	return nil
}

// Commit writes firstSeq into AppData, finalizes the trailer, and
// fsyncs the pack. Returns an error if no ledgers have been
// appended (a zero-item pack would be unreadable).
func (w *ColdStoreWriter) Commit() error {
	if w.nextSeq == w.firstSeq {
		return fmt.Errorf("cold %q: commit with no appends", w.path)
	}
	var ad [appDataSize]byte
	binary.BigEndian.PutUint32(ad[:], w.firstSeq)
	if err := w.pw.Finish(ad[:]); err != nil {
		return translateWriterErr(err)
	}
	return nil
}

func (w *ColdStoreWriter) Close() error { return w.pw.Close() }

// ColdStoreReader is lazy: NewColdStoreReader does no I/O. The
// trailer + AppData are read and validated on the first method
// call, via a sync.OnceValues-cached loadHeader. Read methods
// (FirstSeq, LastSeq, GetLedgerRaw, IterateLedgers) are safe for
// concurrent use; Close is NOT — callers must ensure all in-flight
// reads have returned before invoking it, matching the underlying
// packfile.Reader.Close contract.
type ColdStoreReader struct {
	r    *packfile.Reader
	path string
	init func() (coldHeader, error)
}

// coldHeader carries the validated firstSeq / lastSeq returned by
// loadHeader and cached by sync.OnceValues.
type coldHeader struct {
	firstSeq, lastSeq uint32
}

// NewColdStoreReader takes a caller-owned decoder, typically a
// single *zstd.Decompressor shared across all readers in the
// process. ColdStoreReader.Close does not touch it. Returns
// immediately without I/O; trailer + AppData read and validation
// happen on the first method call.
func NewColdStoreReader(path string, decoder *zstd.Decompressor) (*ColdStoreReader, error) {
	if path == "" {
		return nil, stores.ErrInvalidConfig
	}
	if decoder == nil {
		return nil, stores.ErrInvalidConfig
	}
	c := &ColdStoreReader{
		r:    packfile.Open(path, packfile.ReaderOptions{RecordDecoder: decoder}),
		path: path,
	}
	c.init = sync.OnceValues(c.loadHeader)
	return c, nil
}

// loadHeader reads the trailer + AppData, enforces format, AppData
// layout, and uint32 overflow on the derived lastSeq. Cached by
// sync.OnceValues; runs at most once per reader.
func (c *ColdStoreReader) loadHeader() (coldHeader, error) {
	tr, err := c.r.Trailer()
	if err != nil {
		return coldHeader{}, fmt.Errorf("cold: open %q: %w", c.path, err)
	}
	if tr.Format != formatLedgerCold {
		return coldHeader{}, fmt.Errorf("cold %q: expected format %d, got %d", c.path, formatLedgerCold, tr.Format)
	}
	if tr.TotalItems == 0 {
		return coldHeader{}, fmt.Errorf("cold %q: pack contains no items", c.path)
	}
	ad, err := c.r.AppData()
	if err != nil {
		return coldHeader{}, fmt.Errorf("cold: read AppData %q: %w", c.path, err)
	}
	if len(ad) != appDataSize {
		return coldHeader{}, fmt.Errorf("cold %q: expected %d-byte AppData, got %d", c.path, appDataSize, len(ad))
	}
	first := binary.BigEndian.Uint32(ad)
	if uint64(first)+uint64(tr.TotalItems)-1 > math.MaxUint32 {
		return coldHeader{}, fmt.Errorf("cold %q: lastSeq overflows uint32 (firstSeq=%d, items=%d)", c.path, first, tr.TotalItems)
	}
	return coldHeader{firstSeq: first, lastSeq: first + tr.TotalItems - 1}, nil
}

func (c *ColdStoreReader) FirstSeq() (uint32, error) { h, err := c.init(); return h.firstSeq, err }
func (c *ColdStoreReader) LastSeq() (uint32, error)  { h, err := c.init(); return h.lastSeq, err }

func (c *ColdStoreReader) GetLedgerRaw(seq uint32) ([]byte, error) {
	h, err := c.init()
	if err != nil {
		return nil, err
	}
	if seq < h.firstSeq || seq > h.lastSeq {
		return nil, stores.ErrNotFound
	}
	pos := int(seq - h.firstSeq)
	var out []byte
	rerr := c.r.ReadItem(pos, func(b []byte) error {
		// b is borrowed from packfile and only valid inside this
		// callback; clone so the returned bytes outlive ReadItem.
		out = bytes.Clone(b)
		return nil
	})
	if rerr != nil {
		return nil, translateReaderErr(rerr)
	}
	return out, nil
}

func (c *ColdStoreReader) IterateLedgers(start, end uint32) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		h, err := c.init()
		if err != nil {
			yield(Entry{}, err)
			return
		}
		// Short-circuit so post-clamp start <= end always holds;
		// otherwise (end - start) would underflow uint32 in the
		// count calc below.
		if start > end || end < h.firstSeq || start > h.lastSeq {
			return
		}
		if start < h.firstSeq {
			start = h.firstSeq
		}
		if end > h.lastSeq {
			end = h.lastSeq
		}
		startPos := int(start - h.firstSeq)
		count := int(end-start) + 1

		seq := start
		for item, err := range c.r.ReadRange(startPos, count) {
			if err != nil {
				yield(Entry{}, translateReaderErr(err))
				return
			}
			// item is borrowed from packfile and only valid until the
			// next iteration; clone so the caller can retain Entry.Bytes.
			if !yield(Entry{Seq: seq, Bytes: bytes.Clone(item)}, nil) {
				return
			}
			seq++
		}
	}
}

func (c *ColdStoreReader) Close() error { return c.r.Close() }

// translateWriterErr maps packfile-level lifecycle errors to the
// pkg/stores sentinels so callers depend only on stores.* errors.
func translateWriterErr(err error) error {
	if errors.Is(err, packfile.ErrWriterClosed) {
		return stores.ErrStoreClosed
	}
	return err
}

// translateReaderErr maps packfile- and os-level errors to the
// pkg/stores sentinels.
func translateReaderErr(err error) error {
	if errors.Is(err, os.ErrClosed) {
		return stores.ErrStoreClosed
	}
	if errors.Is(err, packfile.ErrCorrupt) {
		return fmt.Errorf("%w: %w", stores.ErrCorrupt, err)
	}
	return err
}
