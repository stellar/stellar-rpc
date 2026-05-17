package ledger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

const formatLedgerCold packfile.Format = 1

const appDataSize = 4

// ColdStoreWriter is two-phase: Commit finalizes; Close cleans up
// a partial pack when Commit hasn't run. Idiomatic use:
//
//	w, _ := NewColdStoreWriter(path, firstSeq, log)
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
	logger   *supportlog.Entry
	path     string
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
		ItemsPerRecord:   1,
		Format:           formatLedgerCold,
		Overwrite:        true,
		NewRecordEncoder: func() packfile.RecordEncoder { return zstd.NewCompressor() },
	})
	if err != nil {
		return nil, fmt.Errorf("cold: create packfile %q: %w", path, err)
	}
	logger.Infof("cold writer opened: path=%q firstSeq=%d", path, firstSeq)
	return &ColdStoreWriter{
		pw:       pw,
		firstSeq: firstSeq,
		nextSeq:  firstSeq,
		logger:   logger,
		path:     path,
	}, nil
}

func (w *ColdStoreWriter) AppendLedger(seq uint32, ledgerBytes []byte) error {
	if seq != w.nextSeq {
		return fmt.Errorf("cold: expected seq %d, got %d", w.nextSeq, seq)
	}
	if err := w.pw.AppendItem(ledgerBytes); err != nil {
		return err
	}
	w.nextSeq++
	return nil
}

func (w *ColdStoreWriter) Commit() error {
	var ad [appDataSize]byte
	binary.BigEndian.PutUint32(ad[:], w.firstSeq)
	if err := w.pw.Finish(ad[:]); err != nil {
		return err
	}
	w.logger.Infof("cold writer committed: path=%q firstSeq=%d count=%d", w.path, w.firstSeq, w.nextSeq-w.firstSeq)
	return nil
}

func (w *ColdStoreWriter) Close() error { return w.pw.Close() }

type ColdStoreReader struct {
	reader   *packfile.Reader
	firstSeq uint32
	lastSeq  uint32
	logger   *supportlog.Entry
	path     string
}

// NewColdStoreReader takes a caller-owned decoder, typically a
// single *zstd.Decompressor shared across all readers in the
// process. ColdStoreReader.Close does not touch it.
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
	r := packfile.Open(path, packfile.ReaderOptions{
		RecordDecoder: decoder,
	})
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
	logger.Debugf("cold reader opened: path=%q firstSeq=%d lastSeq=%d", path, firstSeq, lastSeq)
	return &ColdStoreReader{
		reader:   r,
		firstSeq: firstSeq,
		lastSeq:  lastSeq,
		logger:   logger,
		path:     path,
	}, nil
}

func (c *ColdStoreReader) FirstSeq() uint32 { return c.firstSeq }
func (c *ColdStoreReader) LastSeq() uint32  { return c.lastSeq }

func (c *ColdStoreReader) GetLedgerRaw(seq uint32) ([]byte, error) {
	if seq < c.firstSeq || seq > c.lastSeq {
		return nil, stores.ErrNotFound
	}
	pos := int(seq - c.firstSeq)
	var out []byte
	err := c.reader.ReadItem(pos, func(b []byte) error {
		// b is borrowed from packfile and only valid inside this
		// callback; clone so the returned bytes outlive ReadItem.
		out = bytes.Clone(b)
		return nil
	})
	return out, err
}

func (c *ColdStoreReader) IterateLedgers(start, end uint32) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		// Short-circuit before subtracting below to avoid uint32
		// underflow when the caller passes start < firstSeq.
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
			// item is borrowed from packfile and only valid until the
			// next iteration; clone so the caller can retain Entry.Bytes.
			if !yield(Entry{Seq: seq, Bytes: bytes.Clone(item)}, nil) {
				return
			}
			seq++
		}
	}
}

func (c *ColdStoreReader) Close() error { return c.reader.Close() }
