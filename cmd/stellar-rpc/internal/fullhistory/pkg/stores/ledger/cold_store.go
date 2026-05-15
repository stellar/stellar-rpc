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

type ColdStore struct {
	reader   *packfile.Reader
	decoder  *zstd.Decompressor
	firstSeq uint32
	lastSeq  uint32
	closed   atomic.Bool
}

// OpenColdStore takes a caller-owned decoder, typically a single
// *zstd.Decompressor shared across all ColdStores in the process.
// ColdStore.Close does not touch it.
func OpenColdStore(
	path string,
	decoder *zstd.Decompressor,
	logger *supportlog.Entry,
) (*ColdStore, error) {
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
	return &ColdStore{
		reader:   r,
		decoder:  decoder,
		firstSeq: firstSeq,
		lastSeq:  lastSeq,
	}, nil
}

func (c *ColdStore) FirstSeq() uint32 { return c.firstSeq }
func (c *ColdStore) LastSeq() uint32  { return c.lastSeq }

// GetLedgerRaw returns stores.ErrNotFound if seq is outside
// [FirstSeq, LastSeq].
func (c *ColdStore) GetLedgerRaw(seq uint32) ([]byte, error) {
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
func (c *ColdStore) IterateLedgers(start, end uint32) iter.Seq2[Entry, error] {
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
func (c *ColdStore) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.reader.Close()
}
