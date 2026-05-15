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

// ColdStore is the read side of a single cold-ledger packfile.
// firstSeq is recovered from the trailer's AppData; lastSeq is
// derived as firstSeq + TotalItems - 1.
// Sibling to HotStore — no federating interface; cross-pack reads
// live in a separate slice.
// The packfile is opened in passthrough mode (packfile sees only
// opaque compressed bytes); zstd decoding happens here so the
// codec stays a cold-layer concern.
type ColdStore struct {
	reader   *packfile.Reader
	decoder  *zstd.Decompressor
	firstSeq uint32
	lastSeq  uint32
	closed   atomic.Bool
}

// OpenColdStore opens path, parses the trailer (synchronously via
// TotalItems), and recovers firstSeq from the 4-byte big-endian
// AppData.
// decoder is shared across all ColdStores in the process (zstd
// decompressors are concurrent-safe and pool DCtxs internally);
// its lifecycle is the caller's — ColdStore.Close does not touch
// it.
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
	total, err := r.TotalItems()
	if err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("cold: open %q: %w", path, err)
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
	if total == 0 {
		_ = r.Close()
		return nil, fmt.Errorf("cold: pack %q contains no items", path)
	}
	firstSeq := binary.BigEndian.Uint32(ad)
	//nolint:gosec // packfile.TotalItems is non-negative; range bounded by uint32 trailer field
	lastSeq := firstSeq + uint32(total) - 1
	return &ColdStore{
		reader:   r,
		decoder:  decoder,
		firstSeq: firstSeq,
		lastSeq:  lastSeq,
	}, nil
}

func (c *ColdStore) FirstSeq() uint32 { return c.firstSeq }
func (c *ColdStore) LastSeq() uint32  { return c.lastSeq }

// GetLedgerRaw reads the (compressed) item at position
// seq-firstSeq from the packfile, zstd-decodes it, and returns the
// uncompressed bytes.
// Returns stores.ErrNotFound if seq is outside [FirstSeq, LastSeq].
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

// IterateLedgers walks (seq, decompressed-bytes) pairs in
// [start, end] inclusive, ascending.
// start > end yields no entries.
// A window entirely outside [FirstSeq, LastSeq] yields no entries;
// a partially-overlapping window is clamped.
// On a closed store, the iterator yields stores.ErrStoreClosed
// once and returns.
func (c *ColdStore) IterateLedgers(start, end uint32) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		if c.closed.Load() {
			yield(Entry{}, stores.ErrStoreClosed)
			return
		}
		// Empty-window short-circuits before any arithmetic so a
		// caller-passed start < firstSeq doesn't underflow when we
		// subtract below.
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

// Close releases the underlying packfile reader.
// Idempotent.
// Does not close the caller-owned decoder.
func (c *ColdStore) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.reader.Close()
}
