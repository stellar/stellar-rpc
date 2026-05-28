package ledger

import (
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

// formatLedgerCold tags the packfile format used by the cold ledger
// store. Shared by the reader and the writer (same package).
const formatLedgerCold packfile.Format = 1

// appDataSize — firstSeq (4 BE). lastSeq is derived from
// trailer.TotalItems at open. Shared by the reader and the writer
// (same package).
const appDataSize = 4

// coldPackDecoder is the process-wide zstd decoder for cold ledger
// pack records. packfile.RecordDecoder must be concurrent-safe and
// zstd.Decompressor satisfies that, so a single shared instance
// serves every ColdReader. Mirrors eventstore's pattern.
//
//nolint:gochecknoglobals // shared by design; the decoder is stateless + concurrent-safe
var coldPackDecoder = zstd.NewDecompressor()

// ColdReader is lazy: OpenColdReader does no synchronous I/O and
// returns no error. packfile.Open begins the open in a background
// goroutine immediately; the trailer + AppData are read and validated
// on the first method call, via a sync.OnceValues-cached loadHeader,
// where a failed open also surfaces. Read methods (FirstSeq, LastSeq,
// GetLedgerRaw, IterateLedgers) are safe for concurrent use; Close
// is NOT — callers must ensure all in-flight reads have returned
// before invoking it, matching the underlying packfile.Reader.Close
// contract.
type ColdReader struct {
	r    *packfile.Reader
	path string
	init func() (coldHeader, error)
}

// coldHeader carries the validated firstSeq / lastSeq returned by
// loadHeader and cached by sync.OnceValues.
type coldHeader struct {
	firstSeq, lastSeq uint32
}

// OpenColdReader returns a lazy reader for the cold pack at path.
// It does no synchronous I/O and returns no error for a valid path;
// packfile.Open starts the open in the background immediately, and
// trailer + AppData read/validation (plus any open failure) surface
// on the first method call. Uses the package-level coldPackDecoder,
// shared across all readers in the process.
func OpenColdReader(path string) (*ColdReader, error) {
	if path == "" {
		return nil, stores.ErrInvalidConfig
	}
	c := &ColdReader{
		r:    packfile.Open(path, packfile.ReaderOptions{RecordDecoder: coldPackDecoder}),
		path: path,
	}
	c.init = sync.OnceValues(c.loadHeader)
	return c, nil
}

// loadHeader reads the trailer + AppData, enforces format, AppData
// layout, and uint32 overflow on the derived lastSeq. Cached by
// sync.OnceValues; runs at most once per reader.
//
//nolint:funcorder // grouped near init/Open call site for readability; the exported reader API follows
func (c *ColdReader) loadHeader() (coldHeader, error) {
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
		return coldHeader{}, fmt.Errorf(
			"cold %q: lastSeq overflows uint32 (firstSeq=%d, items=%d)",
			c.path, first, tr.TotalItems)
	}
	return coldHeader{firstSeq: first, lastSeq: first + tr.TotalItems - 1}, nil
}

func (c *ColdReader) FirstSeq() (uint32, error) { h, err := c.init(); return h.firstSeq, err }
func (c *ColdReader) LastSeq() (uint32, error)  { h, err := c.init(); return h.lastSeq, err }

// GetLedgerRaw reads the raw LedgerCloseMeta bytes for seq into a fresh,
// caller-owned buffer. Sequential bulk readers should prefer IterateLedgers,
// which yields borrows without the per-ledger copy.
func (c *ColdReader) GetLedgerRaw(seq uint32) ([]byte, error) {
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
		// b is borrowed from packfile (valid only inside this callback);
		// copy so the returned bytes are owned by the caller.
		out = append(out, b...)
		return nil
	})
	if rerr != nil {
		return nil, translateReaderErr(rerr)
	}
	return out, nil
}

func (c *ColdReader) IterateLedgers(start, end uint32) iter.Seq2[Entry, error] {
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
			// Entry.Bytes is BORROWED from packfile and valid only until the
			// next iteration step — copy it if you need to retain it past the
			// loop body. Callers that consume each ledger in-scope (the ingest
			// and read benches) avoid a per-ledger clone this way.
			if !yield(Entry{Seq: seq, Bytes: item}, nil) {
				return
			}
			seq++
		}
	}
}

func (c *ColdReader) Close() error { return c.r.Close() }

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
