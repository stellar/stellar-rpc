package event

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// ColdIndexReader serves term lookups from an index.pack + index.hash pair
// written by WriteColdIndexFiles, with no events.pack beside it. It is for
// index families that reuse the events index format to map a term to a bitmap
// of ids of their own (the ledger entry change index maps a ledger key to the
// ledgers that changed it).
//
// Without an events.pack, the pair is checked only against itself (format,
// record checksum, build stamp, and that index.hash and index.pack agree on
// the key count), not against a payload count.
//
// Read methods are safe to call concurrently with each other, not with Close.
type ColdIndexReader struct {
	path       string
	index      *stores.PackReader
	waitMPHF   func() (*mphf, error)
	waitLayout func() (indexLayout, error)
	closed     atomic.Bool
}

// OpenColdIndexReader prepares a reader over the index pair at packPath and
// hashPath. Like OpenColdReader it does no synchronous I/O: errors that depend
// on file contents surface from the first LookupKeys.
func OpenColdIndexReader(packPath, hashPath string, opts ColdReaderOptions) (*ColdIndexReader, error) {
	if opts.Concurrency < 0 {
		return nil, fmt.Errorf("events: ColdReaderOptions.Concurrency must be >= 0, got %d", opts.Concurrency)
	}
	c := &ColdIndexReader{
		path: packPath,
		index: stores.OpenPack(packPath, packfile.ReaderOptions{
			Concurrency: opts.Concurrency,
			FirstRead:   indexPackFirstRead,
		}),
	}
	type mphfResult struct {
		idx *mphf
		err error
	}
	ch := make(chan mphfResult, 1)
	go func() {
		m, err := openMPHF(hashPath)
		ch <- mphfResult{idx: m, err: err}
	}()
	c.waitMPHF = sync.OnceValues(func() (*mphf, error) {
		res := <-ch
		return res.idx, res.err
	})
	c.waitLayout = sync.OnceValues(func() (indexLayout, error) {
		idx, err := c.waitMPHF()
		if err != nil {
			return indexLayout{}, err
		}
		tr, err := c.index.Trailer()
		if err != nil {
			return indexLayout{}, fmt.Errorf("events: open %s: %w", packPath, err)
		}
		if tr.Format != indexPackFormat {
			return indexLayout{}, fmt.Errorf("events: %s: expected format %#x, got %#x (mis-pointed or foreign pack)",
				packPath, indexPackFormat, tr.Format)
		}
		if !tr.HasRecordChecksum {
			return indexLayout{}, fmt.Errorf("%w: %s: built without a record checksum (stale build)",
				stores.ErrCorrupt, packPath)
		}
		layout, err := loadIndexLayout(packPath, c.index)
		if err != nil {
			return indexLayout{}, err
		}
		if !idx.isEmpty() {
			if err := layout.check(packPath, idx.numKeys(), tr.TotalItems); err != nil {
				return indexLayout{}, err
			}
		}
		return layout, nil
	})
	return c, nil
}

// LookupKeys returns each key's whole bitmap, aligned with keys, nil for a
// key the index does not hold.
func (c *ColdIndexReader) LookupKeys(ctx context.Context, keys []TermKey) ([]*roaring.Bitmap, error) {
	if c.closed.Load() {
		return nil, stores.ErrStoreClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	layout, err := c.waitLayout()
	if err != nil {
		return nil, err
	}
	m, err := c.waitMPHF()
	if err != nil {
		return nil, err
	}
	bitmaps, _, err := lookupIndexKeys(ctx, c.path, c.index, m, layout, keys, IDRange{End: math.MaxUint32})
	return bitmaps, err
}

// Close releases the file handles. Idempotent.
func (c *ColdIndexReader) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	m, _ := c.waitMPHF()
	var first error
	if m != nil {
		if err := m.Close(); err != nil {
			first = fmt.Errorf("events: close index.hash: %w", err)
		}
	}
	if err := c.index.Close(); err != nil && first == nil {
		first = fmt.Errorf("events: close index.pack: %w", err)
	}
	return first
}
