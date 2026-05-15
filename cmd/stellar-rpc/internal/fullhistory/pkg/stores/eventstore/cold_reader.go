package eventstore

// cold_reader.go is the read side of a frozen Chunk. It opens the
// three cold artifacts produced by ColdWriter + WriteColdIndex
// (events.pack, index.pack, index.hash), decodes the embedded
// events.LedgerOffsets app-data block, and serves the events.Reader
// interface against them.
//
// Lifecycle: each ColdReader owns its file handles (two
// packfile.Reader instances + one MPHF mmap). Close releases them.
// Multiple ColdReaders can be open against the same chunk
// directory concurrently — packfile.Reader is safe for concurrent
// reads and the MPHF is read-only after Open.
//
// Concurrency contract: read methods (Lookup, FetchEvents, All) are
// safe to call concurrently with each other on the same ColdReader.
// They are NOT safe to call concurrently with Close — the caller is
// responsible for draining all in-flight reads (including consuming
// any FetchEvents/All iterators to completion) before calling Close.
// The post-Close atomic guard catches Lookup/Fetch calls that begin
// after Close returns, but it cannot rescue a read already past its
// entry check when Close starts tearing down the underlying handles.
//
// Close semantics by method:
//
//   - Lookup, FetchEvents, All: return / yield ErrClosed after Close.
//   - ChunkID, EventCount, Offsets: are populated at Open from the
//     packfile trailer and survive Close. They remain valid for the
//     lifetime of the Go value, so callers can use them for logging,
//     metrics, or error context after closing the reader.
//
// Caching, pooling, or per-query lifecycle policy is the consumer's
// problem (e.g., the future chunk router in PR-3c). ColdReader is a
// primitive: New (well, Open) and Close.

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"math"
	"path/filepath"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
)

// ColdReader is the read side of a frozen Chunk. Implements
// events.Reader.
type ColdReader struct {
	chunkID chunk.ID
	dir     string

	events  *packfile.Reader      // events.pack
	index   *packfile.Reader      // index.pack
	mphf    *mphf                 // index.hash
	offsets *events.LedgerOffsets // decoded from events.AppData()
	count   uint32                // events.TotalItems(), cached

	closed atomic.Bool
}

// Compile-time guard.
var _ Reader = (*ColdReader)(nil)

// OpenColdReader opens the three cold artifacts for chunkID inside
// bucketDir ({chunkID:08d}-events.pack, {chunkID:08d}-index.pack,
// {chunkID:08d}-index.hash) and returns a ready-to-use reader. On
// any open failure the partially-opened resources are released
// before returning the error.
//
// bucketDir is the orchestrator-supplied bucket directory
// ({events_root}/{bucketID:05d}/); this reader does not compose it.
// chunkID drives both error messages and the per-chunk filename
// composition (see EventsPackName / IndexPackName / IndexHashName).
// It is not validated against the on-disk files (no contents-vs-name
// binding); the orchestrator is trusted to lay out the bucket dir
// correctly.
//
//nolint:nonamedreturns // named return is needed so the deferred cleanup can observe the final err
func OpenColdReader(chunkID chunk.ID, bucketDir string) (cr *ColdReader, err error) {
	c := &ColdReader{
		chunkID: chunkID,
		dir:     bucketDir,
	}

	// Defer-close on any error path before we return successfully.
	// Cleanup error is intentionally dropped — the real failure is
	// already in err, and the caller can't act on a teardown failure.
	defer func() {
		if err != nil {
			_ = c.closeUnderlying()
		}
	}()

	eventsName := EventsPackName(chunkID)
	c.events = packfile.Open(filepath.Join(bucketDir, eventsName), packfile.ReaderOptions{
		RecordDecoder: eventsPackDecoder,
	})
	// First read on the packfile.Reader drives the synchronous open;
	// errors surface here rather than from packfile.Open itself.
	total, err := c.events.TotalItems()
	if err != nil {
		return nil, fmt.Errorf("events: open %s/%s: %w", bucketDir, eventsName, err)
	}
	if total < 0 || uint64(total) > math.MaxUint32 {
		return nil, fmt.Errorf("events: implausible item count %d in %s/%s", total, bucketDir, eventsName)
	}
	c.count = uint32(total)

	appData, err := c.events.AppData()
	if err != nil {
		return nil, fmt.Errorf("events: read app data from %s/%s: %w", bucketDir, eventsName, err)
	}
	offsets, err := DecodeLedgerOffsets(appData)
	if err != nil {
		return nil, fmt.Errorf("events: decode offsets from %s/%s: %w", bucketDir, eventsName, err)
	}

	// Cross-check that the file's contents agree with the chunkID
	// composed into its path. A mismatch means the orchestrator
	// misrouted the file (replication bug, partial filesystem op,
	// bucket-rename gone wrong) — without this guard we'd silently
	// serve another chunk's data under this chunk's identity.
	if got := chunk.IDFromLedger(offsets.StartLedger()); got != chunkID {
		return nil, fmt.Errorf("events: chunk-ID mismatch in %s: path says %s, contents start at ledger %d (chunk %s)",
			bucketDir, chunkID, offsets.StartLedger(), got)
	}
	c.offsets = offsets

	indexPackPath := filepath.Join(bucketDir, IndexPackName(chunkID))
	c.index = packfile.Open(indexPackPath, packfile.ReaderOptions{})
	if _, err := c.index.TotalItems(); err != nil {
		return nil, fmt.Errorf("events: open %s: %w", indexPackPath, err)
	}

	indexHashPath := filepath.Join(bucketDir, IndexHashName(chunkID))
	c.mphf, err = openMPHF(indexHashPath)
	if err != nil {
		return nil, fmt.Errorf("events: open %s: %w", indexHashPath, err)
	}

	return c, nil
}

// Close releases all underlying file handles. Idempotent.
//
// Must not be called concurrently with Lookup, FetchEvents, or All
// on the same ColdReader. See the type-level concurrency contract
// for the rationale and what the caller is responsible for.
func (c *ColdReader) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.closeUnderlying()
}

// closeUnderlying tears down each resource regardless of whether
// earlier closes fail, returning the first non-nil error. Called by
// Close (steady state) and from OpenColdReader's defer (failure
// path during construction).
//
//nolint:funcorder // kept next to Close (its only caller) for proximity
func (c *ColdReader) closeUnderlying() error {
	var first error
	if c.mphf != nil {
		if err := c.mphf.Close(); err != nil && first == nil {
			first = fmt.Errorf("events: close index.hash: %w", err)
		}
		c.mphf = nil
	}
	if c.index != nil {
		if err := c.index.Close(); err != nil && first == nil {
			first = fmt.Errorf("events: close index.pack: %w", err)
		}
		c.index = nil
	}
	if c.events != nil {
		if err := c.events.Close(); err != nil && first == nil {
			first = fmt.Errorf("events: close events.pack: %w", err)
		}
		c.events = nil
	}
	return first
}

// ChunkID returns the chunk this reader serves. Set at Open;
// survives Close.
func (c *ColdReader) ChunkID() chunk.ID { return c.chunkID }

// EventCount is the total number of events in this Chunk. Equal to
// events.pack's TotalItems(). Cached at Open; survives Close.
func (c *ColdReader) EventCount() uint32 { return c.count }

// Offsets returns the in-memory ledger-offset cache decoded from
// events.pack's app data at Open. The coordinator uses this to
// stitch a multi-ledger query range into chunk-relative event-id
// ranges (see Reader.Offsets).
//
// Survives Close. Callers must treat the returned value as
// read-only — mutations would corrupt every other reader.
func (c *ColdReader) Offsets() *events.LedgerOffsets { return c.offsets }

// Lookup returns the bitmap of event IDs matching key. Returns
// (nil, ErrTermNotFound) when:
//
//   - streamhash's routing-stage check proves key was not in the
//     build set (fast no-match), OR
//   - MPHF returns a slot but the index.pack 4-byte fingerprint at
//     that slot doesn't match key[:4] (residual collision).
//
// The returned bitmap is freshly unmarshalled — callers can mutate
// it freely.
func (c *ColdReader) Lookup(key events.TermKey) (*roaring.Bitmap, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}

	slot, err := c.mphf.Lookup(key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrTermNotFound
		}
		return nil, fmt.Errorf("events: MPHF lookup for chunk %s: %w", c.chunkID, err)
	}

	var bm *roaring.Bitmap
	// record is valid only inside this callback. UnmarshalBinary
	// below copies into roaring's internal state, so the bitmap
	// outlives the callback safely.
	err = c.index.ReadItem(int(slot), func(record []byte) error {
		if len(record) < IndexRecordFingerprintLen {
			return fmt.Errorf("events: index.pack record at slot %d truncated (%d bytes)", slot, len(record))
		}
		if !bytes.Equal(record[:IndexRecordFingerprintLen], key[:IndexRecordFingerprintLen]) {
			// Fingerprint mismatch — residual MPHF collision on an
			// unseen key. Signal not-found; the callback returns nil
			// so ReadItem doesn't surface a synthetic error.
			return nil
		}
		decoded := roaring.New()
		if err := decoded.UnmarshalBinary(record[IndexRecordFingerprintLen:]); err != nil {
			return fmt.Errorf("events: unmarshal bitmap at slot %d: %w", slot, err)
		}
		bm = decoded
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("events: read index.pack slot %d for chunk %s: %w", slot, c.chunkID, err)
	}
	if bm == nil {
		return nil, ErrTermNotFound
	}
	return bm, nil
}

// FetchEvents decodes events_data records for the supplied
// chunk-relative eventIDs, in iteration order. A missing eventID
// (out of range) surfaces as an error on the iteration step that
// requested it.
func (c *ColdReader) FetchEvents(eventIDs []uint32) iter.Seq2[events.Payload, error] {
	return func(yield func(events.Payload, error) bool) {
		if c.closed.Load() {
			yield(events.Payload{}, ErrClosed)
			return
		}
		for _, id := range eventIDs {
			if id >= c.count {
				yield(events.Payload{}, fmt.Errorf("events: eventID %d out of range for chunk %s (count=%d)",
					id, c.chunkID, c.count))
				return
			}
			var p events.Payload
			err := c.events.ReadItem(int(id), p.Unmarshal)
			if err != nil {
				yield(events.Payload{}, fmt.Errorf("events: fetch event %d from chunk %s: %w", id, c.chunkID, err))
				return
			}
			if !yield(p, nil) {
				return
			}
		}
	}
}

// All streams every event in this Chunk in chunk-relative eventID
// order via events.pack.ReadRange.
func (c *ColdReader) All() iter.Seq2[events.Payload, error] {
	return func(yield func(events.Payload, error) bool) {
		if c.closed.Load() {
			yield(events.Payload{}, ErrClosed)
			return
		}
		// ReadRange yields raw item bytes in position order; we
		// decode each on the fly. ReadRange(0, 0) is a natural no-op
		// for an empty chunk.
		for raw, err := range c.events.ReadRange(0, int(c.count)) {
			if err != nil {
				yield(events.Payload{}, fmt.Errorf("events: scan chunk %s: %w", c.chunkID, err))
				return
			}
			var p events.Payload
			if err := p.Unmarshal(raw); err != nil {
				yield(events.Payload{}, fmt.Errorf("events: decode event from chunk %s: %w", c.chunkID, err))
				return
			}
			if !yield(p, nil) {
				return
			}
		}
	}
}
