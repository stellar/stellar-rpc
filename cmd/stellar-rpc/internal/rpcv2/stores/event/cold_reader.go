package event

// cold_reader.go is the read side of a frozen Chunk. It opens the
// three cold artifacts produced by ColdWriter + WriteColdIndex
// (events.pack, index.pack, index.hash), decodes the embedded
// events.LedgerOffsets app-data block, and serves the events.Reader
// interface against them.
//
// Lifecycle: each ColdReader owns two packfile.Reader instances
// plus an in-memory parsed MPHF index. Open returns immediately,
// but all three I/O units start right away in background
// goroutines: packfile.Open opens each file (holding its fd) and
// reads its trailer in the background, and the MPHF read kicks off
// alongside them. Decoded metadata is awaited on the first call
// that needs it. Close drains the MPHF goroutine and releases the
// packfile handles. Multiple ColdReaders can be open against the
// same chunk directory concurrently — packfile.Reader is safe for
// concurrent reads and the MPHF is read-only after load.
//
// Concurrency contract: read methods (LookupKeys,
// FetchEvents, All) are safe to call concurrently with each other
// on the same ColdReader. They are NOT safe to call concurrently
// with Close — the caller is responsible for draining all in-flight
// reads (including consuming any FetchEvents/All iterators to
// completion) before calling Close. The post-Close atomic guard
// catches calls that begin after Close returns, but it cannot
// rescue a read already past its entry check when Close starts
// tearing down the underlying handles.
//
// Close semantics by method:
//
//   - LookupKeys, FetchEvents, All, EventCount, Offsets:
//     return / yield stores.ErrStoreClosed after Close.
//   - ChunkID: is the constructor-supplied chunk ID; never reads
//     from disk and is unaffected by Close. Callers can use it for
//     logging, metrics, or error context after closing the reader.
//
// Caching, pooling, or per-query lifecycle policy is the consumer's
// problem (e.g., the future chunk router in PR-3c). ColdReader is a
// primitive: New (well, Open) and Close.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// ColdReader is the read side of a frozen Chunk. Implements
// events.Reader.
//
// Open shape: OpenColdReader does no synchronous I/O beyond options
// validation. packfile.Open starts each file's open + trailer read
// in a background goroutine immediately; events.pack metadata
// (TotalItems + AppData + offsets decode + chunkID cross-check) is
// decoded on first metadata access via a sync.OnceValues-cached
// loader. The MPHF kicks off in a background goroutine at Open and
// is awaited on the first LookupKeys call via a second
// sync.OnceValues. This makes opening N chunks for a query
// non-blocking — each reader returns immediately and the three I/O
// units fan out concurrently across all opened readers.
type ColdReader struct {
	chunkID chunk.ID

	events *packfile.Reader // opened in the background by packfile.Open; reads await it
	index  *packfile.Reader // opened in the background by packfile.Open; reads await it

	// waitMeta returns the events.pack metadata (count + offsets),
	// decoded on first call from the events.pack trailer + AppData.
	// Cached via sync.OnceValues.
	waitMeta func() (coldMeta, error)

	// waitMPHF returns the MPHF loaded by a background goroutine
	// started in OpenColdReader — the handle only, no cross-artifact
	// validation, so Close always gets the real handle to release
	// (a validation failure can never cost it the Close).
	waitMPHF func() (*mphf, error)

	// validateMPHF is the error-only gate over waitMPHF: the
	// load-time cross-checks that bind the index pair to this chunk.
	// The lookup path runs it before using the handle; skipping it on
	// Close also spares an eventless chunk's teardown the events.pack
	// metadata I/O. Both are sync.Once*-cached.
	validateMPHF func() error

	closed atomic.Bool
}

// coldMeta carries the validated events.pack metadata returned by
// the deferred loader cached behind waitMeta.
type coldMeta struct {
	count   uint32
	offsets *events.LedgerOffsets
}

// Compile-time guard.
var _ Reader = (*ColdReader)(nil)

// ColdReaderOptions configures OpenColdReader.
type ColdReaderOptions struct {
	// Concurrency is forwarded to packfile.ReaderOptions.Concurrency
	// for both events.pack and index.pack. The zero value is
	// normalized by the packfile layer to 1 (serial coalesced reads);
	// callers who want ReadItems to fan out across goroutines must
	// set this explicitly to a value > 1. Negative values are
	// rejected by the packfile reader at first use.
	Concurrency int
}

// OpenColdReader prepares a ColdReader for chunkID inside bucketDir.
// It does no synchronous I/O — packfile.Open starts each file's open
// in a background goroutine (holding its fd once open), the
// events.pack metadata decode is sync.OnceValues-deferred, and the
// MPHF loader runs in a background goroutine awaited via
// sync.OnceValues on first LookupKeys. Validation errors that depend
// on file contents (chunkID cross-check, format, AppData layout,
// MPHF parse) surface from the first method that needs the data, not
// from Open itself.
//
// bucketDir is the orchestrator-supplied bucket directory
// ({events_root}/{bucketID:05d}/); this reader does not compose it.
// chunkID drives both error messages and the per-chunk filename
// composition (see EventsPackName / IndexPackName / IndexHashName).
func OpenColdReader(chunkID chunk.ID, bucketDir string, opts ColdReaderOptions) (*ColdReader, error) {
	if opts.Concurrency < 0 {
		return nil, fmt.Errorf("events: ColdReaderOptions.Concurrency must be >= 0, got %d", opts.Concurrency)
	}

	eventsPath := filepath.Join(bucketDir, EventsPackName(chunkID))
	indexPackPath := filepath.Join(bucketDir, IndexPackName(chunkID))
	indexHashPath := filepath.Join(bucketDir, IndexHashName(chunkID))

	c := &ColdReader{
		chunkID: chunkID,
		events: packfile.Open(eventsPath, packfile.ReaderOptions{
			RecordDecoder: eventsPackDecoder,
			Concurrency:   opts.Concurrency,
		}),
		index: packfile.Open(indexPackPath, packfile.ReaderOptions{
			Concurrency: opts.Concurrency,
		}),
	}

	// Spawn the MPHF load in the background so other Opens (and
	// the caller's query-prep CPU work) overlap the I/O.
	type mphfResult struct {
		idx *mphf
		err error
	}
	ch := make(chan mphfResult, 1)
	go func() {
		// openMPHF already wraps with the path on error — pass
		// through without re-wrapping to avoid "events: read X:
		// events: open X: ..." double prefixes.
		m, err := openMPHF(indexHashPath)
		ch <- mphfResult{idx: m, err: err}
	}()
	c.waitMPHF = sync.OnceValues(func() (*mphf, error) {
		res := <-ch
		return res.idx, res.err
	})
	c.validateMPHF = sync.OnceValue(func() error {
		idx, err := c.waitMPHF()
		if err != nil {
			return err
		}
		if idx.isEmpty() {
			// A zero-term index is only valid for an eventless chunk: cross-check
			// events.pack's count so a mispaired empty index fails loudly instead
			// of silently matching nothing.
			m, merr := c.waitMeta()
			if merr != nil {
				return fmt.Errorf("events: validate empty index for chunk %s: %w", c.chunkID, merr)
			}
			if m.count != 0 {
				return fmt.Errorf(
					"events: %s holds zero terms but events.pack holds %d events for chunk %s (torn or mispaired index)",
					indexHashPath, m.count, c.chunkID)
			}
			return nil
		}
		// Non-empty index: bind the pair to this chunk before serving from
		// it — index.pack/index.hash carry no chunk ID of their own, so a
		// mispaired index would silently return an incomplete subset of
		// matches. Three cheap checks: index.pack's trailer Format,
		// index.hash keys == index.pack records (halves of one build), and
		// non-empty index ⇒ non-empty events.pack (converse of the
		// empty-index check above).
		tr, terr := c.index.Trailer()
		if terr != nil {
			return fmt.Errorf("events: open %s: %w", indexPackPath, terr)
		}
		if tr.Format != indexPackFormat {
			return fmt.Errorf("events: %s: expected format %#x, got %#x (mis-pointed or foreign pack)",
				indexPackPath, indexPackFormat, tr.Format)
		}
		if uint64(tr.TotalItems) != idx.numKeys() {
			return fmt.Errorf(
				"events: index pair mismatch for chunk %s: index.hash holds %d keys "+
					"but index.pack holds %d records (mispaired artifacts)",
				c.chunkID, idx.numKeys(), tr.TotalItems)
		}
		m, merr := c.waitMeta()
		if merr != nil {
			return fmt.Errorf("events: validate index for chunk %s: %w", c.chunkID, merr)
		}
		if m.count == 0 {
			return fmt.Errorf(
				"events: %s holds %d terms but events.pack is eventless for chunk %s (mispaired index)",
				indexHashPath, idx.numKeys(), c.chunkID)
		}
		return nil
	})

	// events.pack metadata loader — runs on first call to
	// EventCount / Offsets / FetchEvents / All.
	c.waitMeta = sync.OnceValues(func() (coldMeta, error) {
		return c.loadMeta(eventsPath)
	})

	return c, nil
}

// Close releases all underlying file handles. Idempotent. Drains
// the MPHF background goroutine before tearing down so an
// in-flight load doesn't write to a half-closed handle.
//
// Must not be called concurrently with LookupKeys, FetchEvents, or
// All on the same ColdReader. See the type-level concurrency
// contract for the rationale.
func (c *ColdReader) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	// Drain the MPHF goroutine before tearing down. Its result may
	// be (nil, err) if the load failed — in either case the
	// goroutine has exited and the channel send has happened.
	// waitMPHF is validation-free, so this always gets the real
	// handle to release, and skipping validateMPHF spares an
	// eventless chunk's teardown the events.pack metadata I/O.
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
	if err := c.events.Close(); err != nil && first == nil {
		first = fmt.Errorf("events: close events.pack: %w", err)
	}
	return first
}

// ChunkID returns the chunk this reader serves. Set at Open from
// the caller-supplied parameter; infallible and survives Close.
func (c *ColdReader) ChunkID() chunk.ID { return c.chunkID }

// EventCount is the total number of events in this Chunk. The
// underlying value is read from events.pack's trailer on first
// metadata access (lazy); subsequent calls return the cached
// value. Returns (0, stores.ErrStoreClosed) after Close.
func (c *ColdReader) EventCount() (uint32, error) {
	if c.closed.Load() {
		return 0, stores.ErrStoreClosed
	}
	m, err := c.waitMeta()
	if err != nil {
		return 0, err
	}
	return m.count, nil
}

// Offsets returns the in-memory ledger-offset cache decoded from
// events.pack's app data on first metadata access. The coordinator
// uses this to stitch a multi-ledger query range into
// chunk-relative event-id ranges (see Reader.Offsets).
//
// Returns (nil, stores.ErrStoreClosed) after Close. Callers must treat the
// returned value as read-only — mutations would corrupt every
// other reader holding the same cached snapshot.
func (c *ColdReader) Offsets() (*events.LedgerOffsets, error) {
	if c.closed.Load() {
		return nil, stores.ErrStoreClosed
	}
	m, err := c.waitMeta()
	if err != nil {
		return nil, err
	}
	return m.offsets, nil
}

// verifyAndDeserializeBitmap checks the index.pack record's leading
// fingerprint against key's prefix and, on match, unmarshals a fresh
// bitmap. On fingerprint mismatch (residual MPHF collision on an
// unseen key) it returns (nil, nil) — the caller treats nil as
// not-found. record is valid only inside ReadItem's callback;
// UnmarshalBinary copies into roaring's internal state so the
// returned bitmap outlives the callback safely.
func verifyAndDeserializeBitmap(record []byte, key events.TermKey, slot uint32) (*roaring.Bitmap, error) {
	if len(record) < IndexRecordFingerprintLen {
		return nil, fmt.Errorf("events: index.pack record at slot %d truncated (%d bytes)", slot, len(record))
	}
	if !bytes.Equal(record[:IndexRecordFingerprintLen], key[:IndexRecordFingerprintLen]) {
		return nil, nil //nolint:nilnil // not-found signaled by nil bitmap, no error
	}
	bm := roaring.New()
	if err := bm.UnmarshalBinary(record[IndexRecordFingerprintLen:]); err != nil {
		return nil, fmt.Errorf("events: unmarshal bitmap at slot %d: %w", slot, err)
	}
	return bm, nil
}

// LookupKeys returns bitmaps for each key, aligned positionally with
// the input slice (result[i] corresponds to keys[i]). See
// Reader.LookupKeys for the semantics.
//
// Cold-side implementation:
//
//  1. MPHF-resolve every key. Keys rejected at the routing stage
//     (streamhash ErrKeyNotFound) get result[i] = nil and never
//     touch index.pack.
//  2. Sort the surviving (key, slot) pairs by slot and dedupe —
//     pathological residual collisions can map two distinct keys
//     to the same MPHF rank.
//  3. One c.index.ReadItems pass over the unique slot list. The
//     packfile reader coalesces adjacent slots into single ReadAt
//     calls and fans out across the worker count configured via
//     ColdReaderOptions.Concurrency.
//  4. In the callback, verify each pending key's fingerprint
//     against the record header and unmarshal a fresh bitmap per
//     match. Misses (fingerprint mismatch) leave result[i] = nil.
//
//nolint:cyclop // the four documented steps above, inline; splitting obscures the pass structure
func (c *ColdReader) LookupKeys(ctx context.Context, keys []events.TermKey) ([]*roaring.Bitmap, error) {
	if c.closed.Load() {
		return nil, stores.ErrStoreClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	if err := c.validateMPHF(); err != nil {
		return nil, err
	}
	mphf, err := c.waitMPHF()
	if err != nil {
		return nil, err
	}

	results := make([]*roaring.Bitmap, len(keys))

	type pendingKey struct {
		outIdx int
		slot   uint32
	}
	pending := make([]pendingKey, 0, len(keys))
	for i, key := range keys {
		slot, err := mphf.Lookup(key)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				continue // result[i] stays nil
			}
			return nil, fmt.Errorf("events: LookupKeys MPHF for chunk %s: %w", c.chunkID, err)
		}
		pending = append(pending, pendingKey{outIdx: i, slot: slot})
	}
	if len(pending) == 0 {
		return results, nil
	}

	sort.Slice(pending, func(i, j int) bool { return pending[i].slot < pending[j].slot })

	// Build the unique slots list. Multiple pending entries may share
	// a slot when an unbuilt key residually collides into the same
	// MPHF rank as a built one.
	positions := make([]int, 0, len(pending))
	pendingBySlot := make([][]int, 0, len(pending)) // pendingBySlot[readIdx] = indices into pending[]
	for k, p := range pending {
		if len(positions) > 0 && positions[len(positions)-1] == int(p.slot) {
			pendingBySlot[len(pendingBySlot)-1] = append(pendingBySlot[len(pendingBySlot)-1], k)
			continue
		}
		positions = append(positions, int(p.slot))
		pendingBySlot = append(pendingBySlot, []int{k})
	}

	if err := c.index.ReadItems(ctx, positions, func(readIdx int, record []byte) error {
		// Multiple pending keys may share this slot (residual MPHF
		// collision). verifyAndDeserializeBitmap returns a fresh
		// bitmap per match and (nil, nil) on fingerprint mismatch —
		// leaving results[outIdx] = nil for misses.
		for _, pIdx := range pendingBySlot[readIdx] {
			p := pending[pIdx]
			bm, err := verifyAndDeserializeBitmap(record, keys[p.outIdx], p.slot)
			if err != nil {
				return err
			}
			results[p.outIdx] = bm
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("events: LookupKeys read for chunk %s: %w", c.chunkID, err)
	}

	return results, nil
}

// FetchEvents decodes events_data records for the supplied
// chunk-relative eventIDs and returns them positionally aligned
// with the input slice. See Reader.FetchEvents for the sorted-input
// precondition.
//
// Implementation: validates eventIDs are sorted ascending with no
// duplicates (returns wrapped ErrUnsortedEventIDs otherwise), then
// delegates to packfile.ReadItems, which coalesces consecutive
// records into single ReadAt calls and optionally fans out across
// the worker count set via ColdReaderOptions.Concurrency.
// result[idx] writes from concurrent workers do not race — each
// idx is unique.
func (c *ColdReader) FetchEvents(ctx context.Context, eventIDs []uint32) ([]events.Payload, error) {
	if c.closed.Load() {
		return nil, stores.ErrStoreClosed
	}
	if len(eventIDs) == 0 {
		return nil, nil
	}
	if err := validateSortedEventIDs(eventIDs); err != nil {
		return nil, err
	}
	m, err := c.waitMeta()
	if err != nil {
		return nil, err
	}
	positions := make([]int, len(eventIDs))
	for i, id := range eventIDs {
		if id >= m.count {
			return nil, fmt.Errorf("events: eventID %d out of range for chunk %s (count=%d)",
				id, c.chunkID, m.count)
		}
		positions[i] = int(id)
	}
	results := make([]events.Payload, len(eventIDs))
	if err := c.events.ReadItems(ctx, positions, func(idx int, data []byte) error {
		// packfile.ReadItems passes a borrowed data slice valid only for
		// the duration of fn (see Reader.ReadItems docstring). FetchEvents
		// returns the Payloads in a slice that outlives fn, so clone before
		// Unmarshal aliases the bytes into ContractEventBytes.
		return results[idx].Unmarshal(bytes.Clone(data))
	}); err != nil {
		// packfile.ReadItems also validates sorted positions as defense in
		// depth; translate its sentinel to ours so callers can errors.Is
		// against ErrUnsortedEventIDs uniformly.
		if errors.Is(err, packfile.ErrPositionsUnsorted) {
			return nil, fmt.Errorf("%w: %w", ErrUnsortedEventIDs, err)
		}
		return nil, fmt.Errorf("events: fetch from chunk %s: %w", c.chunkID, err)
	}
	return results, nil
}

// FetchRange streams count events starting at chunk-relative event
// ID start, in ascending eventID order via events.pack.ReadRange.
// See Reader.FetchRange for semantics.
//
// Out-of-range arguments yield an error and stop. ctx is checked
// between yielded records — packfile.ReadRange itself doesn't
// accept a ctx, so a single very slow ReadAt could block past
// cancellation until the next yield, but the next iteration step
// will observe the cancel.
//
// Yielded Payloads are borrowed: ContractEventBytes aliases the
// ReadRange buffer and is valid only until the next step — clone to retain.
func (c *ColdReader) FetchRange(ctx context.Context, start, count uint32) iter.Seq2[events.Payload, error] {
	return func(yield func(events.Payload, error) bool) {
		if c.closed.Load() {
			yield(events.Payload{}, stores.ErrStoreClosed)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(events.Payload{}, err)
			return
		}
		if count == 0 {
			return
		}
		m, err := c.waitMeta()
		if err != nil {
			yield(events.Payload{}, err)
			return
		}
		if err := validateFetchRange(start, count, m.count, c.chunkID); err != nil {
			yield(events.Payload{}, err)
			return
		}
		// ReadRange yields raw item bytes in position order; we
		// decode each on the fly.
		for raw, err := range c.events.ReadRange(int(start), int(count)) {
			if err != nil {
				yield(events.Payload{}, fmt.Errorf("events: scan chunk %s: %w", c.chunkID, err))
				return
			}
			if err := ctx.Err(); err != nil {
				yield(events.Payload{}, err)
				return
			}
			var p events.Payload
			// raw is valid only until the next ReadRange step (see
			// Reader.ReadRange); Unmarshal aliases it into
			// ContractEventBytes, so the yielded Payload is borrowed (see
			// the FetchRange doc). A retaining consumer clones.
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

// All streams every event in this Chunk in chunk-relative eventID
// order. Thin wrapper over FetchRange; its yielded Payloads are
// likewise borrowed (valid only for the step). The up-front closed
// check short-circuits to stores.ErrStoreClosed without spinning up the cached
// waitMeta + descending into FetchRange (which would also detect
// the closed state, just one indirection later).
func (c *ColdReader) All(ctx context.Context) iter.Seq2[events.Payload, error] {
	return func(yield func(events.Payload, error) bool) {
		if c.closed.Load() {
			yield(events.Payload{}, stores.ErrStoreClosed)
			return
		}
		m, err := c.waitMeta()
		if err != nil {
			yield(events.Payload{}, err)
			return
		}
		for p, err := range c.FetchRange(ctx, 0, m.count) {
			if !yield(p, err) {
				return
			}
		}
	}
}

// loadMeta drives the events.pack open via TotalItems, reads
// AppData, decodes offsets, and cross-checks the chunkID. Called
// at most once per reader (sync.OnceValues guards). Placed at the
// end of the file (after the exported methods) to satisfy funcorder.
func (c *ColdReader) loadMeta(eventsPath string) (coldMeta, error) {
	tr, err := c.events.Trailer()
	if err != nil {
		return coldMeta{}, fmt.Errorf("events: open %s: %w", eventsPath, err)
	}
	// Check the trailer's Format before touching any record: a
	// mis-pointed pack fails at open, not mid-query with an opaque
	// zstd error (the ledger store does the same).
	if tr.Format != eventsPackFormat {
		return coldMeta{}, fmt.Errorf("events: %s: expected format %#x, got %#x (mis-pointed or foreign pack)",
			eventsPath, eventsPackFormat, tr.Format)
	}
	total := tr.TotalItems
	appData, err := c.events.AppData()
	if err != nil {
		return coldMeta{}, fmt.Errorf("events: read app data from %s: %w", eventsPath, err)
	}
	offsets, err := DecodeLedgerOffsets(appData)
	if err != nil {
		return coldMeta{}, fmt.Errorf("events: decode offsets from %s: %w", eventsPath, err)
	}
	// Cross-check that the file's contents agree with the chunkID
	// composed into its path. A mismatch means the orchestrator
	// misrouted the file (replication bug, partial filesystem op,
	// bucket-rename gone wrong) — without this guard we'd silently
	// serve another chunk's data under this chunk's identity.
	if got := chunk.IDFromLedger(offsets.StartLedger()); got != c.chunkID {
		return coldMeta{}, fmt.Errorf("events: chunk-ID mismatch in %s: path says %s, contents start at ledger %d (chunk %s)",
			eventsPath, c.chunkID, offsets.StartLedger(), got)
	}
	// The offsets blob's cumulative total must equal the pack's item
	// count — a mispaired blob (right chunk ID, wrong build) silently
	// clips tail events off every per-ledger range.
	if offsets.TotalEvents() != total {
		return coldMeta{}, fmt.Errorf(
			"events: %s: offsets blob sums to %d events but the pack holds %d (mispaired offsets)",
			eventsPath, offsets.TotalEvents(), total)
	}
	return coldMeta{count: total, offsets: offsets}, nil
}
