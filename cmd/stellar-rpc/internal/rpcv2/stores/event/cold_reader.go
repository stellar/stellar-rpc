package event

// cold_reader.go is the read side of a frozen Chunk. It opens the
// three cold artifacts produced by ColdWriter + WriteColdIndex
// (events.pack, index.pack, index.hash), decodes the embedded
// LedgerOffsets app-data block, and serves the Reader
// interface against them.
//
// Lifecycle: each ColdReader owns two stores.PackReader instances
// plus an in-memory parsed MPHF index. Open returns immediately,
// but all three I/O units start right away in background
// goroutines: OpenPack opens each file (holding its fd) and
// reads its trailer in the background, and the MPHF read kicks off
// alongside them. Decoded metadata is awaited on the first call
// that needs it. Close drains the MPHF goroutine and releases the
// packfile handles. Multiple ColdReaders can be open against the
// same chunk's files concurrently — the pack handle is safe for
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
// problem (the query ReadView owns cold readers today). ColdReader
// is a primitive: New (well, Open) and Close.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// ColdReader is the read side of a frozen Chunk. Implements
// Reader.
//
// Open shape: OpenColdReader does no synchronous I/O beyond options
// validation. OpenPack starts each file's open + trailer read
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

	events *stores.PackReader // opened in the background by OpenPack; reads await it
	index  *stores.PackReader // opened in the background by OpenPack; reads await it

	// waitMeta returns the events.pack metadata (count + offsets),
	// decoded on first call from the events.pack trailer + AppData.
	// Cached via sync.OnceValues.
	waitMeta func() (coldMeta, error)

	// waitMPHF returns the MPHF loaded by a background goroutine
	// started in OpenColdReader — the handle only, no cross-artifact
	// validation, so Close always gets the real handle to release
	// (a validation failure can never cost it the Close).
	waitMPHF func() (*mphf, error)

	// waitLayout returns index.pack's layout once the load-time
	// cross-checks that bind the index pair to this chunk have passed.
	// The lookup path awaits it before using the MPHF handle.
	waitLayout func() (indexLayout, error)

	closed atomic.Bool
}

// coldMeta carries the validated events.pack metadata returned by
// the deferred loader cached behind waitMeta.
type coldMeta struct {
	count   uint32
	offsets *LedgerOffsets
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

// OpenColdReader prepares a ColdReader for chunkID over dirs.
// It does no synchronous I/O — OpenPack starts each file's open
// in a background goroutine (holding its fd once open), the
// events.pack metadata decode is sync.OnceValues-deferred, and the
// MPHF loader runs in a background goroutine awaited via
// sync.OnceValues on first LookupKeys. Validation errors that depend
// on file contents (chunkID cross-check, format, AppData layout,
// MPHF parse) surface from the first method that needs the data, not
// from Open itself.
//
// dirs are the orchestrator-supplied bucket directories, one per events
// root (geometry.Layout.EventsColdDirs); this reader does not compose
// them. events.pack comes from dirs.Data, index.pack and index.hash from
// dirs.Index.
func OpenColdReader(chunkID chunk.ID, dirs ColdDirs, opts ColdReaderOptions) (*ColdReader, error) {
	if opts.Concurrency < 0 {
		return nil, fmt.Errorf("events: ColdReaderOptions.Concurrency must be >= 0, got %d", opts.Concurrency)
	}

	eventsPath := filepath.Join(dirs.Data, EventsPackName(chunkID))
	indexPackPath := filepath.Join(dirs.Index, IndexPackName(chunkID))
	indexHashPath := filepath.Join(dirs.Index, IndexHashName(chunkID))

	c := &ColdReader{
		chunkID: chunkID,
		events: stores.OpenPack(eventsPath, packfile.ReaderOptions{
			RecordDecoder: eventsPackDecoder,
			Concurrency:   opts.Concurrency,
		}),
		index: stores.OpenPack(indexPackPath, packfile.ReaderOptions{
			Concurrency: opts.Concurrency,
			FirstRead:   indexPackFirstRead,
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
	c.waitLayout = sync.OnceValues(func() (indexLayout, error) {
		idx, err := c.waitMPHF()
		if err != nil {
			return indexLayout{}, err
		}
		// Format, record-checksum, and build-stamp checks run for eventless
		// chunks too, so a foreign, unchecked, or mis-schemed pack refuses on the
		// first indexed lookup regardless of chunk content. The guarantee is
		// lookup-path-only by design: payload reads (FetchEvents, All) never
		// consult the index pair and stay valid against events.pack's own checks.
		tr, terr := c.index.Trailer()
		if terr != nil {
			return indexLayout{}, fmt.Errorf("events: open %s: %w", indexPackPath, terr)
		}
		if tr.Format != indexPackFormat {
			return indexLayout{}, fmt.Errorf("events: %s: expected format %#x, got %#x (mis-pointed or foreign pack)",
				indexPackPath, indexPackFormat, tr.Format)
		}
		// Serving an index whose bitmaps are unchecked is the silent-wrong-answer
		// case indexPackChecksum exists to prevent, and the reader can tell. It is
		// stores.ErrCorrupt for the same reason a failed checksum is: the artifact
		// cannot answer queries and has to be rebuilt.
		if !tr.HasRecordChecksum {
			return indexLayout{}, fmt.Errorf("%w: %s: built without a record checksum (stale build)",
				stores.ErrCorrupt, indexPackPath)
		}
		layout, err := loadIndexLayout(indexPackPath, c.index)
		if err != nil {
			return indexLayout{}, err
		}
		if idx.isEmpty() {
			// A zero-term index is only valid for an eventless chunk: cross-check
			// events.pack's count so a mispaired empty index fails loudly instead
			// of silently matching nothing.
			m, merr := c.waitMeta()
			if merr != nil {
				return indexLayout{}, fmt.Errorf("events: validate empty index for chunk %s: %w", c.chunkID, merr)
			}
			if m.count != 0 {
				return indexLayout{}, fmt.Errorf(
					"events: %s holds zero terms but events.pack holds %d events for chunk %s (torn or mispaired index)",
					indexHashPath, m.count, c.chunkID)
			}
			return layout, nil
		}
		// Non-empty index: bind the pair to this chunk before serving from
		// it — index.pack/index.hash carry no chunk ID of their own, so a
		// mispaired index would silently return an incomplete subset of
		// matches. Two cheap checks beyond the shared format, checksum, and
		// stamp gates above: the layout addresses exactly index.pack's
		// entries from index.hash's keys (halves of one build), and
		// non-empty index ⇒ non-empty events.pack (converse of the
		// empty-index check above).
		if err := layout.check(indexPackPath, idx.numKeys(), tr.TotalItems); err != nil {
			return indexLayout{}, err
		}
		m, merr := c.waitMeta()
		if merr != nil {
			return indexLayout{}, fmt.Errorf("events: validate index for chunk %s: %w", c.chunkID, merr)
		}
		if m.count == 0 {
			return indexLayout{}, fmt.Errorf(
				"events: %s holds %d terms but events.pack is eventless for chunk %s (mispaired index)",
				indexHashPath, idx.numKeys(), c.chunkID)
		}
		return layout, nil
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
	// handle to release, and skipping waitLayout spares an
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
// events.pack's app data on first metadata access, the query side's
// source for translating ledger bounds into event-id windows (see
// Reader.Offsets).
//
// Returns (nil, stores.ErrStoreClosed) after Close. Callers must treat the
// returned value as read-only — mutations would corrupt every
// other reader holding the same cached snapshot.
func (c *ColdReader) Offsets() (*LedgerOffsets, error) {
	if c.closed.Load() {
		return nil, stores.ErrStoreClosed
	}
	m, err := c.waitMeta()
	if err != nil {
		return nil, err
	}
	return m.offsets, nil
}

// entryRead is one index.pack entry a lookup reads. slab is -1 for a whole
// term's entry, which must lead with fp.
type entryRead struct {
	pos  int
	key  int
	slab int
	fp   [IndexRecordFingerprintLen]byte
	bm   *roaring.Bitmap
}

// decodeIndexEntry decodes the entry rd names. A slab entry carries no
// fingerprint, so being one container of slab rd.slab's ids is what shows it
// was read at the right position. UnmarshalBinary copies out of entry, so the
// bitmap outlives the ReadItems callback.
func decodeIndexEntry(entry []byte, rd entryRead) (*roaring.Bitmap, error) {
	if rd.slab < 0 {
		if len(entry) < IndexRecordFingerprintLen {
			return nil, fmt.Errorf("%w: events: index.pack entry %d truncated (%d bytes)",
				stores.ErrCorrupt, rd.pos, len(entry))
		}
		if !bytes.Equal(entry[:IndexRecordFingerprintLen], rd.fp[:]) {
			return nil, nil //nolint:nilnil // not-found signaled by nil bitmap, no error
		}
		entry = entry[IndexRecordFingerprintLen:]
	} else if len(entry) == 0 {
		return nil, nil //nolint:nilnil // an empty slab, nothing to decode
	}
	bm := roaring.New()
	if err := bm.UnmarshalBinary(entry); err != nil {
		return nil, fmt.Errorf("%w: events: unmarshal index.pack entry %d: %w", stores.ErrCorrupt, rd.pos, err)
	}
	// Stats before Minimum: UnmarshalBinary accepts a run container with no
	// runs, which Minimum would index.
	if rd.slab >= 0 {
		if st := bm.Stats(); st.Containers != 1 || st.Cardinality == 0 || int(bm.Minimum()>>indexSlabShift) != rd.slab {
			return nil, fmt.Errorf("%w: events: index.pack entry %d is not one container of slab %d's ids",
				stores.ErrCorrupt, rd.pos, rd.slab)
		}
	}
	return bm, nil
}

// LookupKeys returns bitmaps for each key, aligned positionally with
// the input slice (result[i] corresponds to keys[i]), and the id range
// they answer for. See Reader.LookupKeys for the semantics.
//
// A split term is read only in the slabs the window reaches. Every split
// term shares the chunk's slabs, so the lookup covers their span, up to the
// top when it reaches slab C−1, past which the index holds no id; an empty
// window covers itself. A lookup that reached no split term covers the whole
// id space.
//
//nolint:cyclop,funlen // resolve, read and assemble in one pass; the covered-range rules read best inline
func (c *ColdReader) LookupKeys(
	ctx context.Context, keys []TermKey, window IDRange,
) ([]*roaring.Bitmap, IDRange, error) {
	if c.closed.Load() {
		return nil, IDRange{}, stores.ErrStoreClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, IDRange{}, err
	}
	if err := window.check(); err != nil {
		return nil, IDRange{}, err
	}
	if len(keys) == 0 {
		return nil, IDRange{End: math.MaxUint32}, nil
	}

	layout, err := c.waitLayout()
	if err != nil {
		return nil, IDRange{}, err
	}
	mphf, err := c.waitMPHF()
	if err != nil {
		return nil, IDRange{}, err
	}

	first, last := 1, 0
	splitCovered := window
	if !window.isEmpty() {
		x0, x1 := window.Start>>indexSlabShift, (window.End-1)>>indexSlabShift
		first, last = int(x0), min(int(x1), int(layout.slabs)-1)
		splitCovered = IDRange{Start: x0 << indexSlabShift, End: math.MaxUint32}
		if x1+1 < layout.slabs {
			splitCovered.End = (x1 + 1) << indexSlabShift
		}
	}

	results := make([]*roaring.Bitmap, len(keys))
	reads := make([]entryRead, 0, len(keys))
	covered := IDRange{End: math.MaxUint32}
	for i, hit := range mphf.LookupBatch(keys) {
		if hit.err != nil {
			if errors.Is(hit.err, ErrKeyNotFound) {
				continue // result[i] stays nil
			}
			return nil, IDRange{}, fmt.Errorf("events: LookupKeys MPHF for chunk %s: %w", c.chunkID, hit.err)
		}
		pos, rowFP, split := layout.locate(hit.slot)
		if !split {
			reads = append(reads, entryRead{pos: pos, key: i, slab: -1, fp: hit.fp})
			continue
		}
		if rowFP != hit.fp {
			continue
		}
		results[i] = roaring.New()
		results[i].SetCopyOnWrite(true)
		covered = splitCovered
		for x := first; x <= last; x++ {
			reads = append(reads, entryRead{pos: pos + x, key: i, slab: x})
		}
	}

	if err := readIndexEntries(ctx, c.index, reads); err != nil {
		return nil, IDRange{}, fmt.Errorf("events: LookupKeys read for chunk %s: %w", c.chunkID, err)
	}
	// reads is in position order, so each Or appends a split term's next
	// slab; with copy-on-write on both sides it takes over the slab's
	// containers, which this lookup owns, instead of copying them.
	for _, rd := range reads {
		switch {
		case rd.slab < 0:
			results[rd.key] = rd.bm
		case rd.bm != nil:
			rd.bm.SetCopyOnWrite(true)
			results[rd.key].Or(rd.bm)
		}
	}
	return results, covered, nil
}

// readIndexEntries decodes the entry each read names into its bm, in one
// ReadItems pass, and leaves reads sorted by position.
func readIndexEntries(ctx context.Context, index *stores.PackReader, reads []entryRead) error {
	sort.Slice(reads, func(i, j int) bool { return reads[i].pos < reads[j].pos })
	// starts[p] is positions[p]'s first read. Two keys share a position when
	// they resolve to one slot: the same key twice or a residual MPHF collision.
	positions := make([]int, 0, len(reads))
	starts := make([]int, 0, len(reads))
	for i, rd := range reads {
		if i > 0 && reads[i-1].pos == rd.pos {
			continue
		}
		positions = append(positions, rd.pos)
		starts = append(starts, i)
	}
	// ReadItems may call back from several goroutines; each writes only the
	// reads at its own position.
	return index.ReadItems(ctx, positions, func(p int, entry []byte) error {
		for j := starts[p]; j < len(reads) && reads[j].pos == positions[p]; j++ {
			bm, err := decodeIndexEntry(entry, reads[j])
			if err != nil {
				return err
			}
			reads[j].bm = bm
		}
		return nil
	})
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
func (c *ColdReader) FetchEvents(ctx context.Context, eventIDs []uint32) ([]Payload, error) {
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
	results := make([]Payload, len(eventIDs))
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
func (c *ColdReader) FetchRange(ctx context.Context, start, count uint32) iter.Seq2[Payload, error] {
	return func(yield func(Payload, error) bool) {
		if c.closed.Load() {
			yield(Payload{}, stores.ErrStoreClosed)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(Payload{}, err)
			return
		}
		if count == 0 {
			return
		}
		m, err := c.waitMeta()
		if err != nil {
			yield(Payload{}, err)
			return
		}
		if err := validateFetchRange(start, count, m.count, c.chunkID); err != nil {
			yield(Payload{}, err)
			return
		}
		// ReadRange yields raw item bytes in position order; we
		// decode each on the fly.
		for raw, err := range c.events.ReadRange(int(start), int(count)) {
			if err != nil {
				yield(Payload{}, fmt.Errorf("events: scan chunk %s: %w", c.chunkID, err))
				return
			}
			if err := ctx.Err(); err != nil {
				yield(Payload{}, err)
				return
			}
			var p Payload
			// raw is valid only until the next ReadRange step (see
			// Reader.ReadRange); Unmarshal aliases it into
			// ContractEventBytes, so the yielded Payload is borrowed (see
			// the FetchRange doc). A retaining consumer clones.
			if err := p.Unmarshal(raw); err != nil {
				yield(Payload{}, fmt.Errorf("events: decode event from chunk %s: %w", c.chunkID, err))
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
func (c *ColdReader) All(ctx context.Context) iter.Seq2[Payload, error] {
	return func(yield func(Payload, error) bool) {
		if c.closed.Load() {
			yield(Payload{}, stores.ErrStoreClosed)
			return
		}
		m, err := c.waitMeta()
		if err != nil {
			yield(Payload{}, err)
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
	offsets, err := decodeLedgerOffsets(appData)
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
