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
// same chunk directory concurrently — the pack handle is safe for
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

	// waitDir returns index.pack's app data — the build stamp and the
	// dense-term directory behind it — decoded on first call. Cached via
	// sync.OnceValues; the entry table stays the app-data bytes, which the
	// packfile reader already holds, so nothing is copied per lookup.
	waitDir func() (indexDirectory, error)

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

// OpenColdReader prepares a ColdReader for chunkID inside bucketDir.
// It does no synchronous I/O — OpenPack starts each file's open
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
		events: stores.OpenPack(eventsPath, packfile.ReaderOptions{
			RecordDecoder: eventsPackDecoder,
			Concurrency:   opts.Concurrency,
		}),
		index: stores.OpenPack(indexPackPath, packfile.ReaderOptions{
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
		// Format, record-checksum, and build-stamp checks run for eventless
		// chunks too, so a foreign, unchecked, or mis-schemed pack refuses on the
		// first indexed lookup regardless of chunk content. The guarantee is
		// lookup-path-only by design: payload reads (FetchEvents, All) never
		// consult the index pair and stay valid against events.pack's own checks.
		tr, terr := c.index.Trailer()
		if terr != nil {
			return fmt.Errorf("events: open %s: %w", indexPackPath, terr)
		}
		if tr.Format != indexPackFormat {
			return fmt.Errorf("events: %s: expected format %#x, got %#x (mis-pointed or foreign pack)",
				indexPackPath, indexPackFormat, tr.Format)
		}
		// Serving an index whose bitmaps are unchecked is the silent-wrong-answer
		// case indexPackChecksum exists to prevent, and the reader can tell. It is
		// stores.ErrCorrupt for the same reason a failed checksum is: the artifact
		// cannot answer queries and has to be rebuilt.
		if !tr.HasRecordChecksum {
			return fmt.Errorf("%w: %s: built without a record checksum (stale build)",
				stores.ErrCorrupt, indexPackPath)
		}
		dir, derr := c.waitDir()
		if derr != nil {
			return derr
		}
		// The exact pairing. index.pack and index.hash carry no chunk ID of
		// their own, so a mispaired index would silently return an incomplete
		// subset of matches; and the part addressing is arithmetic off
		// bucketCount, so a pack whose record count does not decompose into
		// exactly the buckets and parts the directory claims cannot be read
		// at all. Three counts, all cheap, all at open.
		if dir.numKeys != idx.numKeys() {
			return fmt.Errorf(
				"events: index pair mismatch for chunk %s: index.hash holds %d keys "+
					"but index.pack's directory claims %d (mispaired artifacts)",
				c.chunkID, idx.numKeys(), dir.numKeys)
		}
		wantBuckets := (dir.numKeys + indexPackItemsPerRecord - 1) / indexPackItemsPerRecord
		if uint64(dir.bucketCount) != wantBuckets {
			return fmt.Errorf(
				"%w: events: %s holds %d keys in %d buckets, want %d",
				stores.ErrCorrupt, indexPackPath, dir.numKeys, dir.bucketCount, wantBuckets)
		}
		if uint64(dir.bucketCount)+uint64(dir.totalParts) != uint64(tr.RecordCount) {
			return fmt.Errorf(
				"%w: events: %s holds %d records, but its directory claims %d buckets and %d parts",
				stores.ErrCorrupt, indexPackPath, tr.RecordCount, dir.bucketCount, dir.totalParts)
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
		// Non-empty index ⇒ non-empty events.pack, the converse of the
		// empty-index check above.
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

	// index.pack app-data loader — the build stamp and the dense-term
	// directory. Runs on the first indexed lookup, behind validateMPHF.
	c.waitDir = sync.OnceValues(func() (indexDirectory, error) {
		return loadIndexAppData(indexPackPath, c.index)
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

// verifyAndDeserializeBitmap checks a bucket item's leading fingerprint
// against key's prefix and, on match, unmarshals a fresh bitmap. On
// fingerprint mismatch (residual MPHF collision on an unseen key) it returns
// (nil, nil) — the caller treats nil as not-found. A matching fingerprint
// over a body roaring cannot decode is corruption, which is also how a
// demoted slot surfaces: its body is deliberately zero-length, so a reader
// that lands on one (the directory and the buckets disagreeing) reports it
// rather than answering with an empty term. record is valid only inside the
// read callback; UnmarshalBinary copies into roaring's internal state so the
// returned bitmap outlives it safely.
func verifyAndDeserializeBitmap(record []byte, key TermKey, slot uint32) (*roaring.Bitmap, error) {
	if len(record) < IndexRecordFingerprintLen {
		return nil, fmt.Errorf("%w: events: index.pack item at slot %d truncated (%d bytes)",
			stores.ErrCorrupt, slot, len(record))
	}
	if !bytes.Equal(record[:IndexRecordFingerprintLen], key[:IndexRecordFingerprintLen]) {
		return nil, nil //nolint:nilnil // not-found signaled by nil bitmap, no error
	}
	bm := roaring.New()
	if err := bm.UnmarshalBinary(record[IndexRecordFingerprintLen:]); err != nil {
		return nil, fmt.Errorf("%w: events: unmarshal bitmap at slot %d: %w", stores.ErrCorrupt, slot, err)
	}
	return bm, nil
}

// deserializePart decodes one part item. Unlike a bucket item, a fingerprint
// mismatch here is not a miss: the reader reached this record through the
// directory, which named this key, so a fingerprint that disagrees means the
// pack and its directory are not halves of one build.
func deserializePart(item []byte, key TermKey, part uint32) (*roaring.Bitmap, error) {
	if len(item) < IndexRecordFingerprintLen ||
		!bytes.Equal(item[:IndexRecordFingerprintLen], key[:IndexRecordFingerprintLen]) {
		return nil, fmt.Errorf("%w: events: part %d does not carry its term's fingerprint", stores.ErrCorrupt, part)
	}
	bm := roaring.New()
	if err := bm.UnmarshalBinary(item[IndexRecordFingerprintLen:]); err != nil {
		return nil, fmt.Errorf("%w: events: unmarshal part %d: %w", stores.ErrCorrupt, part, err)
	}
	return bm, nil
}

// keyPlan is one queried key resolved against the directory: whether it is a
// demoted term, and if so the parts the window reaches, in span order.
type keyPlan struct {
	dense bool
	first uint32            // the first part index the window reaches
	parts []*roaring.Bitmap // one slot per part in [first, first+len), span order
}

// assemble unions a term's parts back together. The parts tile disjoint,
// ascending spans of the id space, so this is an append in disguise: each Or
// adds containers past everything the accumulator already holds, never
// merging into one. It unions into a fresh bitmap rather than into the first
// part because a part may be one the caller already held from an earlier
// window, shared with that window's result and with the query's cache.
func (p keyPlan) assemble() *roaring.Bitmap {
	acc := roaring.New()
	for _, part := range p.parts {
		if part != nil {
			acc.Or(part)
		}
	}
	return acc
}

// partRead is one item this lookup has to read: where it is, which result it
// feeds, and which part of that term it is (-1 for a bucket item).
type partRead struct {
	pos  int
	out  int
	part int32
}

// LookupKeys returns bitmaps for each key, aligned positionally with
// the input slice (result[i] corresponds to keys[i]). See
// Reader.LookupKeys for the semantics.
//
// Cold-side implementation:
//
//  1. Resolve every key against the directory. A demoted (dense) term names
//     the part records covering window; an empty window, or a window past
//     the term's last part, is a non-nil empty result and no I/O at all.
//  2. Every other key goes through the MPHF. Keys rejected at the routing
//     stage (streamhash ErrKeyNotFound) get result[i] = nil and never touch
//     index.pack; the rest name their bucket item by slot.
//  3. One c.index.ReadItems pass over every position, sorted and deduped —
//     two keys can share a bucket item (a residual MPHF collision), but
//     never a part. The packfile reader coalesces adjacent positions into
//     single ReadAt calls and fans out across the worker count configured
//     via ColdReaderOptions.Concurrency. The callbacks only copy bytes:
//     every decode happens afterwards, on this goroutine.
//  4. Verify and decode. A bucket item is this term's whole posting set, or
//     a fingerprint miss; a term's parts are unioned back together in span
//     order.
//
// held, when non-nil, is the calling query's memory of the parts it has
// already been handed (see LookupParts): a part already in it is not read
// again, and every part this call decodes is added to it. It is state of the
// query, not of the reader — two queries against the same chunk share
// nothing.
//
//nolint:cyclop,gocognit // the four documented passes above, inline; splitting obscures the structure
func (c *ColdReader) LookupKeys(
	ctx context.Context, keys []TermKey, window IDRange, held *LookupParts,
) ([]*roaring.Bitmap, error) {
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
	dir, err := c.waitDir()
	if err != nil {
		return nil, err
	}

	results := make([]*roaring.Bitmap, len(keys))
	plans := make([]keyPlan, len(keys))
	reads := make([]partRead, 0, len(keys))

	for i, key := range keys {
		if entry, ok := dir.lookup(key); ok {
			plans[i].dense = true
			first, last, any := entry.window(window)
			if !any {
				// Present in the chunk, nothing of it in the window.
				results[i] = roaring.New()
				continue
			}
			plans[i].first = first
			plans[i].parts = make([]*roaring.Bitmap, last-first+1)
			for part := first; part <= last; part++ {
				if bm, ok := held.get(key, part); ok {
					plans[i].parts[part-first] = bm
					continue
				}
				reads = append(reads, partRead{
					pos:  int(entry.firstRecord+part) * indexPackItemsPerRecord,
					out:  i,
					part: int32(part), //nolint:gosec // bounded by partCount (uint16)
				})
			}
			continue
		}
		slot, lerr := mphf.Lookup(key)
		if lerr != nil {
			if errors.Is(lerr, ErrKeyNotFound) {
				continue // result[i] stays nil
			}
			return nil, fmt.Errorf("events: LookupKeys MPHF for chunk %s: %w", c.chunkID, lerr)
		}
		reads = append(reads, partRead{pos: int(slot), out: i, part: -1})
	}
	if len(reads) == 0 {
		return results, nil
	}

	sort.Slice(reads, func(i, j int) bool { return reads[i].pos < reads[j].pos })

	// One position per distinct item. Two reads share a position only when
	// two keys residually collide into the same MPHF rank.
	positions := make([]int, 0, len(reads))
	readIdx := make([]int, len(reads))
	for i, r := range reads {
		if i > 0 && reads[i-1].pos == r.pos {
			readIdx[i] = len(positions) - 1
			continue
		}
		positions = append(positions, r.pos)
		readIdx[i] = len(positions) - 1
	}

	items := make([][]byte, len(positions))
	if err := c.index.ReadItems(ctx, positions, func(idx int, data []byte) error {
		// ReadItems lends data only for the callback, and may call back from
		// several goroutines. Copy, decode later, serially.
		items[idx] = bytes.Clone(data)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("events: LookupKeys read for chunk %s: %w", c.chunkID, err)
	}

	for i, r := range reads {
		item := items[readIdx[i]]
		if r.part < 0 {
			bm, derr := verifyAndDeserializeBitmap(item, keys[r.out], uint32(r.pos)) //nolint:gosec // a slot
			if derr != nil {
				return nil, derr
			}
			results[r.out] = bm
			continue
		}
		part := uint32(r.part) //nolint:gosec // non-negative here
		bm, derr := deserializePart(item, keys[r.out], part)
		if derr != nil {
			return nil, derr
		}
		plans[r.out].parts[part-plans[r.out].first] = bm
		held.put(keys[r.out], part, bm)
	}
	for i := range plans {
		if plans[i].dense && plans[i].parts != nil {
			results[i] = plans[i].assemble()
		}
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
// idx is unique — and the payload arena they share is locked (see
// the callback).
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
	// One arena per call, shared by every worker, since a call's payloads
	// live and die together. ReadItems calls back from up to Concurrency
	// goroutines and the arena is a single appended buffer, hence the lock.
	// It covers the copy alone, so the read, decode and Unmarshal overlap.
	var (
		arenaMu sync.Mutex
		arena   byteArena
	)
	if err := c.events.ReadItems(ctx, positions, func(idx int, data []byte) error {
		// packfile.ReadItems passes a borrowed data slice valid only for
		// the duration of fn (see Reader.ReadItems docstring). FetchEvents
		// returns the Payloads in a slice that outlives fn, so copy before
		// Unmarshal aliases the bytes into ContractEventBytes.
		arenaMu.Lock()
		owned := arena.copy(data)
		arenaMu.Unlock()
		return results[idx].Unmarshal(owned)
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
