package event

// cold_index.go is the index half of the cold-Chunk pipeline. It
// produces index.pack (per-slot bitmap records) + index.hash (the
// serialized MPHF) inside a Chunk's cold directory.
//
// The events.pack writer half lives in cold_writer.go. Shared format
// constants, the LedgerOffsets app-data wire format, and the
// MPHF wrapper live in cold_format.go.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"os"
	"path/filepath"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/writeback"
)

// coldRoutingDomain is the DeriveIndexSecret domain for events cold indexes
// (distinct from txhash's "txhash").
const coldRoutingDomain = "events"

// ColdIndexSecret derives chunkID's routing secret from the build-side master
// key. The single derivation both ingest and the index build use, so rebuilds
// stay byte-identical.
func ColdIndexSecret(catalogSecret []byte, chunkID chunk.ID) [stores.SecretLen]byte {
	return stores.DeriveIndexSecret(catalogSecret, coldRoutingDomain, uint32(chunkID))
}

// ColdIndexBuilder builds a chunk's index.hash and index.pack from its
// postings with bounded memory. One slab of postings is in memory; when the
// next slab starts or the buffer fills, it is written as a run under the
// scratch directory in the background while the next fills, and Write
// merges the runs. Cold backfill feeds it
// every event's terms, derived from raw LCMs with TermsForBytes; the
// live-chunk freeze feeds it the hot index's sealed slabs as they are
// (AddSlab) and derives terms only for the events after them. The
// directory is the chunk's, so one builder works on a chunk at a time,
// which the backfill plan guarantees: it builds a chunk once per pass.
type ColdIndexBuilder struct {
	chunkID      chunk.ID
	dir          string
	secret       [stores.SecretLen]byte
	bytesPerSync int
	runs         coldRuns
	live         *hotSlab
	// spare is the slab being spilled, or the free one once that is over;
	// spilling yields the outcome of the spill in flight, at most one.
	spare    *hotSlab
	spilling chan error
	spillErr error
	maxID    uint32
	any      bool
	written  bool
}

// NewColdIndexBuilder returns a builder that writes chunkID's index into
// dirs.Index, which must exist, with its runs under dirs.Scratch. secret is
// the chunk's deterministic routing secret (ColdIndexSecret). bytesPerSync
// is index.pack's writeback cadence, as ColdWriterOptions.BytesPerSync is
// events.pack's.
func NewColdIndexBuilder(
	chunkID chunk.ID, dirs ColdDirs, secret [stores.SecretLen]byte, bytesPerSync int,
) *ColdIndexBuilder {
	return &ColdIndexBuilder{
		chunkID:      chunkID,
		dir:          dirs.Index,
		secret:       secret,
		bytesPerSync: bytesPerSync,
		runs:         coldRuns{dir: filepath.Join(dirs.Scratch, IndexRunsDirName(chunkID)), bytesPerSync: bytesPerSync},
		live:         newHotSlab(0),
	}
}

// Add indexes one event's terms. Event ids must not decrease across calls.
func (b *ColdIndexBuilder) Add(eventID uint32, keys []TermKey) error {
	if b.written {
		return errors.New("events: index already written")
	}
	slab := eventID >> indexSlabShift
	for _, k := range keys {
		if slab != b.live.slab || b.live.n == hotSlabPostings {
			if err := b.spill(slab); err != nil {
				return err
			}
		}
		b.live.add(routedKey(b.secret, k), eventID)
	}
	if len(keys) > 0 {
		b.maxID, b.any = max(b.maxID, eventID), true
	}
	return nil
}

// Close removes the runs of a build that is not written.
func (b *ColdIndexBuilder) Close() error {
	_ = b.awaitSpill() // a spill error fails the build Close abandons
	return b.runs.remove()
}

// Write produces index.pack + index.hash and removes the runs. Both files
// are fsync'd before it returns. A chunk without terms gets a real (empty)
// index.hash and a zero-record index.pack. The runs are merged once, into a
// scratch file holding each term's index.pack entries; index.hash is built
// from that file's keys in order, and index.pack is written from it again in
// slot order, a big term as one entry per slab (layout in cold_format.go).
// On error, any index.hash or index.pack produced is removed. A
// streamhash.ErrBlockOverflow is not retryable: the same secret routes the
// same keys to the same blocks.
func (b *ColdIndexBuilder) Write(ctx context.Context) (err error) {
	if b.written {
		return errors.New("events: index already written")
	}
	b.written = true
	defer func() {
		if rerr := b.runs.remove(); rerr != nil {
			err = errors.Join(err, fmt.Errorf("events: remove index runs: %w", rerr))
		}
	}()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("events: build index: %w", err)
	}
	if err := b.spill(0); err != nil {
		return err
	}
	if err := b.awaitSpill(); err != nil {
		return err
	}
	var slabs uint32
	if b.any {
		slabs = b.maxID>>indexSlabShift + 1
	}
	termsPath := filepath.Join(b.runs.dir, "terms")
	terms, err := writeTerms(termsPath, b.runs.merge(), slabs, b.bytesPerSync)
	if err != nil {
		return fmt.Errorf("events: merge index runs: %w", err)
	}
	if err := b.runs.removeRuns(); err != nil {
		return fmt.Errorf("events: remove index runs: %w", err)
	}

	indexHashPath := filepath.Join(b.dir, IndexHashName(b.chunkID))
	// index.hash is removed on error.
	defer func() {
		if err == nil {
			return
		}
		if rmErr := os.Remove(indexHashPath); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("events: remove orphan %s: %w", indexHashPath, rmErr))
		}
	}()
	m, err := buildMPHF(ctx, termKeys(termsPath, slabs), terms, indexHashPath, b.secret)
	if err != nil {
		return fmt.Errorf("events: build MPHF: %w", err)
	}
	defer m.Close()
	return b.writePack(m, termsPath, slabs, filepath.Join(b.dir, IndexPackName(b.chunkID)))
}

// AddSlab adds a full slab whose terms are already in key order, each with
// its serialized bitmap: the form a sealed hot slab holds them in (see
// hotIndex.seal), which is also the form a spilled slab takes, so the slab
// becomes the next run as it is. Slabs must be added in slab order before
// any Add. A slab with no terms is an error: a full slab has an event, and
// every event has terms.
func (b *ColdIndexBuilder) AddSlab(slab uint32, terms iter.Seq2[TermKey, []byte]) error {
	if b.written {
		return errors.New("events: index already written")
	}
	if b.live.n > 0 || slab < b.live.slab {
		return fmt.Errorf("events: index slab %d added after slab %d", slab, b.live.slab)
	}
	if err := b.awaitSpill(); err != nil {
		return err
	}
	n, err := b.runs.spill(slab, terms)
	if err != nil {
		return fmt.Errorf("events: spill index slab %d: %w", slab, err)
	}
	if n == 0 {
		return fmt.Errorf("events: index slab %d has no terms", slab)
	}
	b.live.reset(slab + 1)
	b.maxID, b.any = slab<<indexSlabShift|(hotSlabEvents-1), true
	return nil
}

// spill starts writing the postings in memory, if any, as the next run and
// empties a slab for slab. The spill before it is over first, so the runs
// keep their order.
func (b *ColdIndexBuilder) spill(slab uint32) error {
	if b.live.n == 0 {
		b.live.reset(slab)
		return nil
	}
	if err := b.awaitSpill(); err != nil {
		return err
	}
	full := b.live
	if b.spare == nil {
		b.spare = newHotSlab(slab)
	}
	b.live, b.spare = b.spare, full
	b.live.reset(slab)
	done := make(chan error, 1)
	b.spilling = done
	go func() {
		if _, err := b.runs.spill(full.slab, full.terms()); err != nil {
			done <- fmt.Errorf("events: spill index slab %d: %w", full.slab, err)
		}
		close(done)
	}()
	return nil
}

// awaitSpill waits for the spill in flight, if any. A spill error fails
// every later call.
func (b *ColdIndexBuilder) awaitSpill() error {
	if b.spilling != nil {
		b.spillErr = <-b.spilling
		b.spilling = nil
	}
	return b.spillErr
}

// writePack writes every term of the terms file at the slot m gives it.
func (b *ColdIndexBuilder) writePack(m *mphf, termsPath string, slabs uint32, path string) (err error) {
	opts := indexPackWriterOptions()
	opts.BytesPerSync = b.bytesPerSync
	pw, err := packfile.Create(path, opts)
	if err != nil {
		return fmt.Errorf("events: create index.pack at %s: %w", path, err)
	}
	defer func() {
		// pw.Close removes the partial pack.
		if err != nil {
			if closeErr := pw.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("events: close partial index.pack: %w", closeErr))
			}
		}
	}()
	var r entryReader
	if err := r.open(termsPath, 1<<20, termsValueMax(slabs)); err != nil {
		return err
	}
	defer r.f.Close()
	w := newIndexPackWriter(pw, slabs, m.idx.MaxBlockKeys())
	for !r.done {
		slot, fp, err := m.lookupRouted(r.key)
		if err != nil {
			return fmt.Errorf("events: MPHF lookup during index.pack build: %w", err)
		}
		if err := w.add(indexEntry{slot: slot, fp: fp}, r.value); err != nil {
			return err
		}
		if err := r.next(); err != nil {
			return err
		}
	}
	return w.finish()
}

// A term's entries in the terms file are tagged with how index.pack stores
// them.
const (
	termWhole byte = iota // the term's bitmap
	termSplit             // per slab: uvarint(len) || the term's bitmap in the slab, empty without one
)

// writeTerms writes terms to path, each as key || uvarint(len) || its tagged
// index.pack entries, and returns how many it wrote.
func writeTerms(path string, terms iter.Seq2[runTerm, error], slabs uint32, bytesPerSync int) (_ uint64, err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	w := bufio.NewWriterSize(writeback.NewWriter(f, bytesPerSync), 1<<20)
	var (
		enc termEncoder
		n   uint64
	)
	for term, err := range terms {
		if err != nil {
			return 0, err
		}
		entries, err := enc.encode(term.parts, slabs)
		if err != nil {
			return 0, fmt.Errorf("term %x: %w", term.key, err)
		}
		writeEntry(w, term.key, entries)
		n++
	}
	return n, w.Flush()
}

// termsValueMax bounds one term's tagged entries in the terms file of a
// chunk of slabs: its bitmap in every slab.
func termsValueMax(slabs uint32) uint64 {
	return 1 + uint64(slabs)*(binary.MaxVarintLen32+runValueMax)
}

// termKeys yields the keys of a terms file of a chunk of slabs, in order.
func termKeys(path string, slabs uint32) iter.Seq2[TermKey, error] {
	return func(yield func(TermKey, error) bool) {
		var r entryReader
		if err := r.open(path, 1<<20, termsValueMax(slabs)); err != nil {
			yield(TermKey{}, err)
			return
		}
		defer r.f.Close()
		for !r.done {
			if !yield(r.key, nil) {
				return
			}
			if err := r.next(); err != nil {
				yield(TermKey{}, err)
				return
			}
		}
	}
}

// termEncoder encodes merged terms as index.pack stores them, reusing its
// bitmaps and buffers from term to term.
type termEncoder struct {
	union, part, slab roaring.Bitmap
	out, slabOut      bytes.Buffer
}

// encode returns a term's tagged entries from its bitmap in each run, valid
// until the next call. The bitmaps are in their final form, so a term in one
// run is copied as it is, as is each slab of a split term.
func (e *termEncoder) encode(parts []runPart, slabs uint32) ([]byte, error) {
	e.out.Reset()
	if len(parts) == 1 {
		// One slab's bitmap is far under indexSplitBytes.
		e.out.WriteByte(termWhole)
		e.out.Write(parts[0].bitmap)
		return e.out.Bytes(), nil
	}
	if err := e.unite(&e.union, parts); err != nil {
		return nil, err
	}
	if e.union.GetSerializedSizeInBytes() <= indexSplitBytes {
		e.out.WriteByte(termWhole)
		_, _ = e.union.WriteTo(&e.out) // a bytes.Buffer write cannot fail
		return e.out.Bytes(), nil
	}
	e.out.WriteByte(termSplit)
	var length [binary.MaxVarintLen32]byte
	for x := range slabs {
		n := 0
		for n < len(parts) && parts[n].slab == x {
			n++
		}
		var bitmap []byte
		switch n {
		case 0:
		case 1:
			bitmap = parts[0].bitmap
		default: // a slab spilled in several runs
			if err := e.unite(&e.slab, parts[:n]); err != nil {
				return nil, err
			}
			e.slabOut.Reset()
			_, _ = e.slab.WriteTo(&e.slabOut)
			bitmap = e.slabOut.Bytes()
		}
		parts = parts[n:]
		e.out.Write(length[:binary.PutUvarint(length[:], uint64(len(bitmap)))])
		e.out.Write(bitmap)
	}
	if len(parts) > 0 {
		return nil, fmt.Errorf("bitmap in slab %d of %d", parts[0].slab, slabs)
	}
	return e.out.Bytes(), nil
}

// unite sets bm to the union of parts, borrowing their bytes.
func (e *termEncoder) unite(bm *roaring.Bitmap, parts []runPart) error {
	bm.Clear()
	for _, p := range parts {
		if _, err := e.part.FromBuffer(p.bitmap); err != nil {
			return fmt.Errorf("decode bitmap of slab %d: %w", p.slab, err)
		}
		bm.Or(&e.part)
	}
	bm.RunOptimize()
	return nil
}

// indexEntry is a term waiting for its slot, its entries held by the writer.
type indexEntry struct {
	slot       uint32
	fp         [IndexRecordFingerprintLen]byte
	start, end int
}

// indexPackWriter writes entries in slot order as they arrive in key order.
// Keys in order map to MPHF blocks in order and a block's slots are
// contiguous, so an entry is held for at most one block, in a ring at its
// slot.
type indexPackWriter struct {
	pw      *packfile.Writer
	layout  indexLayout
	next    uint32       // the slot to write next
	ring    []indexEntry // a held slot s at ring[s % len(ring)]
	pending int          // how many entries the ring holds
	held    []byte       // the pending entries' tagged entries
}

// newIndexPackWriter returns a writer whose slots arrive at most hold past
// the next one to write: streamhash's MaxBlockKeys.
func newIndexPackWriter(pw *packfile.Writer, slabs, hold uint32) *indexPackWriter {
	return &indexPackWriter{pw: pw, layout: indexLayout{slabs: slabs}, ring: make([]indexEntry, hold)}
}

// add writes e with its tagged entries, or holds them until its slot comes.
func (w *indexPackWriter) add(e indexEntry, entries []byte) error {
	if e.slot < w.next || e.slot-w.next >= uint32(len(w.ring)) { //nolint:gosec // a ring of at most MaxBlockKeys
		return fmt.Errorf("events: MPHF slot %d out of reach of slot %d", e.slot, w.next)
	}
	if e.slot != w.next {
		r := &w.ring[int(e.slot)%len(w.ring)]
		if r.end != 0 {
			return fmt.Errorf("events: MPHF slot %d repeated", e.slot)
		}
		e.start = len(w.held)
		w.held = append(w.held, entries...)
		e.end = len(w.held)
		*r = e
		w.pending++
		return nil
	}
	for {
		if err := w.write(e, entries); err != nil {
			return err
		}
		w.next++
		r := &w.ring[int(w.next)%len(w.ring)]
		if w.pending == 0 || r.end == 0 {
			break
		}
		e, *r = *r, indexEntry{}
		w.pending--
		entries = w.held[e.start:e.end]
	}
	if w.pending == 0 {
		w.held = w.held[:0]
	}
	return nil
}

func (w *indexPackWriter) write(e indexEntry, entries []byte) error {
	if len(entries) == 0 {
		return fmt.Errorf("events: slot %d has no entries", e.slot)
	}
	tag, body := entries[0], entries[1:]
	switch tag {
	case termWhole:
		if err := w.pw.AppendItem(e.fp[:], body); err != nil {
			return fmt.Errorf("events: write slot %d to index.pack: %w", e.slot, err)
		}
		return nil
	case termSplit:
		for x := range w.layout.slabs {
			bitmap, rest, ok := cutPrefixed(body)
			if !ok {
				return fmt.Errorf("events: slot %d: corrupt entry for slab %d", e.slot, x)
			}
			if err := w.pw.AppendItem(bitmap); err != nil {
				return fmt.Errorf("events: write split slot %d to index.pack: %w", e.slot, err)
			}
			body = rest
		}
		w.layout.rows = appendIndexRow(w.layout.rows, e.slot, e.fp)
		return nil
	default:
		return fmt.Errorf("events: slot %d: entries tagged %d", e.slot, tag)
	}
}

// cutPrefixed cuts a uvarint-length-prefixed slice off the front of b.
func cutPrefixed(b []byte) ([]byte, []byte, bool) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)-k) { //nolint:gosec // k is at most len(b)
		return nil, nil, false
	}
	end := k + int(n) //nolint:gosec // n is at most len(b)-k
	return b[k:end], b[end:], true
}

// finish writes the layout and closes the pack. streamhash's MPHF is
// minimal, so every slot in [0, n) must have been written: a slot never
// reached leaves entries pending, and a repeated one never drains.
func (w *indexPackWriter) finish() error {
	if w.pending > 0 {
		return fmt.Errorf("events: non-dense MPHF slots: %d entries left past slot %d", w.pending, w.next)
	}
	return w.pw.Finish(encodeIndexAppData(w.layout))
}
