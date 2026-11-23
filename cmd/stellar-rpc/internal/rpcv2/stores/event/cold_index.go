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
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
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
// postings with bounded memory. The slab being filled is in memory; when the
// next slab starts, the full one is written as a run under the scratch
// directory in the background while the next fills, so at most two slabs are
// in memory, and Write merges the runs. Both cold backfill and the live-chunk
// freeze feed it single-threaded, re-deriving terms from raw LCMs
// (Terms.AppendEvent). The directory is the chunk's, so one builder works on
// a chunk at a time, which the backfill plan guarantees: it builds a chunk
// once per pass.
type ColdIndexBuilder struct {
	chunkID chunk.ID
	dir     string
	secret  [stores.SecretLen]byte
	runs    coldRuns
	live    *hotSlab
	// spare is the slab being spilled, or a free one once that spill is over.
	spare   *hotSlab
	spiller sealer // runs one spill at a time, in the background
	written bool
}

// NewColdIndexBuilder returns a builder that writes chunkID's index into
// dirs.Index, which must exist, with its runs under dirs.Scratch. secret is
// the chunk's deterministic routing secret (ColdIndexSecret).
func NewColdIndexBuilder(chunkID chunk.ID, dirs ColdDirs, secret [stores.SecretLen]byte) *ColdIndexBuilder {
	return &ColdIndexBuilder{
		chunkID: chunkID,
		dir:     dirs.Index,
		secret:  secret,
		runs:    coldRuns{dir: filepath.Join(dirs.Scratch, IndexRunsDirName(chunkID))},
		live:    newHotSlab(0),
		spiller: newSealer(),
	}
}

// Add indexes one event's terms. Event ids must not decrease across calls.
func (b *ColdIndexBuilder) Add(eventID uint32, terms *Terms) error {
	if b.written {
		return errors.New("events: index already written")
	}
	if terms.Len() > maxTermsPerEvent {
		return fmt.Errorf("events: event %d has %d terms", eventID, terms.Len())
	}
	slab := eventID >> indexSlabShift
	if slab < b.live.slab {
		return fmt.Errorf("events: event %d added after slab %d", eventID, b.live.slab)
	}
	if slab > b.live.slab {
		if err := b.spill(); err != nil {
			return err
		}
		b.live.reset(slab)
	}
	for i := range terms.Len() {
		b.live.add(terms.Key(i, b.secret), eventID)
	}
	return nil
}

// Close waits for the spill in flight, if any, and removes the runs.
func (b *ColdIndexBuilder) Close() error {
	_ = b.spiller.wait() // the build is abandoned, so a spill error doesn't matter
	return b.runs.remove()
}

// Write produces index.pack + index.hash and removes the runs. Both files
// are fsync'd before it returns. A chunk without terms gets a real (empty)
// index.hash and a zero-record index.pack. Write merges the runs once, into
// the terms file, which holds each term's index.pack entries. It builds
// index.hash from that file's keys, in order, then reads the file again to
// write index.pack in slot order, a big term as one entry per slab (layout in
// cold_format.go). On error, any index.hash or index.pack produced is
// removed. A streamhash.ErrBlockOverflow is not retryable: the same secret
// routes the same keys to the same blocks.
func (b *ColdIndexBuilder) Write(ctx context.Context) (err error) {
	if b.written {
		return errors.New("events: index already written")
	}
	b.written = true
	defer func() {
		if cerr := b.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("events: remove index runs: %w", cerr))
		}
	}()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("events: build index: %w", err)
	}
	if err := b.spill(); err != nil {
		return err
	}
	if err := b.spiller.wait(); err != nil {
		return err
	}
	b.live, b.spare = nil, nil // the slabs are all in the runs now
	slabs := b.runs.slabs()
	termsPath := filepath.Join(b.runs.dir, "terms")
	terms, err := writeTerms(ctx, termsPath, b.runs.merge(), slabs)
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
	m, err := buildMPHF(ctx, termKeys(ctx, termsPath, slabs), terms, indexHashPath, b.secret)
	if err != nil {
		return fmt.Errorf("events: build MPHF: %w", err)
	}
	defer m.Close()
	return b.writePack(ctx, m, termsPath, slabs, filepath.Join(b.dir, IndexPackName(b.chunkID)))
}

// spill starts writing the live slab's postings, if any, as the next run,
// and swaps in the spare slab for the caller to reset. The spill before it
// is over first, so the runs keep their order.
func (b *ColdIndexBuilder) spill() error {
	if b.live.n == 0 {
		return nil
	}
	if err := b.spiller.wait(); err != nil {
		return err
	}
	full := b.live
	if b.spare == nil {
		b.spare = newHotSlab(0)
	}
	b.live, b.spare = b.spare, full
	b.spiller.start(func() error {
		if err := b.runs.spill(full.slab, full.terms()); err != nil {
			return fmt.Errorf("events: spill index slab %d: %w", full.slab, err)
		}
		return nil
	})
	return nil
}

// writePack writes every term of the terms file at the slot m gives it.
func (b *ColdIndexBuilder) writePack(
	ctx context.Context, m *mphf, termsPath string, slabs uint32, path string,
) (err error) {
	pw, err := packfile.Create(path, indexPackWriterOptions())
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
	if err := r.open(termsPath, termsFileBufferBytes, termsValueMax(slabs), heldEntryMax); err != nil {
		return err
	}
	defer r.f.Close()
	w := newIndexPackWriter(pw, r.f, slabs, m.idx.MaxBlockKeys())
	for !r.done {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("events: build index: %w", err)
		}
		slot, fp, err := m.lookupKey(r.key)
		if err != nil {
			return fmt.Errorf("events: MPHF lookup during index.pack build: %w", err)
		}
		if err := w.add(indexEntry{slot: slot, fp: fp, off: r.at, n: r.n}, r.value); err != nil {
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

const termsFileBufferBytes = 1 << 20

// writeTerms writes terms to path, each as key || uvarint(len) || its tagged
// index.pack entries, and returns how many it wrote.
func writeTerms(
	ctx context.Context, path string, terms iter.Seq2[runTerm, error], slabs uint32,
) (_ uint64, err error) {
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
	w := bufio.NewWriterSize(f, termsFileBufferBytes)
	var (
		enc termEncoder
		n   uint64
	)
	for term, err := range terms {
		if err != nil {
			return 0, err
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := enc.write(w, term.key, term.parts, slabs); err != nil {
			return 0, fmt.Errorf("term %x: %w", term.key, err)
		}
		n++
	}
	return n, w.Flush()
}

// termsValueMax bounds one term's tagged entries in the terms file of a
// chunk of slabs: its bitmap in every slab.
func termsValueMax(slabs uint32) uint64 {
	return 1 + uint64(slabs)*(binary.MaxVarintLen32+slabEntryMaxBytes)
}

// termKeys yields the keys of a terms file of a chunk of slabs, in order.
func termKeys(ctx context.Context, path string, slabs uint32) iter.Seq2[TermKey, error] {
	return func(yield func(TermKey, error) bool) {
		var r entryReader
		if err := r.open(path, termsFileBufferBytes, termsValueMax(slabs), 0); err != nil {
			yield(TermKey{}, err)
			return
		}
		defer r.f.Close()
		for !r.done {
			if err := ctx.Err(); err != nil {
				yield(TermKey{}, err)
				return
			}
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

// termEncoder writes merged terms as index.pack stores them, reusing its
// bitmaps and buffer from term to term.
type termEncoder struct {
	union, part roaring.Bitmap
	whole       bytes.Buffer // a whole term's union, serialized
}

// write writes a term to w as key || uvarint(len) || its tagged index.pack
// entries. The bitmaps are in their final form, so a term in one run is
// copied as it is, as is each slab of a split term, which goes straight to w.
func (e *termEncoder) write(w *bufio.Writer, key TermKey, parts []runPart, slabs uint32) error {
	if len(parts) == 1 {
		// One slab's bitmap is far under indexSplitBytes.
		return writeTagged(w, &key, termWhole, parts[0].bitmap)
	}
	if err := e.unite(parts); err != nil {
		return err
	}
	if e.union.GetSerializedSizeInBytes() <= indexSplitBytes {
		e.whole.Reset()
		_, _ = e.union.WriteTo(&e.whole) // a bytes.Buffer write cannot fail
		return writeTagged(w, &key, termWhole, e.whole.Bytes())
	}
	return writeSplit(w, &key, parts, slabs)
}

// writeTagged writes key || uvarint(len) || tag || body.
func writeTagged(w *bufio.Writer, key *TermKey, tag byte, body []byte) error {
	writeHead(w, key, 1+len(body))
	_ = w.WriteByte(tag)
	_, err := w.Write(body)
	return err
}

// writeSplit writes a split term as writeTagged would, its body being, per
// slab, uvarint(len) || the term's bitmap in the slab, empty without one. A
// slab without one takes one byte, its zero length.
func writeSplit(w *bufio.Writer, key *TermKey, parts []runPart, slabs uint32) error {
	var length [binary.MaxVarintLen64]byte
	n := 1 + int(slabs) - len(parts)
	for _, p := range parts {
		n += binary.PutUvarint(length[:], uint64(len(p.bitmap))) + len(p.bitmap)
	}
	writeHead(w, key, n)
	_ = w.WriteByte(termSplit)
	for x := range slabs {
		var bitmap []byte
		if len(parts) > 0 && parts[0].slab == x {
			bitmap, parts = parts[0].bitmap, parts[1:]
		}
		_, _ = w.Write(binary.AppendUvarint(w.AvailableBuffer(), uint64(len(bitmap))))
		if _, err := w.Write(bitmap); err != nil {
			return err
		}
	}
	return nil
}

// unite sets e.union to the union of parts, borrowing their bytes.
func (e *termEncoder) unite(parts []runPart) error {
	e.union.Clear()
	for _, p := range parts {
		if _, err := e.part.FromBuffer(p.bitmap); err != nil {
			return fmt.Errorf("decode bitmap of slab %d: %w", p.slab, err)
		}
		e.union.Or(&e.part)
	}
	return nil
}

// heldEntryMax is the longest entry the index.pack writer keeps in memory
// until its slot comes. It reads longer ones back from the terms file then.
const heldEntryMax = 64

// indexEntry is a term on its way to its slot: its tagged entries are n
// bytes at off, in the writer's held bytes if n is at most heldEntryMax and
// in the terms file otherwise.
type indexEntry struct {
	slot uint32
	fp   [IndexRecordFingerprintLen]byte
	off  int64
	n    int
}

// indexPackWriter writes entries in slot order as they arrive in key order.
// Keys in order map to MPHF blocks in order and a block's slots are
// contiguous, so an entry waits for at most one block, in a ring at its
// slot. The writer keeps the bytes of entries of at most heldEntryMax and
// only where longer ones are in the terms file, so its memory grows by at
// most heldEntryMax per entry of a block.
type indexPackWriter struct {
	pw      *packfile.Writer
	terms   io.ReaderAt // the terms file, which longer entries are read back from
	layout  indexLayout
	next    uint32       // the slot to write next
	ring    []indexEntry // a held slot s at ring[s % len(ring)]
	pending int          // how many entries the ring holds
	held    []byte       // the short entries the ring holds, emptied with it
	short   bytes.Reader
	sec     io.SectionReader
	entry   *bufio.Reader // the entry being written
	buf     []byte        // the item being written
}

// newIndexPackWriter returns a writer whose slots arrive at most hold past
// the next one to write: streamhash's MaxBlockKeys.
func newIndexPackWriter(pw *packfile.Writer, terms io.ReaderAt, slabs, hold uint32) *indexPackWriter {
	return &indexPackWriter{
		pw: pw, terms: terms, layout: indexLayout{slabs: slabs}, ring: make([]indexEntry, hold),
		entry: bufio.NewReaderSize(nil, 64<<10),
	}
}

// add holds e, whose bytes are value if it is at most heldEntryMax long,
// until its slot comes, then writes it and every held entry whose slot
// follows.
func (w *indexPackWriter) add(e indexEntry, value []byte) error {
	if e.slot < w.next || e.slot-w.next >= uint32(len(w.ring)) { //nolint:gosec // a ring of at most MaxBlockKeys
		return fmt.Errorf("events: MPHF slot %d out of reach of slot %d", e.slot, w.next)
	}
	if e.n == 0 {
		return fmt.Errorf("events: slot %d has no entries", e.slot)
	}
	if e.n <= heldEntryMax {
		e.off = int64(len(w.held))
		w.held = append(w.held, value...)
	}
	if err := w.hold(e); err != nil {
		return err
	}
	return w.drain()
}

// hold keeps e in the ring at its slot.
func (w *indexPackWriter) hold(e indexEntry) error {
	r := &w.ring[int(e.slot)%len(w.ring)]
	if r.n != 0 {
		return fmt.Errorf("events: MPHF slot %d repeated", e.slot)
	}
	*r = e
	w.pending++
	return nil
}

// drain writes the held entries whose slots come next.
func (w *indexPackWriter) drain() error {
	for w.pending > 0 {
		r := &w.ring[int(w.next)%len(w.ring)]
		if r.n == 0 {
			return nil
		}
		e := *r
		*r = indexEntry{}
		w.pending--
		if err := w.write(e); err != nil {
			return err
		}
	}
	w.held = w.held[:0]
	return nil
}

// write reads e's tagged entries, from its held bytes or back from the terms
// file, and appends them to index.pack as the next slot, a split term one
// slab at a time.
func (w *indexPackWriter) write(e indexEntry) error {
	if e.n <= heldEntryMax {
		w.short.Reset(w.held[e.off : e.off+int64(e.n)])
		w.entry.Reset(&w.short)
	} else {
		w.sec = *io.NewSectionReader(w.terms, e.off, int64(e.n))
		w.entry.Reset(&w.sec)
	}
	tag, err := w.entry.ReadByte()
	if err != nil {
		return fmt.Errorf("events: read back slot %d: %w", e.slot, err)
	}
	switch tag {
	case termWhole:
		if e.n-1 > indexSplitBytes {
			return fmt.Errorf("events: slot %d: whole entry of %d bytes", e.slot, e.n-1)
		}
		body, err := w.read(e.n - 1)
		if err != nil {
			return fmt.Errorf("events: read back slot %d: %w", e.slot, err)
		}
		if err := w.pw.AppendItem(e.fp[:], body); err != nil {
			return fmt.Errorf("events: write slot %d to index.pack: %w", e.slot, err)
		}
	case termSplit:
		for x := range w.layout.slabs {
			bitmap, err := w.readSlab()
			if err != nil {
				return fmt.Errorf("events: slot %d: corrupt entry for slab %d: %w", e.slot, x, err)
			}
			if err := w.pw.AppendItem(bitmap); err != nil {
				return fmt.Errorf("events: write split slot %d to index.pack: %w", e.slot, err)
			}
		}
		w.layout.rows = appendIndexRow(w.layout.rows, e.slot, e.fp)
	default:
		return fmt.Errorf("events: slot %d: entries tagged %d", e.slot, tag)
	}
	w.next++
	return nil
}

// readSlab reads the next slab's bitmap of the split entry being written.
func (w *indexPackWriter) readSlab() ([]byte, error) {
	n, err := binary.ReadUvarint(w.entry)
	if err != nil {
		return nil, err
	}
	if n > slabEntryMaxBytes {
		return nil, fmt.Errorf("bitmap of %d bytes", n)
	}
	return w.read(int(n))
}

// read reads the next n bytes of the entry being written.
func (w *indexPackWriter) read(n int) ([]byte, error) {
	w.buf = slices.Grow(w.buf[:0], n)[:n]
	_, err := io.ReadFull(w.entry, w.buf)
	return w.buf, err
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
