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
// postings with bounded memory. One slab of postings is in memory; it is
// written as a run in a directory beside the index files when the next
// slab starts or the buffer fills, and Write merges the runs. Both cold
// backfill and the live-chunk freeze feed it single-threaded, re-deriving
// terms from raw LCMs with TermsForBytes. The directory is the chunk's, so
// one builder works on a chunk at a time, which the backfill plan
// guarantees: it builds a chunk once per pass.
type ColdIndexBuilder struct {
	chunkID chunk.ID
	dir     string
	secret  [stores.SecretLen]byte
	runs    coldRuns
	live    *hotSlab
	maxID   uint32
	any     bool
	written bool
}

// NewColdIndexBuilder returns a builder that writes chunkID's index into
// bucketDir, the chunk's INDEX bucket directory (ColdDirs.Index), which must
// exist. secret is the chunk's deterministic routing secret (ColdIndexSecret).
func NewColdIndexBuilder(chunkID chunk.ID, bucketDir string, secret [stores.SecretLen]byte) *ColdIndexBuilder {
	return &ColdIndexBuilder{
		chunkID: chunkID,
		dir:     bucketDir,
		secret:  secret,
		runs:    coldRuns{dir: filepath.Join(bucketDir, IndexRunsDirName(chunkID))},
		live:    newHotSlab(0),
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
	var slabs uint32
	if b.any {
		slabs = b.maxID>>indexSlabShift + 1
	}
	termsPath := filepath.Join(b.runs.dir, "terms")
	terms, err := writeTerms(termsPath, b.runs.merge(), slabs)
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

// spill writes the postings in memory, if any, as a run and empties the slab
// for slab.
func (b *ColdIndexBuilder) spill(slab uint32) error {
	if b.live.n > 0 {
		if err := b.runs.spill(b.live.slab, b.live.terms()); err != nil {
			return fmt.Errorf("events: spill index slab %d: %w", b.live.slab, err)
		}
	}
	b.live.reset(slab)
	return nil
}

// writePack writes every term of the terms file at the slot m gives it.
func (b *ColdIndexBuilder) writePack(m *mphf, termsPath string, slabs uint32, path string) (err error) {
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
	if err := r.open(termsPath, 1<<20, termsValueMax(slabs)); err != nil {
		return err
	}
	defer r.f.Close()
	w := newIndexPackWriter(pw, slabs)
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
func writeTerms(path string, terms iter.Seq2[runTerm, error], slabs uint32) (_ uint64, err error) {
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
	w := bufio.NewWriterSize(f, 1<<20)
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
// Terms come in MPHF block order and a block's slots are contiguous, so the
// entries held back never exceed one block.
type indexPackWriter struct {
	pw      *packfile.Writer
	layout  indexLayout
	next    uint32 // the slot to write next
	pending heapOf[indexEntry]
	held    []byte // the pending entries' tagged entries
}

func newIndexPackWriter(pw *packfile.Writer, slabs uint32) *indexPackWriter {
	return &indexPackWriter{
		pw:      pw,
		layout:  indexLayout{slabs: slabs},
		pending: heapOf[indexEntry]{less: func(a, b indexEntry) bool { return a.slot < b.slot }},
	}
}

// add writes e with its tagged entries, or holds them until its slot comes.
func (w *indexPackWriter) add(e indexEntry, entries []byte) error {
	if e.slot != w.next {
		e.start = len(w.held)
		w.held = append(w.held, entries...)
		e.end = len(w.held)
		w.pending.push(e)
		return nil
	}
	for {
		if err := w.write(e, entries); err != nil {
			return err
		}
		w.next++
		if len(w.pending.items) == 0 || w.pending.items[0].slot != w.next {
			break
		}
		e = w.pending.pop()
		entries = w.held[e.start:e.end]
	}
	if len(w.pending.items) == 0 {
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
	if n := len(w.pending.items); n > 0 {
		return fmt.Errorf("events: non-dense MPHF slots: %d entries left past slot %d", n, w.next)
	}
	return w.pw.Finish(encodeIndexAppData(w.layout))
}
