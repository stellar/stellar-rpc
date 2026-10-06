package event

// cold_index.go is the index half of the cold-Chunk pipeline. It
// produces index.pack (per-slot bitmap records) + index.hash (the
// serialized MPHF) inside a Chunk's cold directory.
//
// The events.pack writer half lives in cold_writer.go. Shared format
// constants, the LedgerOffsets app-data wire format, and the
// MPHF wrapper live in cold_format.go.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
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
// written as a run under the scratch directory when the next slab starts
// or the buffer fills, and Write merges the runs. Both cold backfill and
// the live-chunk freeze feed it single-threaded, re-deriving
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
// dirs.Index, which must exist, with its runs under dirs.Scratch. secret is
// the chunk's deterministic routing secret (ColdIndexSecret).
func NewColdIndexBuilder(chunkID chunk.ID, dirs ColdDirs, secret [stores.SecretLen]byte) *ColdIndexBuilder {
	return &ColdIndexBuilder{
		chunkID: chunkID,
		dir:     dirs.Index,
		secret:  secret,
		runs:    coldRuns{dir: filepath.Join(dirs.Scratch, IndexRunsDirName(chunkID))},
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
// index.hash and a zero-record index.pack. The runs are merged three times:
// to count the terms, which the MPHF builder needs up front; to build
// index.hash from the keys in order; and to write index.pack in slot order,
// a big term as one entry per slab (layout in cold_format.go). On error,
// any index.hash or index.pack produced is removed. A
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
	terms, err := b.runs.count()
	if err != nil {
		return err
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
	m, err := buildMPHF(ctx, b.runs.keys(), terms, indexHashPath, b.secret)
	if err != nil {
		return fmt.Errorf("events: build MPHF: %w", err)
	}
	defer m.Close()
	return b.writePack(m, filepath.Join(b.dir, IndexPackName(b.chunkID)))
}

// spill writes the postings in memory, if any, as a run and empties the slab
// for slab.
func (b *ColdIndexBuilder) spill(slab uint32) error {
	if b.live.n > 0 {
		if err := b.runs.spill(b.live); err != nil {
			return fmt.Errorf("events: spill index slab %d: %w", b.live.slab, err)
		}
	}
	b.live.reset(slab)
	return nil
}

// writePack writes every term's bitmap at the slot m gives it.
func (b *ColdIndexBuilder) writePack(m *mphf, path string) (err error) {
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
	var slabs uint32
	if b.any {
		slabs = b.maxID>>indexSlabShift + 1
	}
	w := newIndexPackWriter(pw, slabs)
	for term, err := range b.runs.terms() {
		if err != nil {
			return err
		}
		slot, fp, err := m.lookupRouted(term.key)
		if err != nil {
			return fmt.Errorf("events: MPHF lookup during index.pack build: %w", err)
		}
		if err := w.add(indexEntry{slot: slot, fp: fp, bitmap: term.bitmap}); err != nil {
			return err
		}
	}
	return w.finish()
}

type indexEntry struct {
	slot   uint32
	fp     [IndexRecordFingerprintLen]byte
	bitmap *roaring.Bitmap
}

// indexPackWriter writes entries in slot order as they arrive in key order.
// Terms come in MPHF block order and a block's slots are contiguous, so the
// entries held back never exceed one block.
type indexPackWriter struct {
	pw      *packfile.Writer
	layout  indexLayout
	buf     bytes.Buffer
	next    uint32 // the slot to write next
	pending heapOf[indexEntry]
}

func newIndexPackWriter(pw *packfile.Writer, slabs uint32) *indexPackWriter {
	return &indexPackWriter{
		pw:      pw,
		layout:  indexLayout{slabs: slabs},
		pending: heapOf[indexEntry]{less: func(a, b indexEntry) bool { return a.slot < b.slot }},
	}
}

func (w *indexPackWriter) add(e indexEntry) error {
	w.pending.push(e)
	for len(w.pending.items) > 0 && w.pending.items[0].slot == w.next {
		if err := w.write(w.pending.pop()); err != nil {
			return err
		}
		w.next++
	}
	return nil
}

func (w *indexPackWriter) write(e indexEntry) error {
	e.bitmap.RunOptimize() // RUN containers serialize more compactly
	if e.bitmap.GetSerializedSizeInBytes() > indexSplitBytes {
		if err := appendSplitTerm(w.pw, e.bitmap, w.layout.slabs, &w.buf); err != nil {
			return fmt.Errorf("events: write split slot %d to index.pack: %w", e.slot, err)
		}
		w.layout.rows = appendIndexRow(w.layout.rows, e.slot, e.fp)
		return nil
	}
	w.buf.Reset()
	if _, err := e.bitmap.WriteTo(&w.buf); err != nil {
		return fmt.Errorf("events: serialize bitmap at slot %d: %w", e.slot, err)
	}
	if err := w.pw.AppendItem(e.fp[:], w.buf.Bytes()); err != nil {
		return fmt.Errorf("events: write slot %d to index.pack: %w", e.slot, err)
	}
	return nil
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

func appendSplitTerm(pw *packfile.Writer, bm *roaring.Bitmap, slabs uint32, buf *bytes.Buffer) error {
	for x := range uint64(slabs) {
		mask := roaring.New()
		mask.AddRange(x<<indexSlabShift, (x+1)<<indexSlabShift)
		slab := roaring.And(mask, bm)
		buf.Reset()
		if !slab.IsEmpty() {
			slab.RunOptimize()
			if _, err := slab.WriteTo(buf); err != nil {
				return err
			}
		}
		if err := pw.AppendItem(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}
