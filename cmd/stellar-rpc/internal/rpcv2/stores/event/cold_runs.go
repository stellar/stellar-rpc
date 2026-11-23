package event

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/writeback"
)

// coldRuns are a chunk's postings spilled one slab at a time. Run i holds
// the terms of one slab in key order, each as
// key || uvarint(len) || serialized bitmap. Merging the runs yields every
// term once, in key order, with its bitmap from each run holding it. The
// runs are appended to one file, runGap apart, so the merge holds one file
// open however many runs a chunk spills.
type coldRuns struct {
	dir          string
	bytesPerSync int               // writeback cadence of the runs file
	f            *os.File          // the runs, one after another; nil before the first spill
	w            *writeback.Writer // appends to f
	spans        []runSpan
}

// runSpan is where a run sits in the runs file, and its slab.
type runSpan struct {
	off, n int64
	slab   uint32
}

// runGap is the hole left before each run after the first. Near a run's end,
// readahead reads on past it, and a hole reads as zeros without touching the
// disk. Without the hole, readahead would read the start of the next run,
// which the merge read long before, from disk a second time once memory
// pressure had evicted it.
const runGap = 1 << 20

// spill appends slab's terms, in key order with their serialized bitmaps, to
// the runs file as the next run.
func (r *coldRuns) spill(slab uint32, terms iter.Seq2[TermKey, []byte]) error {
	if r.f == nil {
		if err := os.MkdirAll(r.dir, 0o755); err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(r.dir, "runs"))
		if err != nil {
			return err
		}
		r.f, r.w = f, writeback.NewWriter(f, r.bytesPerSync)
	} else if err := r.w.Skip(runGap); err != nil {
		return err
	}
	off, err := r.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := writeRun(r.w, terms); err != nil {
		return err
	}
	end, err := r.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	r.spans = append(r.spans, runSpan{off: off, n: end - off, slab: slab})
	return nil
}

func writeRun(w io.Writer, terms iter.Seq2[TermKey, []byte]) error {
	bw := bufio.NewWriterSize(w, 1<<16)
	for key, bitmap := range terms {
		writeHead(bw, &key, len(bitmap))
		_, _ = bw.Write(bitmap)
	}
	return bw.Flush()
}

// writeHead writes the head of an entry, key || uvarint(n), n being the
// length of what follows. A bufio.Writer keeps its first error and returns it
// from every later write and from Flush.
func writeHead(w *bufio.Writer, key *TermKey, n int) {
	b := append(w.AvailableBuffer(), key[:]...)
	_, _ = w.Write(binary.AppendUvarint(b, uint64(n))) //nolint:gosec // n is a length
}

// slabs is how many slabs the runs cover: up to the last one spilled.
func (r *coldRuns) slabs() uint32 {
	if len(r.spans) == 0 {
		return 0
	}
	return r.spans[len(r.spans)-1].slab + 1
}

// removeRuns deletes the runs file and leaves the directory, which also
// holds the terms file.
func (r *coldRuns) removeRuns() error {
	if r.f == nil {
		return nil
	}
	_ = r.f.Close()
	err := os.Remove(r.f.Name())
	r.f, r.w, r.spans = nil, nil, nil
	return err
}

// remove deletes the runs and the directory holding them.
func (r *coldRuns) remove() error {
	return errors.Join(r.removeRuns(), os.RemoveAll(r.dir))
}

// runTerm is one term of the merged runs.
type runTerm struct {
	key   TermKey
	parts []runPart // in slab order
}

// runPart is a term's bitmap in one run, valid until the merge yields the
// next term.
type runPart struct {
	slab   uint32
	bitmap []byte
}

// merge yields every term of the runs once, in key order.
func (r *coldRuns) merge() iter.Seq2[runTerm, error] {
	return func(yield func(runTerm, error) bool) {
		m, err := r.open()
		if err != nil {
			yield(runTerm{}, err)
			return
		}
		var (
			term runTerm
			runs []*runReader // the runs holding term, out of the heap while it is yielded
		)
		for len(m.items) > 0 {
			term.key = m.items[0].key
			term.parts, runs = term.parts[:0], runs[:0]
			for len(m.items) > 0 && m.items[0].key == term.key {
				rr := m.pop()
				runs = append(runs, rr)
				term.parts = append(term.parts, runPart{slab: rr.slab, bitmap: rr.value})
			}
			if !yield(term, nil) {
				return
			}
			for _, rr := range runs {
				if err := rr.next(); err != nil {
					yield(runTerm{}, err)
					return
				}
				if !rr.done {
					m.push(rr)
				}
			}
		}
	}
}

// open reads the first entry of every run and returns the runs with entries
// left in a heap: the smallest key first and, among equal keys, the earliest
// slab.
func (r *coldRuns) open() (*heapOf[*runReader], error) {
	m := &heapOf[*runReader]{less: func(a, b *runReader) bool {
		return cmp.Or(bytes.Compare(a.key[:], b.key[:]), cmp.Compare(a.slab, b.slab)) < 0
	}}
	// Each run reads through a buffer of 16 MiB over the number of runs, kept
	// between 4 KiB and 64 KiB: 16 MiB in all up to 4,096 runs, 4 KiB a run
	// beyond that.
	bufSize := min(max(16<<20/max(len(r.spans), 1), 4<<10), 64<<10)
	for _, s := range r.spans {
		rr := &runReader{slab: s.slab}
		name := fmt.Sprintf("%s, slab %d's run", r.f.Name(), s.slab)
		sec := io.NewSectionReader(r.f, s.off, s.n)
		if err := rr.start(sec, name, bufSize, slabEntryMaxBytes, slabEntryMaxBytes); err != nil {
			return nil, err
		}
		if !rr.done {
			m.push(rr)
		}
	}
	return m, nil
}

// runReader reads one run's entries in order.
type runReader struct {
	entryReader

	slab uint32
}

// entryReader reads key || uvarint(len) || value entries in order.
type entryReader struct {
	f     *os.File // set by open; nil for a run in the runs file
	name  string   // what errors call the entries' source
	src   posReader
	br    *bufio.Reader
	max   uint64 // the longest value accepted
	key   TermKey
	value []byte // valid until the next call to next, unless it was skipped
	at    int64  // where the value starts in the source
	n     int    // the value's length
	// keep is the longest value read into value. Longer ones are skipped,
	// leaving at and n to say where they are.
	keep int
	done bool
}

// posReader keeps its position in the source it reads.
type posReader struct {
	r   io.Reader
	pos int64
}

func (p *posReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.pos += int64(n)
	return n, err
}

// open opens path and reads its first entry.
func (r *entryReader) open(path string, bufSize int, maxValue uint64, keep int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	r.f = f
	if err := r.start(f, f.Name(), bufSize, maxValue, keep); err != nil {
		_ = f.Close()
		return err
	}
	return nil
}

// start reads entries from src, called name in errors, and reads the first.
func (r *entryReader) start(src io.Reader, name string, bufSize int, maxValue uint64, keep int) error {
	r.src = posReader{r: src}
	r.name, r.br, r.max, r.keep = name, bufio.NewReaderSize(&r.src, bufSize), maxValue, keep
	return r.next()
}

func (r *entryReader) next() error {
	if _, err := io.ReadFull(r.br, r.key[:]); err != nil {
		if errors.Is(err, io.EOF) {
			r.done = true
			return nil
		}
		return fmt.Errorf("events: %s: %w", r.name, err)
	}
	if err := r.readValue(); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF // a key without its entry
		}
		return fmt.Errorf("events: %s: %w", r.name, err)
	}
	return nil
}

// readValue reads the length and the value after a key, skipping a value
// longer than keep.
func (r *entryReader) readValue() error {
	n, err := binary.ReadUvarint(r.br)
	if err != nil {
		return err
	}
	if n > r.max {
		return fmt.Errorf("entry of %d bytes", n)
	}
	r.at, r.n = r.src.pos-int64(r.br.Buffered()), int(n) //nolint:gosec // n is at most r.max
	if r.n > r.keep {
		_, err = r.br.Discard(r.n)
		return err
	}
	r.value = slices.Grow(r.value[:0], r.n)[:r.n]
	_, err = io.ReadFull(r.br, r.value)
	return err
}

// heapOf is a binary min-heap by less.
type heapOf[T any] struct {
	items []T
	less  func(a, b T) bool
}

func (h *heapOf[T]) push(x T) {
	h.items = append(h.items, x)
	for i := len(h.items) - 1; i > 0; {
		p := (i - 1) / 2
		if !h.less(h.items[i], h.items[p]) {
			return
		}
		h.items[i], h.items[p] = h.items[p], h.items[i]
		i = p
	}
}

// pop removes the smallest item.
func (h *heapOf[T]) pop() T {
	x := h.items[0]
	last := len(h.items) - 1
	h.items[0] = h.items[last]
	h.items = h.items[:last]
	h.down()
	return x
}

// down restores the order after the smallest item changed in place.
func (h *heapOf[T]) down() {
	n := len(h.items)
	for i := 0; ; {
		m := i
		if l := 2*i + 1; l < n && h.less(h.items[l], h.items[m]) {
			m = l
		}
		if r := 2*i + 2; r < n && h.less(h.items[r], h.items[m]) {
			m = r
		}
		if m == i {
			return
		}
		h.items[i], h.items[m] = h.items[m], h.items[i]
		i = m
	}
}
