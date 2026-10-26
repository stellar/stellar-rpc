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

// runGap is the hole left before each run after the first. Readahead near a
// run's end reads past it: a hole reads as zeros without touching the disk,
// where the start of the next run, which the merge read long before, would be
// read from disk again once memory pressure had evicted it.
const runGap = 1 << 20

// spill appends slab's terms, in key order with their serialized bitmaps, to
// the runs file as the next run, and returns how many it wrote.
func (r *coldRuns) spill(slab uint32, terms iter.Seq2[TermKey, []byte]) (int, error) {
	if r.f == nil {
		if err := os.MkdirAll(r.dir, 0o755); err != nil {
			return 0, err
		}
		f, err := os.Create(filepath.Join(r.dir, "runs"))
		if err != nil {
			return 0, err
		}
		r.f, r.w = f, writeback.NewWriter(f, r.bytesPerSync)
	} else if err := r.w.Skip(runGap); err != nil {
		return 0, err
	}
	off, err := r.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	n, err := writeRun(r.w, terms)
	if err != nil {
		return 0, err
	}
	end, err := r.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	r.spans = append(r.spans, runSpan{off: off, n: end - off, slab: slab})
	return n, nil
}

func writeRun(w io.Writer, terms iter.Seq2[TermKey, []byte]) (int, error) {
	bw := bufio.NewWriterSize(w, 1<<16)
	n := 0
	for key, bitmap := range terms {
		writeEntry(bw, key, bitmap)
		n++
	}
	return n, bw.Flush()
}

// writeEntry writes key || uvarint(len(value)) || value. A bufio.Writer
// keeps the first error and reports it from Flush.
func writeEntry(w *bufio.Writer, key TermKey, value []byte) {
	var length [binary.MaxVarintLen64]byte
	_, _ = w.Write(key[:])
	_, _ = w.Write(length[:binary.PutUvarint(length[:], uint64(len(value)))])
	_, _ = w.Write(value)
}

// removeRuns deletes the runs file and leaves the directory.
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
	if r.f != nil {
		_ = r.f.Close()
	}
	r.f, r.w, r.spans = nil, nil, nil
	return os.RemoveAll(r.dir)
}

// runTerm is one term of the merged runs.
type runTerm struct {
	key   TermKey
	parts []runPart // in run order, so in slab order
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
			held []byte
			ends []int
		)
		for len(m.items) > 0 {
			term.key = m.items[0].key
			term.parts, held, ends = term.parts[:0], held[:0], ends[:0]
			for len(m.items) > 0 && m.items[0].key == term.key {
				rr := m.items[0]
				held = append(held, rr.value...)
				ends = append(ends, len(held))
				term.parts = append(term.parts, runPart{slab: rr.slab})
				if err := m.advance(); err != nil {
					yield(runTerm{}, err)
					return
				}
			}
			start := 0
			for i, end := range ends {
				term.parts[i].bitmap = held[start:end:end]
				start = end
			}
			if !yield(term, nil) {
				return
			}
		}
	}
}

// runMerge holds the open runs with entries left, the smallest key first
// and, among equal keys, the earliest run.
type runMerge struct {
	heapOf[*runReader]
}

func (r *coldRuns) open() (*runMerge, error) {
	m := &runMerge{}
	m.less = func(a, b *runReader) bool {
		return cmp.Or(bytes.Compare(a.key[:], b.key[:]), cmp.Compare(a.run, b.run)) < 0
	}
	// The runs share 16 MiB of read buffers, between 4 KiB and 64 KiB each.
	bufSize := min(max(16<<20/max(len(r.spans), 1), 4<<10), 64<<10)
	for i, s := range r.spans {
		rr := &runReader{run: i, slab: s.slab}
		name := fmt.Sprintf("%s run %d", r.f.Name(), i)
		if err := rr.start(io.NewSectionReader(r.f, s.off, s.n), name, bufSize, runValueMax); err != nil {
			return nil, err
		}
		if !rr.done {
			m.push(rr)
		}
	}
	return m, nil
}

// advance moves the smallest run to its next entry.
func (m *runMerge) advance() error {
	rr := m.items[0]
	if err := rr.next(); err != nil {
		return err
	}
	if rr.done {
		m.pop()
	} else {
		m.down()
	}
	return nil
}

// runReader reads one run's entries in order.
type runReader struct {
	entryReader

	run  int
	slab uint32
}

// runValueMax bounds one entry's bitmap: a slab's ids serialize to a few
// kilobytes, so anything larger is a corrupt run.
const runValueMax = 1 << 20

// entryReader reads key || uvarint(len) || value entries in order.
type entryReader struct {
	f     *os.File // set by open; nil for a run in the runs file
	name  string   // what errors call the entries' source
	br    *bufio.Reader
	max   uint64 // the longest value accepted
	key   TermKey
	value []byte // valid until the next call to next
	done  bool
}

// open opens path and reads its first entry.
func (r *entryReader) open(path string, bufSize int, maxValue uint64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	r.f = f
	if err := r.start(f, f.Name(), bufSize, maxValue); err != nil {
		_ = f.Close()
		return err
	}
	return nil
}

// start reads entries from src, called name in errors, and reads the first.
func (r *entryReader) start(src io.Reader, name string, bufSize int, maxValue uint64) error {
	r.name, r.br, r.max = name, bufio.NewReaderSize(src, bufSize), maxValue
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
	n, err := binary.ReadUvarint(r.br)
	if err == nil {
		if n > r.max {
			return fmt.Errorf("events: %s: entry of %d bytes", r.name, n)
		}
		r.value = slices.Grow(r.value[:0], int(n))[:n] //nolint:gosec // n is at most r.max
		_, err = io.ReadFull(r.br, r.value)
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF // a key without its entry
		}
		return fmt.Errorf("events: %s: %w", r.name, err)
	}
	return nil
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
