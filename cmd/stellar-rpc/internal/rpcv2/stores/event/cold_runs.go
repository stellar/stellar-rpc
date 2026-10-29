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
)

// coldRuns are a chunk's postings spilled one slab at a time. Run i holds
// the terms of one slab in key order, each as
// key || uvarint(len) || serialized bitmap. Merging the runs yields every
// term once, in key order, with its bitmap from each run holding it.
type coldRuns struct {
	dir   string
	paths []string
	slabs []uint32 // each run's slab
}

// spill writes slab's terms, in key order with their serialized bitmaps, as
// the next run.
func (r *coldRuns) spill(slab uint32, terms iter.Seq2[TermKey, []byte]) error {
	if len(r.paths) == 0 {
		if err := os.MkdirAll(r.dir, 0o755); err != nil {
			return err
		}
	}
	path := filepath.Join(r.dir, fmt.Sprintf("%05d", len(r.paths)))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	err = writeRun(f, terms)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	r.paths = append(r.paths, path)
	r.slabs = append(r.slabs, slab)
	return nil
}

func writeRun(f *os.File, terms iter.Seq2[TermKey, []byte]) error {
	w := bufio.NewWriterSize(f, 1<<16)
	for key, bitmap := range terms {
		writeEntry(w, key, bitmap)
	}
	return w.Flush()
}

// writeEntry writes key || uvarint(len(value)) || value. A bufio.Writer
// keeps the first error and reports it from Flush.
func writeEntry(w *bufio.Writer, key TermKey, value []byte) {
	var length [binary.MaxVarintLen64]byte
	_, _ = w.Write(key[:])
	_, _ = w.Write(length[:binary.PutUvarint(length[:], uint64(len(value)))])
	_, _ = w.Write(value)
}

// removeRuns deletes the run files and leaves the directory.
func (r *coldRuns) removeRuns() error {
	for _, path := range r.paths {
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	r.paths, r.slabs = nil, nil
	return nil
}

// remove deletes the runs and the directory holding them.
func (r *coldRuns) remove() error {
	r.paths, r.slabs = nil, nil
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
		defer m.close()
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
	bufSize := min(max(16<<20/max(len(r.paths), 1), 4<<10), 64<<10)
	for i, path := range r.paths {
		rr := &runReader{run: i, slab: r.slabs[i]}
		if err := rr.open(path, bufSize, runValueMax); err != nil {
			return m, err
		}
		if rr.done {
			_ = rr.f.Close()
			continue
		}
		m.push(rr)
	}
	return m, nil
}

func (m *runMerge) close() {
	for _, rr := range m.items {
		_ = rr.f.Close()
	}
}

// advance moves the smallest run to its next entry.
func (m *runMerge) advance() error {
	rr := m.items[0]
	if err := rr.next(); err != nil {
		return err
	}
	if rr.done {
		m.pop()
		_ = rr.f.Close()
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
	f     *os.File
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
	r.f, r.br, r.max = f, bufio.NewReaderSize(f, bufSize), maxValue
	if err := r.next(); err != nil {
		_ = f.Close()
		return err
	}
	return nil
}

func (r *entryReader) next() error {
	if _, err := io.ReadFull(r.br, r.key[:]); err != nil {
		if errors.Is(err, io.EOF) {
			r.done = true
			return nil
		}
		return fmt.Errorf("events: %s: %w", r.f.Name(), err)
	}
	n, err := binary.ReadUvarint(r.br)
	if err == nil {
		if n > r.max {
			return fmt.Errorf("events: %s: entry of %d bytes", r.f.Name(), n)
		}
		r.value = slices.Grow(r.value[:0], int(n))[:n] //nolint:gosec // n is at most r.max
		_, err = io.ReadFull(r.br, r.value)
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF // a key without its entry
		}
		return fmt.Errorf("events: %s: %w", r.f.Name(), err)
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
