package event

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// coldRuns are a chunk's postings spilled one slab at a time. Run i holds
// the terms of the i-th spill in key order, each as
// key || uvarint(len) || serialized bitmap. Merging the runs yields every
// term once, in key order, with the union of its bitmaps.
type coldRuns struct {
	dir   string
	paths []string
}

// spill writes the slab's terms as the next run.
func (r *coldRuns) spill(s *hotSlab) (err error) {
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
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	w := bufio.NewWriterSize(f, 1<<16)
	var length [binary.MaxVarintLen64]byte
	for key, bitmap := range s.terms() {
		// A bufio.Writer keeps the first error and reports it from Flush.
		_, _ = w.Write(key[:])
		_, _ = w.Write(length[:binary.PutUvarint(length[:], uint64(len(bitmap)))])
		_, _ = w.Write(bitmap)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	r.paths = append(r.paths, path)
	return nil
}

// remove deletes the runs.
func (r *coldRuns) remove() error {
	r.paths = nil
	return os.RemoveAll(r.dir)
}

// runTerm is one term of the merged runs.
type runTerm struct {
	key    TermKey
	bitmap *roaring.Bitmap // the term's event ids
}

// terms merges the runs: every term once, in key order, with the union of
// its slabs' bitmaps.
func (r *coldRuns) terms() iter.Seq2[runTerm, error] { return r.merge(true) }

// keys is terms without the bitmaps.
func (r *coldRuns) keys() iter.Seq2[TermKey, error] {
	return func(yield func(TermKey, error) bool) {
		for term, err := range r.merge(false) {
			if !yield(term.key, err) {
				return
			}
		}
	}
}

func (r *coldRuns) merge(withBitmaps bool) iter.Seq2[runTerm, error] {
	return func(yield func(runTerm, error) bool) {
		m, err := r.open(withBitmaps)
		defer m.close()
		if err != nil {
			yield(runTerm{}, err)
			return
		}
		for len(m.items) > 0 {
			term, err := m.next()
			if err != nil {
				yield(runTerm{}, err)
				return
			}
			if !yield(term, nil) {
				return
			}
		}
	}
}

// count returns how many distinct terms the runs hold.
func (r *coldRuns) count() (uint64, error) {
	var n uint64
	for _, err := range r.keys() {
		if err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}

// runMerge holds the open runs with entries left, the smallest key first.
type runMerge struct {
	heapOf[*runReader]

	withBitmaps bool
}

func (r *coldRuns) open(withBitmaps bool) (*runMerge, error) {
	m := &runMerge{withBitmaps: withBitmaps}
	m.less = func(a, b *runReader) bool { return bytes.Compare(a.key[:], b.key[:]) < 0 }
	for _, path := range r.paths {
		rr, err := openRun(path, withBitmaps)
		if err != nil {
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

// next takes the smallest key out of every run holding it.
func (m *runMerge) next() (runTerm, error) {
	term := runTerm{key: m.items[0].key}
	if m.withBitmaps {
		term.bitmap = roaring.New()
	}
	for len(m.items) > 0 && m.items[0].key == term.key {
		rr := m.items[0]
		if m.withBitmaps {
			// UnmarshalBinary copies: the reader's buffer is reused.
			part := roaring.New()
			if err := part.UnmarshalBinary(rr.value); err != nil {
				return runTerm{}, fmt.Errorf("events: run %s: decode bitmap: %w", rr.f.Name(), err)
			}
			term.bitmap.Or(part)
		}
		if err := rr.next(); err != nil {
			return runTerm{}, err
		}
		if rr.done {
			m.pop()
			_ = rr.f.Close()
		} else {
			m.down()
		}
	}
	return term, nil
}

// runReader reads one run's entries in order, skipping the bitmaps unless
// asked for them.
type runReader struct {
	f           *os.File
	br          *bufio.Reader
	withBitmaps bool
	key         TermKey
	value       []byte // the bitmap, valid until the next call to next
	done        bool
}

func openRun(path string, withBitmaps bool) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rr := &runReader{f: f, br: bufio.NewReaderSize(f, 1<<16), withBitmaps: withBitmaps}
	if err := rr.next(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return rr, nil
}

// runValueMax bounds one entry's bitmap: a slab's ids serialize to a few
// kilobytes, so anything larger is a corrupt run.
const runValueMax = 1 << 20

func (rr *runReader) next() error {
	if _, err := io.ReadFull(rr.br, rr.key[:]); err != nil {
		if errors.Is(err, io.EOF) {
			rr.done = true
			return nil
		}
		return fmt.Errorf("events: run %s: %w", rr.f.Name(), err)
	}
	n, err := binary.ReadUvarint(rr.br)
	if err == nil {
		if n > runValueMax {
			return fmt.Errorf("events: run %s: entry of %d bytes", rr.f.Name(), n)
		}
		if rr.withBitmaps {
			rr.value = slices.Grow(rr.value[:0], int(n))[:n]
			_, err = io.ReadFull(rr.br, rr.value)
		} else {
			_, err = rr.br.Discard(int(n))
		}
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF // a key without its entry
		}
		return fmt.Errorf("events: run %s: %w", rr.f.Name(), err)
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
