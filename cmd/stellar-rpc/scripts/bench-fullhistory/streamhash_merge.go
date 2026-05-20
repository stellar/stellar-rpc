package main

// streamhash_merge.go is a near-verbatim port of the merge tree from
// github.com/tamirms/streamhash/cmd/bench (commit ca41413750cb,
// 2026-04-10). The upstream code lives under package main of a CLI
// tool and is therefore not importable; the bench harness needs the
// same primitives (fileReader, streamReader, mergeBatch, k-way merge,
// fan-in tree, header scanning) to feed sorted entries from many
// per-chunk .bin files into streamhash.SortedBuilder for the cold
// txhash index build. Behavior is unchanged from upstream — the only
// differences are package=main here and project import paths.
//
// Entry file format (must match the producer in
// bench_ingest_raw_txhash.go):
//
//	header  uint64 LE  entry count
//	entry   [16]byte    key (first 16 bytes of txhash; streamhash routing)
//	        uint32 LE   payload (absolute ledger seq; build phase subtracts MinLedger)
//
// Entries within a file are sorted ascending by big-endian uint64
// prefix of the key — the same ordering streamhash's SortedBuilder
// expects.

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// Entry file constants. benchEntrySize/keySize/payloadBytes are
// derived from the upstream bench tool's on-disk format. maxFanIn
// caps the merge tree's per-node fan-in.
const (
	benchEntrySize = 20
	keySize        = 16
	payloadBytes   = 4
	maxFanIn       = 4
)

// --- File reader ---

type fileReader struct {
	f      *os.File
	buf    []byte
	cursor int
	valid  int
}

func newFileReader(path string, bufsize int) (*fileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(8, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	bufsize = (bufsize / benchEntrySize) * benchEntrySize
	if bufsize < benchEntrySize {
		f.Close()
		return nil, fmt.Errorf("bufsize too small (min %d bytes)", benchEntrySize)
	}
	r := &fileReader{f: f, buf: make([]byte, bufsize)}
	n, _ := io.ReadFull(f, r.buf)
	r.valid = n
	return r, nil
}

func (r *fileReader) advance() bool {
	r.cursor += benchEntrySize
	if r.cursor+benchEntrySize <= r.valid {
		return true
	}
	n, _ := io.ReadFull(r.f, r.buf)
	r.cursor = 0
	r.valid = n
	return n >= benchEntrySize
}

func (r *fileReader) prepareFirst() bool { return r.cursor+benchEntrySize <= r.valid }
func (r *fileReader) entry() []byte      { return r.buf[r.cursor : r.cursor+benchEntrySize] }
func (r *fileReader) prefix() uint64     { return binary.BigEndian.Uint64(r.buf[r.cursor:]) }
func (r *fileReader) close()             { r.f.Close() }

// --- Merge primitives ---

type mergeEntry struct {
	prefix uint64
	idx    int
}

const mergeBatchSize = 4096

type mergeBatch struct {
	data  [mergeBatchSize * benchEntrySize]byte
	count int
}

type streamReader struct {
	ch     <-chan *mergeBatch
	pool   chan *mergeBatch
	batch  *mergeBatch
	cursor int
	prefix uint64
	done   bool
}

func (s *streamReader) advance() bool {
	s.cursor++
	if s.cursor < s.batch.count {
		off := s.cursor * benchEntrySize
		s.prefix = binary.BigEndian.Uint64(s.batch.data[off:])
		return true
	}
	s.pool <- s.batch
	b, ok := <-s.ch
	if !ok {
		s.done = true
		return false
	}
	s.batch = b
	s.cursor = 0
	s.prefix = binary.BigEndian.Uint64(b.data[0:])
	return true
}

func (s *streamReader) entry() []byte {
	off := s.cursor * benchEntrySize
	return s.batch.data[off : off+benchEntrySize]
}

func mergeStream(files []string, bufsize int, out chan<- *mergeBatch, pool chan *mergeBatch) {
	defer close(out)

	readers := make([]*fileReader, len(files))
	defer func() {
		for _, r := range readers {
			if r != nil {
				r.close()
			}
		}
	}()
	for i, path := range files {
		r, err := newFileReader(path, bufsize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mergeStream: open %s: %v\n", path, err)
			return
		}
		readers[i] = r
	}

	heap := make([]mergeEntry, 0, len(files))
	for i, r := range readers {
		if !r.prepareFirst() {
			continue
		}
		heap = append(heap, mergeEntry{prefix: r.prefix(), idx: i})
	}
	heapSize := len(heap)
	for i := heapSize/2 - 1; i >= 0; i-- {
		siftDown(heap, i, heapSize)
	}

	batch := <-pool
	pos := 0

	for heapSize > 0 {
		ri := heap[0].idx
		r := readers[ri]
		off := pos * benchEntrySize
		copy(batch.data[off:off+benchEntrySize], r.entry())
		pos++

		if pos == mergeBatchSize {
			batch.count = pos
			out <- batch
			batch = <-pool
			pos = 0
		}

		if r.advance() {
			heap[0].prefix = r.prefix()
			siftDown(heap, 0, heapSize)
		} else {
			heapSize--
			if heapSize > 0 {
				heap[0] = heap[heapSize]
				siftDown(heap, 0, heapSize)
			}
		}
	}

	if pos > 0 {
		batch.count = pos
		out <- batch
	}
}

func finalMerge(streams []*streamReader, out chan<- *mergeBatch, pool chan *mergeBatch) {
	defer close(out)

	G := len(streams)
	heap := make([]mergeEntry, 0, G)
	for i, s := range streams {
		if !s.done {
			heap = append(heap, mergeEntry{prefix: s.prefix, idx: i})
		}
	}
	heapSize := len(heap)
	for i := heapSize/2 - 1; i >= 0; i-- {
		siftDown(heap, i, heapSize)
	}

	batch := <-pool
	pos := 0

	for heapSize > 0 {
		si := heap[0].idx
		s := streams[si]
		off := pos * benchEntrySize
		copy(batch.data[off:off+benchEntrySize], s.entry())
		pos++

		if pos == mergeBatchSize {
			batch.count = pos
			out <- batch
			batch = <-pool
			pos = 0
		}

		if s.advance() {
			heap[0].prefix = s.prefix
			siftDown(heap, 0, heapSize)
		} else {
			heapSize--
			if heapSize > 0 {
				heap[0] = heap[heapSize]
				siftDown(heap, 0, heapSize)
			}
		}
	}

	if pos > 0 {
		batch.count = pos
		out <- batch
	}
}

func siftDown(h []mergeEntry, i, n int) {
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && h[right].prefix < h[j].prefix {
			j = right
		}
		if h[i].prefix <= h[j].prefix {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}

// --- Merge tree helpers ---

func launchMergeStream(files []string, bufsize int) *streamReader {
	ch := make(chan *mergeBatch, 2)
	pool := make(chan *mergeBatch, 3)
	for range 3 {
		pool <- &mergeBatch{}
	}
	go mergeStream(files, bufsize, ch, pool)

	b, ok := <-ch
	if !ok {
		return &streamReader{done: true}
	}
	return &streamReader{
		ch:     ch,
		pool:   pool,
		batch:  b,
		prefix: binary.BigEndian.Uint64(b.data[0:]),
	}
}

func launchFinalMerge(streams []*streamReader) *streamReader {
	ch := make(chan *mergeBatch, 2)
	pool := make(chan *mergeBatch, 3)
	for range 3 {
		pool <- &mergeBatch{}
	}
	go finalMerge(streams, ch, pool)

	b, ok := <-ch
	if !ok {
		return &streamReader{done: true}
	}
	return &streamReader{
		ch:     ch,
		pool:   pool,
		batch:  b,
		prefix: binary.BigEndian.Uint64(b.data[0:]),
	}
}

// --- Header scanning ---

// scanHeaders concurrently reads the 8-byte LE header of each file
// and returns the sum (total entry count across all files). Adapted
// from streamhash cmd/bench/bench_files.go:scanHeaders — the only
// deviations are wg.Go (Go 1.25) in place of manual Add/Done and a
// comma-ok type assertion on the recorded error.
func scanHeaders(files []string) (uint64, error) {
	var total atomic.Uint64
	var firstErr atomic.Value

	var wg sync.WaitGroup
	for _, path := range files {
		wg.Go(func() {
			f, err := os.Open(path)
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			defer f.Close()
			var header [8]byte
			if _, err := io.ReadFull(f, header[:]); err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			total.Add(binary.LittleEndian.Uint64(header[:]))
		})
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		err, ok := v.(error)
		if !ok {
			return 0, fmt.Errorf("scanHeaders: unexpected error value %T", v)
		}
		return 0, err
	}
	return total.Load(), nil
}
