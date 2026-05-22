package main

// streamhash_merge.go is a near-verbatim port of the merge tree from
// github.com/tamirms/streamhash/cmd/bench (commit ca41413750cb,
// 2026-04-10). The upstream code lives under package main of a CLI
// tool and is therefore not importable; the bench harness needs the
// same primitives (fileReader, streamReader, mergeBatch, k-way merge,
// fan-in tree, header scanning) to feed sorted entries from many
// per-chunk .bin files into streamhash.SortedBuilder for the cold
// txhash index build. Behavior is unchanged from upstream — the only
// differences are package=main here, project import paths, and the
// fileReader's aligned-ReadAt refill pattern (described below).
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
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// Entry file constants. benchEntrySize/keySize/payloadBytes are
// derived from the upstream bench tool's on-disk format. maxFanIn
// caps the merge tree's per-node fan-in. blockSize is the alignment
// we use for buffers and read offsets — required by O_DIRECT, and a
// no-op everywhere else.
const (
	benchEntrySize = 20
	keySize        = 16
	payloadBytes   = 4
	maxFanIn       = 4
	blockSize      = 4096
)

// --- File reader ---
//
// One code path for both buffered and O_DIRECT modes: the file is
// always opened in read-only mode (with O_DIRECT|O_RDONLY when
// requested), read via ReadAt at block-aligned offsets into a
// block-aligned user buffer. The only platform-specific bit is the
// open() flag, which lives in file_open_{linux,other}.go.
//
// On Linux with O_DIRECT: reads bypass the page cache, going DMA
// straight into our aligned buffer. On Linux without O_DIRECT, or
// on any non-Linux platform, the same aligned-ReadAt code runs and
// reads go through the page cache as normal — alignment is harmless
// when not strictly required.

type fileReader struct {
	f      *os.File
	buf    []byte // aligned sub-slice of bufBase
	cursor int
	valid  int
	// bufBase holds the original (unaligned) backing allocation. Kept
	// referenced so GC doesn't free it while buf — a slice into it —
	// is in use. Relies on the Go runtime's non-moving GC.
	bufBase []byte
	// fileOff is the file offset of buf[0]. Always a multiple of
	// blockSize; cursor + fileOff gives the absolute file position
	// of the next entry to read.
	fileOff int64
}

// newFileReader opens path (optionally with O_DIRECT on platforms
// that support it), allocates a block-aligned buffer of at least
// 2*blockSize, and primes it with the first read so the 8-byte file
// header is already skipped past when the first entry() call
// returns.
//
// The 2-block minimum is required because the file's 8-byte header
// offsets every entry by 8, so a 20-byte entry can straddle one
// block boundary — a single-block buffer would be unable to read
// such an entry in any single aligned read. A 1 MiB default is
// preferred in practice: large enough to amortize syscall overhead
// and keep the NVMe queue fed under O_DIRECT (where kernel readahead
// is unavailable), small enough that resident memory stays bounded
// at hundreds of MB even with 1000 files open.
func newFileReader(path string, bufsize int, oDirect bool) (*fileReader, error) {
	const minBufsize = 2 * blockSize
	if bufsize < minBufsize {
		bufsize = minBufsize
	}
	if bufsize%blockSize != 0 {
		bufsize = ((bufsize + blockSize - 1) / blockSize) * blockSize
	}

	f, err := openBinFile(path, oDirect)
	if err != nil {
		return nil, err
	}

	buf, backing := alignedBuffer(bufsize)
	n, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		f.Close()
		return nil, fmt.Errorf("ReadAt[0] %s: %w", path, err)
	}
	// Require the 8-byte header but not any entries — zero-entry files
	// are legal (downstream prepareFirst returns false and the file
	// contributes nothing). Matches the pre-unification bufio behavior.
	if n < 8 {
		f.Close()
		return nil, fmt.Errorf("file %s too short for header (%d bytes)", path, n)
	}
	return &fileReader{
		f:       f,
		buf:     buf,
		bufBase: backing,
		cursor:  8, // skip 8-byte LE entry-count header
		valid:   n,
		fileOff: 0,
	}, nil
}

// advance steps past the current entry. On buffer exhaustion it
// re-reads at the largest block-aligned offset that still includes
// the next unconsumed entry. The few re-read bytes at the start of
// each refill (up to blockSize-1) are wasted bandwidth, but with
// bufsize >= 1 MiB the overhead is well under 1%.
//
// Tail-of-file is handled by the kernel returning n < bufsize. The
// O_DIRECT size-alignment rule applies to the *requested* size only
// (always blockSize-aligned here), not to what's actually returned,
// so reads that cross EOF just yield a short return without error.
func (r *fileReader) advance() bool {
	r.cursor += benchEntrySize
	if r.cursor+benchEntrySize <= r.valid {
		return true
	}
	filePos := r.fileOff + int64(r.cursor)
	newOff := filePos &^ int64(blockSize-1)
	n, _ := r.f.ReadAt(r.buf, newOff)
	r.fileOff = newOff
	r.valid = n
	r.cursor = int(filePos - newOff)
	return r.cursor+benchEntrySize <= r.valid
}

func (r *fileReader) prepareFirst() bool { return r.cursor+benchEntrySize <= r.valid }
func (r *fileReader) entry() []byte      { return r.buf[r.cursor : r.cursor+benchEntrySize] }
func (r *fileReader) prefix() uint64     { return binary.BigEndian.Uint64(r.buf[r.cursor:]) }
func (r *fileReader) close()             { r.f.Close() }

// alignedBuffer returns a sub-slice of size that starts on a
// blockSize boundary. The returned backing slice holds the original
// allocation; callers must keep it reachable so GC doesn't free it
// while the aligned slice is still in use.
func alignedBuffer(size int) (aligned, backing []byte) {
	backing = make([]byte, size+blockSize)
	addr := uintptr(unsafe.Pointer(&backing[0]))
	off := int(addr & (blockSize - 1))
	if off != 0 {
		off = blockSize - off
	}
	aligned = backing[off : off+size : off+size]
	return aligned, backing
}

// openBinFile is the only platform-specific code path. See
// file_open_linux.go (sets O_DIRECT when requested) and
// file_open_other.go (ignores oDirect; cache-skip is best-effort
// since non-Linux platforms either lack O_DIRECT or expose it
// differently).
func openBinFile(path string, oDirect bool) (*os.File, error) {
	flag := syscall.O_RDONLY
	if oDirect {
		flag |= platformDirectFlag
	}
	fd, err := syscall.Open(path, flag, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s (o_direct=%v): %w", path, oDirect, err)
	}
	return os.NewFile(uintptr(fd), path), nil
}

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

func mergeStream(files []string, bufsize int, oDirect bool, out chan<- *mergeBatch, pool chan *mergeBatch) {
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
		r, err := newFileReader(path, bufsize, oDirect)
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

func launchMergeStream(files []string, bufsize int, oDirect bool) *streamReader {
	ch := make(chan *mergeBatch, 2)
	pool := make(chan *mergeBatch, 3)
	for range 3 {
		pool <- &mergeBatch{}
	}
	go mergeStream(files, bufsize, oDirect, ch, pool)

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
