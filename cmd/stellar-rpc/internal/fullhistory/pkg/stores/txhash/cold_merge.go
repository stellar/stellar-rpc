package txhash

// cold_merge.go is the parallel k-way merge that feeds BuildColdIndex.
// It is a port of streamhash's own cmd/bench sorted-merge (bench_files.go
// + bench_merge.go), adapted to the .bin input format and wired for
// error propagation instead of the bench's best-effort stderr logging.
//
// Shape (a pipelined fan-in tree, all stages running concurrently):
//
//	files ─┬─ leaf mergeStream ─┐
//	       ├─ leaf mergeStream ─┤
//	       │        ...         ├─ finalMerge ─┐
//	       ├─ leaf mergeStream ─┘              ├─ finalMerge ─→ AddKey
//	       └─ leaf mergeStream ─ ... ──────────┘
//
//   - Each leaf goroutine heap-merges a slice of files, reading them with
//     a large whole-entry buffer (zero-copy entry access), and emits
//     fixed-size batches over a pooled channel.
//   - A fan-in tree (mergeFanIn streams per node) collapses the leaves to
//     a single sorted stream.
//   - The main goroutine (feedMergedKeys) drains the final stream and
//     calls SortedBuilder.AddKey, so I/O, merge CPU, and MPHF building all
//     overlap.
//
// Ordering: the heap keys on the full 16-byte key as two big-endian
// uint64s (k0 primary, k1 tiebreak). k0 alone satisfies streamhash's
// block routing (block index is monotonic in the big-endian prefix); the
// k1 tiebreak makes the merge a total order, so the built index is
// byte-identical regardless of input file order — a reproducibility
// property worth keeping for an immutable artifact. The tiebreak only
// fires on a 64-bit prefix collision, so it costs nothing in the common
// case.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/stellar/streamhash"
)

const (
	// mergeBatchSize is the number of entries carried per inter-goroutine
	// batch — large enough to amortize channel hand-off over many entries.
	mergeBatchSize = 4096
	// mergeFanIn caps how many streams one intermediate merge node
	// combines, bounding each node's heap depth.
	mergeFanIn = 4
	// mergeFileBufBytes is the per-file read buffer, floored to a whole
	// number of entries. Throughput is flat from 64 KiB to 512 KiB and
	// degrades above it (1–4 MiB) on a cold Linux NVMe O_DIRECT sweep of
	// real data (BenchmarkRealMergeBufBytes): the merge is CPU-bound there
	// (~600 MB/s), well under the device's ~3 GB/s, so it is not IOPS-bound
	// and larger reads only add memory traffic for no gain. 128 KiB sits in
	// the flat region and is kept. (Warm cache was likewise flat 16–128 KiB,
	// BenchmarkMergeBufBytes.)
	mergeFileBufBytes = 128 << 10
	// mergePoolDepth / mergeChanDepth size each stage's batch pool and
	// hand-off channel.
	mergePoolDepth = 3
	mergeChanDepth = 2
	// blockSize is the alignment (bytes) for O_DIRECT reads: buffer
	// address, file offset, and read length must all be multiples of it.
	// 4 KiB covers every common logical block size (and is a multiple of
	// 512). Buffers are floored to at least 2 blocks so a single 20-byte
	// entry — offset 8 bytes past the start by the header — can never
	// straddle more than one block boundary within a buffer.
	blockSize = 4096
)

// ──────────────────────────────────────────────────────────────────
// Coordinator — first error wins and cancels the whole pipeline.
// ──────────────────────────────────────────────────────────────────

// merger carries the cancellation + first-error state shared by every
// goroutine in one merge tree. A producer that hits an error calls fail,
// which records the cause and cancels ctx so the other goroutines unwind;
// every blocking channel op selects on ctx.Done so none of them hang.
type merger struct {
	parent context.Context //nolint:containedctx // op-scoped fan-in coordinator
	ctx    context.Context //nolint:containedctx // derived cancel ctx for all goroutines
	cancel context.CancelFunc
	once   sync.Once
	mu     sync.Mutex
	err    error
}

func newMerger(parent context.Context) *merger {
	ctx, cancel := context.WithCancel(parent)
	return &merger{parent: parent, ctx: ctx, cancel: cancel}
}

// fail records the first error and cancels the pipeline. Later calls are
// no-ops, so the root cause is what surfaces.
func (m *merger) fail(err error) {
	m.once.Do(func() {
		m.mu.Lock()
		m.err = err
		m.mu.Unlock()
		m.cancel()
	})
}

// firstErr returns the recorded producer failure, or the parent
// context's error if the caller canceled the build. nil on clean
// completion.
func (m *merger) firstErr() error {
	m.mu.Lock()
	err := m.err
	m.mu.Unlock()
	if err != nil {
		return err
	}
	return m.parent.Err()
}

// stop releases the derived context. Safe to call once the pipeline has
// drained; idempotent.
func (m *merger) stop() { m.cancel() }

// send delivers b on out, returning false if the pipeline was canceled.
func (m *merger) send(out chan<- *mergeBatch, b *mergeBatch) bool {
	select {
	case out <- b:
		return true
	case <-m.ctx.Done():
		return false
	}
}

// take pulls a recycled batch from pool, returning nil if canceled.
func (m *merger) take(pool chan *mergeBatch) *mergeBatch {
	select {
	case b := <-pool:
		return b
	case <-m.ctx.Done():
		return nil
	}
}

// ──────────────────────────────────────────────────────────────────
// Batches and the merge heap.
// ──────────────────────────────────────────────────────────────────

// mergeBatch is the unit of inter-goroutine hand-off: up to
// mergeBatchSize fixed-width entries.
type mergeBatch struct {
	data  [mergeBatchSize * binEntrySize]byte
	count int
}

func newBatchPool() chan *mergeBatch {
	pool := make(chan *mergeBatch, mergePoolDepth)
	for range mergePoolDepth {
		pool <- &mergeBatch{}
	}
	return pool
}

// mergeEntry is one source's heap slot: the two big-endian halves of its
// current 16-byte key plus the source index (into the readers/streams
// slice of the owning merge node).
type mergeEntry struct {
	k0, k1 uint64
	idx    int
}

// less reports whether e sorts before o by full-16-byte big-endian key.
func (e mergeEntry) less(o mergeEntry) bool {
	if e.k0 != o.k0 {
		return e.k0 < o.k0
	}
	return e.k1 < o.k1
}

// siftDown restores the min-heap rooted at i over h[:n].
func siftDown(h []mergeEntry, i, n int) {
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && h[right].less(h[j]) {
			j = right
		}
		if !h[j].less(h[i]) { // h[i] <= h[j]: heap property already holds
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}

// ──────────────────────────────────────────────────────────────────
// Leaf source — one .bin file, read O_DIRECT into a block-aligned
// buffer with zero per-entry copies.
// ──────────────────────────────────────────────────────────────────

// openDirect opens path for the merge. On Linux it requests O_DIRECT
// (page-cache bypass); if the filesystem rejects it (EINVAL — e.g.
// tmpfs, some network mounts) it retries with a cached open, since the
// aligned-ReadAt path below is correct either way. On non-Linux
// directOpenFlag is 0, so this is a plain read-only open.
func openDirect(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|directOpenFlag(), 0)
	if err != nil && directOpenFlag() != 0 && errors.Is(err, syscall.EINVAL) {
		fd, err = syscall.Open(path, syscall.O_RDONLY, 0)
	}
	if err != nil {
		return nil, fmt.Errorf("txhash: open %s: %w", path, err)
	}
	return os.NewFile(uintptr(fd), path), nil //nolint:gosec // fd is a valid descriptor from syscall.Open
}

// alignedBuffer returns (aligned, backing): aligned is a size-byte slice
// starting on a blockSize boundary (required for O_DIRECT), and backing
// is the real allocation. The caller must keep backing reachable so the
// GC doesn't free it while the aligned sub-slice is in use (Go's GC is
// non-moving, so the alignment holds for the slice's lifetime).
func alignedBuffer(size int) ([]byte, []byte) {
	backing := make([]byte, size+blockSize)
	// Distance from the allocation start to the next blockSize boundary.
	misalign := int(uintptr(unsafe.Pointer(&backing[0])) % blockSize)
	off := (blockSize - misalign) % blockSize
	return backing[off : off+size : off+size], backing
}

// fileReader streams entries out of one .bin file with zero per-entry
// copies: entries are sliced directly out of buf, which is filled by
// block-aligned ReadAt calls. The 8-byte header lives in the first
// buffer and is skipped via the initial cursor; the count was validated
// in scanAndValidate, so EOF bounds the stream at exactly count entries.
type fileReader struct {
	path    string
	f       *os.File
	buf     []byte // block-aligned sub-slice of bufBase
	bufBase []byte // backing allocation, kept referenced for the GC
	cursor  int
	valid   int
	fileOff int64 // file offset of buf[0]; always a blockSize multiple
	err     error
}

func newFileReader(path string, bufBytes int) (*fileReader, error) {
	if bufBytes < 2*blockSize {
		bufBytes = 2 * blockSize
	}
	if rem := bufBytes % blockSize; rem != 0 {
		bufBytes += blockSize - rem
	}
	f, err := openDirect(path)
	if err != nil {
		return nil, err
	}
	buf, backing := alignedBuffer(bufBytes)
	// Read from offset 0 so the 8-byte header lands in buf[0:8]; cursor
	// starts past it. ReadAt returns io.EOF on a short read at end of
	// file, which is expected for files smaller than the buffer.
	n, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		_ = f.Close()
		return nil, fmt.Errorf("txhash: read %s: %w", path, err)
	}
	if n < binHeaderSize {
		_ = f.Close()
		return nil, fmt.Errorf("txhash: %s too short for header (%d bytes)", path, n)
	}
	return &fileReader{
		path:    path,
		f:       f,
		buf:     buf,
		bufBase: backing,
		cursor:  binHeaderSize,
		valid:   n,
	}, nil
}

func (r *fileReader) prepareFirst() bool { return r.cursor+binEntrySize <= r.valid }
func (r *fileReader) entry() []byte      { return r.buf[r.cursor : r.cursor+binEntrySize] }
func (r *fileReader) k0() uint64         { return binary.BigEndian.Uint64(r.buf[r.cursor:]) }
func (r *fileReader) k1() uint64         { return binary.BigEndian.Uint64(r.buf[r.cursor+8:]) }
func (r *fileReader) close()             { _ = r.f.Close() }

// advance moves to the next entry. On buffer exhaustion it re-reads at
// the largest block-aligned offset that still covers the next entry,
// re-reading up to blockSize-1 bytes (negligible with a large buffer)
// to keep the offset aligned for O_DIRECT. Returns false at end of file
// or on a read error (check r.err).
func (r *fileReader) advance() bool {
	r.cursor += binEntrySize
	if r.cursor+binEntrySize <= r.valid {
		return true
	}
	filePos := r.fileOff + int64(r.cursor)
	newOff := filePos &^ int64(blockSize-1)
	n, err := r.f.ReadAt(r.buf, newOff)
	if err != nil && !errors.Is(err, io.EOF) {
		r.err = fmt.Errorf("txhash: read %s: %w", r.path, err)
		return false
	}
	r.fileOff = newOff
	r.valid = n
	r.cursor = int(filePos - newOff)
	return r.cursor+binEntrySize <= r.valid
}

// mergeStream is a leaf goroutine: heap-merge files into sorted batches
// on out, recycling batches from pool. close(out) signals completion;
// any error is reported via m.fail.
func (m *merger) mergeStream(files []string, bufBytes int, out chan<- *mergeBatch, pool chan *mergeBatch) {
	defer close(out)

	readers := make([]*fileReader, 0, len(files))
	defer func() {
		for _, r := range readers {
			r.close()
		}
	}()

	h := make([]mergeEntry, 0, len(files))
	for _, path := range files {
		r, err := newFileReader(path, bufBytes)
		if err != nil {
			m.fail(err)
			return
		}
		readers = append(readers, r)
		if r.prepareFirst() {
			h = append(h, mergeEntry{k0: r.k0(), k1: r.k1(), idx: len(readers) - 1})
		}
	}
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		siftDown(h, i, n)
	}

	batch := m.take(pool)
	if batch == nil {
		return
	}
	pos := 0
	for n > 0 {
		r := readers[h[0].idx]
		copy(batch.data[pos*binEntrySize:], r.entry())
		pos++
		if pos == mergeBatchSize {
			batch.count = pos
			if !m.send(out, batch) {
				return
			}
			if batch = m.take(pool); batch == nil {
				return
			}
			pos = 0
		}

		if r.advance() {
			h[0] = mergeEntry{k0: r.k0(), k1: r.k1(), idx: h[0].idx}
			siftDown(h, 0, n)
		} else {
			if r.err != nil {
				m.fail(r.err)
				return
			}
			n--
			if n > 0 {
				h[0] = h[n]
				siftDown(h, 0, n)
			}
		}
	}
	if pos > 0 {
		batch.count = pos
		m.send(out, batch) // last batch; nothing to do if canceled
	}
}

// ──────────────────────────────────────────────────────────────────
// Intermediate / final source — a sorted stream over a channel.
// ──────────────────────────────────────────────────────────────────

// streamReader consumes one producer's sorted batch stream, exposing the
// same current-entry view as fileReader. Consumed batches are recycled to
// the producer's pool.
type streamReader struct {
	ch    <-chan *mergeBatch
	pool  chan *mergeBatch
	batch *mergeBatch
	cur   int
	done  bool
}

func (s *streamReader) entry() []byte {
	off := s.cur * binEntrySize
	return s.batch.data[off : off+binEntrySize]
}
func (s *streamReader) k0() uint64 { return binary.BigEndian.Uint64(s.batch.data[s.cur*binEntrySize:]) }
func (s *streamReader) k1() uint64 {
	return binary.BigEndian.Uint64(s.batch.data[s.cur*binEntrySize+8:])
}

// advance steps to the next entry, recycling the spent batch and pulling
// the next one when the current batch is exhausted. Returns false when
// the stream ends or the pipeline is canceled (m.ctx done).
func (s *streamReader) advance(m *merger) bool {
	s.cur++
	if s.cur < s.batch.count {
		return true
	}
	if !m.send(s.pool, s.batch) {
		s.done = true
		return false
	}
	b, ok := s.recv(m)
	if !ok {
		s.done = true
		return false
	}
	s.batch = b
	s.cur = 0
	return true
}

func (s *streamReader) recv(m *merger) (*mergeBatch, bool) {
	select {
	case b, ok := <-s.ch:
		return b, ok
	case <-m.ctx.Done():
		return nil, false
	}
}

// finalMerge is an intermediate or top-level goroutine: heap-merge
// streams into sorted batches on out, recycling from pool.
func (m *merger) finalMerge(streams []*streamReader, out chan<- *mergeBatch, pool chan *mergeBatch) {
	defer close(out)

	h := make([]mergeEntry, 0, len(streams))
	for i, s := range streams {
		if !s.done {
			h = append(h, mergeEntry{k0: s.k0(), k1: s.k1(), idx: i})
		}
	}
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		siftDown(h, i, n)
	}

	batch := m.take(pool)
	if batch == nil {
		return
	}
	pos := 0
	for n > 0 {
		s := streams[h[0].idx]
		copy(batch.data[pos*binEntrySize:], s.entry())
		pos++
		if pos == mergeBatchSize {
			batch.count = pos
			if !m.send(out, batch) {
				return
			}
			if batch = m.take(pool); batch == nil {
				return
			}
			pos = 0
		}

		if s.advance(m) {
			h[0] = mergeEntry{k0: s.k0(), k1: s.k1(), idx: h[0].idx}
			siftDown(h, 0, n)
		} else {
			if m.ctx.Err() != nil { // canceled, not a clean stream end
				return
			}
			n--
			if n > 0 {
				h[0] = h[n]
				siftDown(h, 0, n)
			}
		}
	}
	if pos > 0 {
		batch.count = pos
		m.send(out, batch)
	}
}

// ──────────────────────────────────────────────────────────────────
// Tree assembly + the AddKey consumer.
// ──────────────────────────────────────────────────────────────────

// newStream launches producer and returns a primed streamReader over its
// output. Priming pulls the first batch so finalMerge can seed its heap.
func (m *merger) newStream(ch chan *mergeBatch, pool chan *mergeBatch) *streamReader {
	s := &streamReader{ch: ch, pool: pool}
	select {
	case b, ok := <-ch:
		if !ok {
			s.done = true
		} else {
			s.batch = b
		}
	case <-m.ctx.Done():
		s.done = true
	}
	return s
}

func (m *merger) launchMergeStream(files []string, bufBytes int) *streamReader {
	ch, pool := make(chan *mergeBatch, mergeChanDepth), newBatchPool()
	go m.mergeStream(files, bufBytes, ch, pool)
	return m.newStream(ch, pool)
}

func (m *merger) launchFinalMerge(streams []*streamReader) *streamReader {
	ch, pool := make(chan *mergeBatch, mergeChanDepth), newBatchPool()
	go m.finalMerge(streams, ch, pool)
	return m.newStream(ch, pool)
}

// buildMergeTree spawns the leaf merge goroutines over the partitioned
// files, collapses them through a fan-in tree, and returns the top-level
// sorted stream plus its batch pool. inputs must be non-empty.
func (m *merger) buildMergeTree(inputs []string, numLeaves, bufBytes int) (<-chan *mergeBatch, chan *mergeBatch) {
	perGroup := (len(inputs) + numLeaves - 1) / numLeaves
	var streams []*streamReader
	for i := 0; i < len(inputs); i += perGroup {
		streams = append(streams, m.launchMergeStream(inputs[i:min(i+perGroup, len(inputs))], bufBytes))
	}

	for len(streams) > mergeFanIn {
		var next []*streamReader
		for i := 0; i < len(streams); i += mergeFanIn {
			group := streams[i:min(i+mergeFanIn, len(streams))]
			if len(group) == 1 {
				next = append(next, group[0])
			} else {
				next = append(next, m.launchFinalMerge(group))
			}
		}
		streams = next
	}

	finalCh, finalPool := make(chan *mergeBatch, mergeChanDepth), newBatchPool()
	go m.finalMerge(streams, finalCh, finalPool)
	return finalCh, finalPool
}

// feedMergedKeys drains the merged stream and feeds SortedBuilder.AddKey,
// converting each entry's absolute seq into its MinLedger offset payload.
// On a consumer-side error it cancels the pipeline and drains so every
// producer goroutine unwinds before returning. Returns the number of keys
// added.
func feedMergedKeys(
	builder *streamhash.SortedBuilder,
	finalCh <-chan *mergeBatch,
	finalPool chan *mergeBatch,
	m *merger,
	minLedger uint32,
) (uint64, error) {
	var added uint64
	for batch := range finalCh {
		data := batch.data[:batch.count*binEntrySize]
		for off := 0; off < len(data); off += binEntrySize {
			entry := data[off : off+binEntrySize]
			seq := binary.LittleEndian.Uint32(entry[binKeySize:])
			if seq < minLedger {
				return drainAndFail(finalCh, m, added,
					fmt.Errorf("txhash: entry seq %d below index MinLedger %d", seq, minLedger))
			}
			payload := uint64(seq - minLedger)
			if payload > coldPayloadMax {
				return drainAndFail(finalCh, m, added,
					fmt.Errorf("txhash: ledger offset %d exceeds %d-byte payload budget (seq=%d minLedger=%d)",
						payload, ColdPayloadSize, seq, minLedger))
			}
			if err := builder.AddKey(entry[:binKeySize], payload); err != nil {
				return drainAndFail(finalCh, m, added, fmt.Errorf("txhash: add key %d: %w", added, err))
			}
			added++
		}
		if !m.send(finalPool, batch) { // recycle; canceled => producers exiting
			break
		}
	}
	if err := m.firstErr(); err != nil {
		return added, err
	}
	return added, nil
}

// drainAndFail cancels the pipeline with cause, then drains finalCh until
// it closes so no producer goroutine is left blocked on a send.
func drainAndFail(finalCh <-chan *mergeBatch, m *merger, added uint64, cause error) (uint64, error) {
	m.fail(cause)
	for range finalCh { //nolint:revive // intentional drain to unblock producers
	}
	return added, cause
}
