// Package packfile implements an immutable, append-only file format with O(1)
// positional access. Items are accumulated into fixed-size records, optionally
// compressed (zstd) or CRC-protected, and indexed by a compact FOR-encoded
// offset table. An optional chunked SHA-256 content hash covers the logical
// item stream for end-to-end integrity.
//
// The package provides only the writer; the reader is a separate compilation
// unit introduced in a follow-up.
package packfile

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/intpack"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

const defaultItemsPerRecord = 128

// ErrWriterClosed is returned by AppendItem or Finish after the Writer has
// been closed (successfully finalized or aborted).
var ErrWriterClosed = errors.New("packfile: writer is closed")

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
	// ItemsPerRecord is the number of items per record. 0 defaults to 128.
	ItemsPerRecord int

	// Format controls record encoding. Default (zero value) is Compressed.
	// Compressed: zstd with built-in integrity.
	// Uncompressed: raw records with crc32c integrity.
	// Raw: raw records with no integrity wrapper.
	Format RecordFormat

	// Concurrency sets the number of parallel compression goroutines.
	// 0 or 1 means serial. Values above runtime.NumCPU() tend to hurt
	// throughput because scheduler contention dominates the parallelism
	// benefit; pick a value ≤ NumCPU.
	Concurrency int

	// ContentHash enables SHA-256 content hashing over the logical item stream.
	ContentHash bool

	// BytesPerSync initiates background writeback of dirty pages every N bytes
	// written. On Linux this uses sync_file_range(SYNC_FILE_RANGE_WRITE) which
	// is non-blocking — it tells the kernel to start flushing without waiting.
	// This spreads I/O across the write phase so the final fsync in Finish()
	// has less data to flush. 0 disables (default).
	BytesPerSync int

	// Overwrite allows Create to replace an existing file at the path.
	// When false (default), Create fails if the file already exists.
	// When true, any existing file is truncated to zero length. Existing
	// ownership, permissions, and hardlinks are preserved; symlinks are
	// followed (the target is truncated).
	Overwrite bool
}

type blockResult struct {
	blockID   uint32
	data      []byte   // payload → format-processed payload
	forIndex  []byte   // FOR index: [packed][1B W][4B min][4B crc32c]; nil when itemsPerRecord==1
	hashSizes []uint32 // item sizes for hash goroutine
	digest    [sha256.Size]byte
	hasHash   bool
	err       error
}

type hashWork struct {
	data      []byte
	hashSizes []uint32
}

// Writer creates a new packfile with item-level semantics.
// Items are accumulated into records of itemsPerRecord items each,
// format-processed (compressed/CRC/raw), and written with an offset index.
//
// A Writer must be used by a single goroutine; concurrent AppendItem, Finish,
// or Close calls from multiple goroutines are not safe. Internally, a
// concurrent Writer spawns a background pipeline, but the caller-facing API is
// single-threaded.
type Writer struct {
	// File I/O
	file         *os.File
	path         string
	pos          int64
	offsets      []int64
	bytesPerSync int64
	lastSyncPos  int64

	// Record accumulation
	buf            []byte
	sizes          []uint32
	total          int
	itemsPerRecord int
	format         RecordFormat
	compressor     *zstd.Compressor

	// Content hash
	contentHash  bool
	serialHasher *contentHasher // serial path: streams items through contentHasher
	digestHasher hash.Hash      // concurrent path: running SHA-256 over chunk digests
	sizesPool    sync.Pool      // concurrent path: pooled []uint32 for hash goroutines

	// Pipeline (concurrency > 1)
	concurrency int
	nextBlockID uint32
	workCh      chan blockResult
	resultCh    chan blockResult
	writerDone  chan error
	// cancelCh is closed on the first fatal error. Workers and the main
	// goroutine select on it so they don't keep feeding / processing work
	// for a doomed pipeline. Close guarded by cancelOnce.
	cancelCh   chan struct{}
	cancelOnce sync.Once

	// err holds the first fatal error seen by the writer. Written by both the
	// main goroutine (serial path / Finish) and runWriter (concurrent path),
	// so it must be accessed atomically. First error wins; subsequent errors
	// are dropped.
	err    atomic.Pointer[error]
	closed bool
}

// loadErr returns the first recorded error, or nil.
//
//nolint:funcorder // paired with setErr; grouped with Writer state helpers
func (w *Writer) loadErr() error {
	if e := w.err.Load(); e != nil {
		return *e
	}
	return nil
}

// recordErr stores err as the first fatal error if none has been recorded yet.
// No-op when err is nil. Safe to call from any goroutine.
//
//nolint:funcorder // paired with loadErr
func (w *Writer) recordErr(err error) {
	if err != nil {
		w.err.CompareAndSwap(nil, &err)
	}
}

// setErr is a chainable wrapper around recordErr; it returns err unchanged for
// patterns like `return w.setErr(err)`.
//
//nolint:funcorder // paired with loadErr
func (w *Writer) setErr(err error) error {
	w.recordErr(err)
	return err
}

// cancel signals the pipeline to stop. Safe to call multiple times.
// No-op in serial mode (cancelCh is nil).
//
//nolint:funcorder // pipeline state helper; grouped with loadErr/setErr
func (w *Writer) cancel() {
	if w.cancelCh != nil {
		w.cancelOnce.Do(func() { close(w.cancelCh) })
	}
}

//nolint:funcorder // helper for concurrent hash path; kept near Writer for readability
func (w *Writer) getSizes() []uint32 {
	if p := w.sizesPool.Get(); p != nil {
		s, _ := p.(*[]uint32)
		return (*s)[:w.itemsPerRecord]
	}
	return make([]uint32, w.itemsPerRecord)
}

//nolint:funcorder // paired with getSizes
func (w *Writer) putSizes(s []uint32) { w.sizesPool.Put(&s) }

// resolveItemsPerRecord returns the effective record size from opts, defaulting
// to 128 if zero. Returns an error if negative or larger than uint32 max (the
// on-disk trailer stores it as uint32).
func resolveItemsPerRecord(opts WriterOptions) (int, error) {
	rs := opts.ItemsPerRecord
	if rs == 0 {
		return defaultItemsPerRecord, nil
	}
	if rs < 0 {
		return 0, fmt.Errorf("packfile: ItemsPerRecord must be non-negative, got %d", rs)
	}
	if uint64(rs) > math.MaxUint32 {
		return 0, fmt.Errorf("packfile: ItemsPerRecord %d exceeds uint32 max", rs)
	}
	return rs, nil
}

// Create starts writing a new packfile at path. By default, fails if the
// file already exists. Set Overwrite to replace an existing file.
//
//nolint:cyclop // validation chain; each check is small and independent
func Create(path string, opts WriterOptions) (*Writer, error) {
	switch opts.Format {
	case Compressed, Uncompressed, Raw:
	default:
		return nil, fmt.Errorf("packfile: invalid Format %v", opts.Format)
	}
	if opts.Concurrency < 0 {
		return nil, fmt.Errorf("packfile: Concurrency must be non-negative, got %d", opts.Concurrency)
	}
	if opts.BytesPerSync < 0 {
		return nil, fmt.Errorf("packfile: BytesPerSync must be non-negative, got %d", opts.BytesPerSync)
	}

	itemsPerRecord, err := resolveItemsPerRecord(opts)
	if err != nil {
		return nil, err
	}
	if opts.Concurrency > 1 && opts.Format != Compressed && !opts.ContentHash {
		return nil, fmt.Errorf(
			"packfile: Concurrency > 1 requires Format=Compressed or ContentHash=true (got Format=%v, ContentHash=%v)",
			opts.Format, opts.ContentHash,
		)
	}

	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if opts.Overwrite {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o666)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		file:           f,
		path:           path,
		itemsPerRecord: itemsPerRecord,
		concurrency:    opts.Concurrency,
		format:         opts.Format,
		contentHash:    opts.ContentHash,
		bytesPerSync:   int64(opts.BytesPerSync),
	}

	if opts.ContentHash && w.concurrency <= 1 {
		w.serialHasher = newContentHasher(itemsPerRecord)
	}

	if w.concurrency > 1 {
		w.workCh = make(chan blockResult, w.concurrency)
		w.resultCh = make(chan blockResult, w.concurrency)
		w.writerDone = make(chan error, 1)
		w.cancelCh = make(chan struct{})
		if opts.ContentHash {
			w.digestHasher = sha256.New()
		}

		var blockWg sync.WaitGroup
		for range w.concurrency {
			blockWg.Go(w.blockWorker)
		}
		go func() {
			blockWg.Wait()
			close(w.resultCh)
		}()
		go w.runWriter()
	}

	return w, nil
}

// blockWorker reads blocks from workCh, performs format-specific processing
// (compression/CRC) and optional content hashing, then sends results to
// resultCh preserving blockID for reordering.
//
//nolint:funcorder,cyclop,funlen // three-phase compress/hash/assemble pipeline; inherent complexity
func (w *Writer) blockWorker() {
	var c *zstd.Compressor
	if w.format == Compressed {
		c = zstd.NewCompressor()
		defer c.Close()
	}

	var hashIn chan hashWork
	var hashOut chan [sha256.Size]byte
	if w.contentHash {
		hashIn = make(chan hashWork, 1)
		hashOut = make(chan [sha256.Size]byte, 1)
		go func() {
			h := sha256.New()
			var lenBuf [4]byte
			for hw := range hashIn {
				h.Reset()
				offset := 0
				for _, size := range hw.hashSizes {
					binary.LittleEndian.PutUint32(lenBuf[:], size)
					h.Write(lenBuf[:])
					h.Write(hw.data[offset : offset+int(size)])
					offset += int(size)
				}
				w.putSizes(hw.hashSizes)
				var digest [sha256.Size]byte
				h.Sum(digest[:0])
				hashOut <- digest
			}
		}()
		defer close(hashIn)
	}

	for {
		var work blockResult
		var ok bool
		select {
		case <-w.cancelCh:
			return
		case work, ok = <-w.workCh:
			if !ok {
				return
			}
		}

		// Phase 1: queue hash work (hash goroutine reads work.data concurrently).
		if work.hashSizes != nil {
			hashIn <- hashWork{data: work.data, hashSizes: work.hashSizes}
		}

		// Phase 2: format processing (read-only on work.data, concurrent with hash).
		var compressed []byte
		var crc uint32
		var fmtErr error
		switch w.format {
		case Compressed:
			compressed, fmtErr = c.Encode(work.data)
		case Uncompressed:
			crc = crc32c(work.data)
		case Raw:
			// no integrity wrapper; nothing to do
		}

		// Phase 3: collect hash (hash goroutine done reading work.data).
		if work.hashSizes != nil {
			if fmtErr != nil {
				<-hashOut
			} else {
				work.digest = <-hashOut
				work.hasHash = true
			}
			work.hashSizes = nil
		}
		if fmtErr != nil {
			w.resultCh <- blockResult{blockID: work.blockID, err: fmt.Errorf("packfile: block %d: %w", work.blockID, fmtErr)}
			return
		}

		// Assemble: format-specific payload + uncompressed FOR index.
		switch w.format {
		case Compressed:
			work.data = append(append(work.data[:0], compressed...), work.forIndex...)
		case Uncompressed:
			work.data = binary.LittleEndian.AppendUint32(work.data, crc)
			work.data = append(work.data, work.forIndex...)
		case Raw:
			work.data = append(work.data, work.forIndex...)
		}
		work.forIndex = nil
		w.resultCh <- work
	}
}

// runWriter receives processed blocks and writes them in blockID order.
//
//nolint:funcorder // internal pipeline helper; paired with blockWorker
func (w *Writer) runWriter() {
	defer close(w.writerDone)

	pending := make(map[uint32]blockResult)
	nextBlockID := uint32(0)

	for result := range w.resultCh {
		if result.err != nil {
			w.abortPipeline(w.setErr(result.err))
			return
		}
		pending[result.blockID] = result

		for br, ok := pending[nextBlockID]; ok; br, ok = pending[nextBlockID] {
			delete(pending, nextBlockID)
			if err := w.writeBlock(br.data); err != nil {
				// writeBlock already called setErr.
				w.abortPipeline(err)
				return
			}
			if br.hasHash {
				w.digestHasher.Write(br.digest[:])
			}
			nextBlockID++
		}
	}
}

// abortPipeline signals cancellation, reports the first error, and drains
// any remaining worker results so goroutines can exit cleanly.
//
//nolint:funcorder // paired with runWriter
func (w *Writer) abortPipeline(err error) {
	w.cancel()
	w.writerDone <- err
	for range w.resultCh { //nolint:revive // drain remaining worker sends
	}
}

// AppendItem adds a single item. If multiple byte slices are passed, they are
// concatenated into one item. An item may be zero bytes: AppendItem([]byte{})
// records an empty item, while AppendItem() (no arguments) is a no-op. Parts
// are copied; the caller may reuse the argument slices after AppendItem
// returns. Flushes a record when ItemsPerRecord items accumulate.
func (w *Writer) AppendItem(parts ...[]byte) error {
	if err := w.loadErr(); err != nil {
		return err
	}
	if w.closed {
		return ErrWriterClosed
	}

	total := 0
	for _, p := range parts {
		total += len(p)
	}
	if total == 0 && len(parts) == 0 {
		return nil
	}
	if uint64(total) > math.MaxUint32 {
		return fmt.Errorf("packfile: item size %d exceeds uint32 max", total)
	}

	if w.serialHasher != nil {
		w.serialHasher.Add(parts...)
	}

	for _, p := range parts {
		w.buf = append(w.buf, p...)
	}
	w.sizes = append(w.sizes, uint32(total))

	if len(w.sizes) == w.itemsPerRecord {
		if err := w.flush(); err != nil {
			return w.setErr(err)
		}
	}
	w.total++
	return nil
}

// writeBlock appends a format-processed record to the file,
// updates offsets/pos, and initiates background writeback if configured.
//
//nolint:funcorder // internal helper used by runWriter and flush
func (w *Writer) writeBlock(data []byte) error {
	w.offsets = append(w.offsets, w.pos)
	n, err := w.file.Write(data)
	w.pos += int64(n)
	if err != nil {
		return w.setErr(err)
	}
	if w.bytesPerSync > 0 && w.pos-w.lastSyncPos >= w.bytesPerSync {
		initiateWriteback(w.file, w.lastSyncPos, w.pos-w.lastSyncPos)
		w.lastSyncPos = w.pos
	}
	return nil
}

// closeCompressor releases the serial-path compressor, if allocated.
//
//nolint:funcorder // internal helper
func (w *Writer) closeCompressor() {
	if w.compressor != nil {
		w.compressor.Close()
		w.compressor = nil
	}
}

// buildBlock extracts the current payload buffer and (for itemsPerRecord>1) encodes
// the FOR index. The FOR index is never compressed and always carries its own
// crc32c. Payload is allocated with spare capacity for CRC (4B) + forIndex so
// callers can append without reallocation.
//
//nolint:funcorder // helper for flush / blockWorker
func (w *Writer) buildBlock() ([]byte, []byte) {
	var forIndex []byte
	if w.itemsPerRecord > 1 {
		encoded := intpack.EncodeGroup(w.sizes)
		forIndex = binary.LittleEndian.AppendUint32(encoded, crc32c(encoded))
	}
	payload := make([]byte, len(w.buf), len(w.buf)+4+len(forIndex))
	copy(payload, w.buf)
	w.buf = w.buf[:0]
	w.sizes = w.sizes[:0]
	return payload, forIndex
}

//nolint:funcorder // internal helper chains into blockWorker / writeBlock
func (w *Writer) flush() error {
	// Serial path: format-process inline. Content hash is handled by serialHasher in AppendItem.
	if w.concurrency <= 1 {
		payload, forIndex := w.buildBlock()
		var block []byte
		switch w.format {
		case Compressed:
			if w.compressor == nil {
				w.compressor = zstd.NewCompressor()
			}
			compressed, err := w.compressor.Encode(payload)
			if err != nil {
				return err
			}
			block = append(append(payload[:0], compressed...), forIndex...)
		case Uncompressed:
			// CRC_items covers payload only; FOR index has its own CRC.
			payload = binary.LittleEndian.AppendUint32(payload, crc32c(payload))
			block = append(payload, forIndex...) //nolint:gocritic // append to different destination
		case Raw:
			block = append(payload, forIndex...) //nolint:gocritic // append to different destination
		}
		return w.writeBlock(block)
	}

	// Concurrent path: blockWorker handles all formats.
	var hashSizes []uint32
	if w.contentHash {
		hashSizes = w.getSizes()[:len(w.sizes)]
		copy(hashSizes, w.sizes)
	}

	payload, forIndex := w.buildBlock()

	select {
	case <-w.cancelCh:
		if hashSizes != nil {
			w.putSizes(hashSizes)
		}
		return w.loadErr()
	case w.workCh <- blockResult{
		blockID:   w.nextBlockID,
		data:      payload,
		forIndex:  forIndex,
		hashSizes: hashSizes,
	}:
	}
	w.nextBlockID++
	return nil
}

// Finish flushes any partial record, drains the pipeline, writes the index,
// optional app data, and 64-byte trailer, and finalizes the packfile.
// appData is optional caller-injected data stored between the index and
// trailer; pass nil for no app data.
//
//nolint:cyclop,funlen // finalization has many sequential steps; splitting obscures the flow
func (w *Writer) Finish(appData []byte) error {
	if w.closed {
		return ErrWriterClosed
	}

	// Flush any partial record. Skip if an error already occurred (the flush
	// would either enqueue to a doomed pipeline or hit serial-path state that
	// can't safely produce a block). Errors here accumulate in w.err.
	if len(w.sizes) > 0 && w.loadErr() == nil {
		if err := w.flush(); err != nil {
			w.recordErr(err)
		}
	}

	// Always drain the pipeline so goroutines don't leak, even on prior error.
	if w.workCh != nil {
		close(w.workCh)
		if err := <-w.writerDone; err != nil {
			w.recordErr(err)
		}
		w.workCh = nil
	}

	// Bail out if any error was recorded (prior, flush, or pipeline).
	if err := w.loadErr(); err != nil {
		return err
	}

	//nolint:gosec // w.total is monotonically incremented from 0; never negative
	if uint64(w.total) > math.MaxUint32 {
		return w.setErr(fmt.Errorf("packfile: item count %d exceeds uint32 max", w.total))
	}
	if uint64(len(appData)) > math.MaxUint32 {
		return w.setErr(fmt.Errorf("packfile: appData size %d exceeds uint32 max", len(appData)))
	}

	w.offsets = append(w.offsets, w.pos) // end-of-data offset

	// Compute content hash if enabled.
	var fileHash [32]byte
	if w.contentHash {
		if w.serialHasher != nil {
			fileHash = w.serialHasher.Sum()
		} else {
			w.digestHasher.Sum(fileHash[:0])
		}
	}

	// Encode index using FOR-128.
	indexBytes, err := encodeIndex(w.offsets)
	if err != nil {
		return w.setErr(err)
	}
	if uint64(len(indexBytes)) > math.MaxUint32 {
		return w.setErr(fmt.Errorf("packfile: index size %d exceeds uint32 max", len(indexBytes)))
	}
	indexSize := uint32(len(indexBytes)) //nolint:gosec // bounds-checked above

	// Write index section.
	if _, err := w.file.Write(indexBytes); err != nil {
		return w.setErr(err)
	}

	// Write app data (if any).
	appDataSize := uint32(len(appData)) //nolint:gosec // bounds-checked above (len(appData) <= MaxUint32)
	if appDataSize > 0 {
		if _, err := w.file.Write(appData); err != nil {
			return w.setErr(err)
		}
	}

	// Build 64-byte trailer.
	var flags uint8
	if w.format != Compressed {
		flags |= flagNoCompression
	}
	if w.format == Raw {
		flags |= flagNoCRC
	}
	if w.contentHash {
		flags |= flagContentHash
	}

	var trailer [trailerSize]byte
	binary.LittleEndian.PutUint32(trailer[0:], magic)
	trailer[4] = version
	trailer[5] = flags
	binary.LittleEndian.PutUint32(trailer[6:], uint32(len(w.offsets)-1))  //nolint:gosec // recordCount, bounded
	binary.LittleEndian.PutUint32(trailer[10:], uint32(w.total))          //nolint:gosec // checked above
	binary.LittleEndian.PutUint32(trailer[14:], uint32(w.itemsPerRecord)) //nolint:gosec // bounded by config
	binary.LittleEndian.PutUint32(trailer[18:], indexSize)                // indexSize
	binary.LittleEndian.PutUint32(trailer[22:], appDataSize)              // appDataSize
	copy(trailer[26:58], fileHash[:])                                     // contentHash (zeroed if unused)
	binary.LittleEndian.PutUint16(trailer[58:], uint16(groupSize))        // indexForGroupSize

	// crc32c over trailer[0:60] only. App data integrity is the caller's responsibility.
	binary.LittleEndian.PutUint32(trailer[60:], crc32c(trailer[:60]))

	if _, err := w.file.Write(trailer[:]); err != nil {
		return w.setErr(err)
	}

	if err := w.file.Sync(); err != nil {
		return w.setErr(err)
	}
	if err := w.file.Close(); err != nil {
		return w.setErr(err)
	}
	// Mark closed before any further steps. Once the file is durable on disk,
	// a panic in closeCompressor (or anything else) must not cause the deferred
	// Close to remove a valid packfile.
	w.closed = true
	w.file = nil
	w.closeCompressor()
	return nil
}

// Close releases resources. If Finish was not called, the incomplete file
// is removed. If Finish was called, Close is a no-op (the file remains as
// a valid packfile). Safe to call multiple times. Idiomatic usage:
//
//	w, _ := Create(path, opts)
//	defer w.Close()
//	// ... AppendItem ...
//	return w.Finish(nil)
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.closeCompressor()
	if w.workCh != nil {
		w.cancel()
		close(w.workCh)
		<-w.writerDone
	}
	var closeErr error
	if w.file != nil {
		closeErr = w.file.Close()
		w.file = nil
	}
	// Finish was never called — remove the incomplete file.
	removeErr := os.Remove(w.path)
	return errors.Join(closeErr, removeErr)
}
