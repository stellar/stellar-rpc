// Package packfile implements an immutable, append-only file format with O(1)
// positional access. Items are accumulated into fixed-size records, each
// optionally passed through a caller-supplied compressor and indexed by a
// compact FOR-encoded offset table. An optional chunked SHA-256 content hash
// covers the logical item stream for end-to-end integrity.
package packfile

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/intpack"
)

const defaultItemsPerRecord = 128

// ErrWriterClosed is returned by AppendItem or Finish after the Writer has
// been closed (successfully finalized or aborted).
var ErrWriterClosed = errors.New("packfile: writer is closed")

// Format is a caller-assigned identifier stored in the trailer. The packfile
// library does not interpret Format values; readers use them to dispatch to
// the matching decompressor. Callers agree on Format values out of band.
type Format uint32

// Compressor compresses one record (the concatenation of items in a record)
// at a time. Encode is called once per record before write. Close is called
// when the writer no longer needs the compressor (Finish or Close), so
// stateful codecs that hold non-Go resources (e.g. CGo zstd contexts) can
// release them deterministically rather than waiting on GC finalizers.
//
// Encode's returned slice may alias an internal buffer of the compressor and
// is valid until the next call on this Compressor; it must not alias the
// input slice. A Compressor is not safe for concurrent use — the writer
// creates one per worker goroutine via WriterOptions.NewCompressor.
type Compressor interface {
	Encode(in []byte) ([]byte, error)
	io.Closer
}

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
	// ItemsPerRecord is the number of items per record. 0 defaults to 128.
	// Maximum value: math.MaxUint32 (stored as uint32 in the trailer).
	ItemsPerRecord int

	// Format is a caller-assigned identifier written to the trailer.
	Format Format

	// NewCompressor, if non-nil, returns a per-worker Compressor. Called
	// once per worker goroutine; the writer invokes Encode once per record
	// before write and Close on shutdown. Callers supplying a stateful codec
	// (e.g. zstd) should construct fresh state inside this constructor —
	// each worker gets its own.
	//
	// If nil, records are written as-is (passthrough). Use this when the
	// data source already provides items in their final on-disk form
	// (e.g., pre-compressed ledgers stored verbatim in rocksdb).
	NewCompressor func() Compressor

	// ContentHash enables SHA-256 content hashing over the logical item
	// stream. The digest is stored in the trailer.
	ContentHash bool

	// ContentHashExtract, if non-nil, transforms each item into the bytes
	// fed to the content hasher. Use it when items as received aren't the
	// canonical hash input — e.g., to decompress pre-compressed items so
	// the hash is stable across compressor versions.
	//
	// If nil, items are hashed as received by AppendItem. May be called
	// concurrently from worker goroutines, so must be safe for concurrent
	// invocation.
	ContentHashExtract func(item []byte) ([]byte, error)

	// Concurrency sets the number of parallel worker goroutines used when
	// compression or content hashing is enabled. 0 defaults to 1. Writers
	// with neither a compressor nor content hash ignore this value
	// (records are written directly from the main goroutine). Values above
	// runtime.NumCPU() tend to hurt throughput; pick a value ≤ NumCPU.
	Concurrency int

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

// pendingRecord is sent from the main goroutine to a worker.
type pendingRecord struct {
	ordinal   uint32
	data      []byte
	forIndex  []byte // FOR index: [packed][1B W][4B min][4B crc32c]; nil when itemsPerRecord==1
	hashSizes []uint32
}

// record is sent from a worker to runWriter. digest is populated only
// when the writer has ContentHash enabled.
type record struct {
	ordinal uint32
	data    []byte // assembled record bytes (compressed-or-raw payload + forIndex)
	digest  [sha256.Size]byte
	err     error
}

type hashWork struct {
	data      []byte
	hashSizes []uint32
}

// hashResult conveys a chunk digest (or an error from ContentHashExtract)
// from the inner hash goroutine back to its worker.
type hashResult struct {
	digest [sha256.Size]byte
	err    error
}

// Writer builds a packfile with item-level semantics. Items are accumulated
// into records of itemsPerRecord items each; each record is optionally passed
// through a caller-supplied compressor before being written with an offset
// index.
//
// A Writer must be used by a single goroutine; concurrent AppendItem, Finish,
// or Close calls from multiple goroutines are not safe. Internally, a
// concurrent Writer spawns a background pipeline, but the caller-facing API
// is single-threaded.
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
	format         Format
	newCompressor  func() Compressor

	// Content hash
	contentHash        bool
	contentHashExtract func([]byte) ([]byte, error)
	digestHasher       hash.Hash // running SHA-256 over chunk digests; nil if !contentHash
	sizesPool          sync.Pool // pooled []uint32 for hash goroutines

	// Pipeline. Spawned when newCompressor != nil OR contentHash; otherwise the
	// writer is a pure passthrough and main writes records directly.
	concurrency int
	nextOrdinal uint32
	workCh      chan pendingRecord
	resultCh    chan record
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
//nolint:funcorder // paired with recordErr; grouped with Writer state helpers
func (w *Writer) loadErr() error {
	if e := w.err.Load(); e != nil {
		return *e
	}
	return nil
}

// recordErr stores err as the first fatal error if none has been recorded yet.
// Returns err unchanged for `return w.recordErr(err)` chaining. No-op (and
// returns nil) when err is nil. Safe to call from any goroutine.
//
//nolint:funcorder // paired with loadErr
func (w *Writer) recordErr(err error) error {
	if err != nil {
		w.err.CompareAndSwap(nil, &err)
	}
	return err
}

// cancel signals the pipeline to stop. Safe to call multiple times.
// No-op in serial mode (cancelCh is nil).
//
//nolint:funcorder // pipeline state helper; grouped with loadErr/recordErr
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
func Create(path string, opts WriterOptions) (*Writer, error) {
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

	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if opts.Overwrite {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o666)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		file:               f,
		path:               path,
		itemsPerRecord:     itemsPerRecord,
		concurrency:        opts.Concurrency,
		format:             opts.Format,
		newCompressor:      opts.NewCompressor,
		contentHash:        opts.ContentHash,
		contentHashExtract: opts.ContentHashExtract,
		bytesPerSync:       int64(opts.BytesPerSync),
	}

	// Spawn the pipeline when there's CPU work (compress and/or hash).
	// Pure passthrough writes records directly from main.
	if opts.NewCompressor != nil || opts.ContentHash {
		workers := max(w.concurrency, 1)
		w.concurrency = workers
		w.workCh = make(chan pendingRecord, workers)
		w.resultCh = make(chan record, workers)
		w.writerDone = make(chan error, 1)
		w.cancelCh = make(chan struct{})
		if opts.ContentHash {
			w.digestHasher = sha256.New()
		}

		var recordWg sync.WaitGroup
		for range workers {
			recordWg.Go(w.recordWorker)
		}
		go func() {
			recordWg.Wait()
			close(w.resultCh)
		}()
		go w.runWriter()
	}

	return w, nil
}

// recordWorker reads work from workCh, compresses (or passes through),
// computes the content-hash chunk digest in parallel, and ships the result.
//
//nolint:funcorder // pipeline helper; kept near runWriter
func (w *Writer) recordWorker() {
	var compressor Compressor
	if w.newCompressor != nil {
		compressor = w.newCompressor()
		defer func() { _ = w.recordErr(compressor.Close()) }()
	}

	var hashIn chan hashWork
	var hashOut chan hashResult
	if w.contentHash {
		hashIn = make(chan hashWork, 1)
		hashOut = make(chan hashResult, 1)
		go w.hashGoroutine(hashIn, hashOut)
		defer close(hashIn)
	}

	for {
		var work pendingRecord
		var ok bool
		select {
		case <-w.cancelCh:
			return
		case work, ok = <-w.workCh:
			if !ok {
				return
			}
		}

		if work.hashSizes != nil {
			hashIn <- hashWork{data: work.data, hashSizes: work.hashSizes}
		}

		var compressed []byte
		var compressErr error
		if compressor != nil {
			compressed, compressErr = compressor.Encode(work.data)
		}

		var (
			digest  [sha256.Size]byte
			hashErr error
		)
		if work.hashSizes != nil {
			r := <-hashOut
			digest = r.digest
			hashErr = r.err
		}

		if compressErr != nil {
			w.resultCh <- record{
				ordinal: work.ordinal,
				err:     fmt.Errorf("packfile: record %d compress: %w", work.ordinal, compressErr),
			}
			return
		}
		if hashErr != nil {
			w.resultCh <- record{
				ordinal: work.ordinal,
				err:     fmt.Errorf("packfile: record %d hash extract: %w", work.ordinal, hashErr),
			}
			return
		}

		if compressed != nil {
			// compressed may alias the compressor's internal buffer; copy
			// into work.data's scratch.
			work.data = append(work.data[:0], compressed...)
		}
		w.resultCh <- record{
			ordinal: work.ordinal,
			data:    append(work.data, work.forIndex...),
			digest:  digest,
		}
	}
}

// hashGoroutine is the inner per-worker hash goroutine. Length-prefixes each
// (extracted) item and feeds the bytes into a SHA-256, returning the chunk
// digest. Returns sizes to the pool when done.
//
//nolint:funcorder // helper for recordWorker
func (w *Writer) hashGoroutine(hashIn <-chan hashWork, hashOut chan<- hashResult) {
	h := sha256.New()
	var lenBuf [4]byte
	for hw := range hashIn {
		h.Reset()
		offset := 0
		var extractErr error
		for _, size := range hw.hashSizes {
			item := hw.data[offset : offset+int(size)]
			offset += int(size)
			toHash := item
			if w.contentHashExtract != nil {
				var err error
				toHash, err = w.contentHashExtract(item)
				if err != nil {
					extractErr = err
					break
				}
				if uint64(len(toHash)) > math.MaxUint32 {
					extractErr = fmt.Errorf("packfile: extracted item size %d exceeds uint32 max", len(toHash))
					break
				}
			}
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(toHash))) //nolint:gosec // bounds-checked above
			h.Write(lenBuf[:])
			h.Write(toHash)
		}
		w.putSizes(hw.hashSizes)
		var digest [sha256.Size]byte
		h.Sum(digest[:0])
		hashOut <- hashResult{digest: digest, err: extractErr}
	}
}

// runWriter receives processed records and writes them in ordinal order.
//
//nolint:funcorder // internal pipeline helper; paired with recordWorker
func (w *Writer) runWriter() {
	defer close(w.writerDone)

	reorderBuf := make(map[uint32]record, w.concurrency)
	nextOrdinal := uint32(0)

	for result := range w.resultCh {
		if result.err != nil {
			w.abortPipeline(w.recordErr(result.err))
			return
		}
		reorderBuf[result.ordinal] = result

		for r, ok := reorderBuf[nextOrdinal]; ok; r, ok = reorderBuf[nextOrdinal] {
			delete(reorderBuf, nextOrdinal)
			if err := w.writeRecord(r.data); err != nil {
				// writeRecord already called recordErr.
				w.abortPipeline(err)
				return
			}
			if w.contentHash {
				w.digestHasher.Write(r.digest[:])
			}
			nextOrdinal++
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

	if len(parts) == 0 {
		return nil
	}
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	if uint64(total) > math.MaxUint32 {
		return fmt.Errorf("packfile: item size %d exceeds uint32 max", total)
	}

	for _, p := range parts {
		w.buf = append(w.buf, p...)
	}
	w.sizes = append(w.sizes, uint32(total))

	if len(w.sizes) == w.itemsPerRecord {
		if err := w.flush(); err != nil {
			return w.recordErr(err)
		}
	}
	w.total++
	return nil
}

// writeRecord appends a record (compressed or raw) to the file,
// updates offsets/pos, and initiates background writeback if configured.
//
//nolint:funcorder // internal helper used by runWriter and flush
func (w *Writer) writeRecord(data []byte) error {
	w.offsets = append(w.offsets, w.pos)
	n, err := w.file.Write(data)
	w.pos += int64(n)
	if err != nil {
		return w.recordErr(err)
	}
	if w.bytesPerSync > 0 && w.pos-w.lastSyncPos >= w.bytesPerSync {
		initiateWriteback(w.file, w.lastSyncPos, w.pos-w.lastSyncPos)
		w.lastSyncPos = w.pos
	}
	return nil
}

// buildRecord extracts the current payload buffer and (for itemsPerRecord>1)
// encodes the FOR index. Payload is allocated with spare capacity for forIndex
// so callers can append without reallocation.
//
//nolint:funcorder // helper for flush / recordWorker
func (w *Writer) buildRecord() ([]byte, []byte) {
	var forIndex []byte
	if w.itemsPerRecord > 1 {
		encoded := intpack.EncodeGroup(w.sizes)
		forIndex = binary.LittleEndian.AppendUint32(encoded, crc32c(encoded))
	}
	payload := make([]byte, len(w.buf), len(w.buf)+len(forIndex))
	copy(payload, w.buf)
	w.buf = w.buf[:0]
	w.sizes = w.sizes[:0]
	return payload, forIndex
}

// flushSerial encodes the current record inline (no pipeline) and writes it.
// Content hash for serial mode is handled by serialHasher in AppendItem.
//
//nolint:funcorder // internal helper chains into recordWorker / writeRecord
func (w *Writer) flush() error {
	// Pure passthrough: no workers, no hash. Write directly.
	if w.workCh == nil {
		payload, forIndex := w.buildRecord()
		return w.writeRecord(append(payload, forIndex...))
	}

	var hashSizes []uint32
	if w.contentHash {
		hashSizes = w.getSizes()[:len(w.sizes)]
		copy(hashSizes, w.sizes)
	}

	payload, forIndex := w.buildRecord()

	select {
	case <-w.cancelCh:
		if hashSizes != nil {
			w.putSizes(hashSizes)
		}
		return w.loadErr()
	case w.workCh <- pendingRecord{
		ordinal:   w.nextOrdinal,
		data:      payload,
		forIndex:  forIndex,
		hashSizes: hashSizes,
	}:
	}
	w.nextOrdinal++
	return nil
}

// Finish flushes any partial record, drains the pipeline, writes the index,
// optional app data, and trailer, and finalizes the packfile.
// appData is optional caller-injected data stored between the index and
// trailer; pass nil for no app data.
func (w *Writer) Finish(appData []byte) error {
	if w.closed {
		return ErrWriterClosed
	}
	if err := w.drainPipeline(); err != nil {
		return err
	}

	//nolint:gosec // w.total is monotonically incremented from 0; never negative
	if uint64(w.total) > math.MaxUint32 {
		return w.recordErr(fmt.Errorf("packfile: item count %d exceeds uint32 max", w.total))
	}
	if uint64(len(appData)) > math.MaxUint32 {
		return w.recordErr(fmt.Errorf("packfile: appData size %d exceeds uint32 max", len(appData)))
	}

	w.offsets = append(w.offsets, w.pos)

	var fileHash [32]byte
	if w.contentHash {
		w.digestHasher.Sum(fileHash[:0])
	}

	indexBytes, err := encodeIndex(w.offsets)
	if err != nil {
		return w.recordErr(err)
	}
	if uint64(len(indexBytes)) > math.MaxUint32 {
		return w.recordErr(fmt.Errorf("packfile: index size %d exceeds uint32 max", len(indexBytes)))
	}
	if _, err := w.file.Write(indexBytes); err != nil {
		return w.recordErr(err)
	}
	if len(appData) > 0 {
		if _, err := w.file.Write(appData); err != nil {
			return w.recordErr(err)
		}
	}
	//nolint:gosec // both sizes bounds-checked above
	if err := w.writeTrailer(uint32(len(indexBytes)), uint32(len(appData)), fileHash); err != nil {
		return err
	}
	return w.finalize()
}

// finalize fsyncs and closes the file, then releases the serial compressor.
// Called after writeTrailer succeeds.
//
//nolint:funcorder // helper for Finish
func (w *Writer) finalize() error {
	if err := w.file.Sync(); err != nil {
		return w.recordErr(err)
	}
	if err := w.file.Close(); err != nil {
		return w.recordErr(err)
	}
	w.closed = true
	w.file = nil
	return nil
}

// drainPipeline flushes any partial record, closes workCh, and waits for
// runWriter to drain. Returns the first error seen across flush, runWriter,
// or any prior recorded error.
//
//nolint:funcorder // helper for Finish
func (w *Writer) drainPipeline() error {
	if len(w.sizes) > 0 && w.loadErr() == nil {
		_ = w.recordErr(w.flush())
	}
	if w.workCh != nil {
		close(w.workCh)
		_ = w.recordErr(<-w.writerDone)
		w.workCh = nil
	}
	return w.loadErr()
}

//nolint:funcorder // helper for Finish
func (w *Writer) writeTrailer(indexSize, appDataSize uint32, fileHash [32]byte) error {
	var flags uint8
	if w.contentHash {
		flags |= flagContentHash
	}

	var trailer [trailerSize]byte
	binary.LittleEndian.PutUint32(trailer[0:], magic)
	trailer[4] = version
	trailer[5] = flags
	// trailer[6:8] reserved
	binary.LittleEndian.PutUint32(trailer[8:], uint32(w.format))
	binary.LittleEndian.PutUint32(trailer[12:], uint32(len(w.offsets)-1)) //nolint:gosec // recordCount, bounded
	binary.LittleEndian.PutUint32(trailer[16:], uint32(w.total))          //nolint:gosec // bounds-checked by caller
	//nolint:gosec // itemsPerRecord validated in resolveItemsPerRecord to fit uint32
	binary.LittleEndian.PutUint32(trailer[20:], uint32(w.itemsPerRecord))
	binary.LittleEndian.PutUint16(trailer[24:], uint16(groupSize))
	// trailer[26:28] reserved
	binary.LittleEndian.PutUint32(trailer[28:], indexSize)
	binary.LittleEndian.PutUint32(trailer[32:], appDataSize)
	copy(trailer[36:68], fileHash[:])
	// trailer[68:72] reserved
	binary.LittleEndian.PutUint32(trailer[72:], crc32c(trailer[:72]))

	if _, err := w.file.Write(trailer[:]); err != nil {
		return w.recordErr(err)
	}
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
