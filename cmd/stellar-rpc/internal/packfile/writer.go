// Package packfile implements an immutable, append-only file format with O(1)
// positional access. Items are accumulated into fixed-size records, each
// optionally passed through a caller-supplied encoder and indexed by a
// compact FOR-encoded offset table. An optional chunked SHA-256 content hash
// covers the logical item stream for end-to-end integrity.
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
)

const defaultItemsPerRecord = 128

// ErrWriterClosed is returned by AppendItem or Finish after the Writer has
// been closed (successfully finalized or aborted).
var ErrWriterClosed = errors.New("packfile: writer is closed")

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
	// ItemsPerRecord is the number of items per record. 0 defaults to 128.
	// Maximum value: math.MaxUint32 (stored as uint32 in the trailer).
	ItemsPerRecord int

	// Format is a caller-assigned identifier written to the trailer.
	Format Format

	// NewRecordEncoder, if non-nil, returns a per-worker RecordEncoder.
	// Called once per worker goroutine; the writer invokes Encode once per
	// record before write and Close on shutdown. Callers supplying a stateful
	// codec (e.g. zstd) should construct fresh state inside this constructor —
	// each worker gets its own.
	//
	// If nil, records are written as-is (passthrough). Use this when the data
	// source already provides items in their final on-disk form (e.g.,
	// pre-compressed ledgers stored verbatim in rocksdb).
	NewRecordEncoder func() RecordEncoder

	// ContentHash enables SHA-256 content hashing over the logical item
	// stream. The digest is stored in the trailer.
	ContentHash bool

	// ContentHashExtract, if non-nil, transforms each item into the bytes
	// fed to the content hasher. Use it when items as received aren't the
	// canonical hash input — e.g., to decompress pre-compressed items so
	// the hash is stable across encoder versions.
	//
	// If nil, items are hashed as received by AppendItem. May be called
	// concurrently from worker goroutines, so must be safe for concurrent
	// invocation.
	ContentHashExtract func(item []byte) ([]byte, error)

	// Concurrency sets the number of parallel worker goroutines used when
	// a record encoder or content hashing is enabled. 0 defaults to 1.
	// Writers with neither a record encoder nor content hash ignore this
	// value (records are written directly from the main goroutine). Values
	// above runtime.NumCPU() tend to hurt throughput; pick a value ≤ NumCPU.
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

// processedRecord is sent from a worker to runWriter once a pendingRecord
// has had its codec applied (or been passed through) and its content-hash
// chunk digest computed. digest is populated only when ContentHash is enabled.
type processedRecord struct {
	ordinal uint32
	data    []byte // assembled record bytes (encoded-or-raw payload + forIndex)
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

// encodeForIndex packs the per-record item sizes into a FOR group followed
// by a CRC32C of the FOR-encoded bytes. This is the on-disk wire format for
// the per-record item-size index appended to multi-item records;
// decodeForIndex is the inverse.
func encodeForIndex(sizes []uint32) []byte {
	encoded := intpack.EncodeGroup(sizes)
	return binary.LittleEndian.AppendUint32(encoded, crc32c(encoded))
}

// decodeForIndex parses the FOR-index tail of a multi-item record. data must
// be the full on-disk record bytes (payload || FOR-encoded sizes || CRC32C).
// n is the expected number of sizes and must be >= 1 (multi-item records
// always carry at least one size; the writer never emits a zero-size FOR
// group). dst is an optional scratch slice; if its capacity is sufficient
// it will be reused. Returns the parsed sizes slice and the payload (data
// with the FOR index stripped). On a CRC mismatch returns ErrChecksum;
// on a malformed group returns wrapped ErrCorrupt.
func decodeForIndex(data []byte, n int, dst []uint32) ([]uint32, []byte, error) {
	const crcSize = 4
	if len(data) < crcSize {
		return nil, nil, fmt.Errorf("%w: record too short", ErrCorrupt)
	}
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-crcSize:])
	forBuf := data[:len(data)-crcSize]

	sizes, consumed, err := intpack.DecodeGroup(forBuf, n, dst)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	if storedCRC != crc32c(forBuf[len(forBuf)-consumed:]) {
		return nil, nil, fmt.Errorf("%w: FOR index CRC32C", ErrChecksum)
	}
	return sizes, forBuf[:len(forBuf)-consumed], nil
}

// Writer builds a packfile with item-level semantics. Items are accumulated
// into records of itemsPerRecord items each; each record is optionally passed
// through a caller-supplied encoder before being written with an offset
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
	buf              []byte
	sizes            []uint32
	total            int
	itemsPerRecord   int
	format           Format
	newRecordEncoder func() RecordEncoder
	// recordBufPool recycles per-record payload buffers — borrowed in
	// buildRecord, returned in writeRecord — so each flushed record
	// avoids a fresh allocation. Pools *[]byte, mirroring sizesPool.
	recordBufPool sync.Pool

	// Content hash
	contentHash        bool
	contentHashExtract func([]byte) ([]byte, error)
	digestHasher       hash.Hash // running SHA-256 over chunk digests; nil if !contentHash
	sizesPool          sync.Pool // pooled []uint32 for hash goroutines

	// Pipeline. Spawned when newRecordEncoder != nil OR contentHash; otherwise the
	// writer is a pure passthrough and main writes records directly.
	concurrency int
	nextOrdinal uint32
	workCh      chan pendingRecord
	resultCh    chan processedRecord
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

// getRecordBuf borrows a record payload buffer with capacity >= need
// (length 0). It is returned to the pool by writeRecord once the record
// is durable. Mirrors getSizes.
func (w *Writer) getRecordBuf(need int) []byte {
	if p := w.recordBufPool.Get(); p != nil {
		if b := *p.(*[]byte); cap(b) >= need {
			return b[:0]
		}
	}
	return make([]byte, 0, need)
}

//nolint:funcorder // paired with getRecordBuf
func (w *Writer) putRecordBuf(b []byte) { w.recordBufPool.Put(&b) }

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
		newRecordEncoder:   opts.NewRecordEncoder,
		contentHash:        opts.ContentHash,
		contentHashExtract: opts.ContentHashExtract,
		bytesPerSync:       int64(opts.BytesPerSync),
	}

	// Spawn the pipeline when there's CPU work (compress and/or hash).
	// Pure passthrough writes records directly from main.
	if opts.NewRecordEncoder != nil || opts.ContentHash {
		workers := max(w.concurrency, 1)
		w.concurrency = workers
		w.workCh = make(chan pendingRecord, workers)
		w.resultCh = make(chan processedRecord, workers)
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
	var encoder RecordEncoder
	if w.newRecordEncoder != nil {
		encoder = w.newRecordEncoder()
		if encoder != nil {
			defer func() { _ = w.recordErr(encoder.Close()) }()
		}
	}
	// Per-worker scratch for encoder output. The encoder writes into this
	// buffer (growing it if necessary); it persists across iterations so
	// steady-state Encode calls don't allocate.
	var encScratch []byte

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

		var compressErr error
		if encoder != nil {
			encScratch, compressErr = encoder.Encode(encScratch[:0], work.data)
		}

		var (
			digest  [sha256.Size]byte
			hashErr error
		)
		if work.hashSizes != nil {
			res := <-hashOut
			digest = res.digest
			hashErr = res.err
		}

		if compressErr != nil {
			w.resultCh <- processedRecord{
				ordinal: work.ordinal,
				err:     fmt.Errorf("packfile: record %d compress: %w", work.ordinal, compressErr),
			}
			return
		}
		if hashErr != nil {
			w.resultCh <- processedRecord{
				ordinal: work.ordinal,
				err:     fmt.Errorf("packfile: record %d hash extract: %w", work.ordinal, hashErr),
			}
			return
		}

		if encoder != nil {
			// Copy the encoded bytes into work.data, which the main goroutine
			// pre-sized with spare capacity for the forIndex appended below
			// (so the append on the next line doesn't reallocate).
			work.data = append(work.data[:0], encScratch...)
		}
		w.resultCh <- processedRecord{
			ordinal: work.ordinal,
			data:    append(work.data, work.forIndex...),
			digest:  digest,
		}
	}
}

// hashGoroutine is the inner per-worker hash goroutine. Each chunk's items
// are streamed into a SHA-256 via writeLenPrefixed (the shared content-hash
// wire format), and the chunk digest is shipped back. Returns sizes to the
// pool when done.
//
//nolint:funcorder // helper for recordWorker
func (w *Writer) hashGoroutine(hashIn <-chan hashWork, hashOut chan<- hashResult) {
	h := sha256.New()
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
			writeLenPrefixed(h, toHash)
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

	reorderBuf := make(map[uint32]processedRecord, w.concurrency)
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
	// data is a borrowed record buffer (from buildRecord); recycle it once
	// written. file.Write copies synchronously, so data is free on return —
	// this is the single terminal consumer for both the async and
	// passthrough paths.
	defer w.putRecordBuf(data)
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
		forIndex = encodeForIndex(w.sizes)
	}
	// Borrow a buffer with spare capacity for the forIndex that the
	// worker (or the passthrough path) appends, so that append never
	// reallocates and the pooled buffer survives intact to writeRecord.
	payload := w.getRecordBuf(len(w.buf) + len(forIndex))
	payload = append(payload, w.buf...)
	w.buf = w.buf[:0]
	w.sizes = w.sizes[:0]
	return payload, forIndex
}

// flush dispatches the current record to the worker pipeline, or writes it
// directly when the writer is in pure-passthrough mode (no encoder, no hash).
//
//nolint:funcorder // internal helper chains into recordWorker / writeRecord
func (w *Writer) flush() error {
	if w.workCh == nil {
		// Pure passthrough: no workers, no hash. Write directly.
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

// finalize fsyncs and closes the file. Called after writeTrailer succeeds.
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
	t := Trailer{
		Version: version,
		Format:  w.format,
		//nolint:gosec // bounded by len(offsets)
		RecordCount: uint32(len(w.offsets) - 1),
		TotalItems:  uint32(w.total), //nolint:gosec // bounds-checked by Finish
		//nolint:gosec // validated in resolveItemsPerRecord
		ItemsPerRecord:    uint32(w.itemsPerRecord),
		IndexForGroupSize: uint16(groupSize),
		IndexSize:         indexSize,
		AppDataSize:       appDataSize,
		ContentHash:       fileHash,
		HasContentHash:    w.contentHash,
	}

	var trailer [trailerSize]byte
	t.marshal(trailer[:])

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
