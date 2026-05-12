package packfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const (
	readBufSize         = 1 << 20    // 1 MiB pooled coalesced-read buffer
	speculativeReadSize = 256 * 1024 // 256 KiB tail prefetch on Open
)

// Reader-side errors that come from trailer parsing on Open. All three
// wrap ErrCorrupt, so callers can match them generically with
// errors.Is(err, ErrCorrupt) — or specifically with errors.Is(err, ErrMagic)
// etc.
var (
	ErrMagic   = fmt.Errorf("%w: invalid magic number", ErrCorrupt)
	ErrVersion = fmt.Errorf("%w: unsupported version", ErrCorrupt)
	ErrSize    = fmt.Errorf("%w: file size inconsistent with trailer", ErrCorrupt)

	// ErrPositionOutOfRange is returned by ReadItem, ReadRange (yielded
	// once via the iterator), and ReadItems when a requested position is
	// outside [0, TotalItems).
	ErrPositionOutOfRange = errors.New("packfile: position out of range")

	// ErrPositionsUnsorted is returned by ReadItems when positions are not
	// strictly ascending (duplicates or out-of-order entries).
	ErrPositionsUnsorted = errors.New("packfile: positions not strictly sorted")
)

// knownFlags is the bitmask of trailer flags this version of the reader
// understands. Files with unknown bits set are rejected as corrupt.
const knownFlags = flagContentHash

// readBufPool is a process-wide pool of 1 MiB read buffers used to coalesce
// consecutive record reads in ReadRange and ReadItems.
//
//nolint:gochecknoglobals // process-wide read buffer pool
var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, readBufSize)
		return &b
	},
}

// readAtCloser is the minimal interface needed by Reader to access packfile
// data. *os.File satisfies it.
type readAtCloser interface {
	io.ReaderAt
	io.Closer
}

// ReaderOptions configures Reader behavior.
type ReaderOptions struct {
	// RecordDecoder, if non-nil, decodes record payloads. The library calls
	// Decode on it; it must be safe for concurrent use because workers in
	// ReadItems and across multiple Read* calls share this single instance.
	// A nil value means passthrough — records are read verbatim, symmetric
	// to the writer's nil NewRecordEncoder.
	//
	// Typical usage: instantiate once at app startup (e.g.
	// zstd.NewDecompressor) and share across all Readers. The caller owns
	// the decoder's lifecycle; the library never calls anything beyond
	// Decode on it.
	RecordDecoder RecordDecoder

	// ContentHashExtract mirrors WriterOptions.ContentHashExtract. When the
	// file was written with an extract function, Verify must apply the same
	// transformation before hashing or the recomputed digest will not match
	// the stored one. Caller responsibility to keep this in sync with the
	// writer-side option, same as for the codec itself.
	//
	// If nil, items are hashed as read from disk.
	ContentHashExtract func(item []byte) ([]byte, error)

	// Concurrency sets the max parallel goroutines for ReadItems. The
	// zero value is normalized to 1 (serial): ReadItems still coalesces
	// consecutive records into single ReadAt calls but does not fan out
	// across goroutines. Negative values are rejected (deferred error
	// surfaced by the first read call). Callers who want parallel
	// scattered I/O must set this explicitly to a value > 1.
	Concurrency int
}

// Trailer holds the parsed trailer fields of an open packfile. The fields
// mirror the on-disk trailer: a caller can introspect the file's metadata
// (e.g. for diagnostic dumps or for verifying a stored Checksum against an
// independent recomputation).
//
// HasContentHash is the typed view of the only currently-defined flag bit;
// the raw flags byte itself is not exposed because no caller can act on
// unknown bits (Open rejects them via knownFlags).
type Trailer struct {
	Version           uint8
	Format            Format
	RecordCount       uint32
	TotalItems        uint32
	ItemsPerRecord    uint32
	IndexForGroupSize uint16
	IndexSize         uint32
	AppDataSize       uint32
	ContentHash       [32]byte
	HasContentHash    bool
	Checksum          uint32 // CRC32C over the leading bytes of the on-disk trailer; validated by unmarshalTrailer
}

// openResult is the transient result produced by doOpen and consumed once
// in waitOpen — Reader unpacks the fields it needs and discards the rest.
type openResult struct {
	file    readAtCloser
	trailer Trailer
	offsets []int64
	appData []byte

	// Decoded from trailer for internal use (int casts of uint32 trailer fields).
	totalItems     int
	itemsPerRecord int

	err error
}

// ioBatch is one batch unit produced by ReadItems' batch partitioner and
// consumed by Reader.processBatch. Kept at package level (rather than
// scoped inside ReadItems) so processBatch can be a Reader method —
// closures inside ReadItems escape to heap and add an allocation per
// call.
type ioBatch struct {
	idxStart    int // index range in the caller's positions slice
	idxEnd      int
	firstRecord int // record range to read in one coalesced ReadAt
	lastRecord  int
}

// recordWorkspacePool holds *record workspaces (slice headers, no decoder,
// no external resources). Items are stateless data — GC reclaims them
// naturally when sync.Pool drains. Process-wide so workspace allocation
// amortizes across every Reader in the process.
//
//nolint:gochecknoglobals // process-wide pool for record workspaces
var recordWorkspacePool = sync.Pool{
	New: func() any { return &record{} },
}

// Reader provides random access to items in a packfile.
//
// Read methods (ReadItem, ReadRange, ReadItems, Verify, the metadata
// accessors) are safe for concurrent use by multiple goroutines. Close is
// NOT safe to call concurrently with any in-flight read: callers must
// ensure every read has returned before invoking Close, otherwise an
// in-flight ReadAt will see "file already closed" wrapped as a read error.
//
// Open returns immediately; all file I/O runs in a background goroutine.
// Public methods block (via waitOpen) until the goroutine completes.
// Close must always be called to release the underlying file handle.
// The RecordDecoder is caller-owned and outlives the Reader; Close does
// not touch it.
type Reader struct {
	// Hot fields (touched by every read call) first so they share a cache line.
	file           readAtCloser
	offsets        []int64
	totalItems     int
	itemsPerRecord int
	concurrency    int
	recordDecoder  RecordDecoder

	// Cold-ish state: populated by waitOpen, read on metadata accessors and
	// Verify but not the per-record hot path.
	trailer            Trailer
	appData            []byte
	contentHashExtract func([]byte) ([]byte, error)

	waitOpen func() error // blocks until background open completes

	closeOnce sync.Once
	closeErr  error
}

// Open returns a Reader immediately. File I/O runs in a background goroutine;
// the first read call blocks until the file is ready. Open itself does not
// return an error — option-validation and I/O failures are deferred to the
// first method call that needs the result.
// Close must always be called.
func Open(path string, opts ReaderOptions) *Reader {
	r := &Reader{
		recordDecoder:      opts.RecordDecoder,
		contentHashExtract: opts.ContentHashExtract,
	}

	// Reject invalid options synchronously and stash the error so every
	// subsequent read returns it. Done before kicking off the background
	// open so we don't burn an open file descriptor on a doomed Reader.
	if opts.Concurrency < 0 {
		err := fmt.Errorf("packfile: Concurrency must be non-negative, got %d", opts.Concurrency)
		r.waitOpen = sync.OnceValue(func() error { return err })
		return r
	}
	r.concurrency = max(opts.Concurrency, 1)

	r.waitOpen = sync.OnceValue(func() error {
		res := doOpen(path)
		if res.err != nil {
			return res.err
		}
		r.file = res.file
		r.trailer = res.trailer
		r.offsets = res.offsets
		r.appData = res.appData
		r.totalItems = res.totalItems
		r.itemsPerRecord = res.itemsPerRecord
		return nil
	})
	// Drive the open from a background goroutine; later waitOpen() calls
	// (one at the head of each public method) get the cached result.
	go func() { _ = r.waitOpen() }()
	return r
}

// doOpen performs all synchronous I/O for opening a packfile.
// On error it closes the file and returns openResult{err: ...}.
//
//nolint:cyclop,nestif,funlen // step-by-step open flow; splitting hurts readability
func doOpen(path string) openResult {
	f, err := os.Open(path)
	if err != nil {
		return openResult{err: fmt.Errorf("packfile: open %q: %w", path, err)}
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = f.Close()
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return openResult{err: fmt.Errorf("packfile: stat: %w", err)}
	}
	fileSize := fi.Size()

	if fileSize < trailerSize {
		return openResult{err: ErrSize}
	}

	// Speculative read: last min(speculativeReadSize, fileSize) bytes.
	speculativeSize := min(int64(speculativeReadSize), fileSize)
	speculativeOff := fileSize - speculativeSize
	speculativeBuf := make([]byte, speculativeSize)
	if _, err := f.ReadAt(speculativeBuf, speculativeOff); err != nil {
		return openResult{err: fmt.Errorf("packfile: read trailer region: %w", err)}
	}

	trailer, err := unmarshalTrailer(speculativeBuf)
	if err != nil {
		return openResult{err: err}
	}
	if !trailer.HasContentHash {
		trailer.ContentHash = [32]byte{}
	}
	recordCount := int(trailer.RecordCount)
	totalItems := int(trailer.TotalItems)
	itemsPerRecord := int(trailer.ItemsPerRecord)
	indexSize := int(trailer.IndexSize)
	appDataSize := int(trailer.AppDataSize)

	indexBase := fileSize - int64(trailerSize) - int64(appDataSize) - int64(indexSize)
	if indexBase < 0 {
		return openResult{err: ErrSize}
	}

	tailSize := int64(indexSize) + int64(appDataSize) + int64(trailerSize)
	var indexBuf []byte
	var appData []byte

	if tailSize <= speculativeSize {
		// Index + appData are already inside the speculative read.
		tailStart := len(speculativeBuf) - int(tailSize)
		indexBuf = make([]byte, indexSize)
		copy(indexBuf, speculativeBuf[tailStart:tailStart+indexSize])
		if appDataSize > 0 {
			appData = make([]byte, appDataSize)
			adStart := tailStart + indexSize
			copy(appData, speculativeBuf[adStart:adStart+appDataSize])
		}
	} else {
		// Single fallback read for index + appData.
		readSize := indexSize + appDataSize
		buf := make([]byte, readSize)
		if readSize > 0 {
			if _, err := f.ReadAt(buf, indexBase); err != nil {
				return openResult{err: fmt.Errorf("packfile: read index region: %w", err)}
			}
		}
		indexBuf = buf[:indexSize]
		if appDataSize > 0 {
			appData = make([]byte, appDataSize)
			copy(appData, buf[indexSize:indexSize+appDataSize])
		}
	}

	offsets, err := decodeIndex(indexBuf, recordCount, indexSize, indexBase)
	if err != nil {
		return openResult{err: err}
	}

	if itemsPerRecord <= 0 && recordCount > 0 {
		return openResult{err: fmt.Errorf("%w: invalid itemsPerRecord %d in trailer",
			ErrCorrupt, itemsPerRecord)}
	}

	// Cross-validate: number of records implied by totalItems must match recordCount.
	if itemsPerRecord > 0 {
		expectedRecords := (totalItems + itemsPerRecord - 1) / itemsPerRecord
		if totalItems == 0 {
			expectedRecords = 0
		}
		if expectedRecords != recordCount {
			return openResult{err: fmt.Errorf(
				"%w: trailer says %d items / %d itemsPerRecord = %d records, but packfile has %d records",
				ErrCorrupt, totalItems, itemsPerRecord, expectedRecords, recordCount)}
		}
	}

	// Empty packfiles may legitimately have itemsPerRecord==0 on disk;
	// default the *internal* int to 1 so modulo math is well-defined. The
	// Trailer view keeps the on-disk value verbatim.
	internalItemsPerRecord := itemsPerRecord
	if internalItemsPerRecord == 0 {
		internalItemsPerRecord = 1
	}

	res := openResult{
		file:           f,
		trailer:        trailer,
		offsets:        offsets,
		appData:        appData,
		totalItems:     totalItems,
		itemsPerRecord: internalItemsPerRecord,
	}

	cleanup = false
	return res
}

// getRecord borrows a workspace from the process-wide pool and binds this
// Reader to it. Callers MUST `defer r.putRecord(rec)` — a missed Put
// pins the Reader via rec.reader until the workspace is GC'd.
//
//nolint:funcorder // pool plumbing kept near callers; matches writer.go style
func (r *Reader) getRecord() *record {
	rec, _ := recordWorkspacePool.Get().(*record)
	rec.reader = r
	return rec
}

// putRecord returns a workspace to the pool. Owned slices (scratch,
// payload, sizes, offsets) reset to length zero with their capacities
// preserved for steady-state zero-alloc reuse. rec.current is cleared
// because in passthrough mode it aliases the caller's read buffer; that
// buffer is about to be returned to its own pool (readBufPool / scratch)
// and must not stay reachable through the pooled workspace.
//
//nolint:funcorder // paired with getRecord
func (r *Reader) putRecord(rec *record) {
	rec.reader = nil
	rec.scratch = rec.scratch[:0]
	rec.payload = rec.payload[:0]
	rec.current = nil
	rec.sizes = rec.sizes[:0]
	rec.offsets = rec.offsets[:0]
	recordWorkspacePool.Put(rec)
}

// TotalItems returns the total number of logical items in the packfile.
func (r *Reader) TotalItems() (int, error) {
	if err := r.waitOpen(); err != nil {
		return 0, err
	}
	return r.totalItems, nil
}

// Trailer returns the parsed trailer.
func (r *Reader) Trailer() (Trailer, error) {
	if err := r.waitOpen(); err != nil {
		return Trailer{}, err
	}
	return r.trailer, nil
}

// AppData returns the app data section, or nil if appDataSize == 0.
func (r *Reader) AppData() ([]byte, error) {
	if err := r.waitOpen(); err != nil {
		return nil, err
	}
	return r.appData, nil
}

// ContentHash returns the SHA-256 content hash stored in the trailer, if present.
func (r *Reader) ContentHash() ([32]byte, bool, error) {
	if err := r.waitOpen(); err != nil {
		return [32]byte{}, false, err
	}
	return r.trailer.ContentHash, r.trailer.HasContentHash, nil
}

// ReadItem reads a single item by position and passes it to fn.
// The []byte passed to fn is borrowed and must not be retained after fn
// returns — copy if needed. Returns ErrPositionOutOfRange if position is
// out of [0, TotalItems).
func (r *Reader) ReadItem(position int, fn func([]byte) error) error {
	if err := r.waitOpen(); err != nil {
		return err
	}
	if position < 0 || position >= r.totalItems {
		return ErrPositionOutOfRange
	}

	// One IDIV instead of div+mod separately.
	recordIdx := position / r.itemsPerRecord
	localIdx := position - recordIdx*r.itemsPerRecord

	rec := r.getRecord()
	defer r.putRecord(rec)

	start, end := r.offsets[recordIdx], r.offsets[recordIdx+1]
	size := int(end - start)
	if cap(rec.scratch) < size {
		rec.scratch = make([]byte, size)
	} else {
		rec.scratch = rec.scratch[:size]
	}
	if _, err := r.file.ReadAt(rec.scratch, start); err != nil {
		return fmt.Errorf("packfile: read record %d: %w", recordIdx, err)
	}
	if err := rec.decode(rec.scratch, recordIdx); err != nil {
		return err
	}
	return fn(rec.item(localIdx))
}

// ReadRange returns an iterator over count contiguous items starting at start.
// Consecutive records are coalesced into single ReadAt calls using a pooled
// 1MB buffer, minimizing I/O syscalls for large ranges.
// Each yielded []byte is valid only until the next iteration — copy if you
// need to retain it. Safe to break early.
//
// Concurrent ReadRange calls on the same Reader are safe; the returned
// iterator itself is NOT safe for concurrent iteration (it closure-captures
// a pooled record + read buffer) — iterate from one goroutine.
//
// Use ReadRange for in-order streaming reads (iter.Seq2 with break-early
// semantics, no concurrency overhead). Use ReadItems for sorted-or-scattered
// positions with optional worker fan-out; ReadItems may yield out of order
// under concurrency, while ReadRange always yields strictly in order.
//
// Yields ErrPositionOutOfRange (one-shot) if start or count is negative or
// the range falls outside [0, TotalItems).
//
//nolint:gocognit,cyclop // single batched-coalesce loop; splitting hurts readability
func (r *Reader) ReadRange(start, count int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}
		if start < 0 || count < 0 || start > r.totalItems || count > r.totalItems-start {
			yield(nil, fmt.Errorf("%w: ReadRange(%d, %d) out of [0, %d)",
				ErrPositionOutOfRange, start, count, r.totalItems))
			return
		}
		if count == 0 {
			return
		}

		firstRecord := start / r.itemsPerRecord
		lastRecord := (start + count - 1) / r.itemsPerRecord
		end := start + count // one past last item

		rec := r.getRecord()
		defer r.putRecord(rec)

		bp, _ := readBufPool.Get().(*[]byte)
		buf := *bp
		defer readBufPool.Put(bp)

		globalIdx := start

		// yieldRecord decodes a record and yields the items within the
		// [globalIdx, end) range. Returns false if the consumer broke early.
		yieldRecord := func(recData []byte, recIdx int) bool {
			if err := rec.decode(recData, recIdx); err != nil {
				yield(nil, err)
				return false
			}
			recStart := recIdx * r.itemsPerRecord
			lo := globalIdx - recStart
			hi := min(len(rec.sizes), end-recStart)
			for i := lo; i < hi; i++ {
				if !yield(rec.item(i), nil) {
					return false
				}
				globalIdx++
			}
			return true
		}

		// Batch consecutive records into single ReadAt calls.
		batchStart := firstRecord
		for batchStart <= lastRecord {
			batchEnd := batchStart + 1
			for batchEnd <= lastRecord && r.offsets[batchEnd+1]-r.offsets[batchStart] <= int64(len(buf)) {
				batchEnd++
			}

			// If a single record exceeds the buffer, allocate one-off.
			recBytes := r.offsets[batchStart+1] - r.offsets[batchStart]
			if recBytes > int64(len(buf)) {
				oneOff := make([]byte, recBytes)
				if _, err := r.file.ReadAt(oneOff, r.offsets[batchStart]); err != nil {
					yield(nil, fmt.Errorf("packfile: read record %d: %w", batchStart, err))
					return
				}
				if !yieldRecord(oneOff, batchStart) {
					return
				}
				batchStart++
				continue
			}

			batchBytes := r.offsets[batchEnd] - r.offsets[batchStart]
			readBuf := buf[:batchBytes]
			if _, err := r.file.ReadAt(readBuf, r.offsets[batchStart]); err != nil {
				yield(nil, fmt.Errorf("packfile: read records [%d, %d): %w", batchStart, batchEnd, err))
				return
			}

			for j := batchStart; j < batchEnd; j++ {
				lo := r.offsets[j] - r.offsets[batchStart]
				hi := r.offsets[j+1] - r.offsets[batchStart]
				if !yieldRecord(readBuf[lo:hi], j) {
					return
				}
			}

			batchStart = batchEnd
		}
	}
}

// ReadItems reads items at scattered positions and calls fn for each item.
// fn receives the index in the original positions slice and a borrowed data
// slice valid only for the duration of the call — copy if needed.
//
// fn may be called concurrently from up to min(ReaderOptions.Concurrency,
// number of I/O batches) goroutines, and in arbitrary order. With a single
// batch the work runs entirely in the calling goroutine regardless of
// configured Concurrency (no goroutine spawn). The idx argument identifies
// which element in positions the data corresponds to.
//
// Batching notes: many positions inside the same record collapse into a
// single batch (so one worker drains them serially regardless of
// Concurrency). A single record larger than the 1 MiB coalesced-read
// buffer falls back to a one-off allocation for that batch's ReadAt.
//
// positions must be sorted ascending with no duplicates. Returns
// ErrPositionOutOfRange if any position is outside [0, TotalItems) or
// ErrPositionsUnsorted if positions are not strictly sorted.
//
//nolint:gocognit,cyclop // batch partitioning + worker fan-out; splitting hurts readability
func (r *Reader) ReadItems(ctx context.Context, positions []int, fn func(idx int, data []byte) error) error {
	if err := r.waitOpen(); err != nil {
		return err
	}

	for i, pos := range positions {
		if pos < 0 || pos >= r.totalItems {
			return fmt.Errorf("%w: ReadItems position %d out of [0, %d)",
				ErrPositionOutOfRange, pos, r.totalItems)
		}
		if i > 0 && positions[i] <= positions[i-1] {
			return fmt.Errorf("%w: ReadItems positions at %d: %d <= %d",
				ErrPositionsUnsorted, i, positions[i], positions[i-1])
		}
	}

	if len(positions) == 0 {
		return nil
	}

	maxPerBatch := max(1, (len(positions)+r.concurrency-1)/r.concurrency)
	batches := make([]ioBatch, 0, r.concurrency)
	batchIdxStart := 0
	firstRec := positions[0] / r.itemsPerRecord
	lastRec := firstRec
	batchBytes := r.offsets[firstRec+1] - r.offsets[firstRec]

	for i := 1; i < len(positions); i++ {
		rec := positions[i] / r.itemsPerRecord
		if rec == lastRec {
			continue
		}
		recBytes := r.offsets[rec+1] - r.offsets[rec]
		if rec == lastRec+1 &&
			batchBytes+recBytes <= int64(readBufSize) &&
			i-batchIdxStart < maxPerBatch {
			batchBytes += recBytes
			lastRec = rec
		} else {
			batches = append(batches, ioBatch{batchIdxStart, i, firstRec, lastRec})
			batchIdxStart = i
			firstRec = rec
			lastRec = rec
			batchBytes = recBytes
		}
	}
	batches = append(batches, ioBatch{batchIdxStart, len(positions), firstRec, lastRec})

	if err := ctx.Err(); err != nil {
		return err
	}

	numWorkers := min(len(batches), r.concurrency)

	// Serial fast path: no goroutine spawn, no atomic dispatch, no errgroup.
	if numWorkers == 1 {
		rec := r.getRecord()
		defer r.putRecord(rec)
		bp, _ := readBufPool.Get().(*[]byte)
		defer readBufPool.Put(bp)
		for i := range batches {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := r.processBatch(rec, *bp, positions, batches[i], fn); err != nil {
				return err
			}
		}
		return nil
	}

	// Concurrent fan-out: workers steal batches via an atomic counter,
	// errgroup captures the first error and cancels the derived ctx.
	var nextBatch atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	for range numWorkers {
		g.Go(func() error {
			rec := r.getRecord()
			defer r.putRecord(rec)
			bp, _ := readBufPool.Get().(*[]byte)
			defer readBufPool.Put(bp)
			for {
				bi := int(nextBatch.Add(1)) - 1
				if bi >= len(batches) {
					return nil
				}
				if err := gctx.Err(); err != nil {
					return err
				}
				if err := r.processBatch(rec, *bp, positions, batches[bi], fn); err != nil {
					return err
				}
			}
		})
	}
	return g.Wait()
}

// processBatch handles one batch's ReadAt + decode + fn loop. Shared
// between ReadItems' serial fast path (Concurrency=1) and its worker
// fan-out so the per-batch logic lives in one place. Made a method
// (rather than a closure inside ReadItems) so it doesn't capture and
// escape — one less heap alloc per ReadItems call.
//
//nolint:funcorder,lll // helper for ReadItems; positions/fn passed explicitly so processBatch isn't a closure (no escape)
func (r *Reader) processBatch(
	rec *record, buf []byte, positions []int, batch ioBatch, fn func(int, []byte) error,
) error {
	readStart := r.offsets[batch.firstRecord]
	readEnd := r.offsets[batch.lastRecord+1]
	readSize := readEnd - readStart

	var readBuf []byte
	if readSize <= int64(len(buf)) {
		readBuf = buf[:readSize]
	} else {
		readBuf = make([]byte, readSize)
	}

	if _, err := r.file.ReadAt(readBuf, readStart); err != nil {
		return fmt.Errorf("packfile: read records [%d, %d]: %w",
			batch.firstRecord, batch.lastRecord, err)
	}

	prevRec := -1
	for k := batch.idxStart; k < batch.idxEnd; k++ {
		recIdx := positions[k] / r.itemsPerRecord
		localIdx := positions[k] - recIdx*r.itemsPerRecord
		if recIdx != prevRec {
			recOff := r.offsets[recIdx] - readStart
			recEnd := r.offsets[recIdx+1] - readStart
			if err := rec.decode(readBuf[recOff:recEnd], recIdx); err != nil {
				return err
			}
			prevRec = recIdx
		}
		if err := fn(k, rec.item(localIdx)); err != nil {
			return err
		}
	}
	return nil
}

// Verify recomputes the SHA-256 content hash by streaming all items and
// compares it to the hash stored in the trailer. Returns nil if no hash is
// stored or if the hash matches.
func (r *Reader) Verify(ctx context.Context) error {
	if err := r.waitOpen(); err != nil {
		return err
	}
	if !r.trailer.HasContentHash {
		return nil
	}

	hasher := newContentHasher(r.itemsPerRecord)
	i := 0
	for item, err := range r.ReadRange(0, r.totalItems) {
		if err != nil {
			return err
		}
		toHash := item
		if r.contentHashExtract != nil {
			toHash, err = r.contentHashExtract(item)
			if err != nil {
				return fmt.Errorf("packfile: Verify ContentHashExtract item %d: %w", i, err)
			}
		}
		hasher.Add(toHash)
		i++
		if i%r.itemsPerRecord == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	computed := hasher.Sum()
	if computed != r.trailer.ContentHash {
		return fmt.Errorf("%w: expected %x, got %x",
			ErrContentHashMismatch, r.trailer.ContentHash, computed)
	}
	return nil
}

// Close releases the underlying file handle. Safe to call multiple times.
// Must always be called, even if no query methods were called.
//
// Close blocks until the background open finishes (so it can collect the
// open error to return alongside the file-close error via errors.Join).
// If the open is in flight when Close is invoked, expect a small delay.
//
// The caller-supplied RecordDecoder is not closed by Reader.Close — the
// decoder is caller-owned (typically a single shared instance across many
// Readers); its lifecycle is the caller's responsibility.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		openErr := r.waitOpen()
		var closeErr error
		if r.file != nil {
			closeErr = r.file.Close()
		}
		r.closeErr = errors.Join(openErr, closeErr)
	})
	return r.closeErr
}
