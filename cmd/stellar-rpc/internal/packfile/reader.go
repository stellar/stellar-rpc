package packfile

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"sync"
	"sync/atomic"
)

const (
	readBufSize         = 1 << 20    // 1 MiB pooled coalesced-read buffer
	speculativeReadSize = 256 * 1024 // 256 KiB tail prefetch on Open
)

// Reader-side errors that come from trailer parsing on Open.
var (
	ErrMagic   = fmt.Errorf("%w: invalid magic number", ErrCorrupt)
	ErrVersion = fmt.Errorf("%w: unsupported version", ErrCorrupt)
	ErrSize    = fmt.Errorf("%w: file size inconsistent with trailer", ErrCorrupt)

	// ErrPositionOutOfRange is returned by ReadItem when position is outside
	// [0, TotalItems).
	ErrPositionOutOfRange = errors.New("packfile: position out of range")
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
	// NewRecordDecoder, if non-nil, returns a per-decoder RecordDecoder used
	// to decode record payloads. Each pooled decoder constructs its own via
	// this factory and Closes it when the Reader is closed. If nil, records
	// are read verbatim (passthrough — symmetric to the writer's nil
	// NewRecordEncoder).
	NewRecordDecoder func() RecordDecoder

	// Concurrency sets the max parallel goroutines for ReadItems. 0 (the
	// zero value) means serial: ReadItems still coalesces consecutive
	// records into single ReadAt calls but does not fan out across
	// goroutines. Negative values are rejected (deferred error surfaced
	// by the first read call). Callers who want parallel scattered I/O
	// must set this explicitly.
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
	Checksum          uint32
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

// Reader provides random access to items in a packfile.
// Safe for concurrent use by multiple goroutines.
//
// Open returns immediately; all file I/O runs in a background goroutine.
// Public methods block (via waitOpen) until the goroutine completes.
// Close must always be called to release resources.
type Reader struct {
	// File-derived state, populated atomically by waitOpen.
	file           readAtCloser
	trailer        Trailer
	offsets        []int64
	appData        []byte
	totalItems     int
	itemsPerRecord int

	concurrency      int
	newRecordDecoder func() RecordDecoder

	waitOpen func() error // blocks until background open completes

	// All decoders ever created by decoderPool.New, used so Close can close
	// every caller-supplied RecordDecoder deterministically. sync.Pool
	// itself doesn't expose a way to drain.
	decoderMu   sync.Mutex
	decoders    []*decoder
	decoderPool sync.Pool

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
		newRecordDecoder: opts.NewRecordDecoder,
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

	r.decoderPool.New = func() any {
		rd := &decoder{}
		if r.newRecordDecoder != nil {
			rd.rec = r.newRecordDecoder()
		}
		r.decoderMu.Lock()
		r.decoders = append(r.decoders, rd)
		r.decoderMu.Unlock()
		return rd
	}

	ch := make(chan openResult, 1)
	go func() {
		defer func() {
			if rv := recover(); rv != nil {
				ch <- openResult{err: fmt.Errorf("packfile: panic in Open: %v", rv)}
			}
		}()
		ch <- doOpen(path)
	}()
	r.waitOpen = sync.OnceValue(func() error {
		res := <-ch
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
	return r
}

// doOpen performs all synchronous I/O for opening a packfile.
// On error it closes the file and returns openResult{err: ...}.
//
//nolint:cyclop,nestif,funlen // step-by-step open flow; splitting hurts readability
func doOpen(path string) openResult {
	f, err := os.Open(path)
	if err != nil {
		return openResult{err: err}
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = f.Close()
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return openResult{err: err}
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

	// Trailer layout is documented at the tOff* constants in packfile.go.
	tb := speculativeBuf[len(speculativeBuf)-trailerSize:]

	if m := binary.LittleEndian.Uint32(tb[tOffMagic:]); m != magic {
		return openResult{err: ErrMagic}
	}
	v := tb[tOffVersion]
	if v != version {
		return openResult{err: ErrVersion}
	}
	flags := tb[tOffFlags]
	format := Format(binary.LittleEndian.Uint32(tb[tOffFormat:]))
	recordCount := int(binary.LittleEndian.Uint32(tb[tOffRecordCount:]))
	totalItems := int(binary.LittleEndian.Uint32(tb[tOffTotalItems:]))
	itemsPerRecord := int(binary.LittleEndian.Uint32(tb[tOffItemsPerRecord:]))
	indexGroupSize := int(binary.LittleEndian.Uint16(tb[tOffIndexGroupSize:]))
	indexSize := int(binary.LittleEndian.Uint32(tb[tOffIndexSize:]))
	appDataSize := int(binary.LittleEndian.Uint32(tb[tOffAppDataSize:]))
	var contentHash [32]byte
	copy(contentHash[:], tb[tOffContentHash:tEndContentHash])
	storedCRC := binary.LittleEndian.Uint32(tb[tOffCRC:])

	if crc32c(tb[:trailerCRCEnd]) != storedCRC {
		return openResult{err: ErrChecksum}
	}

	if indexGroupSize != groupSize {
		return openResult{err: fmt.Errorf("%w: unsupported index group size %d (expected %d)",
			ErrCorrupt, indexGroupSize, groupSize)}
	}
	if flags&^knownFlags != 0 {
		return openResult{err: fmt.Errorf("%w: unknown trailer flags: %02x", ErrCorrupt, flags)}
	}
	hasContentHash := flags&flagContentHash != 0
	if !hasContentHash {
		contentHash = [32]byte{}
	}

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
		file: f,
		trailer: Trailer{
			Version:           v,
			Format:            format,
			RecordCount:       uint32(recordCount),    //nolint:gosec // bounded by trailer field width
			TotalItems:        uint32(totalItems),     //nolint:gosec // bounded by trailer field width
			ItemsPerRecord:    uint32(itemsPerRecord), //nolint:gosec // bounded by trailer field width
			IndexForGroupSize: uint16(indexGroupSize),
			IndexSize:         uint32(indexSize),   //nolint:gosec // bounded by trailer field width
			AppDataSize:       uint32(appDataSize), //nolint:gosec // bounded by trailer field width
			ContentHash:       contentHash,
			HasContentHash:    hasContentHash,
			Checksum:          storedCRC,
		},
		offsets:        offsets,
		appData:        appData,
		totalItems:     totalItems,
		itemsPerRecord: internalItemsPerRecord,
	}

	cleanup = false
	return res
}

// getDecoder returns a pooled decoder configured for this reader. Placed
// here, between the lifecycle constructors above and the read methods below,
// because every read path goes through it.
//
//nolint:funcorder // pool plumbing kept near callers; matches writer.go style
func (r *Reader) getDecoder() *decoder {
	rd, _ := r.decoderPool.Get().(*decoder)
	rd.totalItems = r.totalItems
	rd.itemsPerRecord = r.itemsPerRecord
	return rd
}

// putDecoder returns a decoder to the pool. Its RecordDecoder, if any, stays
// bound for reuse — Reader.Close closes it on shutdown.
//
//nolint:funcorder // paired with getDecoder
func (r *Reader) putDecoder(rd *decoder) {
	rd.totalItems = 0
	rd.itemsPerRecord = 0
	rd.scratch = rd.scratch[:0]
	rd.decompressed = rd.decompressed[:0]
	rd.sizes = rd.sizes[:0]
	rd.offsets = rd.offsets[:0]
	r.decoderPool.Put(rd)
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

	recordIdx := position / r.itemsPerRecord
	localIdx := position % r.itemsPerRecord

	rd := r.getDecoder()
	defer r.putDecoder(rd)

	start, end := r.offsets[recordIdx], r.offsets[recordIdx+1]
	size := int(end - start)
	if cap(rd.scratch) < size {
		rd.scratch = make([]byte, size)
	} else {
		rd.scratch = rd.scratch[:size]
	}
	if _, err := r.file.ReadAt(rd.scratch, start); err != nil {
		return err
	}
	if err := rd.decodeRecord(rd.scratch, recordIdx); err != nil {
		return err
	}
	return fn(rd.Item(localIdx))
}

// ReadRange returns an iterator over count contiguous items starting at start.
// Consecutive records are coalesced into single ReadAt calls using a pooled
// 1MB buffer, minimizing I/O syscalls for large ranges.
// Each yielded []byte is valid only until the next iteration — copy if you
// need to retain it. Safe to break early. Thread-safe.
//
// Yields ErrPositionOutOfRange (one-shot) if start or count is negative or
// the range falls outside [0, TotalItems).
//
//nolint:gocognit,cyclop // single batched-coalesce loop; splitting hurts readability
func (r *Reader) ReadRange(start, count int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if count == 0 {
			return
		}
		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}
		if start < 0 || count < 0 || start > r.totalItems || count > r.totalItems-start {
			yield(nil, fmt.Errorf("%w: ReadRange(%d, %d) out of [0, %d)",
				ErrPositionOutOfRange, start, count, r.totalItems))
			return
		}

		firstRecord := start / r.itemsPerRecord
		lastRecord := (start + count - 1) / r.itemsPerRecord
		end := start + count // one past last item

		rd := r.getDecoder()
		defer r.putDecoder(rd)

		bp, _ := readBufPool.Get().(*[]byte)
		buf := *bp
		defer readBufPool.Put(bp)

		globalIdx := start

		// yieldRecord decodes a record and yields the items within the
		// [globalIdx, end) range. Returns false if the consumer broke early.
		yieldRecord := func(recData []byte, recIdx int) bool {
			if err := rd.decodeRecord(recData, recIdx); err != nil {
				yield(nil, err)
				return false
			}
			recStart := recIdx * r.itemsPerRecord
			lo := globalIdx - recStart
			hi := min(len(rd.sizes), end-recStart)
			for i := lo; i < hi; i++ {
				if !yield(rd.Item(i), nil) {
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
					yield(nil, err)
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
				yield(nil, err)
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

// ReadItems reads items at scattered positions with parallel I/O and calls
// fn for each item. fn receives the index in the original positions slice and
// a borrowed data slice valid only for the duration of the call — copy if
// needed.
//
// fn is called concurrently from multiple goroutines, in arbitrary order.
// The idx argument identifies which element in positions the data corresponds to.
//
// positions must be sorted ascending with no duplicates. Returns
// ErrPositionOutOfRange if any position is outside [0, TotalItems) or if
// positions are not strictly sorted.
//
//nolint:gocognit,cyclop,funlen // batch partitioning + worker fan-out; splitting hurts readability
func (r *Reader) ReadItems(ctx context.Context, positions []int, fn func(idx int, data []byte) error) error {
	if len(positions) == 0 {
		return nil
	}
	if err := r.waitOpen(); err != nil {
		return err
	}

	for i, pos := range positions {
		if pos < 0 || pos >= r.totalItems {
			return fmt.Errorf("%w: ReadItems position %d out of [0, %d)",
				ErrPositionOutOfRange, pos, r.totalItems)
		}
		if i > 0 && positions[i] <= positions[i-1] {
			return fmt.Errorf("%w: ReadItems positions not strictly sorted at %d: %d <= %d",
				ErrPositionOutOfRange, i, positions[i], positions[i-1])
		}
	}

	// Batch partitioning for concurrent I/O.
	type ioBatch struct {
		idxStart    int
		idxEnd      int
		firstRecord int
		lastRecord  int
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
	var nextBatch atomic.Int64
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	var canceled atomic.Bool

	// setErr records the first error and signals workers to stop. Workers
	// must still `return` after calling it; setErr does not unwind the goroutine.
	setErr := func(err error) {
		errOnce.Do(func() { firstErr = err })
		canceled.Store(true)
	}

	for range numWorkers {
		wg.Go(func() {
			rd := r.getDecoder()
			defer r.putDecoder(rd)
			bp, _ := readBufPool.Get().(*[]byte)
			buf := *bp
			defer readBufPool.Put(bp)

			for {
				bi := int(nextBatch.Add(1)) - 1
				if bi >= len(batches) || canceled.Load() {
					return
				}
				if err := ctx.Err(); err != nil {
					setErr(err)
					return
				}
				batch := batches[bi]

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
					setErr(err)
					return
				}

				prevRec := -1
				for k := batch.idxStart; k < batch.idxEnd; k++ {
					rec, localIdx := positions[k]/r.itemsPerRecord, positions[k]%r.itemsPerRecord
					if rec != prevRec {
						recOff := r.offsets[rec] - readStart
						recEnd := r.offsets[rec+1] - readStart
						if err := rd.decodeRecord(readBuf[recOff:recEnd], rec); err != nil {
							setErr(err)
							return
						}
						prevRec = rec
					}
					if err := fn(k, rd.Item(localIdx)); err != nil {
						setErr(err)
						return
					}
				}
			}
		})
	}
	wg.Wait()
	return firstErr
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
		hasher.Add(item)
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

// Close releases all resources. Safe to call multiple times.
// Must always be called, even if no query methods were called.
//
// Close is NOT safe to call concurrently with in-flight read methods —
// callers must ensure every ReadItem / ReadRange / ReadItems call has
// returned before invoking Close, otherwise a worker mid-Decode may race
// with a RecordDecoder.Close on the decoder it is currently using.
// Read methods themselves are safe to call concurrently with one another;
// only the Close vs read interaction needs serialization at the caller.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		_ = r.waitOpen() // drain goroutine, populate all fields
		var errs []error
		r.decoderMu.Lock()
		for _, rd := range r.decoders {
			if err := rd.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		r.decoders = nil
		r.decoderMu.Unlock()
		if r.file != nil {
			if err := r.file.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}
