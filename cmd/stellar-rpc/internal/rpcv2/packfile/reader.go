package packfile

import (
	"bytes"
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
	maxRunBytes      = 256 << 10 // largest merged read: one EBS I/O
	defaultFirstRead = 256 << 10
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
const knownFlags = flagContentHash | flagRecordChecksum

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

	// Concurrency is the most reads ReadItems keeps in flight; it does not
	// change which reads it makes. Zero means 1, on the calling goroutine. A
	// negative value is an error, returned by the first read call.
	Concurrency int

	// FirstRead sizes Open's first read from the end of the file, given the
	// file size; nil means 256 KiB. Results are clamped to [trailer size,
	// file size]; a tail that does not fit takes one more read for the rest.
	FirstRead func(fileSize int64) int
}

// openResult is the transient result produced by doOpen and consumed once
// in waitOpen — Reader unpacks the fields it needs and discards the rest.
type openResult struct {
	file    readAtCloser
	trailer Trailer
	idx     *index
	appData []byte

	err error
}

// readRun is one ReadAt in ReadItems and the positions it serves.
type readRun struct {
	idxStart int // index range in the caller's positions slice
	idxEnd   int
	start    int64 // the records' bytes in the file
	end      int64
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
	file          readAtCloser
	idx           *index
	concurrency   int
	recordDecoder RecordDecoder
	// recordChecksum mirrors trailer.HasRecordChecksum, hoisted out of the
	// cold trailer struct because record.decode reads it per record.
	recordChecksum bool

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
		res := doOpen(path, opts.FirstRead)
		if res.err != nil {
			return res.err
		}
		r.file = res.file
		r.trailer = res.trailer
		r.idx = res.idx
		r.appData = res.appData
		r.recordChecksum = res.trailer.HasRecordChecksum
		return nil
	})
	// Drive the open from a background goroutine; later waitOpen() calls
	// (one at the head of each public method) get the cached result.
	go func() { _ = r.waitOpen() }()
	return r
}

// doOpen performs all synchronous I/O for opening a packfile.
// On error it closes the file and returns openResult{err: ...}.
func doOpen(path string, firstRead func(int64) int) openResult {
	f, err := os.Open(path)
	if err != nil {
		return openResult{err: fmt.Errorf("packfile: open %q: %w", path, err)}
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return openResult{err: fmt.Errorf("packfile: stat: %w", err)}
	}
	res := openFile(f, fi.Size(), firstRead)
	if res.err != nil {
		_ = f.Close()
	}
	return res
}

func openFile(f readAtCloser, fileSize int64, firstRead func(int64) int) openResult {
	if fileSize < trailerSize {
		return openResult{err: ErrSize}
	}

	first := int64(defaultFirstRead)
	if firstRead != nil {
		first = int64(firstRead(fileSize))
	}
	first = min(max(first, trailerSize), fileSize)
	rec, _ := recordWorkspacePool.Get().(*record)
	defer recordWorkspacePool.Put(rec)
	if int64(cap(rec.scratch)) < first {
		rec.scratch = make([]byte, 0, first)
	}
	tail := rec.scratch[:first]
	if _, err := f.ReadAt(tail, fileSize-first); err != nil {
		return openResult{err: fmt.Errorf("packfile: read trailer region: %w", err)}
	}

	trailer, err := unmarshalTrailer(tail)
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

	// The Reader keeps the index and app data as views into tail, so tail
	// must not be the pooled first-read buffer, which goes back to the pool.
	tailSize := int64(indexSize) + int64(appDataSize) + int64(trailerSize)
	indexBase := fileSize - tailSize
	if indexBase < 0 {
		return openResult{err: ErrSize}
	}
	if tailSize <= first {
		tail = bytes.Clone(tail[first-tailSize:])
	} else {
		full := make([]byte, tailSize)
		copy(full[tailSize-first:], tail)
		if _, err := f.ReadAt(full[:tailSize-first], indexBase); err != nil {
			return openResult{err: fmt.Errorf("packfile: read index region: %w", err)}
		}
		tail = full
	}
	var appData []byte
	if appDataSize > 0 {
		appData = tail[indexSize : indexSize+appDataSize]
	}

	// App data is CRC-covered like the index and the trailer. It has to be:
	// the section is opaque to this library, so nothing here can check it
	// structurally, and a consumer's own decoder cannot tell plausible
	// corruption from real data.
	if computed := crc32c(appData); computed != trailer.AppDataCRC {
		return openResult{err: fmt.Errorf("%w: app data CRC32C (stored %08x, computed %08x)",
			ErrChecksum, trailer.AppDataCRC, computed)}
	}

	idx, err := parseIndex(tail[:indexSize], recordCount, totalItems, itemsPerRecord, indexBase)
	if err != nil {
		return openResult{err: err}
	}

	return openResult{file: f, trailer: trailer, idx: idx, appData: appData}
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
// preserved for steady-state zero-alloc reuse. The group table is emptied,
// since the next borrower may read another pack.
//
//nolint:funcorder // paired with getRecord
func (r *Reader) putRecord(rec *record) {
	rec.reader = nil
	rec.scratch = rec.scratch[:0]
	rec.payload = rec.payload[:0]
	rec.current = nil
	rec.sizes = rec.sizes[:0]
	rec.offsets = rec.offsets[:0]
	rec.tab.n = 0
	recordWorkspacePool.Put(rec)
}

// TotalItems returns the total number of logical items in the packfile.
func (r *Reader) TotalItems() (int, error) {
	if err := r.waitOpen(); err != nil {
		return 0, err
	}
	return r.idx.items, nil
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
//
// Loans are capacity-clipped, so a borrower may read and append freely; it may
// not retain. Lifetimes differ: ReadItem's until fn returns, ReadItems' until
// that item's callback returns (records are reused between callbacks), and
// ReadRange's until the loop body ends, break included.
func (r *Reader) ReadItem(position int, fn func([]byte) error) error {
	if err := r.waitOpen(); err != nil {
		return err
	}
	if position < 0 || position >= r.idx.items {
		return ErrPositionOutOfRange
	}

	rec := r.getRecord()
	defer r.putRecord(rec)

	recordIdx, s, err := rec.tab.locate(r.idx, position)
	if err != nil {
		return err
	}
	buf, err := r.readRecords(rec, s.start, s.end)
	if err != nil {
		return err
	}
	if err := rec.decode(buf, recordIdx, s.n); err != nil {
		return err
	}
	return fn(rec.item(position - s.first))
}

// ReadRange returns an iterator over count contiguous items starting at start.
// Adjacent records merge into reads of up to 256 KiB, made one at a time.
// Each yielded []byte is valid only until the loop body ends, break included — copy if you
// need to retain it. Safe to break early.
//
// Concurrent ReadRange calls on the same Reader are safe; the returned
// iterator itself is NOT safe for concurrent iteration (it closure-captures
// a pooled record). Iterate from one goroutine.
//
// Use ReadRange for in-order streaming reads (iter.Seq2 with break-early
// semantics, no concurrency overhead). Use ReadItems for sorted-or-scattered
// positions with optional worker fan-out; ReadItems may yield out of order
// under concurrency, while ReadRange always yields strictly in order.
//
// Yields ErrPositionOutOfRange (one-shot) if start or count is negative or
// the range falls outside [0, TotalItems).
//
//nolint:gocognit,cyclop // single merged-read loop; splitting hurts readability
func (r *Reader) ReadRange(start, count int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}
		if start < 0 || count < 0 || start > r.idx.items || count > r.idx.items-start {
			yield(nil, fmt.Errorf("%w: ReadRange(%d, %d) out of [0, %d)",
				ErrPositionOutOfRange, start, count, r.idx.items))
			return
		}
		if count == 0 {
			return
		}

		end := start + count // one past last item

		rec := r.getRecord()
		defer r.putRecord(rec)

		firstRecord, _, err := rec.tab.locate(r.idx, start)
		if err != nil {
			yield(nil, err)
			return
		}
		lastRecord, _, err := rec.tab.locate(r.idx, end-1)
		if err != nil {
			yield(nil, err)
			return
		}

		for first := firstRecord; first <= lastRecord; {
			s, err := rec.tab.record(r.idx, first)
			if err != nil {
				yield(nil, err)
				return
			}
			last, runStart, runEnd := first, s.start, s.end
			for last < lastRecord {
				if s, err = rec.tab.record(r.idx, last+1); err != nil {
					yield(nil, err)
					return
				}
				if s.end-runStart > maxRunBytes {
					break
				}
				last, runEnd = last+1, s.end
			}
			buf, err := r.readRecords(rec, runStart, runEnd)
			if err != nil {
				yield(nil, err)
				return
			}
			for j := first; j <= last; j++ {
				if s, err = rec.tab.record(r.idx, j); err != nil {
					yield(nil, err)
					return
				}
				if err := rec.decode(buf[s.start-runStart:s.end-runStart], j, s.n); err != nil {
					yield(nil, err)
					return
				}
				for i := max(start-s.first, 0); i < min(s.n, end-s.first); i++ {
					if !yield(rec.item(i), nil) {
						return
					}
				}
			}
			first = last + 1
		}
	}
}

// ReadItems reads items at scattered positions and calls fn for each item.
// fn receives the index in the original positions slice and a borrowed data
// slice valid only for the duration of the call — copy if needed.
//
// fn runs on up to min(ReaderOptions.Concurrency, number of reads)
// goroutines, concurrently and in arbitrary order; with Concurrency 1 or a
// single read, it runs on the calling goroutine. Positions in one record
// share one read and decode, and adjacent records merge into reads of up to
// 256 KiB.
//
// positions must be sorted ascending with no duplicates. Returns
// ErrPositionOutOfRange if any position is outside [0, TotalItems) or
// ErrPositionsUnsorted if positions are not strictly sorted.
//
//nolint:gocognit,cyclop // run formation + worker fan-out; splitting hurts readability
func (r *Reader) ReadItems(ctx context.Context, positions []int, fn func(idx int, data []byte) error) error {
	if err := r.waitOpen(); err != nil {
		return err
	}

	for i, pos := range positions {
		if pos < 0 || pos >= r.idx.items {
			return fmt.Errorf("%w: ReadItems position %d out of [0, %d)",
				ErrPositionOutOfRange, pos, r.idx.items)
		}
		if i > 0 && positions[i] <= positions[i-1] {
			return fmt.Errorf("%w: ReadItems positions at %d: %d <= %d",
				ErrPositionsUnsorted, i, positions[i], positions[i-1])
		}
	}

	if len(positions) == 0 {
		return nil
	}

	rec := r.getRecord()
	defer r.putRecord(rec)

	var runs []readRun
	for i := 0; i < len(positions); {
		last, s, err := rec.tab.locate(r.idx, positions[i])
		if err != nil {
			return err
		}
		start := s.start
		j := i + 1
		for ; j < len(positions); j++ {
			if positions[j] < s.first+s.n {
				continue
			}
			next, ns, err := rec.tab.locate(r.idx, positions[j])
			if err != nil {
				return err
			}
			if next != last+1 || ns.end-start > maxRunBytes {
				break
			}
			last, s = next, ns
		}
		runs = append(runs, readRun{i, j, start, s.end})
		i = j
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	numWorkers := min(len(runs), r.concurrency)

	// Serial fast path: no goroutine spawn, no atomic dispatch, no errgroup.
	if numWorkers == 1 {
		for i := range runs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := r.processRun(rec, positions, runs[i], fn); err != nil {
				return err
			}
		}
		return nil
	}

	// Concurrent fan-out: workers take runs from an atomic counter.
	var nextRun atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	for range numWorkers {
		g.Go(func() error {
			rec := r.getRecord()
			defer r.putRecord(rec)
			for {
				i := int(nextRun.Add(1)) - 1
				if i >= len(runs) {
					return nil
				}
				if err := gctx.Err(); err != nil {
					return err
				}
				if err := r.processRun(rec, positions, runs[i], fn); err != nil {
					return err
				}
			}
		})
	}
	return g.Wait()
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

	// The writer digests per record, so each digest ends where its record ends.
	hasher := newContentHasher()
	var tab groupTable
	i, rec, left := 0, 0, 0
	for item, err := range r.ReadRange(0, r.idx.items) {
		if err != nil {
			return err
		}
		if left == 0 {
			s, err := tab.record(r.idx, rec)
			if err != nil {
				return err
			}
			rec, left = rec+1, s.n
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
		if left--; left == 0 {
			hasher.flushChunk()
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

func (r *Reader) readRecords(rec *record, start, end int64) ([]byte, error) {
	size := int(end - start)
	if cap(rec.scratch) < size {
		rec.scratch = make([]byte, size)
	} else {
		rec.scratch = rec.scratch[:size]
	}
	if _, err := r.file.ReadAt(rec.scratch, start); err != nil {
		return nil, fmt.Errorf("packfile: read records at bytes [%d, %d): %w", start, end, err)
	}
	return rec.scratch, nil
}

func (r *Reader) processRun(rec *record, positions []int, run readRun, fn func(int, []byte) error) error {
	buf, err := r.readRecords(rec, run.start, run.end)
	if err != nil {
		return err
	}
	var s recordSpan // the record decoded last; the zero value holds no position
	for k := run.idxStart; k < run.idxEnd; k++ {
		pos := positions[k]
		if pos >= s.first+s.n {
			recIdx, ns, err := rec.tab.locate(r.idx, pos)
			if err != nil {
				return err
			}
			if err := rec.decode(buf[ns.start-run.start:ns.end-run.start], recIdx, ns.n); err != nil {
				return err
			}
			s = ns
		}
		if err := fn(k, rec.item(pos-s.first)); err != nil {
			return err
		}
	}
	return nil
}
