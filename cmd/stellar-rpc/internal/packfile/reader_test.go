package packfile

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// --- helpers ----------------------------------------------------------------

// writeTestPackfile is a reader-test convenience over writePackfile that
// defaults ItemsPerRecord to 1 (simplest layout) when the caller leaves it
// unset. Reader tests typically don't care about record packing; writer
// tests do, so they call writePackfile directly.
func writeTestPackfile(tb testing.TB, items [][]byte, opts WriterOptions) string {
	tb.Helper()
	if opts.ItemsPerRecord == 0 {
		opts.ItemsPerRecord = 1
	}
	return writePackfile(tb, opts, items)
}

func makeItems(n, size int) [][]byte {
	items := make([][]byte, n)
	for i := range items {
		item := make([]byte, size)
		_, _ = rand.Read(item)
		items[i] = item
	}
	return items
}

func readItemCopy(t *testing.T, r *Reader, position int) []byte {
	t.Helper()
	var got []byte
	if err := r.ReadItem(position, func(data []byte) error {
		got = bytes.Clone(data)
		return nil
	}); err != nil {
		t.Fatalf("ReadItem(%d): %v", position, err)
	}
	return got
}

// recordCodec pairs a writer-side RecordEncoder factory with a single
// concurrent-safe reader-side RecordDecoder instance. nil/nil = passthrough.
type recordCodec struct {
	name       string
	newEncoder func() RecordEncoder
	decoder    RecordDecoder
}

var allCodecs = []recordCodec{
	{name: "passthrough", newEncoder: nil, decoder: nil},
	{name: "xor", newEncoder: newXorEncoder, decoder: newXorDecoder()},
}

// --- core roundtrips --------------------------------------------------------

func TestRoundTrip(t *testing.T) {
	items := makeItems(500, 1024)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != len(items) {
		t.Fatalf("TotalItems = %d, want %d", tc, len(items))
	}

	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestEmptyFile(t *testing.T) {
	path := writeTestPackfile(t, nil, WriterOptions{})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 0 {
		t.Fatalf("TotalItems = %d, want 0", tc)
	}

	err = r.ReadItem(0, func([]byte) error { return nil })
	if !errors.Is(err, ErrPositionOutOfRange) {
		t.Fatalf("ReadItem(0) on empty: got %v, want ErrPositionOutOfRange", err)
	}
}

func TestSingleItem(t *testing.T) {
	items := makeItems(1, 256)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 1 {
		t.Fatalf("TotalItems = %d, want 1", tc)
	}

	if got := readItemCopy(t, r, 0); !bytes.Equal(got, items[0]) {
		t.Fatal("data mismatch")
	}
}

// TestItemsPerRecord1NoTrailingWaste verifies that with ItemsPerRecord=1 and
// passthrough mode, on-disk records are exactly itemSize bytes — no trailing
// FOR group is written for single-item records.
func TestItemsPerRecord1NoTrailingWaste(t *testing.T) {
	const itemSize = 256
	items := makeItems(10, itemSize)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 1})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	trailer, err := r.Trailer()
	if err != nil {
		t.Fatal(err)
	}
	if trailer.RecordCount != 10 {
		t.Fatalf("RecordCount = %d, want 10", trailer.RecordCount)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	dataSize := fi.Size() - int64(trailer.IndexSize) - int64(trailer.AppDataSize) - trailerSize
	perRecord := dataSize / int64(trailer.RecordCount)
	if perRecord != int64(itemSize) {
		t.Fatalf("per-record size = %d, want %d (passthrough, no trailing FOR)", perRecord, itemSize)
	}

	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestMultiItemRecords(t *testing.T) {
	items := makeItems(300, 512)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 128})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 300 {
		t.Fatalf("TotalItems = %d, want 300", tc)
	}

	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

//nolint:cyclop // four sub-cases (full / partial / break / empty) inline for readability
func TestReadRange(t *testing.T) {
	items := makeItems(50, 2048)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 10})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	// Full range.
	j := 0
	for item, err := range r.ReadRange(0, 50) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item, items[j]) {
			t.Fatalf("ReadRange[%d]: data mismatch", j)
		}
		j++
	}
	if j != 50 {
		t.Fatalf("ReadRange yielded %d items, want 50", j)
	}

	// Partial range crossing record boundary.
	j = 0
	for item, err := range r.ReadRange(8, 5) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item, items[8+j]) {
			t.Fatalf("ReadRange partial [%d]: data mismatch", j)
		}
		j++
	}
	if j != 5 {
		t.Fatalf("ReadRange partial yielded %d, want 5", j)
	}

	// Early break.
	j = 0
	for _, err := range r.ReadRange(0, 50) {
		if err != nil {
			t.Fatal(err)
		}
		j++
		if j == 3 {
			break
		}
	}
	if j != 3 {
		t.Fatalf("Early break: got %d iterations, want 3", j)
	}

	// Empty range.
	j = 0
	for _, err := range r.ReadRange(0, 0) {
		if err != nil {
			t.Fatal(err)
		}
		j++
	}
	if j != 0 {
		t.Fatalf("Empty ReadRange yielded %d, want 0", j)
	}
}

func TestReadItems(t *testing.T) {
	items := makeItems(300, 512)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 128})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	indices := []int{0, 1, 127, 128, 200, 299}
	got := make([][]byte, len(indices))
	err := r.ReadItems(context.Background(), indices, func(idx int, data []byte) error {
		got[idx] = bytes.Clone(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for j, idx := range indices {
		if !bytes.Equal(got[j], items[idx]) {
			t.Fatalf("ReadItems[%d] (item %d): data mismatch", j, idx)
		}
	}
}

func TestReadItemsDuplicates(t *testing.T) {
	items := makeItems(100, 100)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 128})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	err := r.ReadItems(context.Background(), []int{5, 5, 10}, func(int, []byte) error { return nil })
	if !errors.Is(err, ErrPositionsUnsorted) {
		t.Fatalf("expected ErrPositionsUnsorted for duplicate positions, got %v", err)
	}
}

func TestReadItemsUnsorted(t *testing.T) {
	items := makeItems(300, 100)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 128})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	err := r.ReadItems(context.Background(), []int{10, 5, 20}, func(int, []byte) error { return nil })
	if !errors.Is(err, ErrPositionsUnsorted) {
		t.Fatalf("expected ErrPositionsUnsorted for unsorted positions, got %v", err)
	}
}

func TestReadItemsOutOfRange(t *testing.T) {
	items := makeItems(10, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	err := r.ReadItems(context.Background(), []int{0, 5, 100}, func(int, []byte) error { return nil })
	if !errors.Is(err, ErrPositionOutOfRange) {
		t.Fatalf("expected ErrPositionOutOfRange for out-of-range position, got %v", err)
	}
}

func TestReadItemsEmpty(t *testing.T) {
	items := makeItems(10, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	called := false
	err := r.ReadItems(context.Background(), nil, func(int, []byte) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("empty ReadItems should not call fn")
	}
}

// --- corruption / open errors -----------------------------------------------

// corruptAt reads srcPath, applies mutate to the bytes, optionally recomputes
// the trailer CRC over [:trailerCRCEnd], writes a new file in t.TempDir(),
// and returns the new path. Used by tests that need to construct a packfile
// in a specifically-corrupted state.
func corruptAt(t *testing.T, srcPath string, recomputeTrailerCRC bool, mutate func(data []byte)) string {
	t.Helper()
	data, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	mutate(data)
	if recomputeTrailerCRC {
		trailerStart := len(data) - trailerSize
		binary.LittleEndian.PutUint32(data[trailerStart+tOffCRC:],
			crc32c(data[trailerStart:trailerStart+trailerCRCEnd]))
	}
	dstPath := filepath.Join(t.TempDir(), "corrupt.pack")
	if err := os.WriteFile(dstPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return dstPath
}

func TestIndexIntegrity(t *testing.T) {
	items := makeItems(10, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	corruptPath := corruptAt(t, path, false, func(data []byte) {
		trailerStart := len(data) - trailerSize
		indexSize := int(binary.LittleEndian.Uint32(data[trailerStart+tOffIndexSize:]))
		appDataSize := int(binary.LittleEndian.Uint32(data[trailerStart+tOffAppDataSize:]))
		indexStart := trailerStart - appDataSize - indexSize
		if indexStart >= 0 && indexStart < trailerStart {
			data[indexStart] ^= 0xFF
		}
	})

	r := Open(corruptPath, ReaderOptions{})
	defer r.Close()
	_, err := r.TotalItems()
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("Open corrupt index: got %v, want ErrChecksum", err)
	}
}

func TestTrailerIntegrity(t *testing.T) {
	items := makeItems(10, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	// Magic bytes corrupted, CRC NOT recomputed — exercises the magic check.
	corruptPath := corruptAt(t, path, false, func(data []byte) {
		data[len(data)-trailerSize+tOffMagic] ^= 0xFF
	})

	r := Open(corruptPath, ReaderOptions{})
	defer r.Close()
	_, err := r.TotalItems()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open corrupt trailer: got %v, want ErrCorrupt", err)
	}
}

func TestFileTooShort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "short.pack")

	if err := os.WriteFile(path, make([]byte, trailerSize-1), 0o644); err != nil {
		t.Fatal(err)
	}

	r := Open(path, ReaderOptions{})
	defer r.Close()

	_, err := r.TotalItems()
	if !errors.Is(err, ErrSize) {
		t.Fatalf("expected ErrSize for short file, got %v", err)
	}
}

func TestOpenBadPath(t *testing.T) {
	r := Open("/nonexistent/path/to/file.pack", ReaderOptions{})
	defer r.Close()

	err := r.ReadItem(0, func([]byte) error { return nil })
	if err == nil {
		t.Fatal("expected error for bad path")
	}
}

// --- concurrency ------------------------------------------------------------

func TestConcurrentReads(t *testing.T) {
	items := makeItems(100, 512)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 128})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	var wg sync.WaitGroup
	errs := make(chan error, len(items))

	for i := range items {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var got []byte
			if err := r.ReadItem(idx, func(data []byte) error {
				got = bytes.Clone(data)
				return nil
			}); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, items[idx]) {
				errs <- errors.New("data mismatch")
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
}

// --- writer/reader lifecycle -----------------------------------------------

func TestCloseBeforeRead(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDoubleClose(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	err1 := r.Close()
	err2 := r.Close()
	// Both calls must observe the same memoized result: Close uses
	// sync.Once so the second call returns whatever the first computed.
	if (err1 == nil) != (err2 == nil) {
		t.Fatalf("double Close: first=%v, second=%v", err1, err2)
	}
	if err1 != nil && !errors.Is(err1, err2) {
		t.Fatalf("double Close: first=%v not Is second=%v", err1, err2)
	}
}

func TestReadItemOutOfRange(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	for _, p := range []int{-1, 5, 100} {
		err := r.ReadItem(p, func([]byte) error { return nil })
		if !errors.Is(err, ErrPositionOutOfRange) {
			t.Fatalf("ReadItem(%d): got %v, want ErrPositionOutOfRange", p, err)
		}
	}
}

func TestReadRangeOutOfRange(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	// Iterator yields a single (nil, ErrPositionOutOfRange) and stops.
	firstErr := func(seq iter.Seq2[[]byte, error]) error {
		for _, err := range seq {
			return err
		}
		return nil
	}
	for _, tc := range []struct {
		name         string
		start, count int
	}{
		{"negative start", -1, 1},
		{"negative count", 0, -1},
		{"out of range", 3, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := firstErr(r.ReadRange(tc.start, tc.count))
			if !errors.Is(err, ErrPositionOutOfRange) {
				t.Fatalf("got %v, want ErrPositionOutOfRange", err)
			}
		})
	}
}

// --- content hash -----------------------------------------------------------

func TestContentHashRoundTrip(t *testing.T) {
	items := makeItems(500, 200)
	path := writeTestPackfile(t, items, WriterOptions{ContentHash: true})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	hash, ok, err := r.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected content hash to be present")
	}
	if hash == ([32]byte{}) {
		t.Fatal("expected non-zero hash")
	}

	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// TestContentHashExtractVerifyRoundTrip exercises the symmetric case:
// items are written with ContentHashExtract (so the trailer hash covers
// extract(item), not raw item), and Verify is configured with the matching
// extract on the reader side. Before the reader-side option existed, this
// test would have failed with ErrContentHashMismatch.
func TestContentHashExtractVerifyRoundTrip(t *testing.T) {
	items := makeItems(300, 64)
	// Pre-xor the items so on-disk they are xor-encoded, and the writer's
	// extract un-xors them back to the raw form for hashing. The reader's
	// matching extract un-xors what it reads off disk; hashes match.
	xorItems := make([][]byte, len(items))
	for i, item := range items {
		xorItems[i], _ = xorCompress(item)
	}
	path := writeTestPackfile(t, xorItems, WriterOptions{
		ContentHash:        true,
		ContentHashExtract: xorCompress,
	})

	r := Open(path, ReaderOptions{ContentHashExtract: xorCompress})
	defer r.Close()

	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify with matching ContentHashExtract: %v", err)
	}
}

func TestContentHashDeterministic(t *testing.T) {
	items := makeItems(500, 200)
	path1 := writeTestPackfile(t, items, WriterOptions{ContentHash: true})
	path2 := writeTestPackfile(t, items, WriterOptions{ContentHash: true})

	r1 := Open(path1, ReaderOptions{})
	defer r1.Close()
	r2 := Open(path2, ReaderOptions{})
	defer r2.Close()

	hash1, _, err := r1.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	hash2, _, err := r2.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if hash1 != hash2 {
		t.Fatalf("hashes differ: %x vs %x", hash1, hash2)
	}
}

func TestContentHashDisabled(t *testing.T) {
	items := makeItems(100, 200)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	_, ok, err := r.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no content hash when disabled")
	}

	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify should return nil when no hash stored: %v", err)
	}
}

func TestContentHashCorruption(t *testing.T) {
	items := makeItems(200, 200)
	path := writeTestPackfile(t, items, WriterOptions{ContentHash: true})

	r := Open(path, ReaderOptions{})
	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify before corruption: %v", err)
	}
	r.Close()

	// Flip a content-hash byte AND recompute the trailer CRC so the test
	// exercises the hash-mismatch path, not the trailer-CRC path.
	corruptedPath := corruptAt(t, path, true, func(data []byte) {
		data[len(data)-trailerSize+tOffContentHash] ^= 0xFF
	})

	r = Open(corruptedPath, ReaderOptions{})
	defer r.Close()

	verifyErr := r.Verify(context.Background())
	if verifyErr == nil {
		t.Fatal("expected error from corrupted hash")
	}
	if !errors.Is(verifyErr, ErrContentHashMismatch) {
		t.Fatalf("expected ErrContentHashMismatch, got: %v", verifyErr)
	}
}

func TestContentHashWithConcurrency(t *testing.T) {
	items := makeItems(500, 200)

	serialPath := writeTestPackfile(t, items, WriterOptions{ContentHash: true})
	parallelPath := writeTestPackfile(t, items, WriterOptions{ContentHash: true, Concurrency: 4})

	r1 := Open(serialPath, ReaderOptions{})
	defer r1.Close()
	r2 := Open(parallelPath, ReaderOptions{})
	defer r2.Close()

	hash1, _, err := r1.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	hash2, _, err := r2.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if hash1 != hash2 {
		t.Fatalf("serial vs parallel hashes differ: %x vs %x", hash1, hash2)
	}

	if err := r2.Verify(context.Background()); err != nil {
		t.Fatalf("Verify parallel: %v", err)
	}
}

func TestContentHashNonDefaultItemsPerRecord(t *testing.T) {
	items := makeItems(500, 200)

	for _, itemsPerRecord := range []int{64, 256, 500} {
		t.Run(fmt.Sprintf("ItemsPerRecord=%d", itemsPerRecord), func(t *testing.T) {
			path := writeTestPackfile(t, items, WriterOptions{
				ItemsPerRecord: itemsPerRecord,
				ContentHash:    true,
			})

			r := Open(path, ReaderOptions{})
			defer r.Close()

			hash, ok, err := r.ContentHash()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected content hash to be present")
			}
			if hash == ([32]byte{}) {
				t.Fatal("expected non-zero hash")
			}

			if err := r.Verify(context.Background()); err != nil {
				t.Fatalf("Verify: %v", err)
			}
		})
	}
}

// --- trailer / app data -----------------------------------------------------

func TestTrailer(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{Format: 42})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	trailer, err := r.Trailer()
	if err != nil {
		t.Fatal(err)
	}
	if trailer.RecordCount != 5 {
		t.Fatalf("RecordCount = %d, want 5", trailer.RecordCount)
	}
	if trailer.TotalItems != 5 {
		t.Fatalf("TotalItems = %d, want 5", trailer.TotalItems)
	}
	if trailer.ItemsPerRecord != 1 {
		t.Fatalf("ItemsPerRecord = %d, want 1", trailer.ItemsPerRecord)
	}
	if trailer.AppDataSize != 0 {
		t.Fatalf("AppDataSize = %d, want 0", trailer.AppDataSize)
	}
	if trailer.Format != 42 {
		t.Fatalf("Format = %d, want 42", trailer.Format)
	}
	if trailer.HasContentHash {
		t.Fatal("expected HasContentHash=false")
	}
}

func TestAppDataRoundTrip(t *testing.T) {
	appData := []byte("hello-app-data-1234567890")

	dir := t.TempDir()
	path := filepath.Join(dir, "appdata.pack")

	w, err := Create(path, WriterOptions{ItemsPerRecord: 1})
	if err != nil {
		t.Fatal(err)
	}
	items := makeItems(5, 100)
	for _, item := range items {
		if err := w.AppendItem(item); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(appData); err != nil {
		t.Fatal(err)
	}

	r := Open(path, ReaderOptions{})
	defer r.Close()

	got, err := r.AppData()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, appData) {
		t.Fatalf("AppData mismatch: got %q, want %q", got, appData)
	}

	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}

	trailer, err := r.Trailer()
	if err != nil {
		t.Fatal(err)
	}
	if trailer.AppDataSize != uint32(len(appData)) {
		t.Fatalf("trailer.AppDataSize = %d, want %d", trailer.AppDataSize, len(appData))
	}
}

// TestAppDataCorruption verifies that app-data corruption is NOT detected by
// packfile (callers are responsible for their own app-data integrity).
func TestAppDataCorruption(t *testing.T) {
	appData := []byte("important-metadata")

	dir := t.TempDir()
	path := filepath.Join(dir, "appdata.pack")

	w, err := Create(path, WriterOptions{ItemsPerRecord: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendItem([]byte("item")); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(appData); err != nil {
		t.Fatal(err)
	}

	// App data is uncovered by the trailer CRC, so we mutate the data
	// section directly without recomputing the CRC.
	corruptedPath := corruptAt(t, path, false, func(data []byte) {
		trailerStart := len(data) - trailerSize
		appDataSz := int(binary.LittleEndian.Uint32(data[trailerStart+tOffAppDataSize:]))
		if appDataSz == 0 {
			t.Fatal("expected non-zero appDataSize")
		}
		data[trailerStart-appDataSz] ^= 0xFF
	})

	r := Open(corruptedPath, ReaderOptions{})
	defer r.Close()

	got, err := r.AppData()
	if err != nil {
		t.Fatalf("unexpected error opening file with corrupted app data: %v", err)
	}
	if string(got) == string(appData) {
		t.Error("expected corrupted app data to differ from original")
	}
}

func TestAppDataEmpty(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	got, err := r.AppData()
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil app data, got %d bytes", len(got))
	}
}

// --- writer × reader matrix -------------------------------------------------

// TestWriterReaderMatrix exercises codec × concurrency × hash × ItemsPerRecord
// combinations to confirm round-trip correctness across the supported axes.
//
//nolint:gocognit // 4-way nested matrix with sub-test bodies; flattening hurts clarity
func TestWriterReaderMatrix(t *testing.T) {
	items := makeItems(300, 200)
	for _, codec := range allCodecs {
		for _, conc := range []int{0, 4} {
			for _, hash := range []bool{false, true} {
				for _, rs := range []int{1, 128} {
					name := fmt.Sprintf("%s/c%d/hash=%v/rs=%d", codec.name, conc, hash, rs)
					t.Run(name, func(t *testing.T) {
						wopts := WriterOptions{
							NewRecordEncoder: codec.newEncoder,
							Concurrency:      conc,
							ContentHash:      hash,
							ItemsPerRecord:   rs,
						}
						path := writeTestPackfile(t, items, wopts)

						r := Open(path, ReaderOptions{RecordDecoder: codec.decoder})
						defer r.Close()

						tc, err := r.TotalItems()
						if err != nil {
							t.Fatal(err)
						}
						if tc != len(items) {
							t.Fatalf("TotalItems = %d, want %d", tc, len(items))
						}
						for i, want := range items {
							if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
								t.Fatalf("ReadItem(%d): data mismatch", i)
							}
						}

						storedHash, ok, err := r.ContentHash()
						if err != nil {
							t.Fatal(err)
						}
						if hash {
							if !ok {
								t.Fatal("expected content hash to be present")
							}
							if storedHash == ([32]byte{}) {
								t.Fatal("expected non-zero hash")
							}
							if err := r.Verify(context.Background()); err != nil {
								t.Fatalf("Verify: %v", err)
							}
						} else if ok {
							t.Fatal("expected no content hash when disabled")
						}
					})
				}
			}
		}
	}
}

// TestContentHashCrossCodec verifies that the content hash is over the logical
// item stream only — same items must produce the same hash regardless of codec
// and concurrency.
func TestContentHashCrossCodec(t *testing.T) {
	items := makeItems(300, 200)
	hashes := make([][32]byte, 0, 2*len(allCodecs))
	labels := make([]string, 0, 2*len(allCodecs))

	for _, codec := range allCodecs {
		for _, conc := range []int{0, 4} {
			opts := WriterOptions{
				NewRecordEncoder: codec.newEncoder,
				Concurrency:      conc,
				ContentHash:      true,
				ItemsPerRecord:   128,
			}
			path := writeTestPackfile(t, items, opts)
			r := Open(path, ReaderOptions{RecordDecoder: codec.decoder})
			h, ok, err := r.ContentHash()
			r.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected content hash")
			}
			hashes = append(hashes, h)
			labels = append(labels, fmt.Sprintf("%s/c%d", codec.name, conc))
		}
	}

	for i := 1; i < len(hashes); i++ {
		if hashes[i] != hashes[0] {
			t.Fatalf("hash mismatch: %s (%x) != %s (%x)", labels[i], hashes[i], labels[0], hashes[0])
		}
	}
}

// TestRecordWorkerRaceStress exercises the concurrent worker path with small
// records to maximize goroutine interleaving. Run with -race -count=10.
func TestRecordWorkerRaceStress(t *testing.T) {
	items := makeItems(200, 100)
	for _, codec := range allCodecs {
		for _, rs := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("%s/rs=%d", codec.name, rs), func(t *testing.T) {
				opts := WriterOptions{
					NewRecordEncoder: codec.newEncoder,
					Concurrency:      8,
					ContentHash:      true,
					ItemsPerRecord:   rs,
				}
				path := writeTestPackfile(t, items, opts)

				r := Open(path, ReaderOptions{RecordDecoder: codec.decoder})
				defer r.Close()

				tc, err := r.TotalItems()
				if err != nil {
					t.Fatal(err)
				}
				if tc != len(items) {
					t.Fatalf("TotalItems = %d, want %d", tc, len(items))
				}
				if err := r.Verify(context.Background()); err != nil {
					t.Fatalf("Verify: %v", err)
				}
			})
		}
	}
}

// --- option validation / lifecycle invariants ------------------------------

// TestUnknownTrailerFlagsRejected verifies the reader rejects packfiles whose
// trailer flag byte has bits outside knownFlags. Sets bit 0x80 (currently
// unallocated), recomputes the trailer CRC so we exercise the flag-validation
// path rather than the CRC path.
func TestUnknownTrailerFlagsRejected(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	corruptPath := corruptAt(t, path, true, func(data []byte) {
		data[len(data)-trailerSize+tOffFlags] |= 0x80
	})

	r := Open(corruptPath, ReaderOptions{})
	defer r.Close()
	_, err := r.TotalItems()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with unknown flags: got %v, want ErrCorrupt", err)
	}
}

// TestReaderConcurrencyNegativeRejected verifies that ReaderOptions.Concurrency
// < 0 is rejected on the first read call (the error is stashed at Open time
// and surfaced through waitOpen).
func TestReaderConcurrencyNegativeRejected(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{Concurrency: -1})
	defer r.Close()

	_, err := r.TotalItems()
	if err == nil {
		t.Fatal("expected error for negative Concurrency")
	}
	if !strings.Contains(err.Error(), "Concurrency") {
		t.Fatalf("expected Concurrency error, got %v", err)
	}
}

// TestCloseAfterFailedOpen verifies Close handles the case where the
// background open failed before file was assigned — Close should surface
// the open error rather than returning nil.
func TestCloseAfterFailedOpen(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	r := Open(missing, ReaderOptions{})
	err := r.Close()
	if err == nil {
		t.Fatal("expected error from Close after failed Open; got nil")
	}
	// Verify the error originates from the open path (file not found),
	// not from somewhere else (e.g. a file-close error on a closed fd).
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist from Close after Open of missing file; got %v", err)
	}
}

// TestWorkspaceSharedAcrossDecoderModes stress-exercises the package-level
// recordWorkspacePool by alternating between Readers in passthrough mode
// and encoder mode, hoping the pool reuses workspaces across modes. Pool
// reuse is probabilistic (sync.Pool may evict between Put/Get under GC
// pressure), so this isn't a deterministic regression test — it's a
// stress test that's likely to surface alias-invariant regressions over
// many iterations. The deterministic invariant checks live in
// TestPassthroughDecodePreservesPayload and TestPutRecordDropsCurrent.
func TestWorkspaceSharedAcrossDecoderModes(t *testing.T) {
	itemsA := makeItems(100, 256)
	pathA := writeTestPackfile(t, itemsA, WriterOptions{ItemsPerRecord: 32}) // passthrough

	itemsB := makeItems(100, 256)
	pathB := writeTestPackfile(t, itemsB, WriterOptions{
		NewRecordEncoder: newXorEncoder,
		ItemsPerRecord:   32,
	})

	decB := newXorDecoder()

	// Alternate readers many times to encourage workspace migration through
	// the process-wide pool.
	for iter := range 25 {
		rA := Open(pathA, ReaderOptions{})
		for i := range itemsA {
			got := readItemCopy(t, rA, i)
			if !bytes.Equal(got, itemsA[i]) {
				t.Fatalf("iter %d, passthrough Reader item %d: data corrupted", iter, i)
			}
		}
		rA.Close()

		rB := Open(pathB, ReaderOptions{RecordDecoder: decB})
		for i := range itemsB {
			got := readItemCopy(t, rB, i)
			if !bytes.Equal(got, itemsB[i]) {
				t.Fatalf("iter %d, encoder Reader item %d: data corrupted", iter, i)
			}
		}
		rB.Close()
	}
}

// TestReadItemsSerialMultiBatch exercises the Concurrency=1 serial fast
// path with positions spread across many records (so the partitioner
// produces multiple batches). Without this test the only coverage of the
// serial path used tiny position lists that collapsed to one batch.
func TestReadItemsSerialMultiBatch(t *testing.T) {
	items := makeItems(2000, 100)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 16})

	r := Open(path, ReaderOptions{Concurrency: 1})
	defer r.Close()

	// 100 positions spread across the file — at 16 items/record, this
	// touches 100 different records, forcing the batch partitioner to
	// produce multiple batches.
	positions := make([]int, 100)
	for i := range positions {
		positions[i] = i * 20 // stride 20 ensures each position is in a different record
	}

	got := make(map[int][]byte, len(positions))
	err := r.ReadItems(context.Background(), positions, func(idx int, data []byte) error {
		got[idx] = bytes.Clone(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for j, pos := range positions {
		if !bytes.Equal(got[j], items[pos]) {
			t.Errorf("position %d (idx %d): data mismatch", pos, j)
		}
	}
}

// TestReadItemsConcurrencyMatchesBatchCount exercises the edge case where
// the configured concurrency equals the number of I/O batches — each
// worker should claim exactly one batch and then exit cleanly.
func TestReadItemsConcurrencyMatchesBatchCount(t *testing.T) {
	items := makeItems(1024, 100)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 32})

	// Pick positions across exactly 4 widely-separated records so the
	// partitioner produces 4 batches; then set Concurrency=4.
	positions := []int{0, 256, 512, 768}
	r := Open(path, ReaderOptions{Concurrency: 4})
	defer r.Close()

	got := make([][]byte, len(positions))
	err := r.ReadItems(context.Background(), positions, func(idx int, data []byte) error {
		got[idx] = bytes.Clone(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for j, pos := range positions {
		if !bytes.Equal(got[j], items[pos]) {
			t.Errorf("position %d (idx %d): data mismatch", pos, j)
		}
	}
}
