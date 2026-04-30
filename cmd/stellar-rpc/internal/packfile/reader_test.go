package packfile

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// --- helpers ----------------------------------------------------------------

func writeTestPackfile(t *testing.T, items [][]byte, opts WriterOptions) string {
	t.Helper()
	if opts.ItemsPerRecord == 0 {
		opts.ItemsPerRecord = 1 // one item per record = simplest layout
	}
	path := filepath.Join(t.TempDir(), "test.pack")
	w, err := Create(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if err := w.AppendItem(item); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}
	return path
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

// recordCodec pairs a writer-side RecordEncoder factory with the matching
// reader-side RecordDecoder factory. nil/nil = passthrough.
type recordCodec struct {
	name       string
	newEncoder func() RecordEncoder
	newDecoder func() RecordDecoder
}

var allCodecs = []recordCodec{
	{name: "passthrough", newEncoder: nil, newDecoder: nil},
	{name: "xor", newEncoder: newXorEncoder, newDecoder: newXorDecoder},
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

func TestReadItemsDuplicatesPanic(t *testing.T) {
	items := makeItems(100, 100)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 128})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for duplicate positions")
		}
	}()
	_ = r.ReadItems(context.Background(), []int{5, 5, 10}, func(int, []byte) error { return nil })
}

func TestReadItemsUnsortedPanic(t *testing.T) {
	items := makeItems(300, 100)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 128})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for unsorted positions")
		}
	}()
	_ = r.ReadItems(context.Background(), []int{10, 5, 20}, func(int, []byte) error { return nil })
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

func TestIndexIntegrity(t *testing.T) {
	items := makeItems(10, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Trailer (76 bytes): indexSize@28, appDataSize@32.
	trailerStart := len(data) - trailerSize
	indexSize := int(binary.LittleEndian.Uint32(data[trailerStart+28:]))
	appDataSize := int(binary.LittleEndian.Uint32(data[trailerStart+32:]))
	indexStart := trailerStart - appDataSize - indexSize
	if indexStart >= 0 && indexStart < trailerStart {
		data[indexStart] ^= 0xFF
	}

	corruptPath := filepath.Join(t.TempDir(), "corrupt.pack")
	if err := os.WriteFile(corruptPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	r := Open(corruptPath, ReaderOptions{})
	defer r.Close()
	_, err = r.TotalItems()
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("Open corrupt index: got %v, want ErrChecksum", err)
	}
}

func TestTrailerIntegrity(t *testing.T) {
	items := makeItems(10, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the magic bytes at the start of the trailer.
	trailerStart := len(data) - trailerSize
	data[trailerStart] ^= 0xFF

	corruptPath := filepath.Join(t.TempDir(), "corrupt_trailer.pack")
	if err := os.WriteFile(corruptPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	r := Open(corruptPath, ReaderOptions{})
	defer r.Close()
	_, err = r.TotalItems()
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
	// Second Close must observe the same memoized error as the first
	// (sync.Once-guarded). errors.Is handles wrapped/joined error
	// equality without depending on interface comparability.
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

func TestReadRangePanic(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path, ReaderOptions{})
	defer r.Close()

	drain := func(seq func(yield func([]byte, error) bool)) {
		seq(func([]byte, error) bool { return true })
	}
	assertPanics(t, "negative start", func() { drain(r.ReadRange(-1, 1)) })
	assertPanics(t, "negative count", func() { drain(r.ReadRange(0, -1)) })
	assertPanics(t, "out of range", func() { drain(r.ReadRange(3, 5)) })
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

	fileData, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Trailer: contentHash @ trailer[36:68], CRC @ trailer[72:76] over
	// trailer[:72]. Flip a hash byte and recompute the CRC so we exercise
	// the hash-mismatch path, not the trailer-CRC path.
	trailerStart := len(fileData) - trailerSize
	fileData[trailerStart+36] ^= 0xFF
	binary.LittleEndian.PutUint32(fileData[trailerStart+72:],
		crc32c(fileData[trailerStart:trailerStart+72]))

	corruptedPath := filepath.Join(t.TempDir(), "corrupted.pack")
	if err := os.WriteFile(corruptedPath, fileData, 0o644); err != nil {
		t.Fatal(err)
	}

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

	fileData, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// appDataSize @ trailer[32:36]. App data sits immediately before the trailer.
	trailerStart := len(fileData) - trailerSize
	appDataSz := int(binary.LittleEndian.Uint32(fileData[trailerStart+32:]))
	if appDataSz == 0 {
		t.Fatal("expected non-zero appDataSize")
	}
	fileData[trailerStart-appDataSz] ^= 0xFF

	corruptedPath := filepath.Join(t.TempDir(), "corrupted.pack")
	if err := os.WriteFile(corruptedPath, fileData, 0o644); err != nil {
		t.Fatal(err)
	}

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

						r := Open(path, ReaderOptions{NewRecordDecoder: codec.newDecoder})
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
			r := Open(path, ReaderOptions{NewRecordDecoder: codec.newDecoder})
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

				r := Open(path, ReaderOptions{NewRecordDecoder: codec.newDecoder})
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

// countingDecoder wraps newXorDecoder() with atomic counters for instances
// created and Close()s observed, so tests can assert that Reader.Close
// closes every RecordDecoder it ever vended.
type countingDecoder struct {
	inner  RecordDecoder
	closed *atomic.Bool
}

func (c *countingDecoder) Decode(in []byte) ([]byte, error) { return c.inner.Decode(in) }

func (c *countingDecoder) Close() error {
	c.closed.Store(true)
	return c.inner.Close()
}

// TestReaderCloseClosesDecoders verifies Reader.Close closes every
// RecordDecoder created via NewRecordDecoder, including ones currently
// in the pool. Forces multiple decoders to exist by running concurrent
// reads.
func TestReaderCloseClosesDecoders(t *testing.T) {
	items := makeItems(200, 100)
	path := writeTestPackfile(t, items, WriterOptions{
		NewRecordEncoder: newXorEncoder,
		ItemsPerRecord:   16,
	})

	var created []*atomic.Bool
	var mu sync.Mutex
	factory := func() RecordDecoder {
		flag := &atomic.Bool{}
		mu.Lock()
		created = append(created, flag)
		mu.Unlock()
		return &countingDecoder{inner: newXorDecoder(), closed: flag}
	}

	r := Open(path, ReaderOptions{
		NewRecordDecoder: factory,
		Concurrency:      4,
	})

	// Force several decoders to be vended in parallel.
	positions := []int{0, 16, 32, 64, 96, 128, 160, 192}
	if err := r.ReadItems(context.Background(), positions, func(int, []byte) error {
		return nil
	}); err != nil {
		t.Fatalf("ReadItems: %v", err)
	}

	mu.Lock()
	got := len(created)
	mu.Unlock()
	if got == 0 {
		t.Fatal("expected at least one decoder to be created")
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for i, c := range created {
		if !c.Load() {
			t.Errorf("decoder %d was not closed", i)
		}
	}
}

// TestUnknownTrailerFlagsRejected verifies the reader rejects packfiles whose
// trailer flag byte has bits outside knownFlags. Sets bit 0x80 (currently
// unallocated), recomputes the trailer CRC so we exercise the flag-validation
// path rather than the CRC path.
func TestUnknownTrailerFlagsRejected(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	trailerStart := len(data) - trailerSize
	data[trailerStart+5] |= 0x80 // flip an unallocated flag bit
	binary.LittleEndian.PutUint32(data[trailerStart+72:],
		crc32c(data[trailerStart:trailerStart+72]))

	corruptPath := filepath.Join(t.TempDir(), "bad_flags.pack")
	if err := os.WriteFile(corruptPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	r := Open(corruptPath, ReaderOptions{})
	defer r.Close()
	_, err = r.TotalItems()
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
