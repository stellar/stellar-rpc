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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, nil)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 128}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 10}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 128}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 128}, items)

	r := Open(path, ReaderOptions{})
	defer r.Close()

	err := r.ReadItems(context.Background(), []int{5, 5, 10}, func(int, []byte) error { return nil })
	if !errors.Is(err, ErrPositionsUnsorted) {
		t.Fatalf("expected ErrPositionsUnsorted for duplicate positions, got %v", err)
	}
}

func TestReadItemsUnsorted(t *testing.T) {
	items := makeItems(300, 100)
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 128}, items)

	r := Open(path, ReaderOptions{})
	defer r.Close()

	err := r.ReadItems(context.Background(), []int{10, 5, 20}, func(int, []byte) error { return nil })
	if !errors.Is(err, ErrPositionsUnsorted) {
		t.Fatalf("expected ErrPositionsUnsorted for unsorted positions, got %v", err)
	}
}

func TestReadItemsOutOfRange(t *testing.T) {
	items := makeItems(10, 100)
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

	r := Open(path, ReaderOptions{})
	defer r.Close()

	err := r.ReadItems(context.Background(), []int{0, 5, 100}, func(int, []byte) error { return nil })
	if !errors.Is(err, ErrPositionOutOfRange) {
		t.Fatalf("expected ErrPositionOutOfRange for out-of-range position, got %v", err)
	}
}

func TestReadItemsEmpty(t *testing.T) {
	items := makeItems(10, 100)
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 128}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

	r := Open(path, ReaderOptions{})
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDoubleClose(t *testing.T) {
	items := makeItems(5, 100)
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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

// --- trailer / app data -----------------------------------------------------

func TestTrailer(t *testing.T) {
	items := makeItems(5, 100)
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1, Format: 42}, items)

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
}

// writeAppDataPackfile writes a packfile carrying items and appData, and
// returns its path. writePackfile can't serve these tests: it hardcodes
// Finish(nil).
func writeAppDataPackfile(t *testing.T, items [][]byte, appData []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appdata.pack")
	w, err := Create(path, WriterOptions{ItemsPerRecord: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if err := w.AppendItem(item); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(appData); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAppDataRoundTrip(t *testing.T) {
	appData := []byte("hello-app-data-1234567890")
	items := makeItems(5, 100)
	path := writeAppDataPackfile(t, items, appData)

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

// TestAppDataCorruption covers the third tail section. The offsets index and
// the trailer each carry a CRC32C; app data holds things like events.pack's
// cumulative ledger offsets, where a flipped byte can preserve monotonicity
// and the final total and so pass every structural check its decoder makes.
func TestAppDataCorruption(t *testing.T) {
	path := writeAppDataPackfile(t, [][]byte{[]byte("item")}, []byte("important-metadata"))

	// The trailer CRC covers the appDataCRC field but not the section itself,
	// so mutating the section alone leaves the trailer valid.
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

	if _, err := r.AppData(); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Open with corrupted app data: got %v, want ErrChecksum", err)
	}
}

func TestAppDataEmpty(t *testing.T) {
	items := makeItems(5, 100)
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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

// TestWriterReaderMatrix exercises codec × concurrency × ItemsPerRecord
// combinations to confirm round-trip correctness across the supported axes.
func TestWriterReaderMatrix(t *testing.T) {
	items := makeItems(300, 200)
	for _, codec := range allCodecs {
		for _, conc := range []int{0, 4} {
			for _, rs := range []int{1, 128} {
				name := fmt.Sprintf("%s/c%d/rs=%d", codec.name, conc, rs)
				t.Run(name, func(t *testing.T) {
					wopts := WriterOptions{
						NewRecordEncoder: codec.newEncoder,
						Concurrency:      conc,
						ItemsPerRecord:   rs,
					}
					path := writePackfile(t, wopts, items)

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
				})
			}
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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

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
	pathA := writePackfile(t, WriterOptions{ItemsPerRecord: 32}, itemsA) // passthrough

	itemsB := makeItems(100, 256)
	pathB := writePackfile(t, WriterOptions{
		NewRecordEncoder: newXorEncoder,
		ItemsPerRecord:   32,
	}, itemsB)

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

// TestReadLoansAreClipped pins the loan contract at the lender. Several items
// share one record here, which is the case that matters.
func TestReadLoansAreClipped(t *testing.T) {
	const items = 8
	want := make([][]byte, items)
	for i := range want {
		want[i] = bytes.Repeat([]byte{byte('a' + i)}, 32+i)
	}
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 4}, want)

	r := Open(path, ReaderOptions{})
	defer r.Close()

	if err := r.ReadItem(0, func(b []byte) error {
		if len(b) != cap(b) {
			t.Errorf("ReadItem loan not clipped: len %d cap %d", len(b), cap(b))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.ReadItems(context.Background(), []int{1, 2}, func(_ int, b []byte) error {
		if len(b) != cap(b) {
			t.Errorf("ReadItems loan not clipped: len %d cap %d", len(b), cap(b))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for b, err := range r.ReadRange(0, items) {
		if err != nil {
			t.Fatal(err)
		}
		if len(b) != cap(b) {
			t.Errorf("ReadRange loan not clipped: len %d cap %d", len(b), cap(b))
		}
	}

	// Appending to one loan must not reach the next.
	if err := r.ReadItem(0, func(b []byte) error {
		_ = append(b, bytes.Repeat([]byte{0xFF}, 4096)...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(want[i], got) {
			t.Errorf("item %d corrupted by a neighbor's append", i)
		}
	}
}
