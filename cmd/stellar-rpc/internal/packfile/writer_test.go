package packfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- helpers -----------------------------------------------------------------

// parsedTrailer mirrors the fields packed into the 76-byte trailer.
type parsedTrailer struct {
	magic          uint32
	version        uint8
	flags          uint8
	format         Format
	recordCount    uint32
	totalItems     uint32
	itemsPerRecord uint32
	indexGroupSize uint16
	indexSize      uint32
	appDataSize    uint32
	contentHash    [32]byte
	crc            uint32
}

// readTrailer reads the packfile at path and parses its trailer. Also returns
// the file's total size for offset math.
func readTrailer(t *testing.T, path string) (parsedTrailer, int64) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), trailerSize, "file too small for a trailer")

	buf := data[len(data)-trailerSize:]
	tr := parsedTrailer{
		magic:          binary.LittleEndian.Uint32(buf[0:]),
		version:        buf[4],
		flags:          buf[5],
		format:         Format(binary.LittleEndian.Uint32(buf[8:])),
		recordCount:    binary.LittleEndian.Uint32(buf[12:]),
		totalItems:     binary.LittleEndian.Uint32(buf[16:]),
		itemsPerRecord: binary.LittleEndian.Uint32(buf[20:]),
		indexGroupSize: binary.LittleEndian.Uint16(buf[24:]),
		indexSize:      binary.LittleEndian.Uint32(buf[28:]),
		appDataSize:    binary.LittleEndian.Uint32(buf[32:]),
		crc:            binary.LittleEndian.Uint32(buf[72:]),
	}
	copy(tr.contentHash[:], buf[36:68])
	require.Equal(t, crc32c(buf[:72]), tr.crc, "trailer CRC")
	return tr, int64(len(data))
}

// writePackfile creates a packfile at a temp path with the given items and
// opts, calling Finish at the end. Returns the path.
func writePackfile(t *testing.T, opts WriterOptions, items [][]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pack")
	w, err := Create(path, opts)
	require.NoError(t, err)
	defer w.Close()
	for i, item := range items {
		require.NoErrorf(t, w.AppendItem(item), "AppendItem %d", i)
	}
	require.NoError(t, w.Finish(nil))
	return path
}

func mkItems(n, size int) [][]byte {
	items := make([][]byte, n)
	for i := range items {
		items[i] = bytes.Repeat([]byte{byte(i * 7)}, size)
	}
	return items
}

// --- in-test compressor / decompressor --------------------------------------

// xorCompress XORs every byte with 0xA5 (its own inverse). Reversible, so we
// can exercise the compress path end-to-end without pulling in zstd.
func xorCompress(in []byte) ([]byte, error) {
	out := make([]byte, len(in))
	for i, b := range in {
		out[i] = b ^ 0xA5
	}
	return out, nil
}

func newXorCompressor() CompressFunc { return xorCompress }

// failingCompress returns a CompressFunc that succeeds N times and then errors.
// Used to inject failures into the encode path under -race.
func failingCompress(remaining int) CompressFunc {
	return func(in []byte) ([]byte, error) {
		if remaining <= 0 {
			return nil, errors.New("boom: compressor fail")
		}
		remaining--
		out := make([]byte, len(in))
		copy(out, in)
		return out, nil
	}
}

// --- validation --------------------------------------------------------------

func TestCreateValidation(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name   string
		opts   WriterOptions
		errSub string
	}{
		{"negative Concurrency", WriterOptions{Concurrency: -1}, "Concurrency must be non-negative"},
		{"negative BytesPerSync", WriterOptions{BytesPerSync: -1}, "BytesPerSync must be non-negative"},
		{"negative ItemsPerRecord", WriterOptions{ItemsPerRecord: -1}, "ItemsPerRecord must be non-negative"},
		{
			"ItemsPerRecord > uint32 max",
			WriterOptions{ItemsPerRecord: math.MaxUint32 + 1},
			"exceeds uint32 max",
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, fmt.Sprintf("case%d", i))
			_, err := Create(path, tc.opts)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errSub)
		})
	}
}

func TestCreateFailsIfFileExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pack")

	w, err := Create(path, WriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.Finish(nil))

	_, err = Create(path, WriterOptions{})
	require.Error(t, err)

	w2, err := Create(path, WriterOptions{Overwrite: true})
	require.NoError(t, err)
	require.NoError(t, w2.Finish(nil))
}

// --- lifecycle ---------------------------------------------------------------

func TestCloseWithoutFinishRemovesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pack")
	w, err := Create(path, WriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.AppendItem([]byte("hello")))

	_ = w.Close()

	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "file should be removed, stat err=%v", err)
}

func TestFinishTwice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pack")
	w, err := Create(path, WriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.Finish(nil))
	require.ErrorIs(t, w.Finish(nil), ErrWriterClosed)
}

func TestAppendItemAfterFinish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pack")
	w, err := Create(path, WriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.Finish(nil))
	require.ErrorIs(t, w.AppendItem([]byte("hello")), ErrWriterClosed)
}

func TestCloseIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pack")
	w, err := Create(path, WriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.Finish(nil))

	require.NoError(t, w.Close())
	require.NoError(t, w.Close())

	_, err = os.Stat(path)
	require.NoError(t, err)
}

func TestEmptyPackfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pack")
	w, err := Create(path, WriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.Finish(nil))

	tr, _ := readTrailer(t, path)
	require.EqualValues(t, 0, tr.totalItems)
	require.EqualValues(t, 0, tr.recordCount)
	require.EqualValues(t, 0, tr.appDataSize)
	require.EqualValues(t, 0, tr.format)
}

// --- format / trailer --------------------------------------------------------

func TestTrailerFields(t *testing.T) {
	cases := []struct {
		name            string
		opts            WriterOptions
		numItems        int
		itemSize        int
		wantFlags       uint8
		wantItemsPerRec uint32
		wantFormat      Format
	}{
		{
			name:            "passthrough_no_hash",
			opts:            WriterOptions{Format: 5},
			numItems:        100,
			itemSize:        16,
			wantFlags:       0,
			wantItemsPerRec: defaultItemsPerRecord,
			wantFormat:      5,
		},
		{
			name:            "xor_with_hash",
			opts:            WriterOptions{Format: 1, NewCompressor: newXorCompressor, ContentHash: true},
			numItems:        50,
			itemSize:        32,
			wantFlags:       flagContentHash,
			wantItemsPerRec: defaultItemsPerRecord,
			wantFormat:      1,
		},
		{
			name:            "explicit_record_size",
			opts:            WriterOptions{Format: 42, ItemsPerRecord: 64},
			numItems:        200,
			itemSize:        8,
			wantFlags:       0,
			wantItemsPerRec: 64,
			wantFormat:      42,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			items := mkItems(tc.numItems, tc.itemSize)
			path := writePackfile(t, tc.opts, items)
			tr, _ := readTrailer(t, path)

			require.EqualValues(t, magic, tr.magic, "magic")
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			trailerStart := len(data) - trailerSize
			require.Equal(t, []byte("SLCH"), data[trailerStart:trailerStart+4], "magic on-disk bytes")
			require.EqualValues(t, version, tr.version, "version")
			require.Equal(t, tc.wantFlags, tr.flags, "flags")
			require.EqualValues(t, tc.numItems, tr.totalItems)
			require.Equal(t, tc.wantItemsPerRec, tr.itemsPerRecord)
			require.Equal(t, tc.wantFormat, tr.format)
			require.EqualValues(t, groupSize, tr.indexGroupSize)
		})
	}
}

func TestOffsetIndexDecodes(t *testing.T) {
	const numItems = 500
	items := mkItems(numItems, 16)
	path := writePackfile(t, WriterOptions{Format: 1, NewCompressor: newXorCompressor}, items)

	tr, totalSize := readTrailer(t, path)

	indexEnd := totalSize - trailerSize - int64(tr.appDataSize)
	indexStart := indexEnd - int64(tr.indexSize)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	indexBytes := data[indexStart:indexEnd]

	recordCount := int(tr.recordCount)
	offsets, err := decodeIndex(indexBytes, recordCount, int(tr.indexSize), indexStart)
	require.NoError(t, err)
	require.Len(t, offsets, recordCount+1)

	for i := 1; i < len(offsets); i++ {
		require.GreaterOrEqualf(t, offsets[i], offsets[i-1],
			"offsets not monotonic at %d", i)
	}
	require.EqualValues(t, 0, offsets[0])
	require.Equal(t, indexStart, offsets[recordCount])
}

func TestAppDataPreserved(t *testing.T) {
	appData := []byte("application-specific metadata between index and trailer")

	path := filepath.Join(t.TempDir(), "pack")
	w, err := Create(path, WriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.AppendItem([]byte("item")))
	require.NoError(t, w.Finish(appData))

	tr, totalSize := readTrailer(t, path)
	require.EqualValues(t, len(appData), tr.appDataSize)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	appDataStart := totalSize - trailerSize - int64(tr.appDataSize)
	got := data[appDataStart : appDataStart+int64(tr.appDataSize)]
	require.Equal(t, appData, got)
}

// TestItemsPerRecordOne covers the edge case where every item is its own
// record; no FOR index is emitted per record.
func TestItemsPerRecordOne(t *testing.T) {
	items := mkItems(5, 16)
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items)

	tr, _ := readTrailer(t, path)
	require.EqualValues(t, 5, tr.totalItems)
	require.EqualValues(t, 5, tr.recordCount)
	require.EqualValues(t, 1, tr.itemsPerRecord)
}

func TestOverwriteReplaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pack")

	w, err := Create(path, WriterOptions{})
	require.NoError(t, err)
	for _, item := range mkItems(128, 4) {
		require.NoError(t, w.AppendItem(item))
	}
	require.NoError(t, w.Finish(nil))

	w2, err := Create(path, WriterOptions{Overwrite: true})
	require.NoError(t, err)
	for _, item := range mkItems(50, 4) {
		require.NoError(t, w2.AppendItem(item))
	}
	require.NoError(t, w2.Finish(nil))

	tr, _ := readTrailer(t, path)
	require.EqualValues(t, 50, tr.totalItems, "overwrite did not replace contents")
}

// --- passthrough and content-hash hook --------------------------------------

// TestPassthroughStoresVerbatim verifies that with NewCompressor == nil, items
// are written to disk exactly as received (the rocksdb-passthrough use case).
func TestPassthroughStoresVerbatim(t *testing.T) {
	items := [][]byte{
		[]byte("hello world"),
		[]byte("second item"),
		[]byte("third"),
	}
	// ItemsPerRecord=1 + no compressor → record N is exactly item N's bytes.
	path := writePackfile(t, WriterOptions{Format: 99, ItemsPerRecord: 1}, items)

	tr, totalSize := readTrailer(t, path)
	require.EqualValues(t, 99, tr.format)
	require.EqualValues(t, 3, tr.totalItems)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	indexEnd := totalSize - trailerSize - int64(tr.appDataSize)
	indexStart := indexEnd - int64(tr.indexSize)
	records := data[:indexStart]

	want := append([]byte{}, items[0]...)
	want = append(want, items[1]...)
	want = append(want, items[2]...)
	require.Equal(t, want, records, "passthrough did not store items verbatim")
}

// TestContentHashExtract verifies that the extract hook is invoked and its
// output is what's hashed. Writer A (xor compressor, no extract) hashes raw
// items. Writer B (passthrough on already-xor'd input, extract un-xors)
// should produce the same hash.
func TestContentHashExtract(t *testing.T) {
	items := mkItems(10, 20)

	pathA := writePackfile(t, WriterOptions{
		Format:        1,
		NewCompressor: newXorCompressor,
		ContentHash:   true,
	}, items)

	xorItems := make([][]byte, len(items))
	for i, item := range items {
		out, _ := xorCompress(item)
		xorItems[i] = out
	}
	pathB := writePackfile(t, WriterOptions{
		Format:             1,
		ContentHash:        true,
		ContentHashExtract: xorCompress, // xor is its own inverse
	}, xorItems)

	trA, _ := readTrailer(t, pathA)
	trB, _ := readTrailer(t, pathB)
	require.Equal(t, trA.contentHash, trB.contentHash,
		"extract hook did not produce same hash as compressor path")
}

// TestContentHashParity verifies that serial and concurrent modes produce
// the same content hash over the same item stream.
func TestContentHashParity(t *testing.T) {
	items := mkItems(500, 64)

	serialPath := writePackfile(t, WriterOptions{
		Format: 1, NewCompressor: newXorCompressor, ContentHash: true,
	}, items)
	concurrentPath := writePackfile(t, WriterOptions{
		Format: 1, NewCompressor: newXorCompressor, ContentHash: true, Concurrency: 4,
	}, items)

	sTr, _ := readTrailer(t, serialPath)
	cTr, _ := readTrailer(t, concurrentPath)
	require.Equal(t, sTr.contentHash, cTr.contentHash,
		"content hash differs between serial and concurrent paths")

	var zero [32]byte
	require.NotEqual(t, zero, sTr.contentHash, "content hash is zero")
}

// TestContentHashExtractConcurrent verifies the concurrent path invokes
// ContentHashExtract correctly: serial and concurrent produce the same hash
// when items must first be un-xor'd by the extract hook.
func TestContentHashExtractConcurrent(t *testing.T) {
	items := mkItems(500, 64)
	xorItems := make([][]byte, len(items))
	for i, item := range items {
		out, _ := xorCompress(item)
		xorItems[i] = out
	}

	serialPath := writePackfile(t, WriterOptions{
		Format: 1, ContentHash: true, ContentHashExtract: xorCompress,
	}, xorItems)
	concurrentPath := writePackfile(t, WriterOptions{
		Format: 1, ContentHash: true, ContentHashExtract: xorCompress, Concurrency: 4,
	}, xorItems)

	sTr, _ := readTrailer(t, serialPath)
	cTr, _ := readTrailer(t, concurrentPath)
	require.Equal(t, sTr.contentHash, cTr.contentHash,
		"extract hook produced different hashes between serial and concurrent paths")
}

// TestConcurrentContentHashExtractError verifies extract errors from worker
// goroutines surface from AppendItem or Finish.
func TestConcurrentContentHashExtractError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pack")
	var calls atomic.Int64
	w, err := Create(path, WriterOptions{
		Format:      1,
		ContentHash: true,
		ContentHashExtract: func(item []byte) ([]byte, error) {
			if calls.Add(1) > 50 {
				return nil, errors.New("boom: extract fail")
			}
			return item, nil
		},
		ItemsPerRecord: 8,
		Concurrency:    4,
	})
	require.NoError(t, err)
	defer w.Close()

	item := []byte("data")
	var seenErr error
	for range 10_000 {
		if err := w.AppendItem(item); err != nil {
			seenErr = err
			break
		}
	}
	if seenErr == nil {
		seenErr = w.Finish(nil)
	}
	require.Error(t, seenErr)
	require.Contains(t, seenErr.Error(), "boom")
}

// --- error propagation -------------------------------------------------------

// TestSerialCompressErrorSurfaces injects a failing compressor and asserts
// the error is returned from AppendItem or Finish. No /dev/full needed.
func TestSerialCompressErrorSurfaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pack")
	w, err := Create(path, WriterOptions{
		Format:         1,
		NewCompressor:  func() CompressFunc { return failingCompress(2) },
		ItemsPerRecord: 8,
	})
	require.NoError(t, err)
	defer w.Close()

	item := []byte("data")
	var seenErr error
	for range 100 {
		if err := w.AppendItem(item); err != nil {
			seenErr = err
			break
		}
	}
	require.Error(t, seenErr)
	require.Contains(t, seenErr.Error(), "boom")
}

// TestConcurrentCompressErrorSurfaces verifies the atomic-err propagation
// path under -race.
func TestConcurrentCompressErrorSurfaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pack")
	w, err := Create(path, WriterOptions{
		Format:         1,
		NewCompressor:  func() CompressFunc { return failingCompress(2) },
		ItemsPerRecord: 8,
		Concurrency:    4,
	})
	require.NoError(t, err)
	defer w.Close()

	item := []byte("data")
	var seenErr error
	for range 10_000 {
		if err := w.AppendItem(item); err != nil {
			seenErr = err
			break
		}
	}
	require.Error(t, seenErr)
	require.Contains(t, seenErr.Error(), "boom")
}

// TestSerialWriteErrorSurfaces / TestConcurrentWriteErrorSurfaces — the
// /dev/full tests still verify the file-write error path on Linux.

func openDevFullWriter(t *testing.T, opts WriterOptions) *Writer {
	t.Helper()
	opts.Overwrite = true
	w, err := Create("/dev/full", opts)
	require.NoError(t, err)
	w.path = filepath.Join(t.TempDir(), "dev-full-scratch")
	return w
}

func TestConcurrentWriteErrorSurfaces(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires /dev/full")
	}
	w := openDevFullWriter(t, WriterOptions{
		Concurrency:   4,
		ContentHash:   true,
		NewCompressor: newXorCompressor,
	})
	defer w.Close()

	item := make([]byte, 1024)
	const maxIters = 100_000
	for range maxIters {
		if err := w.AppendItem(item); err != nil {
			return
		}
	}
	t.Fatalf("AppendItem never surfaced error after %d iterations", maxIters)
}

func TestSerialWriteErrorSurfaces(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires /dev/full")
	}
	w := openDevFullWriter(t, WriterOptions{NewCompressor: newXorCompressor})
	defer w.Close()

	item := make([]byte, 1024)
	const maxIters = 100_000
	for range maxIters {
		if err := w.AppendItem(item); err != nil {
			return
		}
	}
	t.Fatalf("AppendItem never surfaced error after %d iterations", maxIters)
}
