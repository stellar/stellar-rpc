package packfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- helpers -----------------------------------------------------------------

// parsedTrailer mirrors the fields packed into the 64-byte trailer.
type parsedTrailer struct {
	magic          uint32
	version        uint8
	flags          uint8
	recordCount    uint32
	totalItems     uint32
	itemsPerRecord uint32
	indexSize      uint32
	appDataSize    uint32
	contentHash    [32]byte
	indexGroupSize uint16
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
		recordCount:    binary.LittleEndian.Uint32(buf[6:]),
		totalItems:     binary.LittleEndian.Uint32(buf[10:]),
		itemsPerRecord: binary.LittleEndian.Uint32(buf[14:]),
		indexSize:      binary.LittleEndian.Uint32(buf[18:]),
		appDataSize:    binary.LittleEndian.Uint32(buf[22:]),
		indexGroupSize: binary.LittleEndian.Uint16(buf[58:]),
		crc:            binary.LittleEndian.Uint32(buf[60:]),
	}
	copy(tr.contentHash[:], buf[26:58])
	require.Equal(t, crc32c(buf[:60]), tr.crc, "trailer CRC")
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

// --- validation --------------------------------------------------------------

func TestCreateValidation(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name   string
		opts   WriterOptions
		errSub string
	}{
		{"invalid Format", WriterOptions{Format: RecordFormat(99)}, "invalid Format"},
		{"negative Concurrency", WriterOptions{Concurrency: -1}, "Concurrency must be non-negative"},
		{"negative BytesPerSync", WriterOptions{BytesPerSync: -1}, "BytesPerSync must be non-negative"},
		{"negative ItemsPerRecord", WriterOptions{ItemsPerRecord: -1}, "ItemsPerRecord must be non-negative"},
		{
			"concurrency > 1 with Raw no hash",
			WriterOptions{Format: Raw, Concurrency: 4},
			"requires Format=Compressed or ContentHash=true",
		},
		{
			"concurrency > 1 with Uncompressed no hash",
			WriterOptions{Format: Uncompressed, Concurrency: 4},
			"requires Format=Compressed or ContentHash=true",
		},
		{
			"ItemsPerRecord > uint32 max",
			WriterOptions{ItemsPerRecord: math.MaxUint32 + 1},
			"exceeds uint32 max",
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Use an index-based filename rather than tc.name so Windows
			// (and other picky filesystems) don't choke on reserved chars
			// like '>'. Validation fails before OpenFile runs anyway.
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

	// Second Create without Overwrite fails.
	_, err = Create(path, WriterOptions{})
	require.Error(t, err)

	// With Overwrite=true, succeeds.
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

	// File preserved (Finish completed).
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
	}{
		{
			name:     "compressed_no_hash",
			opts:     WriterOptions{Format: Compressed},
			numItems: 100, itemSize: 16,
			wantFlags: 0, wantItemsPerRec: defaultItemsPerRecord,
		},
		{
			name:     "uncompressed_with_hash",
			opts:     WriterOptions{Format: Uncompressed, ContentHash: true},
			numItems: 50, itemSize: 32,
			wantFlags: flagNoCompression | flagContentHash, wantItemsPerRec: defaultItemsPerRecord,
		},
		{
			name:     "raw_explicit_record_size",
			opts:     WriterOptions{Format: Raw, ItemsPerRecord: 64},
			numItems: 200, itemSize: 8,
			wantFlags: flagNoCompression | flagNoCRC, wantItemsPerRec: 64,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			items := mkItems(tc.numItems, tc.itemSize)
			path := writePackfile(t, tc.opts, items)
			tr, _ := readTrailer(t, path)

			require.EqualValues(t, magic, tr.magic, "magic")
			// On-disk magic bytes must spell "SLCH" in little-endian.
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			trailerStart := len(data) - trailerSize
			require.Equal(t, []byte("SLCH"), data[trailerStart:trailerStart+4], "magic on-disk bytes")
			require.EqualValues(t, version, tr.version, "version")
			require.Equal(t, tc.wantFlags, tr.flags, "flags")
			require.EqualValues(t, tc.numItems, tr.totalItems)
			require.Equal(t, tc.wantItemsPerRec, tr.itemsPerRecord)
			require.EqualValues(t, groupSize, tr.indexGroupSize)
		})
	}
}

func TestOffsetIndexDecodes(t *testing.T) {
	const numItems = 500
	items := mkItems(numItems, 16)
	path := writePackfile(t, WriterOptions{Format: Compressed}, items)

	tr, totalSize := readTrailer(t, path)

	// layout: [records...][index][appData][trailer]
	indexEnd := totalSize - trailerSize - int64(tr.appDataSize)
	indexStart := indexEnd - int64(tr.indexSize)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	indexBytes := data[indexStart:indexEnd]

	recordCount := int(tr.recordCount)
	offsets, err := decodeIndex(indexBytes, recordCount, int(tr.indexSize), indexStart)
	require.NoError(t, err)
	require.Len(t, offsets, recordCount+1)

	// Monotonic increasing.
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
// record, meaning no FOR index is emitted per record (the `if itemsPerRecord > 1`
// branch in buildBlock is skipped). Exercises a code path the other tests miss.
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

	// First: 128 items (exactly one full default record).
	w, err := Create(path, WriterOptions{})
	require.NoError(t, err)
	for _, item := range mkItems(128, 4) {
		require.NoError(t, w.AppendItem(item))
	}
	require.NoError(t, w.Finish(nil))

	// Overwrite with 50 items (partial record only).
	w2, err := Create(path, WriterOptions{Overwrite: true})
	require.NoError(t, err)
	for _, item := range mkItems(50, 4) {
		require.NoError(t, w2.AppendItem(item))
	}
	require.NoError(t, w2.Finish(nil))

	tr, _ := readTrailer(t, path)
	require.EqualValues(t, 50, tr.totalItems, "overwrite did not replace contents")
}

// --- cross-mode invariant ----------------------------------------------------

// TestContentHashParity verifies that serial and concurrent modes produce the
// same content hash over the same item stream. This is the central invariant
// of the hashing design; regressions in either the contentHasher (serial) or
// the digestHasher pipeline (concurrent) would show up here.
func TestContentHashParity(t *testing.T) {
	items := mkItems(500, 64)

	serialPath := writePackfile(t, WriterOptions{ContentHash: true}, items)
	concurrentPath := writePackfile(t,
		WriterOptions{ContentHash: true, Concurrency: 4}, items)

	sTr, _ := readTrailer(t, serialPath)
	cTr, _ := readTrailer(t, concurrentPath)

	require.Equal(t, sTr.contentHash, cTr.contentHash,
		"content hash differs between serial and concurrent paths")

	var zero [32]byte
	require.NotEqual(t, zero, sTr.contentHash, "content hash is zero")
}

// --- error propagation (Linux-only; relies on /dev/full) ---------------------

// openDevFullWriter creates a Writer targeting /dev/full so every Write
// returns ENOSPC, then redirects w.path to a harmless sentinel so Close's
// os.Remove doesn't target the device node if the test runs as root.
func openDevFullWriter(t *testing.T, opts WriterOptions) *Writer {
	t.Helper()
	opts.Overwrite = true
	w, err := Create("/dev/full", opts)
	require.NoError(t, err)
	// Redirect so the deferred Close()'s os.Remove targets a non-existent file
	// under t.TempDir() (auto-cleaned), not /dev/full. Guard against the
	// root-in-container case where Remove("/dev/full") would actually delete
	// the device node.
	w.path = filepath.Join(t.TempDir(), "dev-full-scratch")
	return w
}

// TestConcurrentWriteErrorSurfaces verifies that write errors in the
// background pipeline propagate to the main goroutine's AppendItem via the
// atomic err mechanism. Uses /dev/full (Linux), where all writes return ENOSPC.
func TestConcurrentWriteErrorSurfaces(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires /dev/full")
	}

	w := openDevFullWriter(t, WriterOptions{Concurrency: 4, ContentHash: true})
	defer w.Close()

	item := make([]byte, 1024)
	const maxIters = 100_000
	for range maxIters {
		if err := w.AppendItem(item); err != nil {
			return // success: error surfaced
		}
	}
	t.Fatalf("AppendItem never surfaced error after %d iterations", maxIters)
}

// TestSerialWriteErrorSurfaces is the serial-path counterpart of the above.
func TestSerialWriteErrorSurfaces(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires /dev/full")
	}

	w := openDevFullWriter(t, WriterOptions{Format: Compressed})
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
