package packfile

import (
	"encoding/binary"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type readLog struct {
	readAtCloser

	mu    sync.Mutex
	reads [][2]int64
}

func (l *readLog) ReadAt(p []byte, off int64) (int, error) {
	l.mu.Lock()
	l.reads = append(l.reads, [2]int64{off, int64(len(p))})
	l.mu.Unlock()
	return l.readAtCloser.ReadAt(p, off)
}

func TestOpenFirstRead(t *testing.T) {
	// Larger than the default first read.
	items := makeItems(300, 1<<10)
	appData := []byte("app")
	path := writeAppDataPackfile(t, items, appData)

	tr, fileSize := readTrailer(t, path)
	tailSize := int64(tr.indexSize) + int64(tr.appDataSize) + trailerSize
	tailStart := fileSize - tailSize

	openLogged := func(t *testing.T, path string, firstRead func(int64) int) (openResult, [][2]int64) {
		f, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })
		log := &readLog{readAtCloser: f}
		res := openFile(log, fileSize, firstRead)
		return res, log.reads
	}
	fixed := func(n int) func(int64) int { return func(int64) int { return n } }

	trailerThenRest := [][2]int64{{fileSize - trailerSize, trailerSize}, {tailStart, tailSize - trailerSize}}
	cases := []struct {
		name      string
		firstRead func(int64) int
		reads     [][2]int64
	}{
		{"covers the tail", fixed(int(tailSize)), [][2]int64{{tailStart, tailSize}}},
		{"one byte short", fixed(int(tailSize) - 1), [][2]int64{{tailStart + 1, tailSize - 1}, {tailStart, 1}}},
		{"zero", fixed(0), trailerThenRest},
		{"negative", fixed(-1), trailerThenRest},
		{"past the file", fixed(int(fileSize) + 1), [][2]int64{{0, fileSize}}},
		{"nil means 256 KiB", nil, [][2]int64{{fileSize - 256<<10, 256 << 10}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, reads := openLogged(t, path, tc.firstRead)
			require.NoError(t, res.err)
			assert.Equal(t, tc.reads, reads)
			assert.Equal(t, appData, res.appData)
		})
	}

	t.Run("tail larger than the file", func(t *testing.T) {
		corrupt := corruptAt(t, path, true, func(data []byte) {
			binary.LittleEndian.PutUint32(data[len(data)-trailerSize+tOffIndexSize:], uint32(fileSize))
		})
		res, reads := openLogged(t, corrupt, fixed(trailerSize))
		require.ErrorIs(t, res.err, ErrSize)
		assert.Equal(t, [][2]int64{{fileSize - trailerSize, trailerSize}}, reads)
	})

	t.Run("through Open", func(t *testing.T) {
		var got int64
		r := Open(path, ReaderOptions{FirstRead: func(size int64) int { got = size; return 1 }})
		defer r.Close()
		ad, err := r.AppData()
		require.NoError(t, err)
		assert.Equal(t, appData, ad)
		assert.Equal(t, fileSize, got)
		last := len(items) - 1
		assert.Equal(t, items[last], readItemCopy(t, r, last))
	})
}

type firstReadBuf struct {
	readAtCloser

	buf []byte
}

func (f *firstReadBuf) ReadAt(p []byte, off int64) (int, error) {
	if f.buf == nil {
		f.buf = p
	}
	return f.readAtCloser.ReadAt(p, off)
}

// openTwice opens a then b, with first reads of n bytes, until b's first read
// reuses a's buffer; sync.Pool may drop it or keep it on another P.
func openTwice(t *testing.T, n int, a, b string) (openResult, openResult) {
	t.Helper()
	open := func(path string) (openResult, []byte) {
		f, err := os.Open(path)
		require.NoError(t, err)
		defer func() { _ = f.Close() }()
		fi, err := f.Stat()
		require.NoError(t, err)
		log := &firstReadBuf{readAtCloser: f}
		return openFile(log, fi.Size(), func(int64) int { return n }), log.buf
	}
	for range 100 {
		ra, bufA := open(a)
		rb, bufB := open(b)
		if &bufA[0] == &bufB[0] {
			return ra, rb
		}
	}
	t.Fatal("no open reused the previous open's first-read buffer")
	return openResult{}, openResult{}
}

// packTail returns the pack's tail size and its index section without the CRC.
func packTail(t *testing.T, path string) (int, []byte) {
	t.Helper()
	tr, fileSize := readTrailer(t, path)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	tailSize := int64(tr.indexSize) + int64(tr.appDataSize) + trailerSize
	indexBase := fileSize - tailSize
	return int(tailSize), data[indexBase : indexBase+int64(tr.indexSize)-4]
}

// A first read reusing another open's buffer leaves that open's tail intact.
func TestOpenDoesNotKeepFirstReadBuffer(t *testing.T) {
	appA, appB := []byte("app data a"), []byte("app data b")
	a := writeAppDataPackfile(t, makeItems(300, 64), appA)
	// b's index differs from a's, so b's first read would change a view of a's.
	b := writeAppDataPackfile(t, makeItems(200, 100), appB)
	tailA, indexA := packTail(t, a)
	tailB, indexB := packTail(t, b)

	cases := []struct {
		name string
		n    int
	}{
		{"covers the tail", max(tailA, tailB)},
		{"falls short", min(tailA, tailB) - 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ra, rb := openTwice(t, tc.n, a, b)
			require.NoError(t, ra.err)
			require.NoError(t, rb.err)
			assert.Equal(t, appA, ra.appData)
			assert.Equal(t, indexA, slices.Concat(ra.idx.groups, ra.idx.dir))
			assert.Equal(t, appB, rb.appData)
			assert.Equal(t, indexB, slices.Concat(rb.idx.groups, rb.idx.dir))
		})
	}
}
