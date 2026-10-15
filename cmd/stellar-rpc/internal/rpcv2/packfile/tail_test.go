package packfile

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type readLog struct {
	*os.File

	reads [][2]int64
}

func (l *readLog) ReadAt(p []byte, off int64) (int, error) {
	l.reads = append(l.reads, [2]int64{off, int64(len(p))})
	return l.File.ReadAt(p, off)
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
		log := &readLog{File: f}
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
