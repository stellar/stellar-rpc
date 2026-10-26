package writeback

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriterWritesWhatItIsGiven(t *testing.T) {
	for _, bytesPerSync := range []int{0, 64 << 10} {
		path := filepath.Join(t.TempDir(), "f")
		f, err := os.Create(path)
		require.NoError(t, err)
		w := NewWriter(f, bytesPerSync)
		var want []byte
		for i := range 300 {
			p := bytes.Repeat([]byte{byte(i)}, 4096+i)
			n, err := w.Write(p)
			require.NoError(t, err)
			require.Equal(t, len(p), n)
			want = append(want, p...)
		}
		require.NoError(t, f.Close())
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, got)
		if bytesPerSync == 0 {
			require.Zero(t, w.synced)
		} else {
			require.Greater(t, w.synced, int64(len(want)-bytesPerSync))
		}
	}
}

func TestWriterSkipLeavesAHole(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	f, err := os.Create(path)
	require.NoError(t, err)
	w := NewWriter(f, 4096)
	a, b := bytes.Repeat([]byte{1}, 5000), bytes.Repeat([]byte{2}, 3000)
	_, err = w.Write(a)
	require.NoError(t, err)
	require.NoError(t, w.Skip(1<<16))
	_, err = w.Write(b)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	want := slices.Concat(a, make([]byte, 1<<16), b)
	require.Equal(t, want, got)
	require.Equal(t, int64(len(want)), w.off)
}
