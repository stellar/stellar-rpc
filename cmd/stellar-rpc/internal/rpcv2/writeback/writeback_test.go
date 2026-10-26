package writeback

import (
	"bytes"
	"os"
	"path/filepath"
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
