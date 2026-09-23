package packfile

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSpeculativeTailSize_TunesTheOpenRead pins the knob's three cases: the
// default covers an ordinary pack in one read, a tail sized under the index
// costs exactly one follow-up read and still opens correctly, and a negative
// size is rejected the way every other bad option is — on the first read call,
// since Open itself cannot fail.
func TestSpeculativeTailSize_TunesTheOpenRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tail.pack")
	w, err := Create(path, WriterOptions{ItemsPerRecord: 1})
	require.NoError(t, err)
	const items = 4_000
	for i := range items {
		require.NoError(t, w.AppendItem([]byte{byte(i), byte(i >> 8)}))
	}
	require.NoError(t, w.Finish([]byte("app")))

	for name, tail := range map[string]int{"default": 0, "generous": 1 << 20} {
		t.Run(name, func(t *testing.T) {
			reads, refills := TailReads(), TailRefills()
			r := Open(path, ReaderOptions{SpeculativeTailSize: tail})
			defer func() { _ = r.Close() }()
			tr, terr := r.Trailer()
			require.NoError(t, terr)
			assert.EqualValues(t, items, tr.TotalItems)
			assert.Equal(t, reads+1, TailReads())
			assert.Equal(t, refills, TailRefills(), "the tail covered the index; no second read")
		})
	}

	t.Run("shorter than the index", func(t *testing.T) {
		reads, refills := TailReads(), TailRefills()
		// Below TrailerSize on purpose: the request is raised to the trailer,
		// which is all the tail MUST hold, and the index is then read again.
		r := Open(path, ReaderOptions{SpeculativeTailSize: 1})
		defer func() { _ = r.Close() }()
		tr, terr := r.Trailer()
		require.NoError(t, terr)
		assert.EqualValues(t, items, tr.TotalItems)
		ad, aerr := r.AppData()
		require.NoError(t, aerr)
		assert.Equal(t, []byte("app"), ad)
		assert.Equal(t, reads+1, TailReads())
		assert.Equal(t, refills+1, TailRefills())

		// The records still read, which is the point: the tail is a tuning
		// knob and never a correctness one.
		last := items - 1
		require.NoError(t, r.ReadItem(last, func(b []byte) error {
			assert.Equal(t, []byte{byte(last), byte(last >> 8)}, b)
			return nil
		}))
	})

	t.Run("negative", func(t *testing.T) {
		r := Open(path, ReaderOptions{SpeculativeTailSize: -1})
		defer func() { _ = r.Close() }()
		_, err := r.Trailer()
		require.Error(t, err)
		assert.ErrorContains(t, err, "SpeculativeTailSize")
	})
}
