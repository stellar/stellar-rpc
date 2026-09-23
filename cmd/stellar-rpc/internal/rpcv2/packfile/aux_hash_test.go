package packfile

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAuxHash_FoldsRecordsInOrderWhateverTheConcurrency pins the property the
// auxiliary hash exists for: the extract runs on whichever worker encoded the
// record, but the digests are folded in RECORD order, so the result cannot
// depend on how many workers there were.
func TestAuxHash_FoldsRecordsInOrderWhateverTheConcurrency(t *testing.T) {
	// The extract pulls a record's first four bytes, which the encoder below
	// makes a function of the item — so a fold that lost the order would
	// produce a different hash.
	extract := func(record []byte) ([]byte, error) { return record[:4], nil }

	var want [32]byte
	for _, workers := range []int{1, 2, 8} {
		path := filepath.Join(t.TempDir(), "aux.pack")
		w, err := Create(path, WriterOptions{
			ItemsPerRecord: 1,
			AuxHashExtract: extract,
			Concurrency:    workers,
		})
		require.NoError(t, err)
		for i := range 500 {
			require.NoError(t, w.AppendItem(fmt.Appendf(nil, "%04d-payload-%d", i, i)))
		}
		require.NoError(t, w.Drain())
		got, ok := w.AuxHash()
		require.True(t, ok)
		require.NoError(t, w.Finish(nil))

		if workers == 1 {
			want = got
			continue
		}
		assert.Equal(t, want, got, "%d workers hashed a different order", workers)
	}

	// And a replay of the same items through the exported hasher agrees, which
	// is what a verifier does.
	replay := NewAuxHasher()
	for i := range 500 {
		replay.Add(fmt.Appendf(nil, "%04d-payload-%d", i, i)[:4])
	}
	assert.Equal(t, want, replay.Sum())
}

// TestAuxHash_EmptyItemsStillCount pins that a record with no sidecar folds a
// zero-length item rather than nothing: "no sidecar anywhere" and "fewer
// records" must not collide.
func TestAuxHash_EmptyItemsStillCount(t *testing.T) {
	two := NewAuxHasher()
	two.Add(nil)
	two.Add(nil)
	one := NewAuxHasher()
	one.Add(nil)
	assert.NotEqual(t, one.Sum(), two.Sum())
	assert.NotEqual(t, NewAuxHasher().Sum(), one.Sum())
}

// TestAuxHash_AbsentWithoutAnExtract pins that a writer with no extract
// computes no auxiliary hash at all, rather than the hash of nothing.
func TestAuxHash_AbsentWithoutAnExtract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plain.pack")
	w, err := Create(path, WriterOptions{ItemsPerRecord: 1})
	require.NoError(t, err)
	require.NoError(t, w.AppendItem([]byte("x")))
	require.NoError(t, w.Drain())
	_, ok := w.AuxHash()
	assert.False(t, ok)
	require.NoError(t, w.Finish(nil))
}

// TestAuxHash_ExtractErrorFailsTheWrite pins that a refusing extract is fatal:
// a pack whose auxiliary hash silently covered fewer records than it holds
// would be worse than no pack.
func TestAuxHash_ExtractErrorFailsTheWrite(t *testing.T) {
	sentinel := errors.New("no sidecar here")
	path := filepath.Join(t.TempDir(), "bad.pack")
	w, err := Create(path, WriterOptions{
		ItemsPerRecord: 1,
		AuxHashExtract: func([]byte) ([]byte, error) { return nil, sentinel },
	})
	require.NoError(t, err)
	_ = w.AppendItem([]byte("x"))
	err = w.Finish(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	_ = w.Close()
}
