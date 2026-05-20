package txhash

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

func TestNewColdIndexWriter_ValidatesInputs(t *testing.T) {
	_, err := NewColdIndexWriter(context.Background(), "", 1)
	assert.ErrorIs(t, err, stores.ErrInvalidConfig)

	_, err = NewColdIndexWriter(context.Background(), filepath.Join(t.TempDir(), "x.idx"), 0)
	assert.ErrorIs(t, err, ErrEmptyBuildSet)
}

func TestColdWriter_CommitProducesReadableFile(t *testing.T) {
	const n = 32
	path, entries := buildColdFixture(t, n)

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Positive(t, info.Size(), "committed index file must be non-empty")

	r, err := OpenColdReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	for _, e := range entries {
		got, err := r.Lookup(e.hash)
		require.NoError(t, err)
		assert.Equal(t, e.seq, got)
	}
}

func TestColdWriter_CloseBeforeCommitRemovesPartial(t *testing.T) {
	path := filepath.Join(t.TempDir(), ColdIndexName)
	w, err := NewColdIndexWriter(context.Background(), path, 4)
	require.NoError(t, err)

	// streamhash creates the file in NewBuilder (truncate + reserve).
	// Confirm it exists so the removal check below is meaningful.
	_, err = os.Stat(path)
	require.NoError(t, err, "streamhash should create the output file on NewBuilder")

	// Add one entry but never Commit.
	r := testRNG(1)
	require.NoError(t, w.AddEntry(randHash(r), 1))
	require.NoError(t, w.Close())

	_, err = os.Stat(path)
	assert.ErrorIs(t, err, os.ErrNotExist, "Close on un-committed writer must remove the partial file")
}

func TestColdWriter_CloseIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), ColdIndexName)
	w, err := NewColdIndexWriter(context.Background(), path, 1)
	require.NoError(t, err)
	r := testRNG(2)
	require.NoError(t, w.AddEntry(randHash(r), 1))
	require.NoError(t, w.Commit())

	require.NoError(t, w.Close())
	require.NoError(t, w.Close(), "second Close must be a no-op after Commit")
}

func TestColdWriter_AddAfterCloseErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), ColdIndexName)
	w, err := NewColdIndexWriter(context.Background(), path, 2)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	rng := testRNG(3)
	err = w.AddEntry(randHash(rng), 1)
	assert.ErrorIs(t, err, stores.ErrStoreClosed)

	err = w.Commit()
	assert.ErrorIs(t, err, stores.ErrStoreClosed)
}

func TestColdWriter_CommitAfterCommitErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), ColdIndexName)
	w, err := NewColdIndexWriter(context.Background(), path, 1)
	require.NoError(t, err)
	rng := testRNG(4)
	require.NoError(t, w.AddEntry(randHash(rng), 1))
	require.NoError(t, w.Commit())

	assert.ErrorIs(t, w.Commit(), stores.ErrStoreClosed)
}
