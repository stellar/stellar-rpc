package txhash

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

func TestOpenColdReader_NonExistentErrors(t *testing.T) {
	_, err := OpenColdReader(filepath.Join(t.TempDir(), "missing.idx"))
	assert.Error(t, err)
}

func TestColdReader_LookupHitAndMiss(t *testing.T) {
	path, entries := buildColdFixture(t, 64)

	r, err := OpenColdReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	for _, e := range entries {
		got, err := r.Lookup(e.hash)
		require.NoError(t, err)
		assert.Equal(t, e.seq, got)
	}

	// An unseen key returns ErrNotFound.
	unseenRng := testRNG(0xbeef_face)
	_, err = r.Lookup(randHash(unseenRng))
	assert.ErrorIs(t, err, stores.ErrNotFound)
}

func TestColdReader_CloseIsIdempotent(t *testing.T) {
	path, _ := buildColdFixture(t, 4)
	r, err := OpenColdReader(path)
	require.NoError(t, err)

	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
}

func TestColdReader_LookupAfterCloseReturnsClosed(t *testing.T) {
	path, entries := buildColdFixture(t, 4)
	r, err := OpenColdReader(path)
	require.NoError(t, err)
	require.NoError(t, r.Close())

	_, err = r.Lookup(entries[0].hash)
	assert.ErrorIs(t, err, stores.ErrStoreClosed)
}
