package adapters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

func TestSeedCloseTimes_StampsBothEdges(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	first := testChunk.FirstLedger()
	seedHotLedgers(t, cat, r, testChunk, seqRange(first, first+2)...)
	// The boot state: latest seeded from the catalog, close time unknown.
	r.SetLatestLedger(first+2, 0)

	require.NoError(t, SeedCloseTimes(r))

	view, err := r.NewReadView()
	require.NoError(t, err)
	defer view.Release()
	ct, ok := view.OldestCloseTime()
	assert.True(t, ok)
	assert.Equal(t, closeTimeFor(first), ct)
	ct, ok = view.LatestCloseTime()
	assert.True(t, ok)
	assert.Equal(t, closeTimeFor(first+2), ct)
}

func TestSeedCloseTimes_EmptyCatalogIsANoOp(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	seedHotLedgers(t, cat, r, testChunk)
	r.SetLatestLedger(testChunk.FirstLedger()-1, 0)

	require.NoError(t, SeedCloseTimes(r))

	view, err := r.NewReadView()
	require.NoError(t, err)
	defer view.Release()
	_, ok := view.LatestCloseTime()
	assert.False(t, ok, "nothing to stamp on an empty catalog")
	_, ok = view.OldestCloseTime()
	assert.False(t, ok)
}
