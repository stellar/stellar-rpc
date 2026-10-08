package bench

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
)

// parseIndexFileName accepts only a {lo:08d}-{hi:08d}.idx name with lo <= hi.
func TestParseIndexFileName(t *testing.T) {
	lo, hi, ok := parseIndexFileName("00000001-00000003.idx")
	require.True(t, ok)
	assert.Equal(t, chunk.ID(1), lo)
	assert.Equal(t, chunk.ID(3), hi)

	for _, name := range []string{
		"00000001-00000003.bin",
		"00000001.idx",
		"0000001-00000003.idx",
		"0000000x-00000003.idx",
		"00000003-00000001.idx",
	} {
		_, _, ok := parseIndexFileName(name)
		assert.False(t, ok, name)
	}
}

// A range that spans more than one tx-hash window index is an error.
func TestDiskTxHashCoverageRejectsSpanningRange(t *testing.T) {
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	_, _, err = diskTxHashCoverage(geometry.NewLayout(t.TempDir()), txLayout,
		0, chunk.ID(geometry.ChunksPerTxhashIndex))
	require.ErrorContains(t, err, "span more than one tx-hash window index")
}

// diskTxHashCoverage picks the .idx file that spans the range with the highest
// Hi, and reports no coverage when none spans it or the directory is missing.
func TestDiskTxHashCoverage(t *testing.T) {
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)

	_, ok, err := diskTxHashCoverage(geometry.NewLayout(t.TempDir()), txLayout, 0, 2)
	require.NoError(t, err)
	assert.False(t, ok, "missing directory")

	layout := geometry.NewLayout(t.TempDir())
	dir := layout.TxHashIndexDir(0)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	for _, name := range []string{
		"00000000-00000002.idx",
		"00000000-00000005.idx",
		"00000001-00000004.idx",
		"00000003-00000005.idx",
		"junk.idx",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o600))
	}

	cov, ok, err := diskTxHashCoverage(layout, txLayout, 1, 2)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, geometry.TxHashIndexCoverage{
		Index: 0, Lo: 0, Hi: 5, Key: geometry.TxHashIndexKey(0, 0, 5),
	}, cov)

	_, ok, err = diskTxHashCoverage(layout, txLayout, 0, 6)
	require.NoError(t, err)
	assert.False(t, ok, "no file spans [0, 6]")
}

// With txhash not requested, commitDiskTxHashIndex warns and commits nothing
// when no .idx is on disk or the range spans two tx-hash windows.
func TestCommitDiskTxHashIndexWarnsWhenTxHashNotRequested(t *testing.T) {
	for _, tc := range []struct {
		name   string
		lo, hi chunk.ID
		want   string
	}{
		{"no index on disk", 0, 1, "no tx-hash window index on disk covers chunks"},
		{
			"range spans two windows",
			chunk.ID(geometry.ChunksPerTxhashIndex - 1), chunk.ID(geometry.ChunksPerTxhashIndex),
			"span more than one tx-hash window index",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, _ := rpcv2test.OpenTestCatalog(t, geometry.ChunksPerTxhashIndex)
			logger, output := capturingLogger()
			require.NoError(t, commitDiskTxHashIndex(logger, cat, cat.Layout(), tc.lo, tc.hi, false))
			assert.Contains(t, output.String(), "level=warning")
			assert.Contains(t, output.String(), tc.want)
			keys, err := cat.AllTxHashIndexKeys()
			require.NoError(t, err)
			assert.Empty(t, keys)
		})
	}
}

// An .idx whose coverage is wider than the queried range is committed frozen
// and covers the range.
func TestCommitDiskTxHashIndexWiderCoverage(t *testing.T) {
	cat, _ := rpcv2test.OpenTestCatalog(t, geometry.ChunksPerTxhashIndex)
	cov := geometry.TxHashIndexCoverage{Index: 0, Lo: 0, Hi: 2, Key: geometry.TxHashIndexKey(0, 0, 2)}
	path := cat.Layout().TxHashIndexFilePath(cov)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	require.NoError(t, commitDiskTxHashIndex(testLogger(), cat, cat.Layout(), 0, 1, true))
	frozen, ok, err := cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, cov.Key, frozen.Key)
	covers, err := cat.FrozenIndexCoversRange(0, 0, 1)
	require.NoError(t, err)
	assert.True(t, covers)
}
