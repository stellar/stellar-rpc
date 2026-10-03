package bench

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// treeEntries returns every path under root, relative to root.
func treeEntries(t *testing.T, root string) []string {
	t.Helper()
	var entries []string
	require.NoError(t, filepath.WalkDir(root, func(path string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		entries = append(entries, rel)
		return err
	}))
	return entries
}

// dirNames returns the names of dir's entries.
func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

// touchFiles creates an empty file at each path.
func touchFiles(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, nil, 0o600))
	}
}

// An artifact kind counts as on disk only when every one of its files exists.
func TestArtifactOnDisk(t *testing.T) {
	layout := geometry.NewLayout(t.TempDir())
	events := layout.EventsPaths(0)
	touchFiles(t, layout.LedgerPackPath(0))
	touchFiles(t, events[1:]...)

	onDisk, err := artifactOnDisk(layout, 0, geometry.KindLedgers)
	require.NoError(t, err)
	assert.True(t, onDisk)

	onDisk, err = artifactOnDisk(layout, 0, geometry.KindEvents)
	require.NoError(t, err)
	assert.False(t, onDisk, "%s is missing", events[0])
}

// openColdDataset fails on a missing --cold-dir or a chunk with no ledger
// pack, and leaves the input tree as it found it.
func TestOpenColdDatasetErrors(t *testing.T) {
	t.Run("missing --cold-dir", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "missing")
		_, _, err := openColdDataset(testLogger(), coldQueryOptions{ColdRoot: root, NumChunks: 1})
		require.ErrorContains(t, err, "--cold-dir")
		require.ErrorContains(t, err, root)
		assert.NoDirExists(t, root)
	})
	t.Run("no ledger pack", func(t *testing.T) {
		root := t.TempDir()
		touchFiles(t, geometry.NewLayout(root).LedgerPackPath(0))
		before := treeEntries(t, root)
		_, _, err := openColdDataset(testLogger(), coldQueryOptions{ColdRoot: root, NumChunks: 2})
		require.ErrorContains(t, err, "chunk "+chunk.ID(1).String()+" has no ledger pack")
		assert.Equal(t, before, treeEntries(t, root))
	})
}

// openHotDataset fails on a missing --hot-dir, a missing chunk database, an
// empty database and another chunk's database, and adds nothing to the input
// tree's root.
func TestOpenHotDatasetErrors(t *testing.T) {
	t.Run("missing --hot-dir", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "missing")
		_, _, err := openHotDataset(testLogger(), hotQueryOptions{HotRoot: root})
		require.ErrorContains(t, err, "--hot-dir")
		require.ErrorContains(t, err, root)
		assert.NoDirExists(t, root)
	})
	t.Run("no chunk database", func(t *testing.T) {
		root := t.TempDir()
		path := geometry.NewLayout(root).HotChunkPath(0)
		_, _, err := openHotDataset(testLogger(), hotQueryOptions{HotRoot: root})
		require.ErrorContains(t, err, "hot database for chunk "+chunk.ID(0).String())
		require.ErrorContains(t, err, path)
		assert.Empty(t, treeEntries(t, root)[1:])
	})
	t.Run("empty database", func(t *testing.T) {
		root := t.TempDir()
		db, err := hotchunk.Open(geometry.NewLayout(root).HotChunkPath(0), 0, testLogger())
		require.NoError(t, err)
		require.NoError(t, db.Close())
		before := dirNames(t, root)
		_, _, err = openHotDataset(testLogger(), hotQueryOptions{HotRoot: root})
		require.ErrorContains(t, err, "holds no committed ledger")
		assert.Equal(t, before, dirNames(t, root))
	})
	t.Run("another chunk's database", func(t *testing.T) {
		root := ingestHotChunk(t)
		layout := geometry.NewLayout(root)
		require.NoError(t, os.Rename(layout.HotChunkPath(0), layout.HotChunkPath(1)))
		before := dirNames(t, root)
		_, _, err := openHotDataset(testLogger(), hotQueryOptions{HotRoot: root, Chunk: 1})
		require.ErrorContains(t, err, "open hot chunk "+chunk.ID(1).String())
		assert.Equal(t, before, dirNames(t, root))
	})
}
