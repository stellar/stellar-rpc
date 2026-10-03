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

// openColdDataset fails on a chunk with no ledger pack, and leaves the input
// tree as it found it. A chunk with no events store fails the open only when
// --types includes events.
func TestOpenColdDatasetErrors(t *testing.T) {
	t.Run("no ledger pack", func(t *testing.T) {
		root := t.TempDir()
		touchFiles(t, geometry.NewLayout(root).LedgerPackPath(0))
		before := treeEntries(t, root)
		_, _, err := openColdDataset(testLogger(), coldQueryOptions{ColdRoot: root, NumChunks: 2})
		require.ErrorContains(t, err, "chunk "+chunk.ID(1).String()+" has no ledger pack")
		assert.Equal(t, before, treeEntries(t, root))
	})
	t.Run("no events store", func(t *testing.T) {
		root := ingestColdChunk(t)
		for _, p := range geometry.NewLayout(root).EventsPaths(0) {
			require.NoError(t, os.Remove(p))
		}
		open := func(queryType string) error {
			_, release, err := openColdDataset(testLogger(), coldQueryOptions{
				ColdRoot: root, NumChunks: 1, Plan: queryPlan{Types: []string{queryType}},
			})
			if err == nil {
				release()
			}
			return err
		}
		require.ErrorContains(t, open(queryTypeEvents), "chunk "+chunk.ID(0).String()+" has no servable events store")
		require.NoError(t, open(queryTypeLedgers))
	})
}

// openHotDataset fails on an empty database and another chunk's database, and
// adds nothing to the input tree's root.
func TestOpenHotDatasetErrors(t *testing.T) {
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
