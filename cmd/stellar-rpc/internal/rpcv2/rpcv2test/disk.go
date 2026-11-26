package rpcv2test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// RequireDiskMatchesCatalog checks INV-3: every path under the layout's roots is one a key of cat names.
func RequireDiskMatchesCatalog(t testing.TB, cat *catalog.Catalog) {
	t.Helper()
	layout := cat.Layout()
	files, dirs := namedPaths(t, cat)
	_, scratch := layout.ChunkPaths(0, geometry.KindEvents)
	scratchRoot := filepath.Dir(scratch[0])
	var unnamed, missing []string
	for _, root := range []string{
		layout.HotRoot(), layout.LedgersRoot(), layout.EventsRoot(), layout.EventsIndexRoot(),
		layout.TxHashRawRoot(), layout.TxHashIndexRoot(),
	} {
		require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			switch {
			case path == root && errors.Is(err, fs.ErrNotExist):
				return nil
			case err != nil:
				return err
			case !d.IsDir():
				if _, ok := files[path]; !ok {
					unnamed = append(unnamed, path)
				}
			case dirs[path] || path == layout.CatalogPath():
				return fs.SkipDir
			case filepath.Dir(path) == scratchRoot:
				unnamed = append(unnamed, path)
				return fs.SkipDir
			}
			return nil
		}))
	}
	for p, frozen := range files {
		if _, err := os.Stat(p); frozen && err != nil {
			missing = append(missing, p)
		}
	}
	require.Empty(t, unnamed, "INV-3: on disk, but no catalog key names it")
	require.Empty(t, missing, "INV-3: a frozen key's file is missing")
}

// namedPaths are the files cat's keys name, each true if its key is frozen, and the dirs whose insides are owned.
func namedPaths(t testing.TB, cat *catalog.Catalog) (map[string]bool, map[string]bool) {
	t.Helper()
	layout := cat.Layout()
	files, dirs := map[string]bool{}, map[string]bool{}
	refs, err := cat.ChunkArtifactKeys()
	require.NoError(t, err)
	for _, ref := range refs {
		artifacts, scratch := layout.ChunkPaths(ref.Chunk, ref.Kind)
		for _, p := range artifacts {
			files[p] = ref.State == geometry.StateFrozen
		}
		if ref.State == geometry.StateFreezing {
			for _, p := range scratch {
				dirs[p] = true
			}
		}
	}
	covs, err := cat.AllTxHashIndexKeys()
	require.NoError(t, err)
	for _, cov := range covs {
		files[layout.TxHashIndexFilePath(cov)] = cov.State == geometry.StateFrozen
	}
	hot, err := cat.HotChunkKeys()
	require.NoError(t, err)
	for _, c := range hot {
		dirs[layout.HotChunkPath(c)] = true
	}
	return files, dirs
}
