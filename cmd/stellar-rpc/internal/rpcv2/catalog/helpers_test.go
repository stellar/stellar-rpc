package catalog

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// testCatalog builds a Catalog over a real KV store on a temp dir plus a temp
// artifact dir (the Layout root). Returns the catalog and the artifact root so
// tests can assert against real files on disk.
func testCatalog(t *testing.T) (*Catalog, string) {
	t.Helper()
	metaDir := t.TempDir()
	artifactRoot := t.TempDir()

	cat, err := Open(filepath.Join(metaDir, "rocksdb"), geometry.NewLayout(artifactRoot), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })

	return cat, artifactRoot
}

// writeArtifact materializes a placeholder file at path (creating parents) so a
// sweep has something real to unlink.
func writeArtifact(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("artifact"), 0o644))
}

// assertEveryFileHasKey walks every artifact file under root and asserts a
// non-empty meta-store key exists for it (Design invariant: "every key
// precedes its file"). This is INV-3's disk->meta direction.
func assertEveryFileHasKey(t *testing.T, cat *Catalog, root string) {
	t.Helper()
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		require.NoError(t, err)
		if info.IsDir() {
			return nil
		}
		key, present := keyForArtifactFile(t, cat, path)
		require.True(t, present, "file %q has no resolvable meta key", path)
		ok, err := cat.has(key)
		require.NoError(t, err)
		require.True(t, ok, "file %q on disk but key %q absent", path, key)
		return nil
	})
}

// keyForArtifactFile maps an on-disk artifact path back to its meta-store key
// by inverting the Layout bijection. Returns present=false for paths outside
// the artifact tree (e.g. the meta rocksdb dir, which lives elsewhere here).
func keyForArtifactFile(t *testing.T, cat *Catalog, path string) (string, bool) {
	t.Helper()

	// Index file: {lo}-{hi}.idx, under the cold index root once terminal and
	// under the hot root before; lo names the window either way.
	base := filepath.Base(path)
	if filepath.Ext(base) == ".idx" {
		name := strings.TrimSuffix(base, ".idx")
		loStr, hiStr, found := strings.Cut(name, "-")
		require.True(t, found, "bad idx name %q", base)
		lo, errLo := geometry.ParsePadded(loStr)
		require.NoError(t, errLo)
		hi, errHi := geometry.ParsePadded(hiStr)
		require.NoError(t, errHi)
		cov := geometry.TxHashIndexCoverage{
			Index: cat.TxHashIndexLayout().TxHashIndexID(chunk.ID(lo)), Lo: chunk.ID(lo), Hi: chunk.ID(hi),
		}
		require.Equal(t, cat.layout.TxHashIndexFilePath(cov), path, "an .idx outside its coverage's directory")
		return geometry.TxHashIndexKey(cov.Index, cov.Lo, cov.Hi), true
	}

	// Per-chunk files: identify by reconstructing each kind's path for the
	// chunk id embedded in the filename (the leading 8-digit stem, before any
	// "-events"/".pack"/".bin" suffix).
	stem, _, _ := strings.Cut(base, ".")
	stem, _, _ = strings.Cut(stem, "-")
	cid, errC := geometry.ParsePadded(stem)
	require.NoError(t, errC)
	c := chunk.ID(cid)
	for _, kind := range geometry.AllKinds() {
		if slices.Contains(cat.layout.ArtifactPaths(c, kind), path) {
			return geometry.ChunkKey(c, kind), true
		}
	}
	return "", false
}
