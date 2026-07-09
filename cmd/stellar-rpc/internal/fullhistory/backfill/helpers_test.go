package backfill

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
)

const testCPI = 1000 // chunks_per_txhash_index for tests (the default)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// newStreamingTestCatalog builds a catalog.Catalog over a real KV store on a
// temp dir plus a temp artifact dir (the Layout root), with cpi-wide tx-hash
// indexes. It returns the catalog (closed on cleanup) and the artifact root.
func newStreamingTestCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, string) {
	t.Helper()
	metaDir := t.TempDir()
	artifactRoot := t.TempDir()

	idxLayout, err := geometry.NewTxHashIndexLayout(cpi)
	require.NoError(t, err)

	cat, err := catalog.Open(
		filepath.Join(metaDir, "rocksdb"), geometry.NewLayout(artifactRoot), idxLayout, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })

	return cat, artifactRoot
}

// testCatalog builds a catalog with the default (wide) tx-hash index. Returns
// the catalog and the artifact root so tests can assert against real files.
func testCatalog(t *testing.T) (*catalog.Catalog, string) {
	t.Helper()
	return newStreamingTestCatalog(t, testCPI)
}

// smallTxHashIndexCatalog builds a test catalog whose indexes are cpi chunks
// wide, so a "terminal" (full-index) build needs only a few chunks. Returns the
// catalog and the artifact root.
func smallTxHashIndexCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, string) {
	t.Helper()
	return newStreamingTestCatalog(t, cpi)
}
