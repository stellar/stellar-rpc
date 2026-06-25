package streaming

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

const testCPI = 1000 // chunks_per_txhash_index for tests (the default)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// newStreamingTestCatalog builds a catalog.Catalog over a real metastore.Store
// on a temp dir plus a temp artifact dir (the Layout root), with cpi-wide
// tx-hash indexes. It returns the catalog, the open store (so tests can seed raw
// keys the catalog has no public setter for), and the artifact root. The store
// is closed on cleanup.
func newStreamingTestCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, *metastore.Store, string) {
	t.Helper()
	metaDir := t.TempDir()
	artifactRoot := t.TempDir()

	store, err := metastore.New(filepath.Join(metaDir, "rocksdb"), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	idxLayout, err := geometry.NewTxHashIndexLayout(cpi)
	require.NoError(t, err)

	return catalog.NewCatalog(store, geometry.NewLayout(artifactRoot), idxLayout), store, artifactRoot
}

// testCatalog builds a catalog with the default (wide) tx-hash index. Returns
// the catalog and the artifact root so tests can assert against real files.
func testCatalog(t *testing.T) (*catalog.Catalog, string) {
	t.Helper()
	cat, _, root := newStreamingTestCatalog(t, testCPI)
	return cat, root
}

// smallTxHashIndexCatalog builds a test catalog whose indexes are cpi chunks
// wide, so a "terminal" (full-index) build needs only a few chunks. Returns the
// catalog and the artifact root.
func smallTxHashIndexCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, string) {
	t.Helper()
	cat, _, root := newStreamingTestCatalog(t, cpi)
	return cat, root
}
