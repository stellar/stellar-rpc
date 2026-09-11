package backfill

import (
	"testing"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
)

const testCPI = 1000 // chunks_per_txhash_index for tests (the default)

func silentLogger() *supportlog.Entry {
	return rpcv2test.SilentLogger()
}

// testCatalog builds a catalog with the default (wide) tx-hash index. Returns
// the catalog and the artifact root so tests can assert against real files.
func testCatalog(t *testing.T) (*catalog.Catalog, string) {
	t.Helper()
	return rpcv2test.OpenTestCatalog(t, testCPI)
}

// smallTxHashIndexCatalog builds a test catalog whose indexes are cpi chunks
// wide, so a "terminal" (full-index) build needs only a few chunks. Returns the
// catalog and the artifact root.
func smallTxHashIndexCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, string) {
	t.Helper()
	return rpcv2test.OpenTestCatalog(t, cpi)
}
