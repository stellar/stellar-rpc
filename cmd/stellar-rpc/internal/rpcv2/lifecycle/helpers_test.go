package lifecycle

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// This file provides the shared test scaffolding the lifecycle tests need. The
// catalog/fixture helpers are copied verbatim from the root rpcv2 package's
// helpers_test.go (which still serves the root tests). The hot-tier helpers
// (openHotDBForChunk / openLiveHotDB) create the SAME on-disk "ready" hot DBs the
// real daemon does, so the lifecycle tick's freeze reads the genuine hot DBs by
// path (the way production does after #22).

// testCPI is the tx-hash index width tests build layouts with; equals the
// production constant so on-disk geometry reads back identically.
const testCPI = geometry.ChunksPerTxhashIndex

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// newTestCatalog builds a Catalog over a real KV store on temp dirs with
// cpi-wide tx-hash indexes; returns the catalog (closed via t.Cleanup) and
// artifact root.
func newTestCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, string) {
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

// testCatalog builds a catalog with the default (wide) tx-hash index, returning it
// and the artifact root.
func testCatalog(t *testing.T) (*catalog.Catalog, string) {
	t.Helper()
	cat, root := newTestCatalog(t, testCPI)
	return cat, root
}

// smallTxHashIndexCatalog builds a test catalog whose indexes are cpi chunks
// wide, so a "terminal" (full-index) build needs only a few chunks. Returns the
// catalog and the artifact root.
func smallTxHashIndexCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, string) {
	t.Helper()
	cat, root := newTestCatalog(t, cpi)
	return cat, root
}

// freezeKinds flips the given per-chunk kinds to "frozen" via the one-write protocol.
func freezeKinds(t *testing.T, cat *catalog.Catalog, chunkID chunk.ID, kinds ...geometry.Kind) {
	t.Helper()
	require.NoError(t, cat.MarkChunkFreezing(chunkID, kinds...))
	require.NoError(t, cat.FlipChunkFrozen(chunkID, kinds...))
}

// freezeCoverage marks and commits a frozen index coverage [lo, hi] for index w.
func freezeCoverage(t *testing.T, cat *catalog.Catalog, w geometry.TxHashIndexID, lo, hi chunk.ID) {
	t.Helper()
	cov, err := cat.MarkTxHashIndexFreezing(w, lo, hi)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(cov))
}

// ---------------------------------------------------------------------------
// Hot-tier test scaffolding: a test-local equivalent of the root package's hot
// DB opener (hotloop.go's openHotDBForChunk). It uses only the public
// hotchunk/catalog APIs the production code uses, so a lifecycle test creates the
// SAME on-disk "ready" hot DB the real daemon would — which the freeze then
// opens by Layout path, exactly as production does.
// ---------------------------------------------------------------------------

// openHotDBForChunk creates a "ready" shared hot DB for chunkID under the
// hot:chunk bracket (transient -> create -> ready) and returns an open handle the
// caller owns. The test equivalent of the production opener, trimmed to the
// create branch the lifecycle tests need (no crash-recovery / fsync — those edges
// are covered by the root hotloop_test.go opener tests).
func openHotDBForChunk(cat *catalog.Catalog, chunkID chunk.ID, logger *supportlog.Entry) (*hotchunk.DB, error) {
	dir := cat.Layout().HotChunkPath(chunkID)
	if err := os.RemoveAll(dir); err != nil {
		return nil, fmt.Errorf("wipe leftover hot dir %s: %w", dir, err)
	}
	if err := cat.PutHotTransient(chunkID); err != nil {
		return nil, fmt.Errorf("mark hot transient chunk %s: %w", chunkID, err)
	}
	db, err := hotchunk.Open(dir, chunkID, logger)
	if err != nil {
		return nil, fmt.Errorf("create hot DB chunk %s: %w", chunkID, err)
	}
	if err := cat.FlipHotReady(chunkID); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("flip hot ready chunk %s: %w", chunkID, err)
	}
	return db, nil
}

// openLiveHotDB opens (and brackets ready) the live hot DB for a chunk via the
// test opener, returning the handle.
func openLiveHotDB(t *testing.T, cat *catalog.Catalog, c chunk.ID) *hotchunk.DB {
	t.Helper()
	db, err := openHotDBForChunk(cat, c, silentLogger())
	require.NoError(t, err)
	return db
}
