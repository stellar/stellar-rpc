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
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// This file provides the shared test scaffolding the lifecycle tests need. The
// catalog/fixture helpers are copied verbatim from the root fullhistory package's
// helpers_test.go (which still serves the root tests). The hot-tier helpers
// (openHotDBForChunk / openLiveHotDB) create the SAME on-disk "ready" hot DBs the
// real daemon does, so the lifecycle tick freezes and the watermark refinement
// read the genuine hot DBs by path (the way production does after #22).

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

// newTestCatalog builds a Catalog over a real metastore on temp dirs with
// cpi-wide tx-hash indexes; returns the catalog and artifact root (the store is
// closed via t.Cleanup).
func newTestCatalog(t *testing.T, cpi uint32) (*catalog.Catalog, string) {
	t.Helper()
	metaDir := t.TempDir()
	artifactRoot := t.TempDir()

	store, err := metastore.New(filepath.Join(metaDir, "rocksdb"), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	idxLayout, err := geometry.NewTxHashIndexLayout(cpi)
	require.NoError(t, err)

	return catalog.NewCatalog(store, geometry.NewLayout(artifactRoot), idxLayout), artifactRoot
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

// zeroTxLCMBytes builds wire bytes of a minimal valid zero-tx V2 LedgerCloseMeta;
// zero-tx keeps a full 10k-ledger chunk pass cheap.
func zeroTxLCMBytes(t *testing.T, seq uint32) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(0)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: nil},
			},
			TxProcessing: nil,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// ---------------------------------------------------------------------------
// Hot-tier test scaffolding: a test-local equivalent of the root package's hot
// DB opener (startup.go's openHotDBForChunk). It uses only the public
// hotchunk/catalog APIs the production code uses, so a lifecycle test creates the
// SAME on-disk "ready" hot DB the real daemon would — which the freeze and the
// watermark refinement then open by Layout path, exactly as production does.
// ---------------------------------------------------------------------------

// openHotDBForChunk creates a "ready" shared hot DB for chunkID under the
// hot:chunk bracket (transient -> create -> ready) and returns an open handle the
// caller owns. The test equivalent of the production opener, trimmed to the
// create branch the lifecycle tests need (no crash-recovery / fsync — those edges
// are covered by the root ingest_test.go opener tests).
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
