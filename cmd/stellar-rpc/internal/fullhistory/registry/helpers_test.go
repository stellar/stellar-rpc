package registry

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// newTestCatalog builds a Catalog over a real KV store on temp dirs with the
// production tx-hash index width; closed via t.Cleanup.
func newTestCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	idxLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	cat, err := catalog.Open(
		filepath.Join(t.TempDir(), "rocksdb"), geometry.NewLayout(t.TempDir()), idxLayout, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

// openHotDB creates (or reopens) a real hot chunk DB at c's layout path and
// returns the open write handle. The caller (or the registry it hands the
// handle to) owns the close.
func openHotDB(t *testing.T, layout geometry.Layout, c chunk.ID) *hotchunk.DB {
	t.Helper()
	path := layout.HotChunkPath(c)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	db, err := hotchunk.Open(path, c, silentLogger())
	require.NoError(t, err)
	return db
}

// ingestZeroTxLedgers commits n zero-transaction ledgers into db starting at
// the chunk's first ledger.
func ingestZeroTxLedgers(t *testing.T, db *hotchunk.DB, c chunk.ID, n int) {
	t.Helper()
	first := c.FirstLedger()
	for i := range n {
		seq := first + uint32(i) //nolint:gosec // small test counts
		_, err := db.IngestLedger(seq, xdr.LedgerCloseMetaView(fhtest.ZeroTxLCMBytes(t, seq)))
		require.NoError(t, err)
	}
}

// createReadyHotChunk creates a real hot DB for chunk c holding n committed
// ledgers, closes it, and flips its catalog key "ready" — the durable state
// BuildFromCatalog reopens from.
func createReadyHotChunk(t *testing.T, cat *catalog.Catalog, c chunk.ID, n int) {
	t.Helper()
	db := openHotDB(t, cat.Layout(), c)
	ingestZeroTxLedgers(t, db, c, n)
	require.NoError(t, db.Close())
	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.FlipHotReady(c))
}

// writeLedgerPack writes a real cold ledger pack for chunk c holding n
// zero-transaction ledgers from its first ledger. (A test pack need not span
// the whole chunk; its coverage is [FirstLedger, FirstLedger+n-1].)
func writeLedgerPack(t *testing.T, layout geometry.Layout, c chunk.ID, n int) {
	t.Helper()
	path := layout.LedgerPackPath(c)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	w, err := ledger.NewColdWriter(path, c.FirstLedger(), ledger.ColdWriterOptions{})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	for i := range n {
		seq := c.FirstLedger() + uint32(i) //nolint:gosec // small test counts
		require.NoError(t, w.AppendLedger(seq, fhtest.ZeroTxLCMBytes(t, seq)))
	}
	require.NoError(t, w.Commit())
}

// buildIdxFile builds a real (empty) cold tx-hash index file at cov's layout
// path, spanning cov's chunk range.
func buildIdxFile(t *testing.T, layout geometry.Layout, cov geometry.TxHashIndexCoverage) {
	t.Helper()
	path := layout.TxHashIndexFilePath(cov)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, txhash.BuildColdIndex(t.Context(), nil, path, cov.Lo.FirstLedger(), cov.Hi.LastLedger()))
}

// frozenCoverage is a hand-built frozen coverage for direct SwapTxIndex calls
// (no catalog involved).
func frozenCoverage(w geometry.TxHashIndexID, lo, hi chunk.ID) geometry.TxHashIndexCoverage {
	return geometry.TxHashIndexCoverage{Index: w, Lo: lo, Hi: hi, State: geometry.StateFrozen}
}

// freezeKinds flips the given per-chunk kinds to "frozen" via the one-write
// protocol.
func freezeKinds(t *testing.T, cat *catalog.Catalog, chunkID chunk.ID, kinds ...geometry.Kind) {
	t.Helper()
	require.NoError(t, cat.MarkChunkFreezing(chunkID, kinds...))
	require.NoError(t, cat.FlipChunkFrozen(chunkID, kinds...))
}

// freezeCoverage marks and commits a frozen index coverage [lo, hi] for
// window w, returning the coverage.
func freezeCoverage(
	t *testing.T, cat *catalog.Catalog, w geometry.TxHashIndexID, lo, hi chunk.ID,
) geometry.TxHashIndexCoverage {
	t.Helper()
	cov, err := cat.MarkTxHashIndexFreezing(w, lo, hi)
	require.NoError(t, err)
	require.NoError(t, cat.CommitTxHashIndex(cov))
	return cov
}

// newTestRegistry assembles a bare registry over a fresh layout (no catalog),
// closed via t.Cleanup.
func newTestRegistry(t *testing.T, opts Options) *Registry {
	t.Helper()
	r := newRegistry(geometry.NewLayout(t.TempDir()), silentLogger(), opts)
	t.Cleanup(r.Close)
	return r
}
