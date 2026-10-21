package rpcv2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// requireHotDBClosed fails unless chunk c's hot DB can be opened again, which
// RocksDB's lock refuses while another handle holds it open.
func requireHotDBClosed(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	db, err := hotchunk.OpenReadyWrite(geometry.HotReady, cat.Layout().HotChunkPath(c), c, cat.Secret(), silentLogger())
	require.NoError(t, err, "chunk %s's hot DB is still open", c)
	require.NoError(t, db.Close())
}

// OpenServing closes the live chunk's DB when OpenRegistry fails, and the
// registry with it when SeedCloseTimes fails.
func TestOpenServing_ErrorsLeaveNothingOpen(t *testing.T) {
	t.Run("open registry fails", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// A ready chunk with no DB fails the registry's open of ready handles.
		require.NoError(t, cat.PutHotTransient(0))
		require.NoError(t, cat.FlipHotReady(0))
		_, _, err := OpenServing(cat, geometry.NewRetention(0, 0), chunk.ID(0).LastLedger(), silentLogger())
		require.ErrorContains(t, err, "open registry")
		requireHotDBClosed(t, cat, 1)
	})
	t.Run("seed close times fails", func(t *testing.T) {
		cat, _ := testCatalog(t)
		// The live chunk is created empty, so the window edges have no ledger.
		_, _, err := OpenServing(cat, geometry.NewRetention(0, 0), chunk.FirstLedgerSeq+3, silentLogger())
		require.ErrorContains(t, err, "seed close times")
		requireHotDBClosed(t, cat, 0)
	})
}
