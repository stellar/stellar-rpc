package bench

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// TestRunFreezeFromHot is the end-to-end freeze bench: fabricate a full
// chunk's source pack, populate the hot DB through the production bounded
// ingestion loop, then measure the freeze — RunBackfill with NO bulk backend,
// so the complete hot DB is the only source that can satisfy it. A second run
// with ReuseHot adopts the same hot DB without repopulating. The populate is a
// full LedgersPerChunk hot ingest (one synced WriteBatch per ledger), so this
// is one of the package's slower tests; both modes share the one populate.
func TestRunFreezeFromHot(t *testing.T) {
	chunkID := chunk.ID(0)
	packDir, txLedgers := writeSourcePack(t, t.TempDir(), chunkID, chunk.LedgersPerChunk)
	workRoot := t.TempDir()

	runOnce := func(csvDir string, reuseHot bool) map[string]map[string]int64 {
		t.Helper()
		require.NoError(t, runFreeze(context.Background(), testLogger(), freezeOptions{
			Source:   sourceConfig{Kind: sourcePack, PackDir: packDir},
			Chunk:    chunkID,
			WorkRoot: workRoot,
			ReuseHot: reuseHot,
			OutDir:   csvDir,
		}))
		return readCSV(t, filepath.Join(csvDir, "driver.csv"))
	}

	csvDir := filepath.Join(t.TempDir(), "csv")
	driver := runOnce(csvDir, false)

	// The freeze route ran end to end: the scheduler's whole-run wall, the
	// per-chunk engine total, the per-kind totals, and the window's index
	// rebuild — one sample each, exactly as the cold report shapes them.
	for _, name := range []string{
		"backfill_wall", "index_rebuild", "chunk_total", "ledgers_total", "txhash_total", "events_total",
	} {
		require.Contains(t, driver, name)
		assert.EqualValues(t, 1, driver[name]["n"], name)
	}
	// THE zero-decompression acceptance assertion: the freeze route runs NO
	// raw-ledger walk, so the shared extract row must be absent — its
	// presence would mean the freeze decompressed the chunk to feed a
	// derivation writer (the regression this bench exists to catch).
	require.NotContains(t, driver, "cold_extract")
	assert.EqualValues(t, chunk.LedgersPerChunk, driver["ledgers_total"]["n_items"])
	assert.EqualValues(t, txLedgers, driver["txhash_total"]["n_items"])

	if runtime.GOOS == linuxGOOS {
		require.Contains(t, driver, "peak_rss_bytes")
	}

	// All cold artifacts landed at the Layout-resolved paths.
	layout := geometry.NewLayout(workRoot)
	assert.FileExists(t, layout.LedgerPackPath(chunkID))
	assert.FileExists(t, layout.TxHashBinPath(chunkID))
	for _, p := range layout.EventsPaths(chunkID) {
		assert.FileExists(t, p)
	}

	// ReuseHot: a fresh scratch catalog adopts the same hot DB — no
	// repopulate — and the freeze reruns, overwriting the artifacts.
	driver2 := runOnce(filepath.Join(t.TempDir(), "csv2"), true)
	require.Contains(t, driver2, "backfill_wall")
	assert.EqualValues(t, chunk.LedgersPerChunk, driver2["ledgers_total"]["n_items"])
}

// TestRunFreezeReuseHotMissing: ReuseHot against a work dir with no hot DB
// fails up front with the pointer to populate, not deep inside the backfill.
func TestRunFreezeReuseHotMissing(t *testing.T) {
	err := runFreeze(context.Background(), testLogger(), freezeOptions{
		Chunk:    chunk.ID(0),
		WorkRoot: t.TempDir(),
		ReuseHot: true,
		OutDir:   filepath.Join(t.TempDir(), "csv"),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--reuse-hot")
}
