package bench

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// ingestCold runs bench ingest cold over one chunk of the source packs under
// packDir into coldRoot and returns the run settings it recorded.
func ingestCold(t *testing.T, packDir, coldRoot string, c chunk.ID) (map[string]string, error) {
	t.Helper()
	settings := map[string]string{}
	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: c,
		NumChunks:  1,
		Workers:    1,
		ColdRoot:   coldRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
		Settings:   settings,
	})
	return settings, err
}

// reopenCatalog opens the dataset catalog under root for a test to inspect.
func reopenCatalog(t *testing.T, root string) *catalog.Catalog {
	t.Helper()
	cat, err := openDatasetCatalog(geometry.NewLayout(root), testLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

// A cold ingest keeps its catalog with the pinned earliest ledger, the frozen
// chunk and an empty live chunk after it; an appending ingest replaces the
// live chunk and extends the tx-hash index over both chunks.
func TestColdIngestKeepsAndAppendsCatalog(t *testing.T) {
	srcRoot := t.TempDir()
	packDir, _ := writeSourcePack(t, srcRoot, 0, chunk.LedgersPerChunk)
	_, _ = writeSourcePack(t, srcRoot, 1, chunk.LedgersPerChunk)
	coldRoot := t.TempDir()
	layout := geometry.NewLayout(coldRoot)

	settings, err := ingestCold(t, packDir, coldRoot, 0)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"datasetFirstChunk": "0", "append": "false"}, settings)
	require.DirExists(t, layout.HotChunkPath(1), "the live chunk")
	settings, err = ingestCold(t, packDir, coldRoot, 1)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"datasetFirstChunk": "0", "append": "true"}, settings)
	require.NoDirExists(t, layout.HotChunkPath(1), "the append discards the old live chunk")
	require.DirExists(t, layout.HotChunkPath(2), "the new live chunk")

	cat := reopenCatalog(t, coldRoot)
	earliest, pinned, err := cat.EarliestLedger()
	require.NoError(t, err)
	assert.True(t, pinned)
	assert.Equal(t, chunk.ID(0).FirstLedger(), earliest)
	for _, c := range []chunk.ID{0, 1} {
		for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
			state, err := cat.State(c, kind)
			require.NoError(t, err)
			assert.Equal(t, geometry.StateFrozen, state, "chunk %s %s", c, kind)
		}
	}
	hot, err := cat.HotChunkKeys()
	require.NoError(t, err)
	assert.Equal(t, []chunk.ID{2}, hot)
	covered, err := cat.FrozenIndexCoversRange(cat.TxHashIndexLayout().TxHashIndexID(0), 0, 1)
	require.NoError(t, err)
	assert.True(t, covered, "one frozen tx-hash index covers both chunks")
}

// A cold append whose source fails to open leaves the dataset's live chunk in
// place, so bench query still opens the dataset.
func TestColdAppendWithFailingSourceKeepsLiveChunk(t *testing.T) {
	coldRoot := ingestColdChunk(t)
	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourceBSB, BucketPath: "unused", DatastoreType: "unknown"},
		StartChunk: 1, NumChunks: 1, Workers: 1,
		ColdRoot: coldRoot, OutDir: filepath.Join(t.TempDir(), "csv"),
	})
	require.ErrorContains(t, err, "open BSB backend")

	cat := reopenCatalog(t, coldRoot)
	state, err := cat.HotState(1)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotReady, state)
	require.NoError(t, cat.Close())
	_, release, err := openDataset(testLogger(), queryOptions{DataDir: coldRoot, Plan: testPlan(protocol.FormatBase64)})
	require.NoError(t, err)
	release()
}

// Cold appends leave only the latest tx-hash index coverage, in the catalog
// and on disk.
func TestColdAppendsDestroySupersededTxHashIndex(t *testing.T) {
	srcRoot := t.TempDir()
	packDir, _ := writeSourcePack(t, srcRoot, 0, chunk.LedgersPerChunk)
	coldRoot := t.TempDir()
	for c := range chunk.ID(3) {
		if c > 0 {
			_, _ = writeSourcePack(t, srcRoot, c, chunk.LedgersPerChunk)
		}
		_, err := ingestCold(t, packDir, coldRoot, c)
		require.NoError(t, err)
	}

	cat := reopenCatalog(t, coldRoot)
	covs, err := cat.AllTxHashIndexKeys()
	require.NoError(t, err)
	require.Len(t, covs, 1)
	assert.Equal(t, geometry.StateFrozen, covs[0].State)
	assert.Equal(t, chunk.ID(0), covs[0].Lo)
	assert.Equal(t, chunk.ID(2), covs[0].Hi)
	layout := geometry.NewLayout(coldRoot)
	assert.Equal(t, []string{filepath.Base(layout.TxHashIndexFilePath(covs[0]))},
		dirNames(t, layout.TxHashIndexDir(covs[0].Index)))
}

// A hot ingest keeps its catalog with the pinned earliest ledger, and an
// append that would skip ledgers is refused.
func TestHotIngestKeepsCatalog(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	cat := reopenCatalog(t, hotRoot)
	earliest, pinned, err := cat.EarliestLedger()
	require.NoError(t, err)
	assert.True(t, pinned)
	assert.Equal(t, chunk.ID(0).FirstLedger(), earliest)
	tier, err := datasetTier(cat)
	require.NoError(t, err)
	assert.Equal(t, tierHot, tier)
	require.NoError(t, cat.Close())

	packDir, _ := writeSourcePack(t, t.TempDir(), 1, 10)
	err = runHot(context.Background(), testLogger(), hotOptions{
		Source: sourceConfig{Kind: sourcePack, PackDir: packDir}, StartChunk: 1, NumChunks: 1, NumLedgers: 10,
		HotRoot: hotRoot, OutDir: filepath.Join(t.TempDir(), "csv"),
	})
	require.ErrorContains(t, err, "would leave a gap; it ends mid-chunk, so no chunk can follow it")
}

// prepareIngest pins a new dataset and refuses an ingest that overlaps the
// dataset, skips ledgers, starts below the pin, targets the other tier or
// meets a leftover artifact.
func TestPrepareIngest(t *testing.T) {
	// coldDataset returns a catalog holding frozen chunk 0, pinned at its start.
	coldDataset := func(t *testing.T) *catalog.Catalog {
		cat := testCatalog(t)
		require.NoError(t, cat.PinEarliestLedger(chunk.ID(0).FirstLedger()))
		freezeKeys(t, cat, 0, geometry.KindLedgers, geometry.KindEvents, geometry.KindTxHash)
		return cat
	}

	t.Run("new dataset", func(t *testing.T) {
		cat := testCatalog(t)
		first, err := prepareIngest(cat, tierCold, 3, 4)
		require.NoError(t, err)
		assert.Equal(t, chunk.ID(3), first)
		_, pinned, err := cat.EarliestLedger()
		require.NoError(t, err)
		assert.False(t, pinned, "only a finished ingest pins the dataset")
	})
	t.Run("append", func(t *testing.T) {
		first, err := prepareIngest(coldDataset(t), tierCold, 1, 1)
		require.NoError(t, err)
		assert.Equal(t, chunk.ID(0), first)
	})
	for _, tc := range []struct {
		name       string
		tier       string
		start, end chunk.ID
		want       string
	}{
		{
			"overlap", tierCold, 0, 0,
			"already holds ledgers up to " + strconv.FormatUint(uint64(chunk.ID(0).LastLedger()), 10),
		},
		{"gap", tierCold, 2, 2, "would leave a gap; ingest from chunk " + chunk.ID(1).String()},
		{"other tier", tierHot, 1, 1, "the dataset holds cold chunks; a hot ingest needs its own directory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := prepareIngest(coldDataset(t), tc.tier, tc.start, tc.end)
			require.ErrorContains(t, err, tc.want)
		})
	}
	t.Run("leftover artifact", func(t *testing.T) {
		cat := coldDataset(t)
		require.NoError(t, cat.MarkChunkFreezing(2, geometry.KindLedgers))
		_, err := prepareIngest(cat, tierCold, 1, 2)
		require.ErrorContains(t, err, "already holds a freezing ledgers artifact for chunk "+chunk.ID(2).String()+
			": "+unfinishedHint)
	})
	t.Run("below the pin", func(t *testing.T) {
		cat := testCatalog(t)
		require.NoError(t, cat.PinEarliestLedger(chunk.ID(1).FirstLedger()))
		_, err := prepareIngest(cat, tierCold, 0, 0)
		require.ErrorContains(t, err, "starts below the dataset's earliest ledger")
	})
	t.Run("chunks without a pin", func(t *testing.T) {
		cat := testCatalog(t)
		freezeKeys(t, cat, 0, geometry.KindLedgers)
		_, err := prepareIngest(cat, tierCold, 1, 1)
		require.ErrorContains(t, err, "holds chunks but no earliest ledger: "+unfinishedHint)
	})
}

// A first ingest that fails before it writes a chunk leaves the dataset
// unpinned, so the next ingest into the same directory starts it afresh.
func TestFailedFirstIngestLeavesDirectoryReusable(t *testing.T) {
	packDir, _ := writeSourcePack(t, t.TempDir(), 0, chunk.LedgersPerChunk)
	coldRoot := t.TempDir()
	// An unknown datastore type fails the source open, which runs before the
	// backfill writes any chunk key.
	err := runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourceBSB, BucketPath: "unused", DatastoreType: "unknown"},
		StartChunk: 0, NumChunks: 1, Workers: 1,
		ColdRoot: coldRoot, OutDir: filepath.Join(t.TempDir(), "csv"),
	})
	require.ErrorContains(t, err, "open BSB backend")
	cat := reopenCatalog(t, coldRoot)
	_, pinned, err := cat.EarliestLedger()
	require.NoError(t, err)
	assert.False(t, pinned)
	tier, err := datasetTier(cat)
	require.NoError(t, err)
	assert.Empty(t, tier, "no chunk key was written")
	require.NoError(t, cat.Close())

	_, err = ingestCold(t, packDir, coldRoot, 0)
	require.NoError(t, err)
}

// A hot append after a dataset that ends on a chunk's last ledger replaces the
// empty live chunk, and bench query serves both chunks hot.
func TestHotAppendAfterChunkBoundary(t *testing.T) {
	srcRoot := t.TempDir()
	packDir, _ := writeSourcePack(t, srcRoot, 0, chunk.LedgersPerChunk)
	_, _ = writeSourcePack(t, srcRoot, 1, 10)
	hotRoot := t.TempDir()
	ingest := func(c chunk.ID, n uint32) error {
		return runHot(context.Background(), testLogger(), hotOptions{
			Source: sourceConfig{Kind: sourcePack, PackDir: packDir}, StartChunk: c, NumChunks: 1, NumLedgers: n,
			HotRoot: hotRoot, OutDir: filepath.Join(t.TempDir(), "csv"),
		})
	}
	require.NoError(t, ingest(0, chunk.LedgersPerChunk))
	require.DirExists(t, geometry.NewLayout(hotRoot).HotChunkPath(1), "the empty live chunk")
	require.NoError(t, ingest(1, 10))

	ds, release, err := openDataset(testLogger(), queryOptions{DataDir: hotRoot, Plan: testPlan(protocol.FormatBase64)})
	require.NoError(t, err)
	defer release()
	assert.Equal(t, tierHot, ds.Tier)
	assert.Equal(t, []chunk.ID{0, 1}, ds.Chunks)
	assert.Equal(t, chunk.ID(1).FirstLedger()+9, ds.LastLedger)
}
