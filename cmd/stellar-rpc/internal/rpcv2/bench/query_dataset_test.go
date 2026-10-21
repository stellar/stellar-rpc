package bench

import (
	"context"
	"os"
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

// dirNames returns the names of dir's entries.
func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

// testCatalog opens an empty dataset catalog under a new root.
func testCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	cat, err := openDatasetCatalog(geometry.NewLayout(t.TempDir()), testLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

// freezeKeys marks kinds of chunk c frozen in cat, with no files behind them.
func freezeKeys(t *testing.T, cat *catalog.Catalog, c chunk.ID, kinds ...geometry.Kind) {
	t.Helper()
	require.NoError(t, cat.MarkChunkFreezing(c, kinds...))
	require.NoError(t, cat.FlipChunkFrozen(c, kinds...))
}

// markHotReady marks chunk c's hot key ready in cat, with no database behind it.
func markHotReady(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	require.NoError(t, cat.PutHotTransient(c))
	require.NoError(t, cat.FlipHotReady(c))
}

// The tier comes from the store that serves each chunk; a chunk with no store,
// one served both ways, and a range that mixes tiers are errors.
func TestServedTier(t *testing.T) {
	cat := testCatalog(t)
	freezeKeys(t, cat, 0, geometry.KindLedgers)
	freezeKeys(t, cat, 1, geometry.KindLedgers)
	markHotReady(t, cat, 2)
	markHotReady(t, cat, 3)
	freezeKeys(t, cat, 4, geometry.KindLedgers)
	markHotReady(t, cat, 4)

	for _, tc := range []struct {
		name       string
		start, end chunk.ID
		want, err  string
	}{
		{"frozen chunks", 0, 1, tierCold, ""},
		{"hot chunks", 2, 3, tierHot, ""},
		{"mixed", 1, 2, "", "mix cold and hot chunks (chunk " + chunk.ID(2).String() + " is hot)"},
		{"both", 4, 4, "", "chunk " + chunk.ID(4).String() + " is both frozen and hot"},
		{"no store", 5, 5, "", "chunk " + chunk.ID(5).String() + " has no ledger store"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tier, err := servedTier(cat, chunkRange(tc.start, tc.end))
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, tier)
		})
	}
}

// A catalog with no ingested dataset fails to open, and so does a
// --start-chunk below the dataset's first chunk.
func TestOpenDatasetErrors(t *testing.T) {
	t.Run("no ingested dataset", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, os.MkdirAll(geometry.NewLayout(root).CatalogPath(), 0o755))
		_, _, err := openDataset(testLogger(), queryOptions{DataDir: root})
		require.ErrorContains(t, err, "holds no ingested dataset: "+unfinishedHint)
	})
	t.Run("--start-chunk below the dataset", func(t *testing.T) {
		root := t.TempDir()
		cat, err := openDatasetCatalog(geometry.NewLayout(root), testLogger())
		require.NoError(t, err)
		require.NoError(t, cat.PinEarliestLedger(chunk.ID(1).FirstLedger()))
		require.NoError(t, cat.Close())
		_, _, err = openDataset(testLogger(), queryOptions{DataDir: root, HasStartChunk: true})
		require.ErrorContains(t, err, "--start-chunk=0 is below the dataset's first chunk 1")
	})
}

// --start-chunk and --sample-ledgers bound the served range of a cold dataset:
// the latest ledger is the range's last, and the handlers refuse a ledger
// below its first.
func TestOpenDatasetBounds(t *testing.T) {
	srcRoot := t.TempDir()
	packDir, _ := writeSourcePack(t, srcRoot, 0, chunk.LedgersPerChunk)
	_, _ = writeSourcePack(t, srcRoot, 1, chunk.LedgersPerChunk)
	coldRoot := t.TempDir()
	require.NoError(t, runCold(context.Background(), testLogger(), coldOptions{
		Source: sourceConfig{Kind: sourcePack, PackDir: packDir}, StartChunk: 0, NumChunks: 2, Workers: 2,
		ColdRoot: coldRoot, OutDir: filepath.Join(t.TempDir(), "csv"),
	}))

	plan := testPlan(protocol.FormatBase64)
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: coldRoot, StartChunk: 1, HasStartChunk: true, SampleLedgers: 5, Plan: plan,
	})
	require.NoError(t, err)
	defer release()
	assert.Equal(t, tierCold, ds.Tier)
	assert.Equal(t, []chunk.ID{1}, ds.Chunks)
	assert.Equal(t, chunk.ID(1).FirstLedger(), ds.FirstLedger)
	assert.Equal(t, chunk.ID(1).FirstLedger()+4, ds.LastLedger)

	var latest protocol.GetLatestLedgerResponse
	mustCallResult(t, ds.server, protocol.GetLatestLedgerMethodName, nil, &latest)
	assert.Equal(t, ds.LastLedger, latest.Sequence)

	_, err = requestItems(ds, plan, ledgersRequest, chunk.ID(0).LastLedger(), chunk.ID(0).LastLedger())
	require.ErrorContains(t, err, "must be between the oldest ledger: "+
		strconv.FormatUint(uint64(chunk.ID(1).FirstLedger()), 10), "a ledger below --start-chunk is outside the served range")
}

// A dataset without its live chunk is refused, so bench query creates nothing
// in it.
func TestOpenDatasetRequiresLiveChunk(t *testing.T) {
	coldRoot := ingestColdChunk(t)
	cat := reopenCatalog(t, coldRoot)
	require.NoError(t, cat.PutHotTransient(1))
	require.NoError(t, cat.DestroyHotChunk(1))
	require.NoError(t, cat.Close())
	hotRoot := geometry.NewLayout(coldRoot).HotRoot()
	before := dirNames(t, hotRoot)

	_, _, err := openDataset(testLogger(), queryOptions{DataDir: coldRoot, Plan: testPlan(protocol.FormatBase64)})
	require.ErrorContains(t, err, "the dataset has no live chunk "+chunk.ID(1).String()+": "+unfinishedHint)
	assert.Equal(t, before, dirNames(t, hotRoot))
}

// verifyServes checks the events store only when the types include events.
func TestVerifyServesChecksEventsStore(t *testing.T) {
	coldRoot := ingestColdChunk(t)
	cat := reopenCatalog(t, coldRoot)
	require.NoError(t, cat.DemoteChunkArtifacts([]catalog.ArtifactRef{
		{Chunk: 0, Kind: geometry.KindEvents, State: geometry.StateFrozen},
	}))
	require.NoError(t, cat.Close())

	plan := testPlan(protocol.FormatBase64)
	plan.Types = []string{queryTypeLedgers}
	ds, release, err := openDataset(testLogger(), queryOptions{DataDir: coldRoot, Plan: plan})
	require.NoError(t, err)
	defer release()
	require.ErrorContains(t, ds.verifyServes([]string{queryTypeEvents}),
		"chunk "+chunk.ID(0).String()+" has no servable events store")
}
