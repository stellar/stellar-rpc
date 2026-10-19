package bench

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// reopenCatalog opens the dataset catalog under root for a test to inspect.
func reopenCatalog(t *testing.T, root string) *catalog.Catalog {
	t.Helper()
	cat, err := openDatasetCatalog(geometry.NewLayout(root), testLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

// requireFinishedDataset requires the catalog under root to pin first's first
// ledger as the earliest ledger and to hold a ready live chunk at live.
func requireFinishedDataset(t *testing.T, root string, first, live chunk.ID) {
	t.Helper()
	cat := reopenCatalog(t, root)
	earliest, pinned, err := cat.EarliestLedger()
	require.NoError(t, err)
	assert.True(t, pinned)
	assert.Equal(t, first.FirstLedger(), earliest)
	state, err := cat.HotState(live)
	require.NoError(t, err)
	assert.Equal(t, geometry.HotReady, state, "live chunk %s", live)
	require.NoError(t, cat.Close())
}

// Cold and hot ingests refuse a non-empty dataset directory before they write
// anything into it or into --out.
func TestIngestRefusesNonEmptyDataset(t *testing.T) {
	packDir, _ := writeSourcePack(t, t.TempDir(), 0, 10)
	source := sourceConfig{Kind: sourcePack, PackDir: packDir}
	for _, tc := range []struct {
		name string
		run  func(root, out string) error
		flag string
	}{
		{"cold", func(root, out string) error {
			return runCold(context.Background(), testLogger(), coldOptions{
				Source: source, StartChunk: 0, NumChunks: 1, Workers: 1, ColdRoot: root, OutDir: out,
			})
		}, "--cold-out-dir"},
		{"hot", func(root, out string) error {
			return runHot(context.Background(), testLogger(), hotOptions{
				Source: source, StartChunk: 0, NumChunks: 1, NumLedgers: 10, HotRoot: root, OutDir: out,
			})
		}, "--hot-dir"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "leftover"), nil, 0o600))
			out := filepath.Join(t.TempDir(), "csv")

			err := tc.run(root, out)
			require.ErrorContains(t, err, tc.flag+" "+root+" is not empty (it holds leftover)")
			assert.Equal(t, []string{"leftover"}, dirNames(t, root))
			require.NoDirExists(t, out)
		})
	}
}

// A hot ingest whose report write fails still leaves a finished dataset that
// bench query opens.
func TestHotIngestFinishesDatasetWhenReportFails(t *testing.T) {
	const numLedgers = 10
	packDir, _ := writeSourcePack(t, t.TempDir(), 0, numLedgers)
	hotRoot := filepath.Join(t.TempDir(), "hot")
	out := filepath.Join(t.TempDir(), "csv")
	require.NoError(t, os.WriteFile(out, nil, 0o600))

	err := runHot(context.Background(), testLogger(), hotOptions{
		Source: sourceConfig{Kind: sourcePack, PackDir: packDir}, StartChunk: 0, NumChunks: 1, NumLedgers: numLedgers,
		HotRoot: hotRoot, OutDir: out,
	})
	require.ErrorContains(t, err, "mkdir "+out)

	requireFinishedDataset(t, hotRoot, 0, 0)
	ds, release, err := openDataset(testLogger(), queryOptions{DataDir: hotRoot, Plan: testPlan(protocol.FormatBase64)})
	require.NoError(t, err)
	defer release()
	assert.Equal(t, tierHot, ds.Tier)
	assert.Equal(t, chunk.ID(0).FirstLedger()+numLedgers-1, ds.LastLedger)
}
