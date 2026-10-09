package bench

import (
	"context"
	"io"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// executeQuery runs bench query with args and a new --out, and requires success.
func executeQuery(t *testing.T, args ...string) {
	t.Helper()
	cmd := newQueryCommand()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append(args, "--out", filepath.Join(t.TempDir(), "out")))
	require.NoError(t, cmd.Execute())
}

// requireNoScratchCatalog fails when dir holds a bench query scratch catalog.
func requireNoScratchCatalog(t *testing.T, dir string) {
	t.Helper()
	for _, name := range dirNames(t, dir) {
		require.False(t, strings.HasPrefix(name, scratchPrefixQuery), "%s holds %s", dir, name)
	}
}

// newRequest is the constructor of one ledger-reading request type.
type newRequest func(*queryDataset, queryPlan) queryRequest

// requestItems narrows ds to [lo, hi] and runs one request that reads all of
// it.
func requestItems(ds *queryDataset, p queryPlan, newReq newRequest, lo, hi uint32) (int, error) {
	ds.FirstLedger, ds.LastLedger = lo, hi
	p.LedgersSpan, p.TxPageSpan = hi-lo+1, hi-lo+1
	timing, err := newReq(ds, p)(context.Background(), rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
	return timing.items, err
}

// A one-ledger request reads through the point read, and a ledgers request
// that reads fewer ledgers than its range holds fails.
func TestQueryRequestReads(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	plan := queryPlan{TxPageLimit: defaultTxPageLimit, Passphrase: network.PublicNetworkPassphrase}
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{HotRoot: hotRoot, Chunk: 0, Plan: plan})
	require.NoError(t, err)
	defer release()
	// The first ledger of the fixture carries one transaction.
	first, last := ds.FirstLedger, ds.LastLedger

	for _, tc := range []struct {
		qtype  string
		newReq newRequest
	}{
		{queryTypeLedgers, ledgersRequest},
		{queryTypeTxPage, txPageRequest},
	} {
		t.Run(tc.qtype+" point read", func(t *testing.T) {
			items, err := requestItems(ds, plan, tc.newReq, first, first)
			require.NoError(t, err)
			assert.Equal(t, 1, items)

			// Only the point read reports ErrNotFound; a scan past the store's
			// last ledger yields nothing.
			_, err = requestItems(ds, plan, tc.newReq, last+1, last+1)
			require.ErrorIs(t, err, stores.ErrNotFound)
		})
	}

	t.Run("missing ledgers", func(t *testing.T) {
		_, err := requestItems(ds, plan, ledgersRequest, last-4, last+5)
		require.ErrorContains(t, err, "read 5 of the 10 ledgers")
	})
}

// A successful hot run removes its scratch catalog from the dataset root.
func TestQueryHotRunRemovesScratchCatalog(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	executeQuery(t, queryTierHot, "--chunk", "0", "--hot-dir", hotRoot,
		"--types", queryTypeLedgers, "--target-rps", "1000", "--duration", "10ms")
	requireNoScratchCatalog(t, hotRoot)
}

// A cold run over two chunks reads across the chunk border, and a successful
// run removes its scratch catalog from the dataset root.
func TestQueryColdTwoChunks(t *testing.T) {
	srcRoot := t.TempDir()
	packDir, _ := writeSourcePack(t, srcRoot, 0, chunk.LedgersPerChunk)
	_, _ = writeSourcePack(t, srcRoot, 1, chunk.LedgersPerChunk)
	coldRoot := t.TempDir()
	require.NoError(t, runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: 0,
		NumChunks:  2,
		Workers:    2,
		ColdRoot:   coldRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))

	plan := queryPlan{TxPageLimit: defaultTxPageLimit, Passphrase: network.PublicNetworkPassphrase}
	ds, release, err := openColdDataset(testLogger(), coldQueryOptions{ColdRoot: coldRoot, NumChunks: 2, Plan: plan})
	require.NoError(t, err)
	assert.Equal(t, chunk.ID(1).LastLedger(), ds.LastLedger)
	// Five ledgers on each side of the border; only chunk 1's first ledger
	// carries a transaction.
	lo, hi := chunk.ID(0).LastLedger()-4, chunk.ID(1).FirstLedger()+4
	items, err := requestItems(ds, plan, ledgersRequest, lo, hi)
	require.NoError(t, err)
	assert.Equal(t, 10, items)
	items, err = requestItems(ds, plan, txPageRequest, lo, hi)
	require.NoError(t, err)
	assert.Equal(t, 1, items)
	release()

	executeQuery(t, queryTierCold, "--start-chunk", "0", "--num-chunks", "2", "--cold-dir", coldRoot,
		"--types", queryTypeLedgers+","+queryTypeTxPage, "--target-rps", "1000", "--duration", "10ms")
	requireNoScratchCatalog(t, coldRoot)
}
