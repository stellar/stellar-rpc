package bench

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
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

// newRequest is the constructor of one request type.
type newRequest func(*queryDataset, queryPlan) queryRequest

// requestItems narrows the starts ds picks to [lo, hi], sets the ledgers limit
// to the range's size, and runs one request.
func requestItems(ds *queryDataset, p queryPlan, newReq newRequest, lo, hi uint32) (int, error) {
	ds.FirstLedger, ds.LastLedger = lo, hi
	p.LedgersLimit = hi - lo + 1
	timing, err := newReq(ds, p)(context.Background(), rand.New(rand.NewPCG(defaultSeed, defaultSeed)))
	return timing.items, err
}

// A one-ledger ledgers request reads one ledger, a request past the latest
// ledger fails in the handler, and a ledgers request that returns fewer
// ledgers than its limit fails.
func TestQueryRequestReads(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	plan := testPlan(protocol.FormatBase64)
	ds, release, err := openDataset(testLogger(), queryOptions{DataDir: hotRoot, Plan: plan})
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
		t.Run(tc.qtype+" past the latest ledger", func(t *testing.T) {
			_, err := requestItems(ds, plan, tc.newReq, last+1, last+1)
			require.ErrorContains(t, err, fmt.Sprintf("start ledger (%d) must be between", last+1))
		})
	}

	t.Run("point read", func(t *testing.T) {
		items, err := requestItems(ds, plan, ledgersRequest, first, first)
		require.NoError(t, err)
		assert.Equal(t, 1, items)
	})

	t.Run("missing ledgers", func(t *testing.T) {
		// The dataset claims five ledgers past the last one the store holds.
		_, err := requestItems(ds, plan, ledgersRequest, last-4, last+5)
		require.ErrorContains(t, err, "getLedgers returned 5 of the 10 ledgers")
	})
}

// A hot run creates no hot database in the dataset.
func TestQueryHotRunAddsNoChunk(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	before := dirNames(t, geometry.NewLayout(hotRoot).HotRoot())
	executeQuery(t, "--data-dir", hotRoot,
		"--types", queryTypeLedgers, "--target-rps", "1000", "--duration", "10ms")
	assert.Equal(t, before, dirNames(t, geometry.NewLayout(hotRoot).HotRoot()))
}

// A cold run over two chunks reads across the chunk border and creates no hot
// database in the dataset.
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

	plan := testPlan(protocol.FormatBase64)
	ds, release, err := openDataset(testLogger(), queryOptions{DataDir: coldRoot, Plan: plan})
	require.NoError(t, err)
	assert.Equal(t, chunk.ID(1).LastLedger(), ds.LastLedger)
	// Five ledgers on each side of the border; only chunk 1's first ledger
	// carries a transaction.
	lo, hi := chunk.ID(0).LastLedger()-4, chunk.ID(1).FirstLedger()+4
	items, err := requestItems(ds, plan, ledgersRequest, lo, hi)
	require.NoError(t, err)
	assert.Equal(t, 10, items)
	plan.TxPageLimit = 1
	items, err = requestItems(ds, plan, txPageRequest, lo, lo)
	require.NoError(t, err)
	assert.Equal(t, 1, items)
	release()

	before := dirNames(t, geometry.NewLayout(coldRoot).HotRoot())
	executeQuery(t, "--data-dir", coldRoot,
		"--types", queryTypeLedgers+","+queryTypeTxPage, "--target-rps", "1000", "--duration", "10ms",
		"--format", protocol.FormatJSON)
	assert.Equal(t, before, dirNames(t, geometry.NewLayout(coldRoot).HotRoot()))
}
