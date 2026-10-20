package bench

import (
	"cmp"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// The txhash pool takes one hash from each sampled ledger, so a pool over
// ledgers that each hold several transactions spans as many ledgers as hashes.
func TestBuildTxHashPoolTakesOneHashPerLedger(t *testing.T) {
	const numLedgers, txPerLedger, size = 20, 4, 8
	hotRoot, ledgerOf := ingestMultiTxHotChunk(t, numLedgers, txPerLedger)
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: hotRoot,
		Plan:    queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	pool, err := buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, size)
	require.NoError(t, err)
	require.Len(t, pool.hashes, size)
	ledgers := map[uint32]struct{}{}
	for _, h := range pool.hashes {
		seq, ok := ledgerOf[h]
		require.True(t, ok, "hash %x is not in the fixture", h)
		ledgers[seq] = struct{}{}
	}
	assert.Len(t, ledgers, size)
}

// The same seed builds the same txhash pool.
func TestBuildTxHashPoolIsDeterministic(t *testing.T) {
	hotRoot, _ := ingestMultiTxHotChunk(t, 20, 4)
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: hotRoot,
		Plan:    queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	a, err := buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, 8)
	require.NoError(t, err)
	b, err := buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, 8)
	require.NoError(t, err)
	assert.Equal(t, a.hashes, b.hashes)
}

// ingestMultiTxHotChunk writes numLedgers ledgers of chunk 0, each holding
// txPerLedger transactions, into a hot database. It returns the --hot-dir and
// the ledger of each transaction hash.
func ingestMultiTxHotChunk(t *testing.T, numLedgers uint32, txPerLedger int) (string, map[[32]byte]uint32) {
	t.Helper()
	root := t.TempDir()
	layout := geometry.NewLayout(root)
	packPath := layout.LedgerPackPath(0)
	require.NoError(t, os.MkdirAll(filepath.Dir(packPath), 0o755))
	w, err := ledger.NewColdWriter(packPath, chunk.ID(0).FirstLedger(), ledger.ColdWriterOptions{})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	ledgerOf := map[[32]byte]uint32{}
	first := chunk.ID(0).FirstLedger()
	for seq := first; seq < first+numLedgers; seq++ {
		envelopes := make([]xdr.TransactionEnvelope, txPerLedger)
		processing := make([]xdr.TransactionResultMetaV1, txPerLedger)
		for i := range envelopes {
			envelopes[i] = xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTx,
				V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
				}},
			}
			hash, err := network.HashTransactionInEnvelope(envelopes[i], network.PublicNetworkPassphrase)
			require.NoError(t, err)
			ledgerOf[hash] = seq
			processing[i] = xdr.TransactionResultMetaV1{
				TxApplyProcessing: xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{}},
				Result: xdr.TransactionResultPair{
					TransactionHash: hash,
					Result: xdr.TransactionResult{Result: xdr.TransactionResultResult{
						Code: xdr.TransactionResultCodeTxSuccess, Results: &[]xdr.OperationResult{},
					}},
				},
			}
		}
		require.NoError(t, w.AppendLedger(seq, rpcv2test.V2LCMBytes(t, seq, 0, envelopes, processing)))
	}
	require.NoError(t, w.Commit())

	hotRoot := t.TempDir()
	require.NoError(t, runHot(context.Background(), testLogger(), hotOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: layout.LedgersRoot()},
		StartChunk: 0,
		NumChunks:  1,
		NumLedgers: numLedgers,
		HotRoot:    hotRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))
	return hotRoot, ledgerOf
}

// A canceled context stops the txhash pool build.
func TestBuildTxHashPoolStopsOnCancel(t *testing.T) {
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: ingestHotChunk(t),
		Plan:    queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = buildTxHashPool(ctx, testLogger(), ds, 0, defaultSeed, defaultTxHashPoolSize)
	require.ErrorIs(t, err, context.Canceled)
}

// A txhash pool build fails under the wrong passphrase.
func TestBuildTxHashPoolWrongPassphrase(t *testing.T) {
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: ingestHotChunk(t),
		Plan:    queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.TestNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	_, err = buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, defaultTxHashPoolSize)
	require.ErrorContains(t, err, "a wrong --network-passphrase")
}

// A canceled context stops the events pool build.
func TestBuildEventFilterPoolStopsOnCancel(t *testing.T) {
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: ingestHotChunk(t),
		Plan:    queryPlan{Types: []string{queryTypeEvents}},
	})
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = buildEventFilterPool(ctx, testLogger(), ds, defaultSeed)
	require.ErrorIs(t, err, context.Canceled)
}

// The events pool derives the unfiltered set, a contract set and a (contract,
// first topic) set from the sampled events, and each set matches the fixture's
// events.
func TestBuildEventFilterPool(t *testing.T) {
	ds, release, err := openDataset(testLogger(), queryOptions{
		DataDir: ingestHotChunk(t),
		Plan:    queryPlan{Types: []string{queryTypeEvents}},
	})
	require.NoError(t, err)
	defer release()

	pool, err := buildEventFilterPool(context.Background(), testLogger(), ds, defaultSeed)
	require.NoError(t, err)
	require.Len(t, pool.sets, 3)
	assert.Equal(t, "derived", pool.kind())
	assert.Nil(t, pool.sets[0])
	require.Len(t, pool.sets[1], 1)
	assert.NotEmpty(t, pool.sets[1][0].ContractID)
	assert.Nil(t, pool.sets[1][0].Topic0)
	require.Len(t, pool.sets[2], 1)
	assert.Equal(t, pool.sets[1][0].ContractID, pool.sets[2][0].ContractID)
	assert.NotEmpty(t, pool.sets[2][0].Topic0)

	limit := uint(defaultEventsLimit)
	for i, filters := range pool.sets {
		timing, err := ds.server.call(context.Background(), protocol.QueryEventsMethodName, protocol.QueryEventsRequest{
			MinLedger: ds.FirstLedger, MaxLedger: ds.LastLedger, Filters: filters, Limit: &limit,
		})
		require.NoError(t, err)
		assert.Equal(t, 2, timing.items, "set %d", i)
	}

	assert.Equal(t, "unfiltered", (&eventFilterPool{sets: pool.sets[:1]}).kind())
}

// byDescendingCount orders keys by count, most frequent first, and ties by
// compare.
func TestByDescendingCount(t *testing.T) {
	got := byDescendingCount(map[string]int{"b": 2, "a": 2, "c": 5}, cmp.Compare[string])
	assert.Equal(t, []string{"c", "a", "b"}, got)
}
