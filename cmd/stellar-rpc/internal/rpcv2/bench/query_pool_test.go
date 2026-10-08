package bench

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// The events scan counts only the events of the dataset's ledger range: a hot
// dataset narrowed by --sample-ledgers leaves out the chunk's later events.
func TestScanEventTermsStaysInRange(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	for _, tc := range []struct {
		sampleLedgers uint32
		want          int
	}{
		{0, 2},
		{eventEvery, 1},
	} {
		t.Run(strconv.Itoa(int(tc.sampleLedgers)), func(t *testing.T) {
			ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
				HotRoot: hotRoot, Chunk: 0, SampleLedgers: tc.sampleLedgers,
				Plan: queryPlan{Types: []string{queryTypeEvents}},
			})
			require.NoError(t, err)
			defer release()

			counts := &eventTermCounts{
				contracts: map[string]int{},
				pairs:     map[string]int{},
			}
			require.NoError(t, counts.scanChunk(context.Background(), ds, 0, eventScanCap))
			assert.Equal(t, tc.want, counts.scanned)
		})
	}
}

// The txhash pool takes one hash from each sampled ledger, so a pool over
// ledgers that each hold several transactions spans as many ledgers as hashes.
func TestBuildTxHashPoolTakesOneHashPerLedger(t *testing.T) {
	const numLedgers, txPerLedger, size = 20, 4, 8
	hotRoot, ledgerOf := ingestMultiTxHotChunk(t, numLedgers, txPerLedger)
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: hotRoot, Chunk: 0,
		Plan: queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.PublicNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	pool, err := buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, size)
	require.NoError(t, err)
	require.Len(t, pool.hashes, size)
	assert.Equal(t, size, pool.ledgerCount)
	ledgers := map[uint32]struct{}{}
	for _, h := range pool.hashes {
		seq, ok := ledgerOf[h]
		require.True(t, ok, "hash %x is not in the fixture", h)
		ledgers[seq] = struct{}{}
	}
	assert.Len(t, ledgers, size)
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
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: ingestHotChunk(t), Chunk: 0,
		Plan: queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.PublicNetworkPassphrase},
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
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: ingestHotChunk(t), Chunk: 0,
		Plan: queryPlan{Types: []string{queryTypeTxHash}, Passphrase: network.TestNetworkPassphrase},
	})
	require.NoError(t, err)
	defer release()

	_, err = buildTxHashPool(context.Background(), testLogger(), ds, 0, defaultSeed, defaultTxHashPoolSize)
	require.ErrorContains(t, err, "does not pair")
}

// A canceled context stops the events pool build.
func TestBuildEventFilterPoolStopsOnCancel(t *testing.T) {
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: ingestHotChunk(t), Chunk: 0,
		Plan: queryPlan{Types: []string{queryTypeEvents}},
	})
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = buildEventFilterPool(ctx, testLogger(), ds)
	require.ErrorIs(t, err, context.Canceled)
}

// Each chunk's tx-hash stop point is its cumulative share of size, rounded up,
// and the last chunk's is size.
func TestTxHashStopAt(t *testing.T) {
	for _, tc := range []struct {
		size, n int
		want    []int
	}{
		{10, 1, []int{10}},
		{10, 3, []int{4, 7, 10}},
		{2, 4, []int{1, 1, 2, 2}},
	} {
		got := make([]int, tc.n)
		for i := range tc.n {
			got[i] = txHashStopAt(tc.size, i, tc.n)
		}
		assert.Equal(t, tc.want, got, "size %d, n %d", tc.size, tc.n)
	}
}

// The events scan stays within eventScanCap, gives each scanned chunk at least
// minEventScanPerChunk, and strides by the fewest chunks that keep that share,
// so the scan spans the range.
func TestEventScanPlan(t *testing.T) {
	maxFull := eventScanCap / minEventScanPerChunk
	for _, n := range []int{1, maxFull, maxFull + 1, maxFull + 2, 1000} {
		stride, perChunk := eventScanPlan(n)
		require.GreaterOrEqual(t, stride, 1, "n %d", n)
		assert.GreaterOrEqual(t, perChunk, minEventScanPerChunk, "n %d", n)
		scanned := 0
		for i := 0; i < n; i += stride {
			scanned++
		}
		assert.LessOrEqual(t, scanned*perChunk, eventScanCap, "n %d", n)
		assert.Equal(t, (n+maxFull-1)/maxFull, stride, "n %d", n)
	}
	stride, perChunk := eventScanPlan(1)
	assert.Equal(t, 1, stride)
	assert.Equal(t, eventScanCap, perChunk)
}

// The events pool derives the unfiltered set, a contract set and a (contract,
// first topic) set from the stored events, and each set matches the fixture's
// events.
func TestBuildEventFilterPool(t *testing.T) {
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{
		HotRoot: ingestHotChunk(t), Chunk: 0,
		Plan: queryPlan{Types: []string{queryTypeEvents}},
	})
	require.NoError(t, err)
	defer release()

	pool, err := buildEventFilterPool(context.Background(), testLogger(), ds)
	require.NoError(t, err)
	require.Len(t, pool.sets, 3)
	assert.Equal(t, "derived", pool.kind())
	assert.Nil(t, pool.sets[0])
	require.Len(t, pool.sets[1], 1)
	assert.NotEmpty(t, pool.sets[1][0].ContractID)
	assert.Nil(t, pool.sets[1][0].Topics[0])
	require.Len(t, pool.sets[2], 1)
	assert.Equal(t, pool.sets[1][0].ContractID, pool.sets[2][0].ContractID)
	assert.NotEmpty(t, pool.sets[2][0].Topics[0])

	for i, filters := range pool.sets {
		view, err := ds.view()
		require.NoError(t, err)
		hi := ds.LastLedger
		page, err := view.QueryEvents(context.Background(), query.EventCursor{Scope: query.EventScope{
			MinLedger: ds.FirstLedger, MaxLedger: &hi, Dir: query.Ascending, Filters: filters,
		}}, defaultEventsLimit)
		view.Release()
		require.NoError(t, err)
		assert.Len(t, page.Events, 2, "set %d", i)
	}

	assert.Equal(t, "unfiltered", (&eventFilterPool{sets: pool.sets[:1]}).kind())
}

// byDescendingCount orders keys by count, most frequent first, and ties by key
// bytes.
func TestByDescendingCount(t *testing.T) {
	got := byDescendingCount(map[string]int{"b": 2, "a": 2, "c": 5})
	assert.Equal(t, [][]byte{[]byte("c"), []byte("a"), []byte("b")}, got)
}
