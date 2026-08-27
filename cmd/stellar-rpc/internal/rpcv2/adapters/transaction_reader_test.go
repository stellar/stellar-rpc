package adapters

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

func TestGetTransaction_HotHit(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	lcm, txs := lcmWithTxs(t, testChunk.FirstLedger(),
		txSpec{events: []xdr.ContractEvent{rpcv2test.SymbolContractEvent(xdr.ContractId{0xab}, "transfer", "transfer")}})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm)
	r.SetLatestLedger(testChunk.FirstLedger(), query.CloseTimeAt(closeTimeFor(testChunk.FirstLedger())))
	reader := NewTransactionReader(network.PublicNetworkPassphrase, nil)

	got, err := reader.GetTransaction(viewCtx(t, r), txs[0].hash)
	require.NoError(t, err)

	assert.Equal(t, txs[0].hash.HexString(), got.TransactionHash)
	assert.Equal(t, store.LedgerInfo{
		Sequence: testChunk.FirstLedger(), CloseTime: closeTimeFor(testChunk.FirstLedger()),
	}, got.Ledger)
	assert.True(t, got.Successful)
	assert.False(t, got.FeeBump)
	assert.Equal(t, int32(1), got.ApplicationOrder)
	assert.Equal(t, mustMarshal(t, &txs[0].envelope), got.Envelope)
	assert.Equal(t, mustMarshal(t, &txs[0].result), got.Result)
	assert.Equal(t, mustMarshal(t, &txs[0].meta), got.Meta)
	require.Len(t, got.ContractEvents, 1, "one operation")
	require.Len(t, got.ContractEvents[0], 1, "one event in the operation")
	assert.Equal(t, mustMarshal(t, &txs[0].meta.V4.Operations[0].Events[0]), got.ContractEvents[0][0])
	assert.Empty(t, got.TransactionEvents)
	assert.Empty(t, got.Events, "no diagnostic events in the fixture")
}

func TestGetTransaction_MissIsErrNoTransaction(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	lcm, _ := lcmWithTxs(t, testChunk.FirstLedger(), txSpec{})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm)
	r.SetLatestLedger(testChunk.FirstLedger(), query.CloseTimeAt(closeTimeFor(testChunk.FirstLedger())))
	reader := NewTransactionReader(network.PublicNetworkPassphrase, nil)

	_, err := reader.GetTransaction(viewCtx(t, r), xdr.Hash{0xde, 0xad})
	assert.ErrorIs(t, err, store.ErrNoTransaction)
}

func TestGetTransaction_AboveLatestIsGated(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	lcm1, _ := lcmWithTxs(t, testChunk.FirstLedger(), txSpec{})
	lcm2, txs2 := lcmWithTxs(t, testChunk.FirstLedger()+1, txSpec{})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm1, lcm2)
	// The second ledger is committed but above the view's frozen latest; only
	// the adapter's gate, not the store, can produce the miss.
	r.SetLatestLedger(testChunk.FirstLedger(), query.CloseTimeAt(closeTimeFor(testChunk.FirstLedger())))
	reader := NewTransactionReader(network.PublicNetworkPassphrase, nil)

	_, err := reader.GetTransaction(viewCtx(t, r), txs2[0].hash)
	assert.ErrorIs(t, err, store.ErrNoTransaction)
}

func TestGetTransaction_BelowFloorIsGated(t *testing.T) {
	cat := openTestCatalog(t)
	// Retention floor at chunk 6: chunk 5's handle stays published but its
	// ledgers are below the servable window, so the view's gate must hide it.
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk+1))
	lcm5, txs5 := lcmWithTxs(t, testChunk.FirstLedger(), txSpec{})
	lcm6, txs6 := lcmWithTxs(t, (testChunk + 1).FirstLedger(), txSpec{})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm5)
	seedHotChunkLCMs(t, cat, r, testChunk+1, lcm6)
	r.SetLatestLedger((testChunk + 1).FirstLedger(), query.CloseTimeAt(closeTimeFor((testChunk + 1).FirstLedger())))
	reader := NewTransactionReader(network.PublicNetworkPassphrase, nil)
	ctx := viewCtx(t, r)

	_, err := reader.GetTransaction(ctx, txs5[0].hash)
	assert.ErrorIs(t, err, store.ErrNoTransaction, "hot match below the floor must not be served")

	got, err := reader.GetTransaction(ctx, txs6[0].hash)
	require.NoError(t, err)
	assert.Equal(t, (testChunk + 1).FirstLedger(), got.Ledger.Sequence)
}

func TestGetTransaction_PrunedDuringAcquisitionIsCleanMiss(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk+1))
	lcm5, txs5 := lcmWithTxs(t, testChunk.FirstLedger(), txSpec{})
	lcm6, _ := lcmWithTxs(t, (testChunk + 1).FirstLedger(), txSpec{})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm5)
	seedHotChunkLCMs(t, cat, r, testChunk+1, lcm6)
	r.SetLatestLedger((testChunk + 1).FirstLedger(), query.CloseTimeAt(closeTimeFor((testChunk + 1).FirstLedger())))

	// The mid-prune race a view can observe: testChunk's handle is still
	// published (loaded before the prune) while the catalog snapshot already
	// says the chunk serves nothing. The hot index hit must gate to a clean
	// miss, never reach the unresolvable ledger read.
	require.NoError(t, cat.PutHotTransient(testChunk))

	reader := NewTransactionReader(network.PublicNetworkPassphrase, nil)
	_, err := reader.GetTransaction(viewCtx(t, r), txs5[0].hash)
	assert.ErrorIs(t, err, store.ErrNoTransaction,
		"a just-pruned transaction is a clean miss, not a retryable failure")
}

// coldFixture serves testChunk's ledgers from a frozen pack (no hot handle, so
// the hot indexes are empty) and probes through a frozen window index covering
// chunks [testChunk, testChunk+2]. The index also maps orphanHash to a ledger
// in testChunk+2, which is inside the servable window (latest sits there) but
// has no serving store — the shape that must stay an error, not a miss.
func coldFixture(t *testing.T) (context.Context, *TransactionReader, []fixtureTx, xdr.Hash) {
	t.Helper()
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	require.NoError(t, cat.FlipHotReady(999)) // acquisition needs a ready live chunk

	lcm, txs := lcmWithTxs(t, testChunk.FirstLedger(), txSpec{})
	rpcv2test.WriteFrozenLedgerPack(t, cat, testChunk, lcm)

	orphanHash := xdr.Hash{0x77}
	writeFrozenTxIndex(t, cat, testChunk, testChunk+2, map[xdr.Hash]uint32{
		txs[0].hash: testChunk.FirstLedger(),
		orphanHash:  (testChunk + 2).FirstLedger(),
	})
	// Latest sits in testChunk+2 so the orphan candidate is in-window; a
	// candidate outside the window would be gated to a clean miss instead.
	r.SetLatestLedger((testChunk + 2).FirstLedger(), query.CloseTimeAt(closeTimeFor((testChunk + 2).FirstLedger())))
	return viewCtx(t, r), NewTransactionReader(network.PublicNetworkPassphrase, nil), txs, orphanHash
}

func TestGetTransaction_ColdIndexHit(t *testing.T) {
	ctx, reader, txs, _ := coldFixture(t)

	got, err := reader.GetTransaction(ctx, txs[0].hash)
	require.NoError(t, err)
	assert.Equal(t, txs[0].hash.HexString(), got.TransactionHash)
	assert.Equal(t, testChunk.FirstLedger(), got.Ledger.Sequence)
	assert.True(t, got.Successful)
}

func TestGetTransaction_ColdFingerprintFalsePositiveIsCleanMiss(t *testing.T) {
	ctx, reader, txs, _ := coldFixture(t)

	// The cold index keys on the hash's first 16 bytes, so mutating a tail byte
	// still hits the real transaction's entry; only ledger verification can
	// reject the candidate.
	mutated := txs[0].hash
	mutated[20] ^= 0xff
	_, err := reader.GetTransaction(ctx, mutated)
	assert.ErrorIs(t, err, store.ErrNoTransaction)
}

func TestGetTransaction_UnresolvableCandidateIsAnError(t *testing.T) {
	ctx, reader, _, orphanHash := coldFixture(t)

	// The candidate's chunk has no serving store, so the transaction's absence
	// cannot be verified; a clean not-found would be indistinguishable from the
	// transaction genuinely not existing.
	_, err := reader.GetTransaction(ctx, orphanHash)
	require.Error(t, err)
	assert.NotErrorIs(t, err, store.ErrNoTransaction)
	assert.ErrorContains(t, err, "lookup incomplete")
}

func TestGetTransaction_V1LedgerCloseMeta(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	raw, hash := lcmV1WithClassicTx(t, testChunk.FirstLedger())
	seedHotChunkLCMs(t, cat, r, testChunk, raw)
	r.SetLatestLedger(testChunk.FirstLedger(), query.CloseTimeAt(closeTimeFor(testChunk.FirstLedger())))
	reader := NewTransactionReader(network.PublicNetworkPassphrase, nil)

	got, err := reader.GetTransaction(viewCtx(t, r), hash)
	require.NoError(t, err)
	assert.Equal(t, hash.HexString(), got.TransactionHash)
	assert.Equal(t, store.LedgerInfo{
		Sequence: testChunk.FirstLedger(), CloseTime: closeTimeFor(testChunk.FirstLedger()),
	}, got.Ledger)
	assert.True(t, got.Successful)
	assert.False(t, got.FeeBump)
}

func TestGetTransaction_AgedOutColdCandidateIsCleanMiss(t *testing.T) {
	cat := openTestCatalog(t)
	// Floor at testChunk+1: testChunk fell out of retention and its ledger
	// files are gone (never written here — the same observable state as
	// pruned). The frozen window index [testChunk, testChunk+2] still names its
	// transactions, because an index is pruned only when the WHOLE window falls
	// below the floor. The lookup must answer not-found, not an error.
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk+1))
	require.NoError(t, cat.FlipHotReady(999)) // acquisition needs a ready live chunk

	agedHash := xdr.Hash{0x42}
	writeFrozenTxIndex(t, cat, testChunk, testChunk+2, map[xdr.Hash]uint32{
		agedHash: testChunk.FirstLedger(),
	})
	lcm, _ := lcmWithTxs(t, (testChunk + 1).FirstLedger(), txSpec{})
	rpcv2test.WriteFrozenLedgerPack(t, cat, testChunk+1, lcm)
	r.SetLatestLedger((testChunk + 1).FirstLedger(), query.CloseTimeAt(closeTimeFor((testChunk + 1).FirstLedger())))

	reader := NewTransactionReader(network.PublicNetworkPassphrase, nil)
	_, err := reader.GetTransaction(viewCtx(t, r), agedHash)
	assert.ErrorIs(t, err, store.ErrNoTransaction,
		"an aged-out transaction is a clean miss, not a lookup-incomplete error")
}

func TestGetTransaction_FeeBumpByEitherHash(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	lcm, outerHash, innerHash := feeBumpLCM(t, testChunk.FirstLedger())
	seedHotChunkLCMs(t, cat, r, testChunk, lcm)
	r.SetLatestLedger(testChunk.FirstLedger(), query.CloseTimeAt(closeTimeFor(testChunk.FirstLedger())))
	reader := NewTransactionReader(network.PublicNetworkPassphrase, nil)
	ctx := viewCtx(t, r)

	for _, hash := range []xdr.Hash{outerHash, innerHash} {
		got, err := reader.GetTransaction(ctx, hash)
		require.NoError(t, err)
		assert.True(t, got.FeeBump)
		assert.Equal(t, outerHash.HexString(), got.TransactionHash,
			"an inner-hash lookup still reports the outer (result-pair) hash, matching v1")
	}
}

type inconsistencyCounter struct {
	observability.NopMetrics

	n atomic.Int32
}

func (c *inconsistencyCounter) TxIndexInconsistency() { c.n.Add(1) }

func TestGetTransaction_HotIndexInconsistencyIsCounted(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	lcm, txs := lcmWithTxs(t, testChunk.FirstLedger(), txSpec{})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm)
	r.SetLatestLedger(testChunk.FirstLedger(), query.CloseTimeAt(closeTimeFor(testChunk.FirstLedger())))

	// A frozen ledger pack for the same chunk WITHOUT the transaction: routing
	// serves cold (cold-wins), the exact hot index still hits, and the fetched
	// ledger lacks the transaction — the corruption shape the counter exists
	// for. Only a broken freeze can produce this pack in production.
	rpcv2test.WriteFrozenLedgerPack(t, cat, testChunk, lcmBytes(t, testChunk.FirstLedger()))

	metrics := &inconsistencyCounter{}
	reader := NewTransactionReader(network.PublicNetworkPassphrase, metrics)
	_, err := reader.GetTransaction(viewCtx(t, r), txs[0].hash)
	require.Error(t, err)
	assert.NotErrorIs(t, err, store.ErrNoTransaction)
	assert.Equal(t, int32(1), metrics.n.Load())
}

// TestGetTransaction_AllocatesPerTransactionNotPerLedger is the standing guard
// on the fix: a found lookup must cost transaction-sized garbage, not
// ledger-sized. Measured in BYTES rather than allocation count, because the
// regression this protects against is one object of the wrong size.
func TestGetTransaction_AllocatesPerTransactionNotPerLedger(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	// A ledger many times larger than any one of its transactions, so the two
	// scales are far enough apart for the assertion to mean something.
	specs := make([]txSpec, 64)
	lcm, txs := lcmWithTxs(t, testChunk.FirstLedger(), specs...)
	seedHotChunkLCMs(t, cat, r, testChunk, lcm)
	r.SetLatestLedger(testChunk.FirstLedger(), query.CloseTimeAt(closeTimeFor(testChunk.FirstLedger())))
	reader := NewTransactionReader(network.PublicNetworkPassphrase, nil)
	ctx := viewCtx(t, r)
	hash := txs[len(txs)/2].hash

	got, err := reader.GetTransaction(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, hash.HexString(), got.TransactionHash)

	perCall := allocBytesPerRun(t, 40, func() {
		if _, err := reader.GetTransaction(ctx, hash); err != nil {
			t.Error(err)
		}
	})
	// Generous: the point is the ORDER, not a tight budget. Without the pooled
	// buffer this is at least one whole ledger per call.
	assert.Less(t, perCall, uint64(len(lcm)),
		"a found lookup allocated a ledger's worth (%d bytes) per call; ledger is %d bytes",
		perCall, len(lcm))
}

// allocBytesPerRun reports the average bytes fn allocates per call, after a
// warm-up pass so one-time costs (caches, the buffer pool filling) are not
// charged to the measurement.
func allocBytesPerRun(t *testing.T, runs int, fn func()) uint64 {
	t.Helper()
	for range runs {
		fn()
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range runs {
		fn()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}
