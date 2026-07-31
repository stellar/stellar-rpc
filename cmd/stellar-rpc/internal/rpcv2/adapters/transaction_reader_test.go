package adapters

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

func TestGetTransaction_HotHit(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	lcm, txs := lcmWithTxs(t, testChunk.FirstLedger(),
		txSpec{events: []xdr.ContractEvent{contractEventFixture(0xab, "transfer")}})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm)
	r.SetLatestLedger(testChunk.FirstLedger())
	reader := NewTransactionReader(r, network.PublicNetworkPassphrase)

	got, err := reader.GetTransaction(context.Background(), txs[0].hash)
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
	r.SetLatestLedger(testChunk.FirstLedger())
	reader := NewTransactionReader(r, network.PublicNetworkPassphrase)

	_, err := reader.GetTransaction(context.Background(), xdr.Hash{0xde, 0xad})
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
	r.SetLatestLedger(testChunk.FirstLedger())
	reader := NewTransactionReader(r, network.PublicNetworkPassphrase)

	_, err := reader.GetTransaction(context.Background(), txs2[0].hash)
	assert.ErrorIs(t, err, store.ErrNoTransaction)
}

func TestGetTransaction_BelowFloorIsGated(t *testing.T) {
	cat := openTestCatalog(t)
	// Retention floor at chunk 6: chunk 5's handle stays published (hot indexes
	// are deliberately unfiltered) but its ledgers are below the servable window.
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk+1))
	lcm5, txs5 := lcmWithTxs(t, testChunk.FirstLedger(), txSpec{})
	lcm6, txs6 := lcmWithTxs(t, (testChunk + 1).FirstLedger(), txSpec{})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm5)
	seedHotChunkLCMs(t, cat, r, testChunk+1, lcm6)
	r.SetLatestLedger((testChunk + 1).FirstLedger())
	reader := NewTransactionReader(r, network.PublicNetworkPassphrase)

	_, err := reader.GetTransaction(context.Background(), txs5[0].hash)
	assert.ErrorIs(t, err, store.ErrNoTransaction, "hot match below the floor must not be served")

	got, err := reader.GetTransaction(context.Background(), txs6[0].hash)
	require.NoError(t, err)
	assert.Equal(t, (testChunk + 1).FirstLedger(), got.Ledger.Sequence)
}

// coldFixture serves testChunk's ledgers from a frozen pack (no hot handle, so
// the hot indexes are empty) and probes through a frozen window index covering
// chunks [testChunk, testChunk+2]. The index also maps orphanHash to a ledger in
// testChunk+2, which has no serving store.
func coldFixture(t *testing.T) (*TransactionReader, []fixtureTx, xdr.Hash) {
	t.Helper()
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	require.NoError(t, cat.FlipHotReady(999)) // acquisition needs a ready live chunk

	lcm, txs := lcmWithTxs(t, testChunk.FirstLedger(), txSpec{})
	writeFrozenLedgerPack(t, cat, testChunk, lcm)

	orphanHash := xdr.Hash{0x77}
	writeFrozenTxIndex(t, cat, testChunk, testChunk+2, map[xdr.Hash]uint32{
		txs[0].hash: testChunk.FirstLedger(),
		orphanHash:  (testChunk + 2).FirstLedger(),
	})
	r.SetLatestLedger(testChunk.FirstLedger())
	return NewTransactionReader(r, network.PublicNetworkPassphrase), txs, orphanHash
}

func TestGetTransaction_ColdIndexHit(t *testing.T) {
	reader, txs, _ := coldFixture(t)

	got, err := reader.GetTransaction(context.Background(), txs[0].hash)
	require.NoError(t, err)
	assert.Equal(t, txs[0].hash.HexString(), got.TransactionHash)
	assert.Equal(t, testChunk.FirstLedger(), got.Ledger.Sequence)
	assert.True(t, got.Successful)
}

func TestGetTransaction_ColdFingerprintFalsePositiveIsCleanMiss(t *testing.T) {
	reader, txs, _ := coldFixture(t)

	// The cold index keys on the hash's first 16 bytes, so mutating a tail byte
	// still hits the real transaction's entry; only ledger verification can
	// reject the candidate.
	mutated := txs[0].hash
	mutated[20] ^= 0xff
	_, err := reader.GetTransaction(context.Background(), mutated)
	assert.ErrorIs(t, err, store.ErrNoTransaction)
}

func TestGetTransaction_UnresolvableCandidateIsAnError(t *testing.T) {
	reader, _, orphanHash := coldFixture(t)

	// The candidate's chunk has no serving store, so the transaction's absence
	// cannot be verified; a clean not-found would be indistinguishable from the
	// transaction genuinely not existing.
	_, err := reader.GetTransaction(context.Background(), orphanHash)
	require.Error(t, err)
	assert.NotErrorIs(t, err, store.ErrNoTransaction)
	assert.ErrorContains(t, err, "lookup incomplete")
}

func TestGetTransaction_FeeBumpByEitherHash(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	lcm, outerHash, innerHash := feeBumpLCM(t, testChunk.FirstLedger())
	seedHotChunkLCMs(t, cat, r, testChunk, lcm)
	r.SetLatestLedger(testChunk.FirstLedger())
	reader := NewTransactionReader(r, network.PublicNetworkPassphrase)

	for _, hash := range []xdr.Hash{outerHash, innerHash} {
		got, err := reader.GetTransaction(context.Background(), hash)
		require.NoError(t, err)
		assert.True(t, got.FeeBump)
		assert.Equal(t, outerHash.HexString(), got.TransactionHash,
			"an inner-hash lookup still reports the outer (result-pair) hash, matching v1")
	}
}
