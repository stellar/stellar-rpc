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

// sharedViewFixture seeds chunk 5's first four ledgers with latest pinned to
// the third, leaving headroom to advance the live latest under a frozen view.
func sharedViewFixture(t *testing.T) (*query.Registry, uint32) {
	t.Helper()
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	first := testChunk.FirstLedger()
	seedHotLedgers(t, cat, r, testChunk, seqRange(first, first+3)...)
	r.SetLatestLedger(first+2, closeTimeFor(first+2))
	return r, first
}

func TestSharedView_OneSnapshotPerRequest(t *testing.T) {
	r, first := sharedViewFixture(t)
	reader := NewLedgerReader(r)

	ctx, release := WithSharedView(context.Background())
	defer release()

	got, err := reader.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	require.Equal(t, first+2, got)

	r.SetLatestLedger(first+3, closeTimeFor(first+3))

	got, err = reader.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, first+2, got,
		"the request's snapshot froze at its first acquisition")

	_, found, err := reader.GetLedger(ctx, first+3)
	require.NoError(t, err)
	assert.False(t, found,
		"a ledger committed after the snapshot is invisible to this request")

	fresh, err := reader.GetLatestLedgerSequence(context.Background())
	require.NoError(t, err)
	assert.Equal(t, first+3, fresh,
		"a context without a holder acquires its own fresh view")
}

func TestSharedView_SharedAcrossAdapters(t *testing.T) {
	r, first := sharedViewFixture(t)
	txReader := NewTransactionReader(r, network.PublicNetworkPassphrase)
	ledgerReader := NewLedgerReader(r)

	ctx, release := WithSharedView(context.Background())
	defer release()

	_, err := txReader.GetTransaction(ctx, xdr.Hash{1})
	assert.ErrorIs(t, err, store.ErrNoTransaction)

	r.SetLatestLedger(first+3, closeTimeFor(first+3))

	got, err := ledgerReader.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, first+2, got,
		"the ledger reader sees the snapshot the transaction reader acquired")
}

func TestSharedView_TxDoneLeavesTheRequestViewAlive(t *testing.T) {
	r, first := sharedViewFixture(t)
	reader := NewLedgerReader(r)

	ctx, release := WithSharedView(context.Background())
	defer release()

	tx, err := reader.NewTx(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.Done())

	_, found, err := reader.GetLedger(ctx, first)
	require.NoError(t, err)
	assert.True(t, found,
		"Done releases nothing it does not own; the request's view still serves reads")
}

func TestSharedView_ReleaseWithoutAcquisitionIsANoOp(_ *testing.T) {
	_, release := WithSharedView(context.Background())
	release()
}
