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

func TestWithView_OneSnapshotPerRequest(t *testing.T) {
	r, first := sharedViewFixture(t)
	reader := NewLedgerReader()
	ctx := viewCtx(t, r)

	got, err := reader.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	require.Equal(t, first+2, got)

	r.SetLatestLedger(first+3, closeTimeFor(first+3))

	got, err = reader.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, first+2, got,
		"the request's snapshot froze at acquisition")

	_, found, err := reader.GetLedger(ctx, first+3)
	require.NoError(t, err)
	assert.False(t, found,
		"a ledger committed after the snapshot is invisible to this request")
}

func TestWithView_SharedAcrossAdapters(t *testing.T) {
	r, first := sharedViewFixture(t)
	txReader := NewTransactionReader(network.PublicNetworkPassphrase, nil)
	ledgerReader := NewLedgerReader()
	ctx := viewCtx(t, r)

	_, err := txReader.GetTransaction(ctx, xdr.Hash{1})
	assert.ErrorIs(t, err, store.ErrNoTransaction)

	r.SetLatestLedger(first+3, closeTimeFor(first+3))

	got, err := ledgerReader.GetLatestLedgerSequence(ctx)
	require.NoError(t, err)
	assert.Equal(t, first+2, got,
		"both adapters read through the one view the context carries")
}

func TestViewFrom_ContextWithoutViewIsAnError(t *testing.T) {
	ledgerReader := NewLedgerReader()
	txReader := NewTransactionReader(network.PublicNetworkPassphrase, nil)

	_, err := ledgerReader.GetLatestLedgerSequence(context.Background())
	assert.ErrorIs(t, err, errNoView)

	_, err = ledgerReader.NewTx(context.Background())
	assert.ErrorIs(t, err, errNoView)

	_, err = txReader.GetTransaction(context.Background(), xdr.Hash{1})
	assert.ErrorIs(t, err, errNoView)
}

func TestWithView_TxDoneLeavesTheRequestViewAlive(t *testing.T) {
	r, first := sharedViewFixture(t)
	reader := NewLedgerReader()
	ctx := viewCtx(t, r)

	tx, err := reader.NewTx(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.Done())

	_, found, err := reader.GetLedger(ctx, first)
	require.NoError(t, err)
	assert.True(t, found,
		"Done releases nothing it does not own; the request's view still serves reads")
}
