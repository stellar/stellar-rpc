package methods

import (
	"errors"
	"path"
	"testing"

	"github.com/stellar-experimental/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

func BenchmarkGetProtocolVersion(b *testing.B) {
	dbx := NewTestDB(b)
	daemon := host.MakeNoOpDaemon()

	ledgerReader := sqlitedb.NewLedgerReader(dbx)
	_, exists, err := store.GetLedger(b.Context(), ledgerReader, 1)
	require.NoError(b, err)
	assert.False(b, exists)

	ledgerSequence := uint32(1)
	tx, err := sqlitedb.NewReadWriter(log.DefaultLogger, dbx, daemon, 15, "passphrase").NewTx(b.Context())
	require.NoError(b, err)
	ledgerCloseMeta := createMockLedgerCloseMeta(ledgerSequence)
	require.NoError(b, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(b, tx.Commit(ledgerCloseMeta, nil))

	versions := newProtocolVersionCache(ledgerReader)
	for b.Loop() {
		_, err := versions.get(b.Context())
		if err != nil {
			b.Fatalf("getProtocolVersion failed: %v", err)
		}
	}
}

func TestGetProtocolVersion(t *testing.T) {
	dbx := NewTestDB(t)
	daemon := host.MakeNoOpDaemon()

	ledgerReader := sqlitedb.NewLedgerReader(dbx)
	_, exists, err := store.GetLedger(t.Context(), ledgerReader, 1)
	require.NoError(t, err)
	assert.False(t, exists)

	ledgerSequence := uint32(1)
	tx, err := sqlitedb.NewReadWriter(log.DefaultLogger, dbx, daemon, 15, "passphrase").NewTx(t.Context())
	require.NoError(t, err)
	ledgerCloseMeta := createMockLedgerCloseMeta(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta, nil))

	protocolVersion, err := newProtocolVersionCache(ledgerReader).get(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint32(20), protocolVersion)
}

func TestGetProtocolVersionServesMemoUntilLedgerAdvances(t *testing.T) {
	reader := newMemoLedgerReader(expectedLatestLedgerSequence)
	versions := newProtocolVersionCache(reader)

	for range 2 {
		v, err := versions.get(t.Context())
		require.NoError(t, err)
		assert.Equal(t, expectedLatestLedgerProtocolVersion, v)
	}
	assert.Equal(t, int32(1), reader.rawReads.Load(), "second call must be served from the memo")

	reader.latest.Store(expectedLatestLedgerSequence + 1)
	_, err := versions.get(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(2), reader.rawReads.Load(), "a new latest ledger must re-read the header")
}

func TestGetProtocolVersionErrorsAreNotMemoized(t *testing.T) {
	reader := newMemoLedgerReader(expectedLatestLedgerSequence)
	versions := newProtocolVersionCache(reader)

	reader.seqErr = errors.New("boom")
	_, err := versions.get(t.Context())
	require.ErrorContains(t, err, "boom")
	reader.seqErr = nil

	reader.rawErr = errors.New("disk")
	_, err = versions.get(t.Context())
	require.ErrorContains(t, err, "disk")
	reader.rawErr = nil

	v, err := versions.get(t.Context())
	require.NoError(t, err)
	assert.Equal(t, expectedLatestLedgerProtocolVersion, v)
	assert.Equal(t, int32(2), reader.rawReads.Load())
}

func TestGetNetworkServesProtocolVersionFromMemo(t *testing.T) {
	reader := newMemoLedgerReader(expectedLatestLedgerSequence)
	h := NewGetNetworkHandler("passphrase", "", reader)

	for range 2 {
		respI, err := h(t.Context(), &jrpc2.Request{})
		require.NoError(t, err)
		resp, ok := respI.(protocol.GetNetworkResponse)
		require.True(t, ok, "got %T", respI)
		assert.Equal(t, int(expectedLatestLedgerProtocolVersion), resp.ProtocolVersion)
	}
	assert.Equal(t, int32(1), reader.rawReads.Load(), "second request must be served from the memo")
}

func createMockLedgerCloseMeta(ledgerSequence uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{},
				Header: xdr.LedgerHeader{
					LedgerSeq:     xdr.Uint32(ledgerSequence),
					LedgerVersion: xdr.Uint32(20),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{},
			},
		},
	}
}

func NewTestDB(tb testing.TB) *sqlitedb.DB {
	tmp := tb.TempDir()
	dbPath := path.Join(tmp, "sqlitedb.sqlite")
	dbConn, err := sqlitedb.OpenSQLiteDB(dbPath)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, dbConn.Close())
	})
	return dbConn
}
