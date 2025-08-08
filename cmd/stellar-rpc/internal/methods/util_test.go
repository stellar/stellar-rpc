package methods

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

func BenchmarkGetProtocolVersion(b *testing.B) {
	dbx := NewTestDB(b)
	daemon := interfaces.MakeNoOpDeamon()

	ledgerReader := db.NewLedgerReader(dbx)
	_, exists, err := ledgerReader.GetLedger(context.Background(), 1)
	require.NoError(b, err)
	assert.False(b, exists)

	ledgerSequence := uint32(1)
	tx, err := db.NewReadWriter(log.DefaultLogger, dbx, daemon, 150, 15, "passphrase").NewTx(context.Background())
	require.NoError(b, err)
	ledgerCloseMeta := createMockLedgerCloseMeta(ledgerSequence)
	require.NoError(b, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(b, tx.Commit(ledgerCloseMeta))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := getProtocolVersion(b.Context(), ledgerReader)
		if err != nil {
			b.Fatalf("getProtocolVersion failed: %v", err)
		}
	}
}

func TestGetProtocolVersion(t *testing.T) {
	dbx := NewTestDB(t)
	daemon := interfaces.MakeNoOpDeamon()

	ledgerReader := db.NewLedgerReader(dbx)
	_, exists, err := ledgerReader.GetLedger(context.Background(), 1)
	require.NoError(t, err)
	assert.False(t, exists)

	ledgerSequence := uint32(1)
	tx, err := db.NewReadWriter(log.DefaultLogger, dbx, daemon, 150, 15, "passphrase").NewTx(context.Background())
	require.NoError(t, err)
	ledgerCloseMeta := createMockLedgerCloseMeta(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta))

	protocolVersion, err := getProtocolVersion(t.Context(), ledgerReader)
	require.NoError(t, err)
	require.Equal(t, uint32(20), protocolVersion)
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

func NewTestDB(tb testing.TB) *db.DB {
	tmp := tb.TempDir()
	dbPath := path.Join(tmp, "db.sqlite")
	dbConn, err := db.OpenSQLiteDB(dbPath)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, dbConn.Close())
	})
	return dbConn
}
