package ingest

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

func TestGapDetection(t *testing.T) {
	ctx := context.Background()
	testLogger := supportlog.New()

	tmp := t.TempDir()
	dbPath := path.Join(tmp, "test.sqlite")
	testDB, err := db.OpenSQLiteDB(dbPath)
	require.NoError(t, err)
	defer testDB.Close()

	rw := db.NewReadWriter(testLogger, testDB, interfaces.MakeNoOpDeamon(), 10, 10,
		network.TestNetworkPassphrase)

	writeTx, err := rw.NewTx(ctx)
	require.NoError(t, err)

	// Missing ledger 103
	ledgers := []xdr.LedgerCloseMeta{
		createLedger(100),
		createLedger(101),
		createLedger(102),
		createLedger(104),
		createLedger(105),
	}
	for _, ledger := range ledgers {
		require.NoError(t, writeTx.LedgerWriter().InsertLedger(ledger))
	}
	require.NoError(t, writeTx.Commit(ledgers[len(ledgers)-1], nil))
	backfill := &BackfillMeta{
		logger: testLogger,
		dbInfo: databaseInfo{rw: rw, reader: db.NewLedgerReader(testDB)},
	}
	_, _, err = backfill.verifyDbGapless(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "gap detected in local DB")

	// Now insert the missing ledger and verify no gap is detected
	writeTx, err = rw.NewTx(ctx)
	require.NoError(t, err)
	require.NoError(t, writeTx.LedgerWriter().InsertLedger(createLedger(103)))
	require.NoError(t, writeTx.Commit(ledgers[len(ledgers)-1], nil))

	_, _, err = backfill.verifyDbGapless(ctx)
	require.NoError(t, err)
}

func createLedger(ledgerSequence uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{},
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(ledgerSequence),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{},
			},
		},
	}
}
