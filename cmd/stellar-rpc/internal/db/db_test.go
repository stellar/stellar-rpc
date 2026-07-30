package db

import (
	"context"
	"database/sql"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
)

// TestDeferredIndexNamesMatchMigratedSchema guards the coupling between
// deferredIndexNames and the sqlmigrations files.
func TestDeferredIndexNamesMatchMigratedSchema(t *testing.T) {
	ctx := context.Background()

	// Fails if a deferred name no longer resolves, or if the transactions
	// DDL no longer yields a twin.
	_, _, err := bulkLoadDDLs(ctx)
	require.NoError(t, err)

	ref, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer ref.Close()
	ref.SetMaxOpenConns(1)
	require.NoError(t, runSQLMigrations(ref, "sqlite3"))

	// sql IS NOT NULL excludes sqlite's auto-indexes
	rows, err := ref.QueryContext(ctx,
		"SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL AND tbl_name IN (?, ?)",
		transactionTableName, eventTableName)
	require.NoError(t, err)
	defer rows.Close()
	var indexes []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		indexes = append(indexes, name)
	}
	require.NoError(t, rows.Err())

	require.ElementsMatch(t, deferredIndexNames, indexes,
		"secondary indexes on the bulk-load tables must match deferredIndexNames in db.go")
}

// TestBulkLoadRoundTrip prepares an empty DB for a bulk load, ingests through
// the ordinary write path, finalizes, and verifies rows, indexes, and the
// canonical constraints all survive the schema round-trip.
func TestBulkLoadRoundTrip(t *testing.T) {
	ctx := t.Context()
	logger := log.DefaultLogger
	dbPath := path.Join(t.TempDir(), "db.sqlite")
	testDB, err := OpenSQLiteDB(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testDB.Close()) })

	require.NoError(t, PrepareBulkLoad(ctx, testDB, logger))

	// The twin shape is in place: restore pending, events indexes dropped
	ddls, bulkDDL, err := bulkLoadDDLs(ctx)
	require.NoError(t, err)
	needsRestore, err := transactionsNeedRestore(ctx, testDB, ddls[transactionTableName], bulkDDL)
	require.NoError(t, err)
	require.True(t, needsRestore)
	missing, err := missingDeferredIndexes(ctx, testDB)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"idx_id_contract_id", "idx_id_topic1"}, missing)

	contractID := xdr.ContractId([32]byte{})
	counter := xdr.ScSymbol("COUNTER")
	event := contractEvent(
		contractID,
		xdr.ScVec{xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
		xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
	)

	writer := NewReadWriter(logger, testDB, interfaces.MakeNoOpDeamon(), 100, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)
	lcms := make([]xdr.LedgerCloseMeta, 0, 7)
	ledgerW, txW, eventW := write.LedgerWriter(), write.TransactionWriter(), write.EventWriter()
	for seq := uint32(1); len(lcms) < cap(lcms); seq++ {
		lcm := ledgerCloseMetaWithEvents(seq, time.Now().Unix(), transactionMetaWithEvents(event))
		lcms = append(lcms, lcm)
		require.NoError(t, ledgerW.InsertLedger(lcm))
		require.NoError(t, txW.InsertTransactions(lcm))
		require.NoError(t, eventW.InsertEvents(lcm))
	}
	require.NoError(t, write.Commit(lcms[len(lcms)-1], nil))

	require.NoError(t, FinalizeBulkLoad(ctx, testDB, dbPath, logger))

	// Canonical schema restored
	needsRestore, err = transactionsNeedRestore(ctx, testDB, ddls[transactionTableName], bulkDDL)
	require.NoError(t, err)
	require.False(t, needsRestore)
	missing, err = missingDeferredIndexes(ctx, testDB)
	require.NoError(t, err)
	require.Empty(t, missing)

	// Rows survived the restore copy
	txReader := NewTransactionReader(logger, testDB, passphrase)
	for _, lcm := range lcms {
		_, err := txReader.GetTransaction(ctx, lcm.TransactionHash(0))
		require.NoError(t, err, "transaction of ledger %d missing after finalize", lcm.LedgerSequence())
	}
	eventCount := 0
	eventReader := NewEventReader(logger, testDB, passphrase)
	cursorRange := protocol.CursorRange{
		Start: protocol.Cursor{Ledger: lcms[0].LedgerSequence()},
		End:   protocol.Cursor{Ledger: lcms[len(lcms)-1].LedgerSequence() + 1},
	}
	require.NoError(t, eventReader.GetEvents(ctx, cursorRange, nil, nil, nil,
		func(xdr.DiagnosticEvent, protocol.Cursor, int64, *xdr.Hash) bool {
			eventCount++
			return true
		}))
	// 3 transaction-level events + 1 operation event per ledger
	require.Equal(t, 4*len(lcms), eventCount)

	// The restored hash key enforces uniqueness again
	hash := lcms[0].TransactionHash(0)
	_, err = testDB.ExecRaw(ctx,
		"INSERT INTO "+transactionTableName+" VALUES (?, ?, ?)", hash[:], 99, 1)
	require.ErrorContains(t, err, "UNIQUE constraint failed")

	// Finalize runs at every startup; a second run is a no-op
	require.NoError(t, FinalizeBulkLoad(ctx, testDB, dbPath, logger))
}
