package db

import (
	"context"
	"database/sql"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/log"
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

	lcms := ingestTestLedgers(t, testDB, 7, true)

	require.NoError(t, FinalizeBulkLoad(ctx, testDB, dbPath, logger))

	// Canonical schema restored
	needsRestore, err = transactionsNeedRestore(ctx, testDB, ddls[transactionTableName], bulkDDL)
	require.NoError(t, err)
	require.False(t, needsRestore)
	missing, err = missingDeferredIndexes(ctx, testDB)
	require.NoError(t, err)
	require.Empty(t, missing)

	// Rows survived the restore copy
	requireLedgerData(t, testDB, lcms)

	// The restored hash key enforces uniqueness again
	hash := lcms[0].TransactionHash(0)
	_, err = testDB.ExecRaw(ctx,
		"INSERT INTO "+transactionTableName+" VALUES (?, ?, ?)", hash[:], 99, 1)
	require.ErrorContains(t, err, "UNIQUE constraint failed")

	// Finalize runs at every startup; a second run is a no-op
	require.NoError(t, FinalizeBulkLoad(ctx, testDB, dbPath, logger))
}
