package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
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
