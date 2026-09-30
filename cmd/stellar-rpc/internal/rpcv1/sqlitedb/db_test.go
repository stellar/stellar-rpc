package sqlitedb

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

func indexExists(t *testing.T, testDB *DB, name string) bool {
	var count int
	require.NoError(t, testDB.GetRaw(t.Context(), &count,
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", name))
	return count > 0
}

// TestBulkLoadRoundTrip prepares an empty DB for a bulk load, ingests through
// the ordinary write path, finalizes, and verifies rows, indexes, and the
// hash uniqueness constraint all survive the schema round-trip.
func TestBulkLoadRoundTrip(t *testing.T) {
	logger := log.DefaultLogger
	dbPath := path.Join(t.TempDir(), "db.sqlite")
	testDB, err := OpenSQLiteDB(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testDB.Close()) })

	require.NoError(t, PrepareBulkLoad(t.Context(), testDB, logger))

	for _, idx := range deferredIndexes {
		require.False(t, indexExists(t, testDB, idx.Name))
	}
	require.True(t, indexExists(t, testDB, "index_ledger_sequence"))
	_, err = getMetaValue(t.Context(), testDB, pendingIndexesMetaKey)
	require.NoError(t, err)

	lcms := ingestTestLedgers(t, testDB, 7, true)

	require.NoError(t, FinalizeBulkLoad(t.Context(), testDB, dbPath, logger))

	// All indexes built, pending record cleared
	for _, idx := range deferredIndexes {
		require.True(t, indexExists(t, testDB, idx.Name))
	}
	require.True(t, indexExists(t, testDB, "index_ledger_sequence"))
	_, err = getMetaValue(t.Context(), testDB, pendingIndexesMetaKey)
	require.ErrorIs(t, err, store.ErrEmptyDB)

	// Rows survived the round-trip
	requireLedgerData(t, testDB, lcms)

	// The built hash index enforces uniqueness again
	hash := lcms[0].TransactionHash(0)
	_, err = testDB.ExecRaw(t.Context(),
		"INSERT INTO "+transactionTableName+" VALUES (?, ?, ?)", hash[:], 99, 1)
	require.ErrorContains(t, err, "UNIQUE constraint failed")

	// Finalize runs at every startup; a second run is a no-op
	require.NoError(t, FinalizeBulkLoad(t.Context(), testDB, dbPath, logger))
}

// TestBulkLoadFinalizeResume covers a crash mid-finalize: an index already
// built by the interrupted run is skipped and the rest are completed.
func TestBulkLoadFinalizeResume(t *testing.T) {
	logger := log.DefaultLogger
	dbPath := path.Join(t.TempDir(), "db.sqlite")
	testDB, err := OpenSQLiteDB(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testDB.Close()) })

	require.NoError(t, PrepareBulkLoad(t.Context(), testDB, logger))
	lcms := ingestTestLedgers(t, testDB, 7, true)

	// Simulate a finalize interrupted after its first index build
	_, err = testDB.ExecRaw(t.Context(), deferredIndexes[0].DDL)
	require.NoError(t, err)

	require.NoError(t, FinalizeBulkLoad(t.Context(), testDB, dbPath, logger))

	for _, idx := range deferredIndexes {
		require.True(t, indexExists(t, testDB, idx.Name))
	}
	_, err = getMetaValue(t.Context(), testDB, pendingIndexesMetaKey)
	require.ErrorIs(t, err, store.ErrEmptyDB)
	requireLedgerData(t, testDB, lcms)
}

// TestFinalizeBulkLoadCanonicalNoOp verifies finalize leaves a DB that never
// went through prepare untouched: no pending record, no extra hash index.
func TestFinalizeBulkLoadCanonicalNoOp(t *testing.T) {
	logger := log.DefaultLogger
	dbPath := path.Join(t.TempDir(), "db.sqlite")
	testDB, err := OpenSQLiteDB(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testDB.Close()) })

	require.NoError(t, FinalizeBulkLoad(t.Context(), testDB, dbPath, logger))

	require.False(t, indexExists(t, testDB, "idx_transactions_hash"))
	require.True(t, indexExists(t, testDB, "index_ledger_sequence"))
}
