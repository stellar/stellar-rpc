package sqlitedb

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckBlobLimit(t *testing.T) {
	require.NoError(t, checkBlobLimit(sqliteMaxLength))
	require.ErrorContains(t, checkBlobLimit(1_000_000_000), "-DSQLITE_MAX_LENGTH=2147483647")
}

// TestBlobLimitOfThisBuild reads the limit from a live connection. It skips
// unless the test binary was built with the raised limit, as make builds are.
func TestBlobLimitOfThisBuild(t *testing.T) {
	sqlDB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	limit, err := blobLimit(t.Context(), sqlDB)
	require.NoError(t, err)
	if limit < sqliteMaxLength {
		t.Skipf("built without -DSQLITE_MAX_LENGTH=%d (limit %d)", sqliteMaxLength, limit)
	}
	require.NoError(t, checkBlobLimit(limit))
}
