package methods

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/protocol"
)

var expectedLedgerInfo = protocol.LedgerInfo{
	Hash:            "0000000000000000000000000000000000000000000000000000000000000000",
	Sequence:        1,
	LedgerCloseTime: 125,
	LedgerHeader:    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB9AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",                                                                                                                                                                                                                                                                                                                                                         //nolint:lll
	LedgerMetadata:  "AAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAH0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAEAAAACAAABAIAAAAAAAAAAPww0v5OtDZlx0EzMkPcFURyDiq2XNKSi+w16A/x/6JoAAAABAAAAAP///50AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGw0LNdyu0BUtYvu6oo7T+kmRyH5+FpqPyiaHsX7ibKLQAAAAAAAABkAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==", //nolint:lll
}

func setupTestDB(t *testing.T, numLedgers int) *db.DB {
	testDB := NewTestDB(t)
	daemon := interfaces.MakeNoOpDeamon()
	for sequence := 1; sequence <= numLedgers; sequence++ {
		ledgerCloseMeta := txMeta(uint32(sequence)-100, true)
		tx, err := db.NewReadWriter(log.DefaultLogger, testDB, daemon, 150, 100,
			passphrase, false).NewTx(context.Background())
		require.NoError(t, err)
		require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
		require.NoError(t, tx.Commit(ledgerCloseMeta))
	}
	return testDB
}

func TestGetLedgers_DefaultLimit(t *testing.T) {
	testDB := setupTestDB(t, 50)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := protocol.GetLedgersRequest{
		StartLedger: 1,
	}

	response, err := handler.getLedgers(context.TODO(), request)
	require.NoError(t, err)

	assert.Equal(t, uint32(50), response.LatestLedger)
	assert.Equal(t, ledgerCloseTime(50), response.LatestLedgerCloseTime)
	assert.Equal(t, "5", response.Cursor)
	assert.Len(t, response.Ledgers, 5)
	assert.Equal(t, uint32(1), response.Ledgers[0].Sequence)
	assert.Equal(t, uint32(5), response.Ledgers[4].Sequence)

	// assert a single ledger info structure for sanity purposes
	assert.Equal(t, expectedLedgerInfo, response.Ledgers[0])
}

func TestGetLedgers_CustomLimit(t *testing.T) {
	testDB := setupTestDB(t, 40)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := protocol.GetLedgersRequest{
		StartLedger: 1,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 50,
		},
	}

	response, err := handler.getLedgers(context.TODO(), request)
	require.NoError(t, err)

	assert.Equal(t, uint32(40), response.LatestLedger)
	assert.Equal(t, "40", response.Cursor)
	assert.Len(t, response.Ledgers, 40)
	assert.Equal(t, uint32(1), response.Ledgers[0].Sequence)
	assert.Equal(t, uint32(40), response.Ledgers[39].Sequence)
}

func TestGetLedgers_WithCursor(t *testing.T) {
	testDB := setupTestDB(t, 10)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := protocol.GetLedgersRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Cursor: "5",
			Limit:  3,
		},
	}

	response, err := handler.getLedgers(context.TODO(), request)
	require.NoError(t, err)

	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, "8", response.Cursor)
	assert.Len(t, response.Ledgers, 3)
	assert.Equal(t, uint32(6), response.Ledgers[0].Sequence)
	assert.Equal(t, uint32(8), response.Ledgers[2].Sequence)
}

func TestGetLedgers_InvalidStartLedger(t *testing.T) {
	testDB := setupTestDB(t, 10)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := protocol.GetLedgersRequest{
		StartLedger: 12,
	}

	_, err := handler.getLedgers(context.TODO(), request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start ledger must be between")
}

func TestGetLedgers_LimitExceedsMaxLimit(t *testing.T) {
	testDB := setupTestDB(t, 10)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := protocol.GetLedgersRequest{
		StartLedger: 1,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 101,
		},
	}

	_, err := handler.getLedgers(context.TODO(), request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit must not exceed 100")
}

func TestGetLedgers_InvalidCursor(t *testing.T) {
	testDB := setupTestDB(t, 10)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := protocol.GetLedgersRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Cursor: "invalid",
		},
	}

	_, err := handler.getLedgers(context.TODO(), request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid syntax")
}

func TestGetLedgers_JSONFormat(t *testing.T) {
	testDB := setupTestDB(t, 10)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := protocol.GetLedgersRequest{
		StartLedger: 1,
		Format:      protocol.FormatJSON,
	}

	response, err := handler.getLedgers(context.TODO(), request)
	require.NoError(t, err)

	assert.NotEmpty(t, response.Ledgers)
	ledger := response.Ledgers[0]

	assert.NotEmpty(t, ledger.LedgerHeaderJSON)
	assert.Empty(t, ledger.LedgerHeader)
	assert.NotEmpty(t, ledger.LedgerMetadataJSON)
	assert.Empty(t, ledger.LedgerMetadata)

	var headerJSON map[string]interface{}
	err = json.Unmarshal(ledger.LedgerHeaderJSON, &headerJSON)
	require.NoError(t, err)
	assert.NotEmpty(t, headerJSON)

	var metaJSON map[string]interface{}
	err = json.Unmarshal(ledger.LedgerMetadataJSON, &metaJSON)
	require.NoError(t, err)
	assert.NotEmpty(t, metaJSON)
}

func TestGetLedgers_NoLedgers(t *testing.T) {
	testDB := setupTestDB(t, 0)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := protocol.GetLedgersRequest{
		StartLedger: 1,
	}

	_, err := handler.getLedgers(context.TODO(), request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "[-32603] DB is empty")
}

func TestGetLedgers_CursorGreaterThanLatestLedger(t *testing.T) {
	testDB := setupTestDB(t, 10)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     100,
		defaultLimit: 5,
	}

	request := protocol.GetLedgersRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Cursor: "15",
		},
	}

	_, err := handler.getLedgers(context.TODO(), request)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cursor must be between")
}

func BenchmarkGetLedgers(b *testing.B) {
	testDB := setupBenchmarkingDB(b)
	handler := ledgersHandler{
		ledgerReader: db.NewLedgerReader(testDB),
		maxLimit:     200,
		defaultLimit: 5,
	}

	request := protocol.GetLedgersRequest{
		StartLedger: 1334,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 200, // using the current maximum request limit for getLedgers endpoint
		},
	}

	b.ResetTimer()
	for range b.N {
		response, err := handler.getLedgers(context.TODO(), request)
		require.NoError(b, err)
		assert.Equal(b, uint32(1334), response.Ledgers[0].Sequence)
		assert.Equal(b, uint32(1533), response.Ledgers[199].Sequence)
	}
}

func setupBenchmarkingDB(b *testing.B) *db.DB {
	testDB := NewTestDB(b)
	logger := log.DefaultLogger
	writer := db.NewReadWriter(logger, testDB, interfaces.MakeNoOpDeamon(),
		100, 1_000_000, passphrase, false)
	write, err := writer.NewTx(context.TODO())
	require.NoError(b, err)

	lcms := make([]xdr.LedgerCloseMeta, 0, 100_000)
	for i := range cap(lcms) {
		lcms = append(lcms, txMeta(uint32(1234+i), i%2 == 0))
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(b, ledgerW.InsertLedger(lcm))
		require.NoError(b, txW.InsertTransactions(lcm))
	}
	require.NoError(b, write.Commit(lcms[len(lcms)-1]))
	return testDB
}
