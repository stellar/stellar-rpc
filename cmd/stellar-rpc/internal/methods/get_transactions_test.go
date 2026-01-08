package methods

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

const (
	NetworkPassphrase string = "passphrase"
)

var expectedTransactionInfo = protocol.TransactionInfo{
	TransactionDetails: protocol.TransactionDetails{
		Status:              "SUCCESS",
		TransactionHash:     "d68ad0eb1626ccd8c6c9f9231d170a1409289c86e291547beb5e4df3f91692a4",
		ApplicationOrder:    1,
		FeeBump:             false,
		Ledger:              2,
		EnvelopeXDR:         "AAAAAgAAAQCAAAAAAAAAAD8MNL+TrQ2ZcdBMzJD3BVEcg4qtlzSkovsNegP8f+iaAAAAAQAAAAD///+eAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==", //nolint:lll
		ResultMetaXDR:       "AAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAA",
		ResultXDR:           "AAAAAAAAAGQAAAAAAAAAAAAAAAA=",
		DiagnosticEventsXDR: []string{},
		Events: protocol.Events{
			ContractEventsXDR:    [][]string{{}},
			TransactionEventsXDR: []string{},
		},
	},
	LedgerCloseTime: 150,
}

func TestGetTransactions_DefaultLimit(t *testing.T) { //nolint:dupl
	testDB := setupDB(t, 10, 0)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		StartLedger: 2,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, int64(350), response.LatestLedgerCloseTime)

	// assert pagination
	assert.Equal(t, toid.New(6, 2, 1).String(), response.Cursor)

	// assert transactions result
	assert.Len(t, response.Transactions, 10)

	// assert the transaction structure. We will match only 1 tx for sanity purposes.
	assert.Equal(t, expectedTransactionInfo, response.Transactions[0])
}

func TestGetTransactions_DefaultLimitExceedsLatestLedger(t *testing.T) { //nolint:dupl
	testDB := setupDB(t, 3, 0)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		StartLedger: 2,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), response.LatestLedger)
	assert.Equal(t, int64(175), response.LatestLedgerCloseTime)
	assert.Equal(t, toid.New(3, 2, 1).String(), response.Cursor)
	assert.Len(t, response.Transactions, 4)
	assert.Equal(t, expectedTransactionInfo, response.Transactions[0])
}

func TestGetTransactions_CustomLimit(t *testing.T) {
	testDB := setupDB(t, 10, 0)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		StartLedger: 2,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 2,
		},
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)
	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, int64(350), response.LatestLedgerCloseTime)
	assert.Equal(t, toid.New(2, 2, 1).String(), response.Cursor)
	assert.Len(t, response.Transactions, 2)
	assert.Equal(t, uint32(2), response.Transactions[0].Ledger)
	assert.Equal(t, uint32(2), response.Transactions[1].Ledger)
	assert.Equal(t, expectedTransactionInfo, response.Transactions[0])
}

func TestGetTransactions_CustomLimitAndCursor(t *testing.T) {
	testDB := setupDB(t, 10, 0)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Cursor: toid.New(2, 2, 1).String(),
			Limit:  3,
		},
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)
	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, int64(350), response.LatestLedgerCloseTime)
	assert.Equal(t, toid.New(4, 1, 1).String(), response.Cursor)
	assert.Len(t, response.Transactions, 3)
	assert.Equal(t, uint32(3), response.Transactions[0].Ledger)
	assert.Equal(t, uint32(3), response.Transactions[1].Ledger)
	assert.Equal(t, uint32(4), response.Transactions[2].Ledger)
}

func TestGetTransactions_InvalidStartLedger(t *testing.T) {
	testDB := setupDB(t, 3, 0)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		StartLedger: 4,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)

	expectedErr := fmt.Errorf(
		"[%d] start ledger (4) must be between the oldest ledger: 2 and the latest ledger: 3 for this rpc instance",
		jrpc2.InvalidRequest,
	)
	assert.Equal(t, expectedErr.Error(), err.Error())
	assert.Nil(t, response.Transactions)
}

func TestGetTransactions_LedgerNotFound(t *testing.T) {
	testDB := setupDB(t, 4, 3)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		StartLedger: 3,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	expectedErr := fmt.Errorf("[%d] database does not contain metadata for ledger: 3", jrpc2.InvalidParams)
	assert.Equal(t, expectedErr.Error(), err.Error())
	assert.Nil(t, response.Transactions)
}

func TestGetTransactions_LimitGreaterThanMaxLimit(t *testing.T) {
	testDB := setupDB(t, 3, 0)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		StartLedger: 2,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 200,
		},
	}

	_, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	expectedErr := fmt.Errorf("[%d] limit must not exceed 100", jrpc2.InvalidRequest)
	assert.Equal(t, expectedErr.Error(), err.Error())
}

func TestGetTransactions_InvalidCursorString(t *testing.T) {
	testDB := setupDB(t, 3, 0)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Cursor: "abc",
		},
	}

	_, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	expectedErr := fmt.Errorf("[%d] strconv.ParseInt: parsing \"abc\": invalid syntax", jrpc2.InvalidParams)
	assert.Equal(t, expectedErr.Error(), err.Error())
}

func TestGetTransactions_JSONFormat(t *testing.T) {
	testDB := setupDB(t, 3, 0)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		Format:      protocol.FormatJSON,
		StartLedger: 2,
	}

	js, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)

	// Do a marshaling round-trip on a transaction so we can check that the
	// fields are encoded correctly as JSON.
	txResp := js.Transactions[0]
	jsBytes, err := json.Marshal(txResp)
	require.NoError(t, err)

	var tx map[string]interface{}
	require.NoError(t, json.Unmarshal(jsBytes, &tx))

	require.Nilf(t, tx["envelopeXdr"], "field: 'envelopeXdr'")
	require.NotNilf(t, tx["envelopeJson"], "field: 'envelopeJson'")
	require.Nilf(t, tx["resultXdr"], "field: 'resultXdr'")
	require.NotNilf(t, tx["resultJson"], "field: 'resultJson'")
	require.Nilf(t, tx["resultMetaXdr"], "field: 'resultMetaXdr'")
	require.NotNilf(t, tx["resultMetaJson"], "field: 'resultMetaJson'")
}

func TestGetTransactions_NoResults(t *testing.T) {
	testDB := setupDBNoTxs(t, 5)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		StartLedger: 1,
	}

	txns, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)
	require.NotNil(t, txns.Transactions)
	require.Empty(t, txns.Transactions)
}

// createTestLedger Creates a test ledger with 2 transactions
func createTestLedger(sequence uint32) xdr.LedgerCloseMeta {
	sequence -= 100
	meta := txMeta(sequence, true)
	meta.V2.TxProcessing = append(meta.V2.TxProcessing, xdr.TransactionResultMetaV1{
		TxApplyProcessing: xdr.TransactionMeta{
			V:          3,
			Operations: &[]xdr.OperationMeta{},
			V3:         &xdr.TransactionMetaV3{},
		},
		Result: xdr.TransactionResultPair{
			TransactionHash: txHash(sequence),
			Result:          transactionResult(false),
		},
	})
	return meta
}

// createEmptyTestLedger Creates a test ledger with 2 transactions
func createEmptyTestLedger(sequence uint32) xdr.LedgerCloseMeta {
	sequence -= 100
	return emptyTxMeta(sequence)
}

func setupDB(t *testing.T, numLedgers int, skipLedger int) *db.DB {
	testDB := NewTestDB(t)
	daemon := interfaces.MakeNoOpDeamon()
	for sequence := 2; sequence <= numLedgers; sequence++ {
		if sequence == skipLedger {
			continue
		}
		ledgerCloseMeta := createTestLedger(uint32(sequence))
		tx, err := db.NewReadWriter(log.DefaultLogger, testDB, daemon, 150, 100, passphrase).NewTx(context.Background())
		require.NoError(t, err)
		require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
		require.NoError(t, tx.Commit(ledgerCloseMeta, nil))
	}
	return testDB
}

func setupDBNoTxs(t *testing.T, numLedgers int) *db.DB {
	testDB := NewTestDB(t)
	daemon := interfaces.MakeNoOpDeamon()
	for sequence := 1; sequence <= numLedgers; sequence++ {
		ledgerCloseMeta := createEmptyTestLedger(uint32(sequence))

		tx, err := db.NewReadWriter(log.DefaultLogger, testDB, daemon, 150, 100, passphrase).NewTx(context.Background())
		require.NoError(t, err)
		require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
		require.NoError(t, tx.Commit(ledgerCloseMeta, nil))
	}
	return testDB
}

func TestGetTransactions_UsesDatastoreForOlderHistory(t *testing.T) {
	ctx := t.Context()

	// DB has ledgers 3..5 (skip ledger 2).
	testDB := setupDB(t, 5, 2)

	ds := &MockDatastoreReader{}

	dsRange := protocol.LedgerSeqRange{
		FirstLedger: 2,
		LastLedger:  2,
	}
	ds.On("GetAvailableLedgerRange", mock.Anything).Return(dsRange, nil).Once()

	ledger1 := createTestLedger(2)
	ds.On("GetLedgerCached", mock.Anything, uint32(2)).Return(ledger1, nil).Once()
	handler := transactionsRPCHandler{
		ledgerReader:          db.NewLedgerReader(testDB),
		datastoreLedgerReader: ds,
		maxLimit:              100,
		defaultLimit:          6,
		networkPassphrase:     NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		StartLedger: 2,
	}

	resp, err := handler.getTransactionsByLedgerSequence(ctx, request)
	require.NoError(t, err)

	assert.Equal(t, uint32(3), resp.OldestLedger)
	assert.Equal(t, uint32(5), resp.LatestLedger)
	assert.Len(t, resp.Transactions, 6)
	assert.Equal(t, uint32(2), resp.Transactions[0].Ledger)

	ds.AssertExpectations(t)
}
