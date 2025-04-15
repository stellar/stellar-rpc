package methods

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/protocol"
)

const (
	NetworkPassphrase string = "passphrase"
)

var expectedTransactionInfo = protocol.TransactionInfo{
	TransactionDetails: protocol.TransactionDetails{
		Status:              "SUCCESS",
		TransactionHash:     "b0d0b35dcaed0152d62fbbaa28ed3fa4991c87e7e169a8fca2687b17ee26ca2d",
		ApplicationOrder:    1,
		FeeBump:             false,
		Ledger:              1,
		EnvelopeXDR:         "AAAAAgAAAQCAAAAAAAAAAD8MNL+TrQ2ZcdBMzJD3BVEcg4qtlzSkovsNegP8f+iaAAAAAQAAAAD///+dAAAAAAAAAAAAAAAAAAAAAAAAAAA=", //nolint:lll
		ResultMetaXDR:       "AAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAA",
		ResultXDR:           "AAAAAAAAAGQAAAAAAAAAAAAAAAA=",
		DiagnosticEventsXDR: []string{},
	},
	LedgerCloseTime: 125,
}

// createTestLedger Creates a test ledger with 2 transactions
func createTestLedger(sequence uint32) xdr.LedgerCloseMeta {
	sequence -= 100
	meta := txMeta(sequence, true)
	meta.V1.TxProcessing = append(meta.V1.TxProcessing, xdr.TransactionResultMeta{
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

func setupDB(t *testing.T, numLedgers int, skipLedger int) *db.DB {
	testDB := NewTestDB(t)
	daemon := interfaces.MakeNoOpDeamon()
	for sequence := 1; sequence <= numLedgers; sequence++ {
		if sequence == skipLedger {
			continue
		}
		ledgerCloseMeta := createTestLedger(uint32(sequence))
		tx, err := db.NewReadWriter(log.DefaultLogger, testDB, daemon, 150, 100, passphrase).NewTx(context.Background())
		require.NoError(t, err)
		require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
		require.NoError(t, tx.Commit(ledgerCloseMeta))
	}
	return testDB
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
		StartLedger: 1,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)

	// assert latest ledger details
	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, int64(350), response.LatestLedgerCloseTime)

	// assert pagination
	assert.Equal(t, toid.New(5, 2, 1).String(), response.Cursor)

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
		StartLedger: 1,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), response.LatestLedger)
	assert.Equal(t, int64(175), response.LatestLedgerCloseTime)
	assert.Equal(t, toid.New(3, 2, 1).String(), response.Cursor)
	assert.Len(t, response.Transactions, 6)
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
		StartLedger: 1,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 2,
		},
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)
	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, int64(350), response.LatestLedgerCloseTime)
	assert.Equal(t, toid.New(1, 2, 1).String(), response.Cursor)
	assert.Len(t, response.Transactions, 2)
	assert.Equal(t, uint32(1), response.Transactions[0].Ledger)
	assert.Equal(t, uint32(1), response.Transactions[1].Ledger)
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
			Cursor: toid.New(1, 2, 1).String(),
			Limit:  3,
		},
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	require.NoError(t, err)
	assert.Equal(t, uint32(10), response.LatestLedger)
	assert.Equal(t, int64(350), response.LatestLedgerCloseTime)
	assert.Equal(t, toid.New(3, 1, 1).String(), response.Cursor)
	assert.Len(t, response.Transactions, 3)
	assert.Equal(t, uint32(2), response.Transactions[0].Ledger)
	assert.Equal(t, uint32(2), response.Transactions[1].Ledger)
	assert.Equal(t, uint32(3), response.Transactions[2].Ledger)
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
		"[%d] start ledger (4) must be between the oldest ledger: 1 and the latest ledger: 3 for this rpc instance",
		jrpc2.InvalidRequest,
	)
	assert.Equal(t, expectedErr.Error(), err.Error())
	assert.Nil(t, response.Transactions)
}

func TestGetTransactions_LedgerNotFound(t *testing.T) {
	testDB := setupDB(t, 3, 2)
	handler := transactionsRPCHandler{
		ledgerReader:      db.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	request := protocol.GetTransactionsRequest{
		StartLedger: 1,
	}

	response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
	expectedErr := fmt.Errorf("[%d] database does not contain metadata for ledger: 2", jrpc2.InvalidParams)
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
		StartLedger: 1,
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
		StartLedger: 1,
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
