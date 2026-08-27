package methods

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

const (
	NetworkPassphrase string = "passphrase"
)

var expectedTransactionInfo = protocol.TransactionInfo{
	TransactionDetails: protocol.TransactionDetails{
		Status:              "SUCCESS",
		TransactionHash:     "04ce64806f4c2566e67bbc4472c6469c6f06c44524bf20cf3611885e98b29d50",
		ApplicationOrder:    1,
		FeeBump:             false,
		Ledger:              1,
		EnvelopeXDR:         "AAAAAgAAAQCAAAAAAAAAAD8MNL+TrQ2ZcdBMzJD3BVEcg4qtlzSkovsNegP8f+iaAAAAAQAAAAD///+dAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==", //nolint:lll
		ResultMetaXDR:       "AAAAAwAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAABAAAAAA==",
		ResultXDR:           "AAAAAAAAAGQAAAAAAAAAAAAAAAA=",
		DiagnosticEventsXDR: []string{},
		Events: protocol.Events{
			ContractEventsXDR:    [][]string{{}},
			TransactionEventsXDR: []string{},
		},
	},
	LedgerCloseTime: 125,
}

func TestGetTransactions_DefaultLimit(t *testing.T) { //nolint:dupl
	testDB := setupDB(t, 10, 0)
	handler := transactionsRPCHandler{
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
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
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
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
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
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
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
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

func TestGetTransactions_CaughtUpCursorIsEchoed(t *testing.T) {
	cursors := map[string]string{
		"above the tip":          toid.New(15, 1, 1).String(),
		"at the consumed tip":    toid.New(10, 2, 1).String(),
		"past the tip's last tx": toid.New(10, 5, 1).String(),
	}
	for name, cursor := range cursors {
		t.Run(name, func(t *testing.T) {
			testDB := setupDB(t, 10, 0)
			handler := transactionsRPCHandler{
				ledgerReader:      sqlitedb.NewLedgerReader(testDB),
				maxLimit:          100,
				defaultLimit:      10,
				networkPassphrase: NetworkPassphrase,
			}

			request := protocol.GetTransactionsRequest{
				Pagination: &protocol.LedgerPaginationOptions{
					Cursor: cursor,
				},
			}

			response, err := handler.getTransactionsByLedgerSequence(context.TODO(), request)
			require.NoError(t, err)
			assert.Empty(t, response.Transactions)
			assert.Equal(t, cursor, response.Cursor)
		})
	}
}

func TestGetTransactions_InvalidStartLedger(t *testing.T) {
	testDB := setupDB(t, 3, 0)
	handler := transactionsRPCHandler{
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
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
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
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
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
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
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
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
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
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

	var tx map[string]any
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
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
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
			// The envelope is soroban (Ext V1), so its V3 meta must carry
			// SorobanMeta, as on the real network.
			V3: &xdr.TransactionMetaV3{SorobanMeta: &xdr.SorobanTransactionMeta{
				ReturnValue: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			}},
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

func setupDB(t *testing.T, numLedgers int, skipLedger int) *sqlitedb.DB {
	testDB := NewTestDB(t)
	daemon := host.MakeNoOpDaemon()
	for sequence := 1; sequence <= numLedgers; sequence++ {
		if sequence == skipLedger {
			continue
		}
		ledgerCloseMeta := createTestLedger(uint32(sequence))
		tx, err := sqlitedb.NewReadWriter(log.DefaultLogger, testDB, daemon, 100, passphrase).NewTx(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
		require.NoError(t, tx.Commit(ledgerCloseMeta, nil))
	}
	return testDB
}

func setupDBNoTxs(t *testing.T, numLedgers int) *sqlitedb.DB {
	testDB := NewTestDB(t)
	daemon := host.MakeNoOpDaemon()
	for sequence := 1; sequence <= numLedgers; sequence++ {
		ledgerCloseMeta := createEmptyTestLedger(uint32(sequence))

		tx, err := sqlitedb.NewReadWriter(log.DefaultLogger, testDB, daemon, 100, passphrase).NewTx(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
		require.NoError(t, tx.Commit(ledgerCloseMeta, nil))
	}
	return testDB
}

// sparseLedgerReader serves an arbitrarily wide range of empty ledgers,
// counting point reads, so a test can observe how far the handler walks.
type sparseLedgerReader struct {
	latest uint32
	gets   int
}

func (r *sparseLedgerReader) GetLedger(_ context.Context, seq uint32) (xdr.LedgerCloseMeta, bool, error) {
	r.gets++
	return createEmptyTestLedger(seq), true, nil
}

func (r *sparseLedgerReader) WithLedgerRaw(
	_ context.Context, seq uint32, fn store.WithLedgerRawFn,
) (bool, error) {
	r.gets++
	raw, err := createEmptyTestLedger(seq).MarshalBinary()
	if err != nil {
		return false, err
	}
	return true, fn(raw)
}

func (r *sparseLedgerReader) GetLedgerRange(context.Context) (store.LedgerRange, error) {
	return store.LedgerRange{
		FirstLedger: store.LedgerInfo{Sequence: 1, CloseTime: 100},
		LastLedger:  store.LedgerInfo{Sequence: r.latest, CloseTime: 200},
	}, nil
}

func (r *sparseLedgerReader) BatchGetLedgers(context.Context, uint32, uint32) ([]store.LedgerMetadataChunk, error) {
	return nil, nil
}

func (r *sparseLedgerReader) StreamLedgerRange(context.Context, uint32, uint32, store.StreamLedgerFn) error {
	return nil
}

func (r *sparseLedgerReader) GetLatestLedgerSequence(context.Context) (uint32, error) {
	return r.latest, nil
}

func (r *sparseLedgerReader) NewTx(context.Context) (store.LedgerReaderTx, error) { return r, nil }

func (r *sparseLedgerReader) Done() error { return nil }

func TestGetTransactions_SparseRangeCapsAtLedgerScanLimit(t *testing.T) {
	reader := &sparseLedgerReader{latest: 50_000}
	handler := transactionsRPCHandler{
		ledgerReader:      reader,
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}

	response, err := handler.getTransactionsByLedgerSequence(
		context.TODO(), protocol.GetTransactionsRequest{StartLedger: 1})
	require.NoError(t, err)

	assert.Empty(t, response.Transactions)
	assert.Equal(t, LedgerScanLimit, reader.gets, "the walk stops at the scan limit, not the latest ledger")
	assert.Equal(t, toid.New(LedgerScanLimit, 0, 1).String(), response.Cursor,
		"the cursor points at the last scanned ledger so the client can page on")
	assert.Equal(t, uint32(50_000), response.LatestLedger)
}
