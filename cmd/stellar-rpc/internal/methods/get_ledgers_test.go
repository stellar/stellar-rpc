package methods

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

var expectedLedgerInfo = protocol.LedgerInfo{
	Hash:            "0000000000000000000000000000000000000000000000000000000000000000",
	Sequence:        1,
	LedgerCloseTime: 125,
	LedgerHeader:    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB9AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",                                                                                                                                                                                                                                                                                                                                                                                                         //nolint:lll
	LedgerMetadata:  "AAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAH0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAEAAAACAAABAIAAAAAAAAAAPww0v5OtDZlx0EzMkPcFURyDiq2XNKSi+w16A/x/6JoAAAABAAAAAP///50AAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAEzmSAb0wlZuZ7vERyxkacbwbERSS/IM82EYhemLKdUAAAAAAAAABkAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==", //nolint:lll
}

func setupTestDB(t *testing.T, numLedgers int) *db.DB {
	testDB := NewTestDB(t)
	daemon := interfaces.MakeNoOpDeamon()
	for sequence := 1; sequence <= numLedgers; sequence++ {
		ledgerCloseMeta := txMeta(uint32(sequence)-100, true)
		tx, err := db.
			NewReadWriter(log.DefaultLogger, testDB, daemon, 150, 100, passphrase).
			NewTx(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
		require.NoError(t, tx.Commit(ledgerCloseMeta, nil))
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

	response, err := handler.getLedgers(t.Context(), request)
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

	response, err := handler.getLedgers(t.Context(), request)
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
	assert.Contains(t, err.Error(), "must be between the oldest ledger")
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
	assert.Contains(t, err.Error(), "cursor ('15') must be between")
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

	for b.Loop() {
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
		100, 1_000_000, passphrase)
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
	require.NoError(b, write.Commit(lcms[len(lcms)-1], nil))
	return testDB
}

func createLedgerCloseMeta(ledgerSeq uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(ledgerSeq),
				},
			},
		},
	}
}

func getLedgerRange(sequences []uint32) []xdr.LedgerCloseMeta {
	ledgers := make([]xdr.LedgerCloseMeta, 0, len(sequences))
	for _, seq := range sequences {
		ledgers = append(ledgers, createLedgerCloseMeta(seq))
	}
	return ledgers
}

func TestGetLedgers(t *testing.T) {
	localRange := ledgerbucketwindow.LedgerRange{
		FirstLedger: ledgerbucketwindow.LedgerInfo{Sequence: 100},
		LastLedger:  ledgerbucketwindow.LedgerInfo{Sequence: 200},
	}

	tests := []struct {
		name            string
		start           uint32
		end             uint32
		localRange      protocol.LedgerSeqRange
		expectLocal     []uint32
		expectDatastore []uint32
		expectedLedgers []uint32
	}{
		{
			name:            "LocalOnly",
			start:           150,
			end:             151,
			expectLocal:     []uint32{150, 151},
			expectedLedgers: []uint32{150, 151},
		},
		{
			name:            "DatastoreOnly",
			start:           50,
			end:             51,
			expectDatastore: []uint32{50, 51},
			expectedLedgers: []uint32{50, 51},
		},
		{
			name:            "PartialDatastoreAndLocal",
			start:           98,
			end:             102,
			expectDatastore: []uint32{98, 99},
			expectLocal:     []uint32{100, 101, 102},
			expectedLedgers: []uint32{98, 99, 100, 101, 102},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			mockReader := new(MockLedgerReader)
			mockStore := new(MockDatastoreReader)
			mockReaderTx := new(MockLedgerReaderTx)

			handler := ledgersHandler{
				ledgerReader:          mockReader,
				maxLimit:              100,
				defaultLimit:          5,
				datastoreLedgerReader: mockStore,
			}

			mockReader.On("NewTx", ctx).Return(mockReaderTx, nil)
			mockReaderTx.On("Done").Return(nil)
			mockReaderTx.On("GetLedgerRange", ctx).Return(localRange, nil)
			mockStore.On("GetAvailableLedgerRange", ctx).Return(protocol.LedgerSeqRange{
				FirstLedger: 2,
			}, nil)
			if len(tc.expectLocal) > 0 {
				ledgerChunks, err := metaToChunk(getLedgerRange(tc.expectLocal))
				require.NoError(t, err)
				mockReaderTx.On("BatchGetLedgers", ctx, tc.expectLocal[0], tc.expectLocal[len(tc.expectLocal)-1]).
					Return(ledgerChunks, nil)
			}

			if len(tc.expectDatastore) > 0 {
				mockStore.On("GetLedgers", ctx, tc.expectDatastore[0], tc.expectDatastore[len(tc.expectDatastore)-1]).
					Return(getLedgerRange(tc.expectDatastore), nil)
			}

			request := protocol.GetLedgersRequest{
				StartLedger: tc.start,
				Pagination:  &protocol.LedgerPaginationOptions{Limit: uint(tc.end - tc.start + 1)},
			}

			response, err := handler.getLedgers(ctx, request)
			require.NoError(t, err)
			require.Len(t, response.Ledgers, len(tc.expectedLedgers))

			for i, info := range response.Ledgers {
				require.Equal(t, tc.expectedLedgers[i], info.Sequence)
			}

			mockReader.AssertExpectations(t)
			mockReaderTx.AssertExpectations(t)
			mockStore.AssertExpectations(t)
		})
	}
}

func TestFetchLedgersErrors(t *testing.T) {
	ctx := t.Context()
	localRange := protocol.LedgerSeqRange{FirstLedger: 100, LastLedger: 200}

	t.Run("DB error", func(t *testing.T) {
		mockTx := new(MockLedgerReaderTx)
		mockTx.On("BatchGetLedgers", ctx, uint32(150), uint32(151)).
			Return([]db.LedgerMetadataChunk(nil), errors.New("db error"))

		handler := ledgersHandler{}
		_, err := handler.fetchLedgers(ctx, 150, 151, "default", mockTx, localRange)
		require.Error(t, err)
		require.Contains(t, err.Error(), "db error")
		mockTx.AssertExpectations(t)
	})

	t.Run("Datastore error", func(t *testing.T) {
		mockTx := new(MockLedgerReaderTx)
		mockStore := new(MockDatastoreReader)
		mockStore.On("GetLedgers", ctx, uint32(50), uint32(51)).
			Return([]xdr.LedgerCloseMeta(nil), errors.New("datastore error"))

		handler := ledgersHandler{
			datastoreLedgerReader: mockStore,
		}

		_, err := handler.fetchLedgers(ctx, 50, 51, "default", mockTx, localRange)
		require.Error(t, err)
		require.Contains(t, err.Error(), "datastore error")
		mockTx.AssertExpectations(t)
		mockStore.AssertExpectations(t)
	})

	t.Run("Datastore not configured", func(t *testing.T) {
		mockTx := new(MockLedgerReaderTx)
		handler := ledgersHandler{}

		_, err := handler.fetchLedgers(ctx, 50, 51, "default", mockTx, localRange)
		require.Error(t, err)
		require.Contains(t, err.Error(), "datastore ledger reader not configured")
		mockTx.AssertExpectations(t)
	})
}

// TestGetLedgers_EmptyBatchGetLedgersResult is a regression test that ensures
// when GetLedgerRange reports data but BatchGetLedgers returns an empty slice,
// getLedgers returns an empty page with a stable cursor and does not panic.
func TestGetLedgers_EmptyBatchGetLedgersResult(t *testing.T) {
	ctx := t.Context()

	t.Run("empty result with cursor", func(t *testing.T) {
		mockReader := new(MockLedgerReader)
		mockReaderTx := new(MockLedgerReaderTx)

		handler := ledgersHandler{
			ledgerReader: mockReader,
			maxLimit:     100,
			defaultLimit: 5,
		}

		localRange := ledgerbucketwindow.LedgerRange{
			FirstLedger: ledgerbucketwindow.LedgerInfo{Sequence: 100},
			LastLedger:  ledgerbucketwindow.LedgerInfo{Sequence: 200},
		}

		mockReader.On("NewTx", ctx).Return(mockReaderTx, nil)
		mockReaderTx.On("Done").Return(nil)
		mockReaderTx.On("GetLedgerRange", ctx).Return(localRange, nil)
		// BatchGetLedgers returns empty slice even though GetLedgerRange indicates data exists
		mockReaderTx.On("BatchGetLedgers", ctx, uint32(151), uint32(155)).
			Return([]db.LedgerMetadataChunk{}, nil)

		request := protocol.GetLedgersRequest{
			Pagination: &protocol.LedgerPaginationOptions{
				Cursor: "150",
				Limit:  5,
			},
		}

		response, err := handler.getLedgers(ctx, request)
		require.NoError(t, err)
		assert.Empty(t, response.Ledgers)
		// Cursor should echo the request cursor for stability
		assert.Equal(t, "150", response.Cursor)
		assert.Equal(t, uint32(200), response.LatestLedger)

		mockReader.AssertExpectations(t)
		mockReaderTx.AssertExpectations(t)
	})

	t.Run("empty result without cursor", func(t *testing.T) {
		mockReader := new(MockLedgerReader)
		mockReaderTx := new(MockLedgerReaderTx)

		handler := ledgersHandler{
			ledgerReader: mockReader,
			maxLimit:     100,
			defaultLimit: 5,
		}

		localRange := ledgerbucketwindow.LedgerRange{
			FirstLedger: ledgerbucketwindow.LedgerInfo{Sequence: 100},
			LastLedger:  ledgerbucketwindow.LedgerInfo{Sequence: 200},
		}

		mockReader.On("NewTx", ctx).Return(mockReaderTx, nil)
		mockReaderTx.On("Done").Return(nil)
		mockReaderTx.On("GetLedgerRange", ctx).Return(localRange, nil)
		// BatchGetLedgers returns empty slice even though GetLedgerRange indicates data exists
		mockReaderTx.On("BatchGetLedgers", ctx, uint32(100), uint32(104)).
			Return([]db.LedgerMetadataChunk{}, nil)

		request := protocol.GetLedgersRequest{
			StartLedger: 100,
		}

		response, err := handler.getLedgers(ctx, request)
		require.NoError(t, err)
		assert.Empty(t, response.Ledgers)
		// Cursor should be start-1 (99) when no cursor provided and no results
		assert.Equal(t, "99", response.Cursor)
		assert.Equal(t, uint32(200), response.LatestLedger)

		mockReader.AssertExpectations(t)
		mockReaderTx.AssertExpectations(t)
	})
}
