package ingest

import (
	"context"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/feewindow"
)

type ErrorReadWriter struct{}

func (rw *ErrorReadWriter) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	return 0, errors.New("could not get latest ledger sequence")
}

func (rw *ErrorReadWriter) NewTx(_ context.Context) (db.WriteTx, error) {
	return nil, errors.New("could not create new tx")
}

func TestRetryRunningIngestion(t *testing.T) {
	var retryWg sync.WaitGroup
	retryWg.Add(1)

	numRetries := 0
	var lastErr error
	incrementRetry := func(err error, _ time.Duration) {
		defer retryWg.Done()
		numRetries++
		lastErr = err
	}
	config := Config{
		Logger:            supportlog.New(),
		DB:                &ErrorReadWriter{},
		NetworkPassPhrase: "",
		Archive:           nil,
		LedgerBackend:     nil,
		Timeout:           time.Second,
		OnIngestionRetry:  incrementRetry,
		Daemon:            interfaces.MakeNoOpDeamon(),
	}
	service := NewService(config)
	StartService(service, config)
	retryWg.Wait()
	service.Close()
	assert.Equal(t, 1, numRetries)
	require.Error(t, lastErr)
	require.ErrorContains(t, lastErr, "could not get latest ledger sequence")
}

func TestIngestion(t *testing.T) {
	ctx := context.Background()
	mockDB, mockLedgerBackend, mockTx := setupMocks()
	service := setupService(mockDB, mockLedgerBackend)
	sequence := uint32(3)

	ledger := createTestLedger(t)
	setupMockExpectations(ctx, t, mockDB, mockLedgerBackend, mockTx, ledger, sequence)

	require.NoError(t, service.ingest(ctx, sequence))

	assertMockExpectations(t, mockDB, mockTx, mockLedgerBackend)
}

func setupMocks() (*MockDB, *ledgerbackend.MockDatabaseBackend, *MockTx) {
	mockDB := &MockDB{}
	mockLedgerBackend := &ledgerbackend.MockDatabaseBackend{}
	mockTx := &MockTx{}
	return mockDB, mockLedgerBackend, mockTx
}

func setupService(mockDB *MockDB, mockLedgerBackend *ledgerbackend.MockDatabaseBackend) *Service {
	daemon := interfaces.MakeNoOpDeamon()
	config := Config{
		Logger:            supportlog.New(),
		DB:                mockDB,
		FeeWindows:        feewindow.NewFeeWindows(1, 1, network.TestNetworkPassphrase, nil),
		LedgerBackend:     mockLedgerBackend,
		Daemon:            daemon,
		NetworkPassPhrase: network.TestNetworkPassphrase,
	}
	return newService(config)
}

func createTestLedger(t *testing.T) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader:       createLedgerHeader(),
			TxSet:              createTransactionSet(),
			TxProcessing:       createTransactionProcessing(t),
			UpgradesProcessing: []xdr.UpgradeEntryMeta{},
			EvictedKeys:        []xdr.LedgerKey{createEvictedTempLedgerKey()},
		},
	}
}

func createLedgerHeader() xdr.LedgerHeaderHistoryEntry {
	return xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{LedgerVersion: 10}}
}

func createTransactionSet() xdr.GeneralizedTransactionSet {
	firstTx := createFirstTransaction()
	baseFee := xdr.Int64(100)
	return xdr.GeneralizedTransactionSet{
		V: 1,
		V1TxSet: &xdr.TransactionSetV1{
			PreviousLedgerHash: xdr.Hash{1, 2, 3},
			Phases: []xdr.TransactionPhase{
				{
					V0Components: &[]xdr.TxSetComponent{
						{
							Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
							TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
								BaseFee: &baseFee,
								Txs:     []xdr.TransactionEnvelope{firstTx},
							},
						},
					},
				},
			},
		},
	}
}

func createFirstTransaction() xdr.TransactionEnvelope {
	src := xdr.MustAddress("GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON")
	return xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				Fee:           1,
				SourceAccount: src.ToMuxedAccount(),
			},
		},
	}
}

func createTransactionProcessing(t *testing.T) []xdr.TransactionResultMeta {
	firstTx := createFirstTransaction()
	firstTxHash, err := network.HashTransactionInEnvelope(firstTx, network.TestNetworkPassphrase)
	require.NoError(t, err)

	return []xdr.TransactionResultMeta{
		{
			Result: xdr.TransactionResultPair{
				TransactionHash: firstTxHash,
				Result: xdr.TransactionResult{
					Result: xdr.TransactionResultResult{
						Results: &[]xdr.OperationResult{},
					},
				},
			},
			FeeProcessing: xdr.LedgerEntryChanges{},
			TxApplyProcessing: xdr.TransactionMeta{
				V: 3,
				V3: &xdr.TransactionMetaV3{
					Operations: []xdr.OperationMeta{
						{
							Changes: createOperationChanges(),
						},
					},
				},
			},
		},
	}
}

func createOperationChanges() xdr.LedgerEntryChanges {
	contractAddress := createContractAddress()
	persistentKey := xdr.ScSymbol("TEMPVAL")

	return xdr.LedgerEntryChanges{
		createLedgerEntryState(contractAddress, persistentKey, true),
		createLedgerEntryUpdated(contractAddress, persistentKey, true),
	}
}

func createContractAddress() xdr.ScAddress {
	contractIDBytes, _ := hex.DecodeString("df06d62447fd25da07c0135eed7557e5a5497ee7d15b7fe345bd47e191d8f577")
	var contractID xdr.ContractId
	copy(contractID[:], contractIDBytes)
	return xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &contractID,
	}
}

func createLedgerEntryState(contractAddress xdr.ScAddress, key xdr.ScSymbol, value bool) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 1,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract:   contractAddress,
					Key:        xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &key},
					Durability: xdr.ContractDataDurabilityPersistent,
					Val:        xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &value},
				},
			},
		},
	}
}

func createLedgerEntryUpdated(contractAddress xdr.ScAddress, key xdr.ScSymbol, value bool) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
		Updated: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 1,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract:   contractAddress,
					Key:        xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &key},
					Durability: xdr.ContractDataDurabilityPersistent,
					Val:        xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &value},
				},
			},
		},
	}
}

func createEvictedTempLedgerKey() xdr.LedgerKey {
	contractAddress := createContractAddress()
	tempKey := xdr.ScSymbol("TEMPKEY")

	return xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract:   contractAddress,
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &tempKey},
			Durability: xdr.ContractDataDurabilityTemporary,
		},
	}
}

func setupMockExpectations(ctx context.Context, t *testing.T, mockDB *MockDB,
	mockLedgerBackend *ledgerbackend.MockDatabaseBackend, mockTx *MockTx, ledger xdr.LedgerCloseMeta, sequence uint32,
) {
	mockLedgerEntryWriter := &MockLedgerEntryWriter{}
	mockLedgerWriter := &MockLedgerWriter{}
	mockTxWriter := &MockTransactionWriter{}
	mockEventWriter := &MockEventWriter{}

	mockDB.On("NewTx", ctx).Return(mockTx, nil).Once()
	mockTx.On("Commit", ledger, mock.AnythingOfType("map[string]time.Duration")).Return(nil).Once()
	mockTx.On("Rollback").Return(nil).Once()
	mockTx.On("LedgerWriter").Return(mockLedgerWriter).Once()
	mockTx.On("TransactionWriter").Return(mockTxWriter).Once()
	mockTx.On("EventWriter").Return(mockEventWriter).Once()

	mockLedgerBackend.On("GetLedger", ctx, sequence).Return(ledger, nil).Once()

	setupLedgerEntryWriterExpectations(t, mockLedgerEntryWriter, ledger)
	mockLedgerWriter.On("InsertLedger", ledger).Return(nil).Once()
	mockTxWriter.On("InsertTransactions", ledger).Return(nil).Once()
	mockEventWriter.On("InsertEvents", ledger).Return(nil).Once()
}

func setupLedgerEntryWriterExpectations(t *testing.T, mockLedgerEntryWriter *MockLedgerEntryWriter,
	ledger xdr.LedgerCloseMeta,
) {
	operationChanges := ledger.V1.TxProcessing[0].TxApplyProcessing.V3.Operations[0].Changes
	mockLedgerEntryWriter.On("UpsertLedgerEntry", operationChanges[1].MustUpdated()).
		Return(nil).Once()

	evictedTempLedgerKey := ledger.V1.EvictedKeys[0]
	mockLedgerEntryWriter.On("DeleteLedgerEntry", evictedTempLedgerKey).
		Return(nil).Once()
}

func assertMockExpectations(t *testing.T, mockDB *MockDB, mockTx *MockTx,
	mockLedgerBackend *ledgerbackend.MockDatabaseBackend,
) {
	mockDB.AssertExpectations(t)
	mockTx.AssertExpectations(t)
	mockLedgerBackend.AssertExpectations(t)
}
