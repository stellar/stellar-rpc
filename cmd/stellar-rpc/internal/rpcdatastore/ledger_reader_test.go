package rpcdatastore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
)

type mockLedgerBackend struct {
	mock.Mock
}

var _ ledgerbackend.LedgerBackend = (*mockLedgerBackend)(nil)

func (m *mockLedgerBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint32), args.Error(1) //nolint:forcetypeassert
}

func (m *mockLedgerBackend) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	args := m.Called(ctx, seq)
	return args.Get(0).(xdr.LedgerCloseMeta), args.Error(1) //nolint:forcetypeassert
}

func (m *mockLedgerBackend) PrepareRange(ctx context.Context, r ledgerbackend.Range) error {
	args := m.Called(ctx, r)
	return args.Error(0)
}

func (m *mockLedgerBackend) IsPrepared(ctx context.Context, r ledgerbackend.Range) (bool, error) {
	args := m.Called(ctx, r)
	return args.Bool(0), args.Error(1)
}

func (m *mockLedgerBackend) Close() error {
	args := m.Called()
	return args.Error(0)
}

func createLedgerCloseMeta(ledgerSeq uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(ledgerSeq),
				},
			},
		},
		V1: nil,
	}
}

type mockBackendFactory struct {
	mock.Mock
}

func (m *mockBackendFactory) NewBufferedBackend(cfg ledgerbackend.BufferedStorageBackendConfig,
	ds datastore.DataStore,
	schema datastore.DataStoreSchema,
) (ledgerbackend.LedgerBackend, error) {
	args := m.Called(cfg, ds, schema)
	return args.Get(0).(ledgerbackend.LedgerBackend), args.Error(1) //nolint:forcetypeassert
}

func TestLedgerReaderGetLedgers(t *testing.T) {
	ctx := t.Context()

	mockBackend := new(mockLedgerBackend)
	mockDatastore := new(datastore.MockDataStore)
	mockFactory := mockBackendFactory{}
	start := uint32(100)
	end := uint32(102)

	var expected []xdr.LedgerCloseMeta
	for seq := start; seq <= end; seq++ {
		meta := createLedgerCloseMeta(seq)
		mockBackend.On("GetLedger", ctx, seq).Return(meta, nil)
		expected = append(expected, meta)
	}
	bsbConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 10,
		NumWorkers: 1,
	}
	mockBackend.On("PrepareRange", ctx, ledgerbackend.BoundedRange(start, end)).Return(nil)
	mockBackend.On("Close").Return(nil)
	mockFactory.On("NewBufferedBackend", bsbConfig, mockDatastore, datastore.DataStoreSchema{}).Return(mockBackend, nil)

	reader := &ledgerReader{
		storageBackendConfig: bsbConfig,
		dataStore:            mockDatastore,
		ledgerBackendFactory: &mockFactory,
	}

	ledgers, err := reader.GetLedgers(ctx, start, end)
	require.NoError(t, err)
	require.Equal(t, expected, ledgers)

	mockBackend.AssertExpectations(t)
}
