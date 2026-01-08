package rpcdatastore

import (
	"context"
	"fmt"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

var (
	testSchema = datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 1,
	}
	testBSBConfig = ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 10,
		NumWorkers: 1,
	}
)

type mockLedgerBackend struct{ mock.Mock }

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

type mockBackendFactory struct{ mock.Mock }

func (m *mockBackendFactory) NewBufferedBackend(
	cfg ledgerbackend.BufferedStorageBackendConfig,
	ds datastore.DataStore,
	schema datastore.DataStoreSchema,
) (ledgerbackend.LedgerBackend, error) {
	args := m.Called(cfg, ds, schema)
	return args.Get(0).(ledgerbackend.LedgerBackend), args.Error(1) //nolint:forcetypeassert
}

func lcm(seq uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(seq),
				},
			},
		},
	}
}

func newReader(t *testing.T, ds datastore.DataStore, factory *mockBackendFactory, cacheSize int) *ledgerReader {
	t.Helper()

	var cache *lru.Cache[uint32, xdr.LedgerCloseMeta]
	if cacheSize > 0 {
		var err error
		cache, err = lru.New[uint32, xdr.LedgerCloseMeta](cacheSize)
		require.NoError(t, err)
	}

	return &ledgerReader{
		storageBackendConfig: testBSBConfig,
		dataStore:            ds,
		ledgerBackendFactory: factory,
		ledgerCache:          cache,
		schema:               testSchema,
	}
}

func seedRange(r *ledgerReader, first, last uint32) {
	r.cachedRange = protocol.LedgerSeqRange{FirstLedger: first, LastLedger: last}
	r.cachedRangeAt = time.Now()
	r.rangeTTL = 24 * time.Hour
}

func expectBatchFetch(
	t *testing.T,
	ctx context.Context,
	backend *mockLedgerBackend,
	start, end uint32,
) []xdr.LedgerCloseMeta {
	t.Helper()

	backend.On("PrepareRange", ctx, ledgerbackend.BoundedRange(start, end)).
		Return(nil).Once()

	expected := make([]xdr.LedgerCloseMeta, 0, end-start+1)
	for seq := start; seq <= end; seq++ {
		meta := lcm(seq)
		backend.On("GetLedger", ctx, seq).Return(meta, nil).Once()
		expected = append(expected, meta)
	}
	return expected
}

func TestLedgerReaderGetLedgers(t *testing.T) {
	ctx := t.Context()

	ds := new(datastore.MockDataStore)
	backend := new(mockLedgerBackend)
	factory := new(mockBackendFactory)

	start := uint32(100)
	end := uint32(102)

	expected := expectBatchFetch(t, ctx, backend, start, end)
	backend.On("Close").Return(nil).Once()
	factory.On("NewBufferedBackend", testBSBConfig, ds, datastore.DataStoreSchema{}).Return(backend, nil).Once()

	reader := &ledgerReader{
		storageBackendConfig: testBSBConfig,
		dataStore:            ds,
		ledgerBackendFactory: factory,
	}

	got, err := reader.GetLedgers(ctx, start, end)
	require.NoError(t, err)
	require.Equal(t, expected, got)

	backend.AssertExpectations(t)
	factory.AssertExpectations(t)
}

func TestLedgerReaderGetLedgerCached_CacheHitNoBackendCalls(t *testing.T) {
	ctx := t.Context()

	ds := new(datastore.MockDataStore)
	factory := new(mockBackendFactory)

	cache, err := lru.New[uint32, xdr.LedgerCloseMeta](100)
	require.NoError(t, err)

	const seq = uint32(123)
	expected := lcm(seq)
	cache.Add(seq, expected)

	reader := &ledgerReader{
		storageBackendConfig: testBSBConfig,
		dataStore:            ds,
		ledgerBackendFactory: factory,
		ledgerCache:          cache,
	}

	got, err := reader.GetLedgerCached(ctx, seq)
	require.NoError(t, err)
	require.Equal(t, expected, got)

	factory.AssertNotCalled(t, "NewBufferedBackend", mock.Anything, mock.Anything, mock.Anything)
}

func TestLedgerReaderGetLedgers_FactoryError(t *testing.T) {
	ctx := t.Context()

	ds := new(datastore.MockDataStore)
	factory := new(mockBackendFactory)

	expectedErr := fmt.Errorf("factory failed")
	factory.On("NewBufferedBackend", testBSBConfig, ds, datastore.DataStoreSchema{}).
		Return((*mockLedgerBackend)(nil), expectedErr).Once()

	reader := &ledgerReader{
		storageBackendConfig: testBSBConfig,
		dataStore:            ds,
		ledgerBackendFactory: factory,
	}

	_, err := reader.GetLedgers(ctx, 100, 102)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)

	factory.AssertExpectations(t)
}

func TestLedgerReaderGetLedgers_PrepareRangeError_ClosesBackend(t *testing.T) {
	ctx := t.Context()

	ds := new(datastore.MockDataStore)
	backend := new(mockLedgerBackend)
	factory := new(mockBackendFactory)

	start := uint32(100)
	end := uint32(102)

	expectedErr := fmt.Errorf("prepare failed")
	backend.On("PrepareRange", ctx, ledgerbackend.BoundedRange(start, end)).Return(expectedErr).Once()
	backend.On("Close").Return(nil).Once()

	factory.On("NewBufferedBackend", testBSBConfig, ds, datastore.DataStoreSchema{}).
		Return(backend, nil).Once()

	reader := &ledgerReader{
		storageBackendConfig: testBSBConfig,
		dataStore:            ds,
		ledgerBackendFactory: factory,
	}

	_, err := reader.GetLedgers(ctx, start, end)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)

	backend.AssertExpectations(t)
	factory.AssertExpectations(t)
}

func TestLedgerReaderGetLedgers_GetLedgerError_ClosesBackend(t *testing.T) {
	ctx := t.Context()

	ds := new(datastore.MockDataStore)
	backend := new(mockLedgerBackend)
	factory := new(mockBackendFactory)

	start := uint32(100)
	end := uint32(102)

	expectedErr := fmt.Errorf("get ledger failed")
	backend.On("PrepareRange", ctx, ledgerbackend.BoundedRange(start, end)).Return(nil).Once()
	backend.On("GetLedger", ctx, uint32(100)).Return(lcm(100), nil).Once()
	backend.On("GetLedger", ctx, uint32(101)).Return(xdr.LedgerCloseMeta{}, expectedErr).Once()
	backend.On("Close").Return(nil).Once()

	factory.On("NewBufferedBackend", testBSBConfig, ds, datastore.DataStoreSchema{}).
		Return(backend, nil).Once()

	reader := &ledgerReader{
		storageBackendConfig: testBSBConfig,
		dataStore:            ds,
		ledgerBackendFactory: factory,
	}

	_, err := reader.GetLedgers(ctx, start, end)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)

	backend.AssertExpectations(t)
	factory.AssertExpectations(t)
}

func TestLedgerReaderGetLedgerCached_BatchAndCache(t *testing.T) {
	ctx := t.Context()

	ds := new(datastore.MockDataStore)
	backend := new(mockLedgerBackend)
	factory := new(mockBackendFactory)

	start := uint32(100)
	end := uint32(102)

	// Batch fetch expectation
	expectedBatch := expectBatchFetch(t, ctx, backend, start, end)
	backend.On("Close").Return(nil).Once()
	factory.On("NewBufferedBackend", testBSBConfig, ds, testSchema).Return(backend, nil).Once()

	reader := newReader(t, ds, factory, 100)

	// Seed range so GetAvailableLedgerRange doesn't hit datastore internals.
	seedRange(reader, start, end)

	// First call triggers batch fetch.
	got, err := reader.GetLedgerCached(ctx, start)
	require.NoError(t, err)
	require.Equal(t, expectedBatch[0], got)

	// Subsequent calls should hit cache (backend GetLedger already constrained by .Once()).
	for seq := start; seq <= end; seq++ {
		got, err := reader.GetLedgerCached(ctx, seq)
		require.NoError(t, err)
		require.Equal(t, expectedBatch[seq-start], got)
	}

	backend.AssertExpectations(t)
	factory.AssertExpectations(t)
}

func TestLedgerReaderGetLedgerCached_GetLedgerError(t *testing.T) {
	ctx := t.Context()

	ds := new(datastore.MockDataStore)
	backend := new(mockLedgerBackend)
	factory := new(mockBackendFactory)

	start := uint32(100)
	end := uint32(119) // window

	expectedErr := fmt.Errorf("ledger fetch failed")

	backend.On("PrepareRange", ctx, ledgerbackend.BoundedRange(start, end)).Return(nil).Once()
	backend.On("GetLedger", ctx, start).Return(xdr.LedgerCloseMeta{}, expectedErr).Once()
	backend.On("Close").Return(nil).Once()

	factory.On("NewBufferedBackend", testBSBConfig, ds, testSchema).Return(backend, nil).Once()

	reader := newReader(t, ds, factory, 0)
	seedRange(reader, start, end)

	_, err := reader.GetLedgerCached(ctx, start)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)

	backend.AssertExpectations(t)
	factory.AssertExpectations(t)
}

func TestLedgerReaderGetLedgerCached_NewWindowTriggersSecondBatch(t *testing.T) {
	ctx := t.Context()

	ds := new(datastore.MockDataStore)
	backend := new(mockLedgerBackend)
	factory := new(mockBackendFactory)

	start := uint32(100)
	nextWindowStart := start + windowSize

	// Expect full-window fetch for each window.
	expectBatchFetch(t, ctx, backend, start, start+windowSize-1)
	expectBatchFetch(t, ctx, backend, nextWindowStart, nextWindowStart+windowSize-1)

	// One backend per batch (adjust if your implementation reuses).
	backend.On("Close").Return(nil).Twice()
	factory.On("NewBufferedBackend", testBSBConfig, ds, testSchema).Return(backend, nil).Twice()

	reader := newReader(t, ds, factory, 100)
	seedRange(reader, start, nextWindowStart+windowSize-1)

	_, err := reader.GetLedgerCached(ctx, start)
	require.NoError(t, err)

	_, err = reader.GetLedgerCached(ctx, nextWindowStart)
	require.NoError(t, err)

	backend.AssertExpectations(t)
	factory.AssertExpectations(t)
}
