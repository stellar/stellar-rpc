package methods

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcdatastore"
	"github.com/stellar/stellar-rpc/protocol"
)

var (
	_ db.LedgerReader           = &MockLedgerReader{}
	_ db.LedgerReaderTx         = &MockLedgerReaderTx{}
	_ rpcdatastore.LedgerReader = &MockDatastoreReader{}
)

type MockLedgerReader struct {
	mock.Mock
}

func (m *MockLedgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	args := m.Called(ctx, sequence)
	return args.Get(0).(xdr.LedgerCloseMeta), args.Bool(1), args.Error(2) //nolint:forcetypeassert
}

func (m *MockLedgerReader) StreamAllLedgers(ctx context.Context, f db.StreamLedgerFn) error {
	args := m.Called(ctx, f)
	return args.Error(0)
}

func (m *MockLedgerReader) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	args := m.Called(ctx)
	return args.Get(0).(ledgerbucketwindow.LedgerRange), args.Error(1) //nolint:forcetypeassert
}

func (m *MockLedgerReader) StreamLedgerRange(ctx context.Context, startLedger, endLedger uint32,
	f db.StreamLedgerFn,
) error {
	args := m.Called(ctx, startLedger, endLedger, f)
	return args.Error(0)
}

func (m *MockLedgerReader) NewTx(ctx context.Context) (db.LedgerReaderTx, error) {
	args := m.Called(ctx)
	return args.Get(0).(db.LedgerReaderTx), args.Error(1) //nolint:forcetypeassert
}

func (m *MockLedgerReader) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint32), args.Error(1) //nolint:forcetypeassert
}

type MockLedgerReaderTx struct {
	mock.Mock
}

func (m *MockLedgerReaderTx) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	args := m.Called(ctx)
	return args.Get(0).(ledgerbucketwindow.LedgerRange), args.Error(1) //nolint:forcetypeassert
}

func (m *MockLedgerReaderTx) BatchGetLedgers(ctx context.Context, start, end uint32) ([]db.LedgerMetadataChunk, error) {
	args := m.Called(ctx, start, end)
	return args.Get(0).([]db.LedgerMetadataChunk), args.Error(1) //nolint:forcetypeassert
}

func (m *MockLedgerReaderTx) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	args := m.Called(ctx, sequence)
	return args.Get(0).(xdr.LedgerCloseMeta), args.Bool(1), args.Error(2) //nolint:forcetypeassert
}

func (m *MockLedgerReaderTx) Done() error {
	args := m.Called()
	return args.Error(0)
}

type MockDatastoreReader struct {
	mock.Mock
}

func (m *MockDatastoreReader) GetAvailableLedgerRange(ctx context.Context) (protocol.LedgerSeqRange, error) {
	args := m.Called(ctx)
	return args.Get(0).(protocol.LedgerSeqRange), args.Error(1) //nolint:forcetypeassert
}

func (m *MockDatastoreReader) GetLedgers(ctx context.Context, start, end uint32) ([]xdr.LedgerCloseMeta, error) {
	args := m.Called(ctx, start, end)
	return args.Get(0).([]xdr.LedgerCloseMeta), args.Error(1) //nolint:forcetypeassert
}
