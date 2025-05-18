package methods

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/datastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

var (
	_ db.LedgerReaderTx      = &MockLedgerReaderTx{}
	_ datastore.LedgerReader = &MockDatastoreReader{}
)

type MockLedgerReaderTx struct {
	mock.Mock
}

func (m *MockLedgerReaderTx) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	args := m.Called(ctx)
	return args.Get(0).(ledgerbucketwindow.LedgerRange), args.Error(1) //nolint:forcetypeassert
}

func (m *MockLedgerReaderTx) BatchGetLedgers(ctx context.Context, start, end uint32) ([]xdr.LedgerCloseMeta, error) {
	args := m.Called(ctx, start, end)
	return args.Get(0).([]xdr.LedgerCloseMeta), args.Error(1) //nolint:forcetypeassert
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

func (m *MockDatastoreReader) GetLedgers(ctx context.Context, start, end uint32) ([]xdr.LedgerCloseMeta, error) {
	args := m.Called(ctx, start, end)
	return args.Get(0).([]xdr.LedgerCloseMeta), args.Error(1) //nolint:forcetypeassert
}
