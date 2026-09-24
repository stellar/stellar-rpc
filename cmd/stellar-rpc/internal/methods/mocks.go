package methods

import (
	"context"
	"iter"

	"github.com/stretchr/testify/mock"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcdatastore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

var (
	_ store.LedgerReader        = &MockLedgerReader{}
	_ store.LedgerReaderTx      = &MockLedgerReaderTx{}
	_ rpcdatastore.LedgerReader = &MockDatastoreReader{}
)

type MockLedgerReader struct {
	mock.Mock
}

// GetLedger is the mock's stubbing surface; it is not on store.LedgerReader.
func (m *MockLedgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	args := m.Called(ctx, sequence)
	return args.Get(0).(xdr.LedgerCloseMeta), args.Bool(1), args.Error(2) //nolint:forcetypeassert
}

// ScanLedgers routes each sequence through GetLedger, so tests keep stubbing by sequence.
func (m *MockLedgerReader) ScanLedgers(ctx context.Context, start, end uint32) iter.Seq2[store.RawLedger, error] {
	return store.ScanLedgersFrom(start, end, func(seq uint32) (xdr.LedgerCloseMeta, bool, error) {
		return m.GetLedger(ctx, seq)
	})
}

func (m *MockLedgerReader) GetLedgerRange(ctx context.Context) (store.LedgerRange, error) {
	args := m.Called(ctx)
	return args.Get(0).(store.LedgerRange), args.Error(1) //nolint:forcetypeassert
}

func (m *MockLedgerReader) NewTx(ctx context.Context) (store.LedgerReaderTx, error) {
	args := m.Called(ctx)
	return args.Get(0).(store.LedgerReaderTx), args.Error(1) //nolint:forcetypeassert
}

func (m *MockLedgerReader) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint32), args.Error(1) //nolint:forcetypeassert
}

type MockLedgerReaderTx struct {
	mock.Mock
}

func (m *MockLedgerReaderTx) GetLedgerRange(ctx context.Context) (store.LedgerRange, error) {
	args := m.Called(ctx)
	return args.Get(0).(store.LedgerRange), args.Error(1) //nolint:forcetypeassert
}

// ScanLedgers returns the stubbed ledgers as a stream; a non-nil error is yielded alone.
func (m *MockLedgerReaderTx) ScanLedgers(
	ctx context.Context, start, end uint32,
) iter.Seq2[store.RawLedger, error] {
	args := m.Called(ctx, start, end)
	ledgers := args.Get(0).([]store.RawLedger) //nolint:forcetypeassert
	err := args.Error(1)
	return func(yield func(store.RawLedger, error) bool) {
		for _, l := range ledgers {
			if !yield(l, nil) {
				return
			}
		}
		if err != nil {
			yield(store.RawLedger{}, err)
		}
	}
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
