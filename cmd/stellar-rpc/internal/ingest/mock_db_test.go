package ingest

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"

	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

var (
	_ db.ReadWriter   = (*MockDB)(nil)
	_ db.WriteTx      = (*MockTx)(nil)
	_ db.LedgerWriter = (*MockLedgerWriter)(nil)
)

type MockDB struct {
	mock.Mock
}

func (m *MockDB) NewTx(ctx context.Context) (db.WriteTx, error) {
	args := m.Called(ctx)
	return args.Get(0).(db.WriteTx), args.Error(1) //nolint:forcetypeassert
}

func (m *MockDB) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	args := m.Called(ctx)
	return args.Get(0).(uint32), args.Error(1) //nolint:forcetypeassert
}

type MockTx struct {
	mock.Mock
}

func (m *MockTx) EventWriter() db.EventWriter {
	args := m.Called()
	eventWriter, ok := args.Get(0).(db.EventWriter)
	if !ok {
		return nil
	}
	return eventWriter
}

func (m *MockTx) LedgerWriter() db.LedgerWriter {
	args := m.Called()
	return args.Get(0).(db.LedgerWriter) //nolint:forcetypeassert
}

func (m *MockTx) TransactionWriter() db.TransactionWriter {
	args := m.Called()
	return args.Get(0).(db.TransactionWriter) //nolint:forcetypeassert
}

func (m *MockTx) Commit(ledgerCloseMeta xdr.LedgerCloseMeta) error {
	args := m.Called(ledgerCloseMeta)
	return args.Error(0)
}

func (m *MockTx) Rollback() error {
	args := m.Called()
	return args.Error(0)
}

type MockLedgerEntryWriter struct {
	mock.Mock
}

func (m *MockLedgerEntryWriter) UpsertLedgerEntry(entry xdr.LedgerEntry) error {
	args := m.Called(entry)
	return args.Error(0)
}

func (m *MockLedgerEntryWriter) DeleteLedgerEntry(key xdr.LedgerKey) error {
	args := m.Called(key)
	return args.Error(0)
}

type MockLedgerWriter struct {
	mock.Mock
}

func (m *MockLedgerWriter) InsertLedger(ledger xdr.LedgerCloseMeta) error {
	args := m.Called(ledger)
	return args.Error(0)
}

type MockTransactionWriter struct {
	mock.Mock
}

func (m *MockTransactionWriter) InsertTransactions(ledger xdr.LedgerCloseMeta) error {
	args := m.Called(ledger)
	return args.Error(0)
}

func (m *MockTransactionWriter) RegisterMetrics(ingest, count prometheus.Observer) {
	m.Called(ingest, count)
}

type MockEventWriter struct {
	mock.Mock
}

func (m *MockEventWriter) InsertEvents(ledger xdr.LedgerCloseMeta) error {
	args := m.Called(ledger)
	return args.Error(0)
}
