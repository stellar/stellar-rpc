package sqlitedb

import (
	"context"
	"errors"
	"io"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

type MockTransactionHandler struct {
	passphrase string

	ledgerRange     store.LedgerRange
	txs             map[string]ingest.LedgerTransaction
	txHashToMeta    map[string]*xdr.LedgerCloseMeta
	ledgerSeqToMeta map[uint32]*xdr.LedgerCloseMeta
}

func NewMockTransactionStore(passphrase string) *MockTransactionHandler {
	return &MockTransactionHandler{
		passphrase:      passphrase,
		txs:             make(map[string]ingest.LedgerTransaction),
		txHashToMeta:    make(map[string]*xdr.LedgerCloseMeta),
		ledgerSeqToMeta: make(map[uint32]*xdr.LedgerCloseMeta),
	}
}

func (txn *MockTransactionHandler) InsertTransactions(lcm xdr.LedgerCloseMeta) error {
	txn.ledgerSeqToMeta[lcm.LedgerSequence()] = &lcm

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(txn.passphrase, lcm)
	if err != nil {
		return err
	}

	for {
		tx, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}

		h := tx.Result.TransactionHash.HexString()
		txn.txs[h] = tx
		txn.txHashToMeta[h] = &lcm
	}

	if lcmSeq := lcm.LedgerSequence(); lcmSeq < txn.ledgerRange.FirstLedger.Sequence ||
		txn.ledgerRange.FirstLedger.Sequence == 0 {
		txn.ledgerRange.FirstLedger.Sequence = lcmSeq
		txn.ledgerRange.FirstLedger.CloseTime = lcm.LedgerCloseTime()
	}

	if lcmSeq := lcm.LedgerSequence(); lcmSeq > txn.ledgerRange.LastLedger.Sequence {
		txn.ledgerRange.LastLedger.Sequence = lcmSeq
		txn.ledgerRange.LastLedger.CloseTime = lcm.LedgerCloseTime()
	}

	return nil
}

func (txn *MockTransactionHandler) GetTransaction(_ context.Context, hash xdr.Hash) (
	store.Transaction, error,
) {
	lcm, ok := txn.txHashToMeta[hash.HexString()]
	if !ok {
		return store.Transaction{}, store.ErrNoTransaction
	}
	raw, err := lcm.MarshalBinary()
	if err != nil {
		return store.Transaction{}, err
	}
	txView, found, err := ingest.LedgerTransactionViewByHash(xdr.LedgerCloseMetaView(raw), hash, txn.passphrase)
	if err != nil {
		return store.Transaction{}, err
	}
	if !found {
		return store.Transaction{}, store.ErrNoTransaction
	}
	return store.ParseTransaction(txView), nil
}

func (txn *MockTransactionHandler) RegisterMetrics(_, _ prometheus.Observer) {}

type MockLedgerReader struct {
	txn *MockTransactionHandler
}

func NewMockLedgerReader(txn *MockTransactionHandler) *MockLedgerReader {
	return &MockLedgerReader{
		txn: txn,
	}
}

func (m *MockLedgerReader) GetLedger(_ context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	lcm, ok := m.txn.ledgerSeqToMeta[sequence]
	if !ok {
		return xdr.LedgerCloseMeta{}, false, nil
	}
	return *lcm, true, nil
}

func (m *MockLedgerReader) WithLedgerRaw(_ context.Context, sequence uint32, fn store.WithLedgerRawFn) (bool, error) {
	lcm, ok := m.txn.ledgerSeqToMeta[sequence]
	if !ok {
		return false, nil
	}
	rawMeta, err := lcm.MarshalBinary()
	if err != nil {
		return false, err
	}
	return true, fn(rawMeta)
}

func (m *MockLedgerReader) StreamLedgerRange(_ context.Context, _ uint32, _ uint32, _ store.StreamLedgerFn) error {
	return nil
}

func (m *MockLedgerReader) GetLedgerRange(_ context.Context) (store.LedgerRange, error) {
	return m.txn.ledgerRange, nil
}

func (m *MockLedgerReader) GetLatestLedgerSequence(_ context.Context) (uint32, error) {
	return 0, nil
}

func (m *MockLedgerReader) NewTx(_ context.Context) (store.LedgerReaderTx, error) {
	return nil, errors.New("mock NewTx error")
}

func (m *MockLedgerReader) GetLedgerCountInRange(_ context.Context,
	_ uint32,
	_ uint32,
) (uint32, uint32, uint32, error) {
	return 0, 0, 0, nil
}

var (
	_ store.TransactionReader = &MockTransactionHandler{}
	_ TransactionWriter       = &MockTransactionHandler{}
	_ LedgerReader            = &MockLedgerReader{}
)
