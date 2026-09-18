package sqlitedb

import (
	"context"
	"errors"
	"io"
	"iter"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

type MockTransactionHandler struct {
	passphrase string

	ledgerRange     store.LedgerRange
	txHashToMeta    map[string]*xdr.LedgerCloseMeta
	ledgerSeqToMeta map[uint32]*xdr.LedgerCloseMeta
}

func NewMockTransactionStore(passphrase string) *MockTransactionHandler {
	return &MockTransactionHandler{
		passphrase:      passphrase,
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

		txn.txHashToMeta[tx.Result.TransactionHash.HexString()] = &lcm
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
	return store.ParseTransactionView(txView), nil
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

func (m *MockLedgerReader) ScanLedgers(_ context.Context, start, end uint32) iter.Seq2[store.RawLedger, error] {
	return func(yield func(store.RawLedger, error) bool) {
		for seq := start; seq <= end; seq++ {
			lcm, ok := m.txn.ledgerSeqToMeta[seq]
			if !ok {
				continue
			}
			raw, err := lcm.MarshalBinary()
			if err != nil {
				yield(store.RawLedger{}, err)
				return
			}
			if !yield(store.RawLedger{Sequence: seq, Raw: raw}, nil) {
				return
			}
		}
	}
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
