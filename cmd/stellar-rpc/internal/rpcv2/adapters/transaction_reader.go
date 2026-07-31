package adapters

import (
	"context"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// TransactionReader satisfies store.TransactionReader over the query router:
// each GetTransaction probes the hot tx-hash indexes and then the frozen window
// indexes through one read view, verifies candidates against the full hash, and
// gates the result to the view's servable window.
type TransactionReader struct {
	registry   *query.Registry
	passphrase string
}

// Compile-time interface check: no handler consumes this type until #889 wires
// the v2 method table, so nothing else would catch a signature drift.
var _ store.TransactionReader = (*TransactionReader)(nil)

func NewTransactionReader(registry *query.Registry, networkPassphrase string) *TransactionReader {
	return &TransactionReader{registry: registry, passphrase: networkPassphrase}
}

func (r *TransactionReader) GetTransaction(_ context.Context, hash xdr.Hash) (store.Transaction, error) {
	view, err := r.registry.NewReadView()
	if err != nil {
		return store.Transaction{}, err
	}
	defer view.Release()

	cold, err := view.ColdTxIndexes()
	if err != nil {
		return store.Transaction{}, err
	}
	probe, err := txhash.NewTxReader(view.HotTxHashIndexes(), cold, &viewLedgerSource{view: view}, r.passphrase)
	if err != nil {
		return store.Transaction{}, err
	}
	txv, found, err := probe.GetTransaction(hash)
	if err != nil {
		return store.Transaction{}, err
	}
	if !found {
		return store.Transaction{}, store.ErrNoTransaction
	}
	// HotTxHashIndexes is deliberately unfiltered, so a hot handle predating the
	// view's floor can resolve a ledger outside the servable window; a not-found
	// keeps retention observable behavior, not handle lifecycle.
	if txv.LedgerSequence < view.OldestLedger() || txv.LedgerSequence > view.LatestLedger() {
		return store.Transaction{}, store.ErrNoTransaction
	}
	return transactionFromView(txv), nil
}

// viewLedgerSource resolves per-candidate ledgers for the probe: one routing
// resolve per candidate, no caching — candidate seqs are one-per-index and the
// probe stops at the first verified match, so no ledger is fetched twice.
type viewLedgerSource struct {
	view *query.ReadView
}

func (s *viewLedgerSource) GetLedgerRaw(seq uint32) ([]byte, error) {
	// chunk.IDFromLedger panics below ledger 2; an index naming a sub-genesis
	// ledger is corrupt data, and must fail the candidate, not the process.
	if seq < chunk.FirstLedgerSeq {
		return nil, stores.ErrNotFound
	}
	reader, err := s.view.Ledgers(chunk.IDFromLedger(seq))
	if err != nil {
		return nil, err
	}
	return reader.GetLedgerRaw(seq)
}

// transactionFromView reshapes the SDK's raw-bytes view into the serving
// contract's Transaction. The byte fields alias the ledger buffer the view was
// extracted from; that buffer is owned (GetLedgerRaw returns owned bytes), so
// the aliases stay valid after the read view is released.
func transactionFromView(txv ingest.LedgerTransactionView) store.Transaction {
	return store.Transaction{
		TransactionHash:   xdr.Hash(txv.Hash).HexString(),
		Result:            txv.Result,
		Meta:              txv.Meta,
		Envelope:          txv.Envelope,
		Events:            txv.DiagnosticEvents,
		FeeBump:           txv.FeeBump,
		ApplicationOrder:  txv.ApplicationOrder,
		Successful:        txv.Successful,
		Ledger:            store.LedgerInfo{Sequence: txv.LedgerSequence, CloseTime: txv.LedgerCloseTime},
		TransactionEvents: txv.TransactionEvents,
		ContractEvents:    txv.ContractEvents,
	}
}
