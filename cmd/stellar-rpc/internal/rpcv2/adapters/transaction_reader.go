package adapters

import (
	"context"
	"errors"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// TransactionReader satisfies store.TransactionReader over the query router:
// each GetTransaction probes the hot tx-hash indexes and — only when every hot
// index misses — the frozen window indexes, through one read view, verifying
// candidates against the full hash. Both tiers come from the view already
// window-gated (see query.ReadView.HotTxHashIndexes).
type TransactionReader struct {
	passphrase string
	metrics    observability.Metrics
}

func NewTransactionReader(networkPassphrase string, metrics observability.Metrics) *TransactionReader {
	return &TransactionReader{passphrase: networkPassphrase, metrics: observability.MetricsOrNop(metrics)}
}

func (r *TransactionReader) GetTransaction(ctx context.Context, hash xdr.Hash) (store.Transaction, error) {
	view, err := query.ViewFrom(ctx)
	if err != nil {
		return store.Transaction{}, err
	}

	probe, err := txhash.NewTxReader(
		view.HotTxHashIndexes(), view.ColdTxIndexes, view, r.passphrase)
	if err != nil {
		return store.Transaction{}, err
	}
	txv, found, err := probe.GetTransaction(hash)
	if err != nil {
		// An exact hot index disagreeing with the ledger store is corruption;
		// count it so operators see it (the error itself reaches the client
		// only as a generic internal error).
		if errors.Is(err, txhash.ErrInconsistent) {
			r.metrics.TxIndexInconsistency()
		}
		return store.Transaction{}, err
	}
	if !found {
		return store.Transaction{}, store.ErrNoTransaction
	}
	// Only a compactView-produced view may be reshaped here: the type does not
	// carry that, and a view straight off the SDK still aliases the ledger it
	// was read from — probe.GetTransaction guarantees it (see compactView).
	return store.ParseTransaction(txv)
}
