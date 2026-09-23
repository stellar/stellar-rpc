package adapters

import (
	"context"
	"encoding/hex"
	"errors"
	"io"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
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
	logger     *supportlog.Entry
}

func NewTransactionReader(
	networkPassphrase string, metrics observability.Metrics, logger *supportlog.Entry,
) *TransactionReader {
	return &TransactionReader{
		passphrase: networkPassphrase,
		metrics:    observability.MetricsOrNop(metrics),
		logger:     loggerOrDiscard(logger),
	}
}

// loggerOrDiscard is observability.MetricsOrNop for the log sink: a reader
// built without one discards, so the failure path below has no branch.
func loggerOrDiscard(l *supportlog.Entry) *supportlog.Entry {
	if l != nil {
		return l
	}
	discard := supportlog.New()
	discard.SetOutput(io.Discard)
	return discard
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
		// The client gets a generic internal error, so this is the last place
		// the reason exists: a failed lookup names the ledger, the tier and
		// what disagreed, and every one of them means a stored artifact — an
		// index, a ledger or a span table — is bad. Logging it here is what
		// puts that in front of an operator; the JSON-RPC layer above logs
		// only the status.
		r.logger.WithField("tx", hex.EncodeToString(hash[:])).WithError(err).
			Error("getTransaction failed to read a candidate ledger")
		// An exact hot index disagreeing with the ledger store is corruption;
		// count it so operators see it too.
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
	return store.ParseTransactionView(txv), nil
}
