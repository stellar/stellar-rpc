package adapters

import (
	"bytes"
	"context"
	"errors"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
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
	view, err := viewFrom(ctx)
	if err != nil {
		return store.Transaction{}, err
	}

	probe, err := txhash.NewTxReader(
		view.HotTxHashIndexes(), view.ColdTxIndexes,
		&viewLedgerSource{view: view}, r.passphrase)
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
// contract's Transaction. The view's byte fields all alias ONE buffer holding
// the entire raw ledger, so returning them as-is would pin that multi-MB
// buffer until the response is fully serialized. Copying just this
// transaction's bytes (typically a few KB) frees the ledger buffer for GC as
// soon as this function returns.
func transactionFromView(txv ingest.LedgerTransactionView) store.Transaction {
	return store.Transaction{
		TransactionHash:   xdr.Hash(txv.Hash).HexString(),
		Result:            bytes.Clone(txv.Result),
		Meta:              bytes.Clone(txv.Meta),
		Envelope:          bytes.Clone(txv.Envelope),
		Events:            cloneByteSlices(txv.DiagnosticEvents),
		FeeBump:           txv.FeeBump,
		ApplicationOrder:  txv.ApplicationOrder,
		Successful:        txv.Successful,
		Ledger:            store.LedgerInfo{Sequence: txv.LedgerSequence, CloseTime: txv.LedgerCloseTime},
		TransactionEvents: cloneByteSlices(txv.TransactionEvents),
		ContractEvents:    cloneNestedByteSlices(txv.ContractEvents),
	}
}

func cloneByteSlices(in [][]byte) [][]byte {
	if in == nil {
		return nil
	}
	out := make([][]byte, len(in))
	for i, b := range in {
		out[i] = bytes.Clone(b)
	}
	return out
}

func cloneNestedByteSlices(in [][][]byte) [][][]byte {
	if in == nil {
		return nil
	}
	out := make([][][]byte, len(in))
	for i, s := range in {
		out[i] = cloneByteSlices(s)
	}
	return out
}
