package adapters

import (
	"bytes"
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

func NewTransactionReader(registry *query.Registry, networkPassphrase string) *TransactionReader {
	return &TransactionReader{registry: registry, passphrase: networkPassphrase}
}

func (r *TransactionReader) GetTransaction(ctx context.Context, hash xdr.Hash) (store.Transaction, error) {
	view, err := r.registry.NewReadView()
	if err != nil {
		return store.Transaction{}, err
	}
	defer view.Release()

	cold, err := view.ColdTxIndexes()
	if err != nil {
		return store.Transaction{}, err
	}
	gated := make([]txhash.HashIndex, 0, len(cold))
	for _, idx := range cold {
		gated = append(gated, &windowGatedIndex{inner: idx, view: view})
	}
	probe, err := txhash.NewTxReader(view.HotTxHashIndexes(), gated, &viewLedgerSource{view: view}, r.passphrase)
	if err != nil {
		return store.Transaction{}, err
	}
	txv, found, err := probe.GetTransaction(hash)
	if err != nil {
		return store.Transaction{}, markErr(ctx, err)
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

// windowGatedIndex wraps a cold tx-hash index so a hit outside the view's
// servable window [OldestLedger, LatestLedger] reads as a plain miss.
//
// Why this gate exists: a frozen tx-hash index covers 1000 chunks and is
// deleted only when its WHOLE window falls below the retention floor, but each
// chunk's ledger files are deleted as soon as that one chunk falls below the
// floor. So for most of its life a frozen index names transactions whose
// ledgers no longer exist. Example: the index covers chunks 0–999 and the
// floor sits at chunk 500. A client asks for a transaction that lived in
// chunk 3. The index still answers with chunk 3's ledger, but chunk 3's files
// are gone — without this gate the probe records "candidate could not be
// verified" and the client gets an internal error ("lookup incomplete") where
// v1 answers NOT_FOUND. Skipping the candidate here is safe: even a fully
// verified below-floor match would be turned into ErrNoTransaction by the
// window gate in GetTransaction, so not-found is the answer either way.
type windowGatedIndex struct {
	inner txhash.HashIndex
	view  *query.ReadView
}

func (g *windowGatedIndex) Get(hash [32]byte) (uint32, error) {
	seq, err := g.inner.Get(hash)
	if err != nil {
		return 0, err
	}
	if seq < g.view.OldestLedger() || seq > g.view.LatestLedger() {
		return 0, stores.ErrNotFound
	}
	return seq, nil
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
