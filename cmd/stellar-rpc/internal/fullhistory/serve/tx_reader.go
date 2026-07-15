package serve

import (
	"bytes"
	"context"
	"encoding/hex"
	"maps"
	"slices"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

// txReader adapts the serve Registry to the db.TransactionReader interface the
// v1 getTransaction handler consumes, so methods.NewGetTransactionHandler runs
// verbatim over full-history storage: a by-hash lookup probes the hot tx-hash
// CFs (exact, newest first), then the cold window indexes (fingerprinted,
// newest first), verifying every candidate against the resolved ledger.
type txReader struct {
	reg        *Registry
	layout     geometry.Layout
	passphrase string
}

// NewTransactionReader returns a db.TransactionReader backed by the serve Registry.
func NewTransactionReader(reg *Registry, layout geometry.Layout, passphrase string) db.TransactionReader {
	return txReader{reg: reg, layout: layout, passphrase: passphrase}
}

// GetTransaction admits once, assembles the hot/cold probe chain against that
// one immutable View, and runs the shared txhash reader. A miss is
// db.ErrNoTransaction; the view's raw byte fields (which alias the per-call
// ledger buffer the LedgerSource cloned) become the returned Transaction.
func (r txReader) GetTransaction(_ context.Context, hash xdr.Hash) (db.Transaction, error) {
	_, v := r.reg.Admit()

	// Hot indexes newest chunk first (higher ID = newer).
	hotIDs := slices.Sorted(maps.Keys(v.Hot))
	hot := make([]txhash.HashIndex, 0, len(hotIDs))
	for i := len(hotIDs) - 1; i >= 0; i-- {
		hot = append(hot, v.Hot[hotIDs[i]].Txhash())
	}
	// Cold windows newest first (TxIdx is ascending by Lo).
	cold := make([]txhash.HashIndex, 0, len(v.TxIdx))
	for i := len(v.TxIdx) - 1; i >= 0; i-- {
		cold = append(cold, v.TxIdx[i].Reader)
	}

	src := txLedgerSource{
		view:   v,
		layout: r.layout,
		floor:  max(v.Floor.FirstLedger(), v.Earliest),
	}
	tr, err := txhash.NewTxReader(hot, cold, src, r.passphrase)
	if err != nil {
		return db.Transaction{}, err
	}

	txv, found, err := tr.GetTransaction([32]byte(hash))
	if err != nil {
		return db.Transaction{}, err
	}
	if !found {
		return db.Transaction{}, db.ErrNoTransaction
	}
	return viewToTransaction(txv), nil
}

// txLedgerSource resolves a ledger's raw bytes for the txhash reader over one
// pinned View, opening/closing the per-chunk reader per call. It returns
// not-found for a seq below the retention floor so a cold fingerprint candidate
// naming pruned history is skipped rather than served (design: R2 for by-hash
// lookups).
type txLedgerSource struct {
	view   *View
	layout geometry.Layout
	floor  uint32
}

func (s txLedgerSource) GetLedgerRaw(seq uint32) ([]byte, error) {
	if seq < s.floor {
		return nil, stores.ErrNotFound
	}
	lc, err := s.view.ResolveLedgers(chunk.IDFromLedger(seq), s.layout)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lc.Close() }()
	raw, err := lc.Get(seq)
	if err != nil {
		return nil, err
	}
	// The reader's view aliases these bytes past the call; copy the borrowed buffer.
	return bytes.Clone(raw), nil
}

// viewToTransaction maps the raw-bytes LedgerTransactionView field-for-field
// onto db.Transaction (the view is the view-path parallel of ParseTransaction's
// output, already holding the same XDR wire bytes). The byte slices alias the
// buffer txLedgerSource cloned per call, so the returned Transaction owns them.
func viewToTransaction(v ingest.LedgerTransactionView) db.Transaction {
	return db.Transaction{
		TransactionHash:   hex.EncodeToString(v.Hash[:]),
		Result:            v.Result,
		Meta:              v.Meta,
		Envelope:          v.Envelope,
		Events:            v.DiagnosticEvents,
		FeeBump:           v.FeeBump,
		ApplicationOrder:  v.ApplicationOrder,
		Successful:        v.Successful,
		Ledger:            ledgerbucketwindow.LedgerInfo{Sequence: v.LedgerSequence, CloseTime: v.LedgerCloseTime},
		TransactionEvents: v.TransactionEvents,
		ContractEvents:    v.ContractEvents,
	}
}
