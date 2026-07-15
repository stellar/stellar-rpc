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

	// POC: this probe chain finds a fee-bump transaction by its OUTER hash only,
	// never its inner hash — a v1 parity gap. v1 indexes a fee-bump under both
	// hashes (db/transaction.go: transactions[tx.Result.InnerHash()] = tx), but the
	// full-history write path stores exactly ONE key per tx — the outer TxProcessing
	// result hash — in BOTH tiers (the cold .bin/.idx build in ingest/txhash.go and
	// the hot txhash CF in hot_store.go), and the SDK verify chain
	// (ingest.LedgerTransactionViewByHash) matches that outer hash only. So
	// GetTransaction(innerHash) returns a clean NOT_FOUND where v1 returns the
	// fee-bump tx. Not fixable at this read layer. Productionization requires
	// inner-hash index entries at build time (hot CF + .bin codec + .idx build +
	// backfill) or an inner-hash fallback in the SDK extractor.

	// Hot indexes newest chunk first (higher ID = newer).
	hotIDs := slices.Sorted(maps.Keys(v.Hot))
	hot := make([]txhash.HashIndex, 0, len(hotIDs))
	for i := len(hotIDs) - 1; i >= 0; i-- {
		hot = append(hot, v.Hot[hotIDs[i]].Txhash())
	}
	// Cold windows newest first (TxIdx is ascending by Lo), each floor-gated: a
	// window index may keep naming ledgers prune has removed (design R2), so a
	// below-floor candidate is a clean index miss, not an unverifiable probe.
	floor := max(v.Floor.FirstLedger(), v.Earliest)
	cold := make([]txhash.HashIndex, 0, len(v.TxIdx))
	for i := len(v.TxIdx) - 1; i >= 0; i-- {
		cold = append(cold, floorGatedIndex{inner: v.TxIdx[i].Reader, floor: floor})
	}

	src := txLedgerSource{view: v, layout: r.layout}
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

// floorGatedIndex drops a cold candidate whose ledger sits below the retention
// floor. A window index legitimately keeps naming ledgers prune has already
// removed (design R2), so a below-floor hit is a clean index miss — not a
// candidate the ledger source must then fail to verify.
type floorGatedIndex struct {
	inner txhash.HashIndex
	floor uint32
}

func (g floorGatedIndex) Get(hash [32]byte) (uint32, error) {
	seq, err := g.inner.Get(hash)
	if err != nil {
		return 0, err
	}
	if seq < g.floor {
		return 0, stores.ErrNotFound
	}
	return seq, nil
}

// txLedgerSource resolves a ledger's raw bytes for the txhash reader over one
// pinned View, opening/closing the per-chunk reader per call. A genuine
// resolution failure stays an error (the reader's "never a false not-found"
// path); below-floor filtering happens at the index layer above.
type txLedgerSource struct {
	view   *View
	layout geometry.Layout
}

func (s txLedgerSource) GetLedgerRaw(seq uint32) ([]byte, error) {
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
