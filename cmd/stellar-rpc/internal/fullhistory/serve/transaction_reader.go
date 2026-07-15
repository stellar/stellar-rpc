package serve

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/registry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

// TransactionReader serves the db.TransactionReader interface from the
// registry. Each GetTransaction admits one Snapshot, assembles the tx-hash
// probe sets from it (hot chunk indexes and window .idx readers, newest
// first), and runs the existing txhash.TxReader probe-and-verify chain over
// them. Like LedgerReader, it must not outlive the run whose registry it
// wraps.
type TransactionReader struct {
	reg        *registry.Registry
	passphrase string
}

var _ db.TransactionReader = (*TransactionReader)(nil)

// NewTransactionReader builds the veneer. networkPassphrase is required — the
// probe chain re-hashes candidate envelopes to verify a fingerprinted index
// hit, and a wrong passphrase would reject every candidate.
func NewTransactionReader(reg *registry.Registry, networkPassphrase string) (*TransactionReader, error) {
	if reg == nil {
		return nil, fmt.Errorf("serve: nil registry: %w", stores.ErrInvalidConfig)
	}
	if networkPassphrase == "" {
		return nil, fmt.Errorf("serve: empty network passphrase: %w", stores.ErrInvalidConfig)
	}
	return &TransactionReader{reg: reg, passphrase: networkPassphrase}, nil
}

// GetTransaction resolves hash within one admitted Snapshot and returns
// db.ErrNoTransaction when it is not found — including when the only
// candidates live below the admitted floor (R2: pruned history reads as
// not-found) or above the admitted latest (a query never observes a ledger
// its watermark has not reached).
func (r *TransactionReader) GetTransaction(_ context.Context, hash xdr.Hash) (db.Transaction, error) {
	snap := r.reg.Admit()
	txr, err := txhash.NewTxReader(
		hotProbeSet(snap), coldProbeSet(snap),
		routingLedgerSource{reg: r.reg, snap: snap}, r.passphrase)
	if err != nil {
		return db.Transaction{}, fmt.Errorf("serve: assemble tx reader: %w", err)
	}
	txv, found, err := txr.GetTransaction(hash)
	if err != nil {
		return db.Transaction{}, fmt.Errorf("serve: lookup tx %s: %w", hex.EncodeToString(hash[:]), err)
	}
	if !found {
		return db.Transaction{}, db.ErrNoTransaction
	}
	// LedgerTransactionView carries the same raw-XDR fields db.Transaction
	// wants, so the mapping is direct — no re-decode, matching what v1's
	// ParseTransaction produces field by field.
	return db.Transaction{
		TransactionHash:  hex.EncodeToString(txv.Hash[:]),
		Result:           txv.Result,
		Meta:             txv.Meta,
		Envelope:         txv.Envelope,
		Events:           txv.DiagnosticEvents,
		FeeBump:          txv.FeeBump,
		ApplicationOrder: txv.ApplicationOrder,
		Successful:       txv.Successful,
		Ledger: ledgerbucketwindow.LedgerInfo{
			Sequence:  txv.LedgerSequence,
			CloseTime: txv.LedgerCloseTime,
		},
		TransactionEvents: txv.TransactionEvents,
		ContractEvents:    txv.ContractEvents,
	}, nil
}

// hotProbeSet returns the admitted View's hot chunk tx-hash indexes, newest
// chunk first (recent transactions are the common lookup).
func hotProbeSet(snap registry.Snapshot) []txhash.HashIndex {
	ids := snap.View.HotChunks() // ascending
	out := make([]txhash.HashIndex, 0, len(ids))
	for i := len(ids) - 1; i >= 0; i-- {
		hotDB, ok := snap.View.HotDB(ids[i])
		if !ok {
			continue // unreachable: ids came from the same immutable View
		}
		out = append(out, boundedIndex{idx: hotDB.Txhash(), snap: snap})
	}
	return out
}

// coldProbeSet returns the admitted View's window .idx readers, newest window
// first.
func coldProbeSet(snap registry.Snapshot) []txhash.HashIndex {
	covs := snap.View.Indexes() // ascending by window
	out := make([]txhash.HashIndex, 0, len(covs))
	for i := len(covs) - 1; i >= 0; i-- {
		out = append(out, boundedIndex{idx: covs[i].Idx, snap: snap})
	}
	return out
}

// boundedIndex gates an index's candidates against the admitted
// [floor, latest] BEFORE the TxReader sees them, by mapping an out-of-bounds
// hit to stores.ErrNotFound.
//
// The gate must sit here, on the index, and not on the ledger source: an
// index miss (stores.ErrNotFound from Get) is the one signal TxReader.scan
// skips cleanly, whereas a ledger-fetch failure — any error at all — is
// recorded as a soft error that turns an otherwise-missed lookup into a
// "lookup incomplete" failure. A window .idx legitimately keeps naming
// ledgers that prune has removed (its coverage low bound trails the floor),
// so a below-floor candidate must read as a clean miss (R2), not as an
// incomplete lookup.
type boundedIndex struct {
	idx  txhash.HashIndex
	snap registry.Snapshot
}

func (b boundedIndex) Get(hash [32]byte) (uint32, error) {
	seq, err := b.idx.Get(hash)
	if err != nil {
		return 0, err
	}
	if seq < b.snap.View.FloorLedger() || seq > b.snap.Latest {
		return 0, stores.ErrNotFound
	}
	return seq, nil
}

// routingLedgerSource is the txhash.LedgerSource over the admitted Snapshot:
// candidate ledgers resolve through the registry (cold wins over hot). Both
// store tiers return fresh caller-owned buffers, satisfying the
// LedgerSource contract that the bytes outlive the call (the returned
// transaction view aliases them).
type routingLedgerSource struct {
	reg  *registry.Registry
	snap registry.Snapshot
}

var _ txhash.LedgerSource = routingLedgerSource{}

func (s routingLedgerSource) GetLedgerRaw(seq uint32) ([]byte, error) {
	// Candidates arrive pre-gated by boundedIndex; this re-check keeps the
	// source safe for out-of-bounds sequences anyway (chunk.IDFromLedger
	// panics below the genesis ledger).
	if seq < s.snap.View.FloorLedger() || seq > s.snap.Latest {
		return nil, stores.ErrNotFound
	}
	h, err := s.reg.LedgerReaderFor(s.snap.View, chunk.IDFromLedger(seq))
	if err != nil {
		return nil, err
	}
	return h.GetLedgerRaw(seq)
}
