package store

import (
	"context"
	"encoding/hex"
	"errors"
	"math"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

var ErrNoTransaction = errors.New("no transaction with this hash exists")

type Transaction struct {
	TransactionHash  string
	Result           []byte   // XDR encoded xdr.TransactionResult
	Meta             []byte   // XDR encoded xdr.TransactionMeta
	Envelope         []byte   // XDR encoded xdr.TransactionEnvelope
	Events           [][]byte // XDR encoded xdr.DiagnosticEvent
	FeeBump          bool
	ApplicationOrder int32
	Successful       bool
	Ledger           LedgerInfo

	TransactionEvents [][]byte   // XDR encoded xdr.TransactionEvent
	ContractEvents    [][][]byte // XDR encoded xdr.ContractEvent
}

// LedgerSeqBounds is the inclusive range of ledgers a lookup considers.
type LedgerSeqBounds struct {
	First uint32
	Last  uint32
}

// AllLedgers is the unbounded lookup.
func AllLedgers() LedgerSeqBounds { return LedgerSeqBounds{Last: math.MaxUint32} }

func (b LedgerSeqBounds) Contains(seq uint32) bool {
	return b.First <= seq && seq <= b.Last
}

// TransactionReader provides all the public ways to read transactions from the backend.
type TransactionReader interface {
	// GetTransaction resolves hash to the transaction, considering only
	// ledgers within bounds. A transaction outside bounds, like an unknown
	// or pruned one, is ErrNoTransaction.
	GetTransaction(ctx context.Context, hash xdr.Hash, bounds LedgerSeqBounds) (Transaction, error)
}

// ParseTransactionView reshapes an SDK transaction view into a Transaction; the
// byte fields alias the view's buffer.
func ParseTransactionView(txView ingest.LedgerTransactionView) Transaction {
	return Transaction{
		TransactionHash:  hex.EncodeToString(txView.Hash[:]),
		Result:           txView.Result,
		Meta:             txView.Meta,
		Envelope:         txView.Envelope,
		Events:           txView.DiagnosticEvents,
		FeeBump:          txView.FeeBump,
		ApplicationOrder: txView.ApplicationOrder,
		Successful:       txView.Successful,
		Ledger: LedgerInfo{
			Sequence:  txView.LedgerSequence,
			CloseTime: txView.LedgerCloseTime,
		},
		TransactionEvents: txView.TransactionEvents,
		ContractEvents:    txView.ContractEvents,
	}
}
