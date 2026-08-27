package store

import (
	"context"
	"encoding/hex"
	"errors"

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

// TransactionReader provides all the public ways to read transactions from the backend.
type TransactionReader interface {
	GetTransaction(ctx context.Context, hash xdr.Hash) (Transaction, error)
}

// ParseTransaction reshapes the SDK's raw-bytes transaction view into the
// serving contract's Transaction. The byte fields alias the view's ledger
// buffer rather than being copied.
func ParseTransaction(txView ingest.LedgerTransactionView) Transaction {
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
