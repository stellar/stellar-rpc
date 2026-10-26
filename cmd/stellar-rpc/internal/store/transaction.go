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

// ParseTransactionView reshapes an SDK transaction view into a Transaction; the
// byte fields alias the view's buffer.
func ParseTransactionView(txView ingest.LedgerTransactionView) Transaction {
	return Transaction{
		TransactionHash:  hex.EncodeToString(txView.Hash[:]),
		Result:           txView.Result,
		Meta:             txView.Meta,
		Envelope:         txView.Envelope,
		Events:           wireBytes(txView.DiagnosticEvents),
		FeeBump:          txView.FeeBump,
		ApplicationOrder: txView.ApplicationOrder,
		Successful:       txView.Successful,
		Ledger: LedgerInfo{
			Sequence:  txView.LedgerSequence,
			CloseTime: txView.LedgerCloseTime,
		},
		TransactionEvents: wireBytes(txView.TransactionEvents),
		ContractEvents:    opWireBytes(txView.ContractEvents),
	}
}

// wireBytes returns each view's wire bytes without copying them, keeping a nil
// list nil.
func wireBytes[T ~[]byte](views []T) [][]byte {
	if views == nil {
		return nil
	}
	out := make([][]byte, len(views))
	for i, v := range views {
		out[i] = v
	}
	return out
}

func opWireBytes(ops [][]xdr.ContractEventView) [][][]byte {
	if ops == nil {
		return nil
	}
	out := make([][][]byte, len(ops))
	for i, op := range ops {
		out[i] = wireBytes(op)
	}
	return out
}
