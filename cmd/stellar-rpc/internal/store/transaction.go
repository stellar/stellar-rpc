package store

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

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

// ParseTransaction reshapes an SDK transaction view into a Transaction; the
// byte fields alias the view's buffer.
func ParseTransaction(txView ingest.LedgerTransactionView) (Transaction, error) {
	tx := Transaction{
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
	err := repairV3OperationArity(&tx)
	return tx, err
}

// repairV3OperationArity aligns ContractEvents with the decode path's arity in
// the one shape where the SDK's view extractor disagrees: TransactionMeta V3 +
// Soroban envelope + absent SorobanMeta (a Soroban tx charged but never
// executed — real on protocol 20-22 history) must serve [[]], not []. Pure
// view reads, no decode. DELETE at the go-stellar-sdk pin bump that includes
// stellar/go-stellar-sdk#5997, which repairs the arity inside the SDK.
func repairV3OperationArity(tx *Transaction) error {
	if len(tx.ContractEvents) > 0 {
		return nil
	}
	metaVersion, err := xdr.TransactionMetaView(tx.Meta).V()
	if err != nil {
		return fmt.Errorf("couldn't read transaction meta version: %w", err)
	}
	if metaVersion != 3 {
		return nil
	}
	soroban, err := envelopeIsSoroban(tx.Envelope)
	if err != nil {
		return err
	}
	if soroban {
		tx.ContractEvents = [][][]byte{{}}
	}
	return nil
}

// envelopeIsSoroban reports whether a marshaled envelope carries
// SorobanTransactionData (Tx.Ext discriminant 1; the inner tx's for a fee
// bump). The SDK computes this flag internally but does not expose it.
func envelopeIsSoroban(raw []byte) (bool, error) {
	env := xdr.TransactionEnvelopeView(raw)
	var soroban bool
	err := xdr.TryVoid(func() {
		switch env.MustType() {
		case xdr.EnvelopeTypeEnvelopeTypeTx:
			soroban = env.MustV1().MustTx().MustExt().MustV() == 1
		case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
			soroban = env.MustFeeBump().MustTx().MustInnerTx().MustV1().MustTx().MustExt().MustV() == 1
		default:
			// TX_V0 predates Soroban; no other discriminant is a tx envelope.
		}
	})
	if err != nil {
		return false, fmt.Errorf("couldn't read the envelope's Soroban flag: %w", err)
	}
	return soroban, nil
}
