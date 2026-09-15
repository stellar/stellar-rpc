package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"

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

func ParseTransaction(lcm xdr.LedgerCloseMeta, ingestTx ingest.LedgerTransaction) (Transaction, error) {
	var tx Transaction
	var err error

	//
	// On-the-fly ingestion: extract all of the fields, return best effort.
	//
	tx.FeeBump = ingestTx.Envelope.IsFeeBump()
	applicationOrder, convErr := strconv.ParseInt(strconv.FormatUint(uint64(ingestTx.Index), 10), 10, 32)
	if convErr != nil {
		return tx, fmt.Errorf("transaction index %d exceeds supported range", ingestTx.Index)
	}
	tx.ApplicationOrder = int32(applicationOrder)
	tx.Successful = ingestTx.Result.Successful()
	tx.Ledger = LedgerInfo{
		Sequence:  lcm.LedgerSequence(),
		CloseTime: lcm.LedgerCloseTime(),
	}
	tx.TransactionHash = ingestTx.Result.TransactionHash.HexString()

	if tx.Result, err = ingestTx.Result.Result.MarshalBinary(); err != nil {
		return tx, fmt.Errorf("couldn't encode transaction Result: %w", err)
	}
	if tx.Meta, err = ingestTx.UnsafeMeta.MarshalBinary(); err != nil {
		return tx, fmt.Errorf("couldn't encode transaction UnsafeMeta: %w", err)
	}
	if tx.Envelope, err = ingestTx.Envelope.MarshalBinary(); err != nil {
		return tx, fmt.Errorf("couldn't encode transaction Envelope: %w", err)
	}

	allEvents, err := ingestTx.GetTransactionEvents()
	if err != nil {
		return tx, fmt.Errorf("couldn't encode transaction Events: %w", err)
	}

	diagEvents, err := ingestTx.GetDiagnosticEvents()
	if err != nil {
		return tx, errors.Join(errors.New("couldn't encode diagnostic events"), err)
	}

	tx.Events = make([][]byte, 0, len(diagEvents))
	for i, event := range diagEvents {
		bytes, ierr := event.MarshalBinary()
		if ierr != nil {
			return tx, fmt.Errorf("couldn't encode transaction DiagnosticEvent %d: %w", i, ierr)
		}
		tx.Events = append(tx.Events, bytes)
	}

	if err = parseEvents(allEvents, &tx); err != nil {
		return tx, err
	}

	return tx, err
}

// parseEvents parses diagnostic, transaction and contract events
func parseEvents(allEvents ingest.TransactionEvents, tx *Transaction) error {
	// encode TransactionEvents
	tx.TransactionEvents = make([][]byte, 0, len(allEvents.TransactionEvents))
	for i, event := range allEvents.TransactionEvents {
		bytes, ierr := event.MarshalBinary()
		if ierr != nil {
			return fmt.Errorf("couldn't encode TransactionEvent %d: %w", i, ierr)
		}
		tx.TransactionEvents = append(tx.TransactionEvents, bytes)
	}

	// encode ContractEvents (slice of slices)
	tx.ContractEvents = make([][][]byte, 0, len(allEvents.OperationEvents))
	for opIndex, opEvents := range allEvents.OperationEvents {
		events := make([][]byte, 0, len(opEvents))
		for i, event := range opEvents {
			bytes, ierr := event.MarshalBinary()
			if ierr != nil {
				return fmt.Errorf("couldn't encode ContractEvent %d for operation %d: %w", i, opIndex, ierr)
			}
			events = append(events, bytes)
		}

		tx.ContractEvents = append(tx.ContractEvents, events)
	}
	return nil
}
