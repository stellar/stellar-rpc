package events

import (
	"errors"
	"fmt"
	"io"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// LCMToPayloads walks a LedgerCloseMeta and produces one Payload
// per emitted event in chronological order matching the existing
// SQL ingest path. Cursor compatibility with the v1 getEvents API
// is preserved through the TxIdx/OpIdx/EventIdx stage semantics.
//
// Term derivation is the caller's responsibility — call TermsFor on
// each payload's ContractEvent at the point where the chunk-relative
// event ID is known.
//
// passphrase is the network passphrase the underlying
// ingest.LedgerTransactionReader uses to verify transaction hashes.
//
//nolint:nonamedreturns // named return needed so the deferred Close can join into err
func LCMToPayloads(passphrase string, lcm xdr.LedgerCloseMeta) (payloads []Payload, err error) {
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(passphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("events: open tx reader for ledger %d: %w",
			lcm.LedgerSequence(), err)
	}
	defer func() {
		if closeErr := txReader.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("events: close tx reader for ledger %d: %w",
				lcm.LedgerSequence(), closeErr))
		}
	}()

	ledgerSeq := lcm.LedgerSequence()
	ledgerClosedAt := lcm.LedgerCloseTime()

	var (
		beforeIndex  uint32
		afterIndex   uint32
		txAfterIndex uint32
	)

	for {
		tx, err := txReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("events: read tx from ledger %d: %w",
				lcm.LedgerSequence(), err)
		}

		allEvents, err := tx.GetTransactionEvents()
		if err != nil {
			return nil, fmt.Errorf("events: get events for tx %d in ledger %d: %w",
				tx.Index, lcm.LedgerSequence(), err)
		}

		// Reset the per-tx after-tx counter at every transaction.
		txAfterIndex = 0

		// Order matches db/event.go::InsertEvents: tx-level events
		// first (in the order they appear in allEvents.TransactionEvents),
		// then operation events by (op, event) order. Locking this in
		// keeps the chunk-relative event-ID assignment deterministic.
		for _, ev := range allEvents.TransactionEvents {
			var txIdx, opIdx, eventIdx uint32
			switch ev.Stage {
			case xdr.TransactionEventStageTransactionEventStageBeforeAllTxs:
				txIdx, opIdx, eventIdx = 0, 0, beforeIndex
				beforeIndex++
			case xdr.TransactionEventStageTransactionEventStageAfterAllTxs:
				txIdx, opIdx, eventIdx = uint32(toid.TransactionMask), 0, afterIndex
				afterIndex++
			case xdr.TransactionEventStageTransactionEventStageAfterTx:
				txIdx, opIdx, eventIdx = tx.Index, uint32(toid.OperationMask), txAfterIndex
				txAfterIndex++
			default:
				return nil, fmt.Errorf("events: unhandled event stage %q in ledger %d",
					ev.Stage.String(), lcm.LedgerSequence())
			}
			payloads = append(payloads, Payload{
				TxHash:         tx.Hash,
				LedgerSequence: ledgerSeq,
				TxIdx:          txIdx,
				OpIdx:          opIdx,
				LedgerClosedAt: ledgerClosedAt,
				EventIdx:       eventIdx,
				ContractEvent:  ev.Event,
			})
		}

		for opIndex, innerOpEvents := range allEvents.OperationEvents {
			for eventIndex, contractEvent := range innerOpEvents {
				payloads = append(payloads, Payload{
					TxHash:         tx.Hash,
					LedgerSequence: ledgerSeq,
					TxIdx:          tx.Index,
					OpIdx:          uint32(opIndex),
					LedgerClosedAt: ledgerClosedAt,
					EventIdx:       uint32(eventIndex),
					ContractEvent:  contractEvent,
				})
			}
		}
	}

	return payloads, nil
}
