package events

import (
	"errors"
	"fmt"
	"io"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// StageSentinels maps a V4 top-level TransactionEvent stage to the
// (TxIdx, OpIdx) sentinels the v1 getEvents cursor encoding uses. applyIdx is
// the 1-based transaction apply index (consumed by the AfterTx arm). This is
// cursor-encoding POLICY shared by the parsed (LCMToPayloads) and view
// (views.ExtractEvents) full-history ingest paths.
//
// NOTE: the legacy SQL path carries the same mapping inline in
// db/event.go's InsertEvents (it additionally selects per-stage event
// counters this helper deliberately doesn't model) — a new stage or sentinel
// revision must land in BOTH places to keep getEvents cursors compatible
// across storage backends.
func StageSentinels(stage xdr.TransactionEventStage, applyIdx uint32) (uint32, uint32, error) {
	switch stage {
	case xdr.TransactionEventStageTransactionEventStageBeforeAllTxs:
		return 0, 0, nil
	case xdr.TransactionEventStageTransactionEventStageAfterAllTxs:
		return uint32(toid.TransactionMask), 0, nil
	case xdr.TransactionEventStageTransactionEventStageAfterTx:
		return applyIdx, uint32(toid.OperationMask), nil
	default:
		return 0, 0, fmt.Errorf("events: unhandled event stage %v", stage)
	}
}

// LCMToPayloads walks a LedgerCloseMeta and produces one Payload
// per emitted event in chronological order matching the existing
// SQL ingest path. Cursor compatibility with the v1 getEvents API
// is preserved through the TxIdx/OpIdx stage sentinels
// (StageSentinels); the per-event index is positional and not part
// of the payload (see the package doc's reconstruction caveat).
//
// Term derivation is the caller's responsibility — call TermsForBytes on
// each payload's ContractEventBytes at the point where the chunk-relative
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

		// Order matches db/event.go::InsertEvents: tx-level events
		// first (in the order they appear in allEvents.TransactionEvents),
		// then operation events by (op, event) order. Locking this in
		// keeps the chunk-relative event-ID assignment deterministic. The
		// per-event index is NOT stored — only the (txIdx, opIdx) cursor
		// sentinels derived from the stage are.
		for _, ev := range allEvents.TransactionEvents {
			txIdx, opIdx, serr := StageSentinels(ev.Stage, tx.Index)
			if serr != nil {
				return nil, fmt.Errorf("%w in ledger %d", serr, lcm.LedgerSequence())
			}
			evBytes, err := ev.Event.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("events: marshal tx event in ledger %d: %w",
					lcm.LedgerSequence(), err)
			}
			payloads = append(payloads, Payload{
				TxHash:             tx.Hash,
				LedgerSequence:     ledgerSeq,
				TxIdx:              txIdx,
				OpIdx:              opIdx,
				LedgerClosedAt:     ledgerClosedAt,
				ContractEventBytes: evBytes,
			})
		}

		payloads, err = appendOpEventPayloads(payloads, tx.Hash, tx.Index,
			ledgerSeq, ledgerClosedAt, allEvents.OperationEvents)
		if err != nil {
			return nil, err
		}
	}

	return payloads, nil
}

// appendOpEventPayloads marshals each per-operation contract event and
// appends one Payload per event to dst in (op, event) order, returning the
// grown slice.
func appendOpEventPayloads(
	dst []Payload,
	txHash xdr.Hash,
	txIdx, ledgerSeq uint32,
	ledgerClosedAt int64,
	opEvents [][]xdr.ContractEvent,
) ([]Payload, error) {
	for opIndex, innerOpEvents := range opEvents {
		for _, contractEvent := range innerOpEvents {
			evBytes, err := contractEvent.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("events: marshal op event in ledger %d: %w",
					ledgerSeq, err)
			}
			dst = append(dst, Payload{
				TxHash:             txHash,
				LedgerSequence:     ledgerSeq,
				TxIdx:              txIdx,
				OpIdx:              uint32(opIndex),
				LedgerClosedAt:     ledgerClosedAt,
				ContractEventBytes: evBytes,
			})
		}
	}
	return dst, nil
}
