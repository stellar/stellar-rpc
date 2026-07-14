package events

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// PayloadsFromLedgerEvents shapes an already-extracted per-transaction event
// slice (ingest.ExtractLedgerEvents output) into one Payload per emitted
// contract event, in ASCENDING getEvents cursor order — the order the SQLite
// path serves (ORDER BY id ASC in db/event.go). The event store serves in write
// order (event IDs are assigned by arrival position and the term bitmaps iterate
// in ID order), so emission order here IS the cursor contract. Concretely, per
// ledger:
//
//  1. every transaction's BeforeAllTxs events (cursor (0, 0)), in tx apply
//     order — a stable partition of the SDK's per-tx traversal;
//  2. per transaction in apply order: its per-operation events in
//     (op, event) order (cursor (txIdx, opIdx)), THEN its AfterTx events
//     (cursor (txIdx, OperationMask), which sorts after every op);
//  3. every transaction's AfterAllTxs events (cursor (TransactionMask, 0)).
//
// ALL ContractEventBytes — top-level and per-op — alias the view buffer
// (zero-copy); callers copy what they retain.
//
// Each payload's EventIdx is the event's position within its (txIdx, opIdx)
// cursor group — an op event's index within its operation, a stage event's
// index within its stage group (BeforeAllTxs/AfterAllTxs counted ledger-wide
// in tx-apply order, AfterTx counted per transaction). This is the same
// assignment the SQLite path makes (db/event.go), so the trailing
// <TOID>-<eventIdx> component of the v1 getEvents ID matches across backends.
// The navigation, per-tx hashing, and event grouping live in the SDK
// (ingest.ExtractLedgerEvents — one TxProcessing walk yields hash + events
// together). This function adds only the RPC-specific Payload shape, the
// Stage→(TxIdx, OpIdx) cursor-sentinel mapping, EventIdx, and the cursor
// ordering.
//
// Taking the already-extracted txEvents (rather than re-walking a view) lets a
// caller that already holds them — the hot ingest path and the cold materializer,
// both of which also need the paired tx hashes (txEvents[i].Hash) — feed BOTH
// txhash and events from ONE ExtractLedgerEvents call instead of walking
// TxProcessing twice. ledgerSeq and ledgerClosedAt are the view's header values
// (cheap reads, not a walk).
func PayloadsFromLedgerEvents(
	txEvents []ingest.LedgerTransactionEvents, ledgerSeq uint32, ledgerClosedAt int64,
) ([]Payload, error) {
	var err error
	at := func(i int) (uint32, xdr.Hash) {
		return uint32(i) + 1, xdr.Hash(txEvents[i].Hash) //nolint:gosec // 1-based, matching ingest reader's tx.Index
	}
	// Every top-level TransactionEvent emits exactly one payload (in whichever
	// pass matches its stage) and every per-op event one more, so the total is
	// known up front — preallocate to avoid the append growth.
	payloads := make([]Payload, 0, countPayloads(txEvents))
	// Pass 1 — BeforeAllTxs across the whole ledger (cursor (0, 0); eventIdx
	// is the apply-order position within the group, so the counter spans the
	// whole ledger).
	var beforeIdx uint32
	for i := range txEvents {
		applyIdx, txHash := at(i)
		payloads, err = appendStageEventPayloads(payloads, txEvents[i].TransactionEvents,
			xdr.TransactionEventStageTransactionEventStageBeforeAllTxs,
			txHash, applyIdx, ledgerSeq, ledgerClosedAt, &beforeIdx)
		if err != nil {
			return nil, err
		}
	}
	// Pass 2 — per tx: op events, then the tx's AfterTx events (whose
	// (txIdx, OperationMask) cursor sorts after every op event of the tx).
	for i := range txEvents {
		applyIdx, txHash := at(i)
		for opIdx, opEvents := range txEvents[i].OperationEvents {
			// eventIdx is the event's position within its operation.
			for evIdx, evRaw := range opEvents {
				payloads = append(payloads, Payload{
					TxHash:             txHash,
					LedgerSequence:     ledgerSeq,
					TxIdx:              applyIdx,
					OpIdx:              uint32(opIdx),
					LedgerClosedAt:     ledgerClosedAt,
					EventIdx:           uint32(evIdx),
					ContractEventBytes: evRaw,
				})
			}
		}
		// AfterTx cursor (txIdx, OperationMask) is per-transaction, so eventIdx
		// resets for each tx.
		var afterTxIdx uint32
		payloads, err = appendStageEventPayloads(payloads, txEvents[i].TransactionEvents,
			xdr.TransactionEventStageTransactionEventStageAfterTx,
			txHash, applyIdx, ledgerSeq, ledgerClosedAt, &afterTxIdx)
		if err != nil {
			return nil, err
		}
	}
	// Pass 3 — AfterAllTxs across the whole ledger (cursor
	// (TransactionMask, 0), the ledger tail; eventIdx counts ledger-wide in
	// apply order, like BeforeAllTxs).
	var afterAllIdx uint32
	for i := range txEvents {
		applyIdx, txHash := at(i)
		payloads, err = appendStageEventPayloads(payloads, txEvents[i].TransactionEvents,
			xdr.TransactionEventStageTransactionEventStageAfterAllTxs,
			txHash, applyIdx, ledgerSeq, ledgerClosedAt, &afterAllIdx)
		if err != nil {
			return nil, err
		}
	}
	return payloads, nil
}

// countPayloads sums the per-tx top-level event + per-op event counts — the
// exact number of payloads PayloadsFromLedgerEvents emits across its three passes,
// used to size the result slice once.
func countPayloads(txEvents []ingest.LedgerTransactionEvents) int {
	total := 0
	for i := range txEvents {
		total += len(txEvents[i].TransactionEvents)
		for _, opEvents := range txEvents[i].OperationEvents {
			total += len(opEvents)
		}
	}
	return total
}

// appendStageEventPayloads emits the V4 top-level TransactionEvents whose
// Stage equals wantStage, skipping the other (known) stages — PayloadsFromLedgerEvents
// calls it once per stage per its cursor-ordered pass structure. An UNKNOWN
// stage errors in every pass (via StageSentinels), so a new protocol stage can
// never be silently dropped by the stage filter. Stage and the inner
// ContractEvent bytes are both read through the generated zero-copy view
// accessors — no UnmarshalBinary, no re-encode. This matters: under CAP-67
// every protocol-23+ transaction emits 1-2 fee TransactionEvents, so top-level
// events scale with tx count, not per-ledger. The emitted ContractEventBytes
// ALIAS the LCM view buffer, the same lifetime contract as the per-operation
// events.
//
// eventIdx points at the caller's per-group counter: ledger-wide for the
// BeforeAllTxs/AfterAllTxs passes, per-transaction for AfterTx. It is read for
// each emitted (matching-stage) payload and then incremented, so it tracks the
// event's position within its cursor group.
func appendStageEventPayloads(
	dst []Payload, txEventRaws [][]byte, wantStage xdr.TransactionEventStage,
	txHash xdr.Hash, applyIdx, ledgerSeq uint32, ledgerClosedAt int64, eventIdx *uint32,
) ([]Payload, error) {
	for _, raw := range txEventRaws {
		tev := xdr.TransactionEventView(raw)
		stageView, err := tev.Stage()
		if err != nil {
			return nil, fmt.Errorf("events: tx event Stage: %w", err)
		}
		stage, err := stageView.Value()
		if err != nil {
			return nil, fmt.Errorf("events: tx event Stage value: %w", err)
		}
		// StageSentinels also validates the stage: an unknown stage errors
		// here, in whichever pass sees it first.
		txIdx, opIdx, err := StageSentinels(stage, applyIdx)
		if err != nil {
			return nil, err
		}
		if stage != wantStage {
			continue
		}
		evView, err := tev.Event()
		if err != nil {
			return nil, fmt.Errorf("events: tx event Event: %w", err)
		}
		evRaw, err := evView.Raw()
		if err != nil {
			return nil, fmt.Errorf("events: tx ContractEvent.Raw: %w", err)
		}
		dst = append(dst, Payload{
			TxHash:             txHash,
			LedgerSequence:     ledgerSeq,
			TxIdx:              txIdx,
			OpIdx:              opIdx,
			LedgerClosedAt:     ledgerClosedAt,
			EventIdx:           *eventIdx,
			ContractEventBytes: evRaw,
		})
		*eventIdx++
	}
	return dst, nil
}
