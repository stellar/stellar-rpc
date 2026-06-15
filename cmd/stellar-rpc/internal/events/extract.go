package events

import (
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ErrV0Unsupported is returned by LCMViewToPayloads on an LCM with
// discriminator V0. V0 (pre-Soroban) ledgers carry no contract events at all,
// so the events extractor has nothing to produce; the sentinel lets callers
// distinguish "can't have events" (treated as a zero-payload ledger by the
// ingest tiers) from a genuinely event-free V1+ ledger.
var ErrV0Unsupported = errors.New("events: LCM V0 carries no contract events (caller should skip / fall back)")

// LCMViewToPayloads walks a zero-copy LedgerCloseMetaView and returns one
// Payload per emitted contract event, in ASCENDING getEvents cursor order —
// the order the SQLite path serves (ORDER BY id ASC in db/event.go). The event
// store serves in write order (event IDs are assigned by arrival position and
// the term bitmaps iterate in ID order), so emission order here IS the cursor
// contract. Concretely, per ledger:
//
//  1. every transaction's BeforeAllTxs events (cursor (0, 0)), in tx apply
//     order — a stable partition of the SDK's per-tx traversal;
//  2. per transaction in apply order: its per-operation events in
//     (op, event) order (cursor (txIdx, opIdx)), THEN its AfterTx events
//     (cursor (txIdx, OperationMask), which sorts after every op);
//  3. every transaction's AfterAllTxs events (cursor (TransactionMask, 0)).
//
// ALL ContractEventBytes — top-level and per-op — alias the view buffer
// (zero-copy); callers copy what they retain. Returns ErrV0Unsupported for V0
// LCMs.
//
// The per-event index is NOT computed here: with cursor-ordered emission
// every (txIdx, opIdx) group is contiguous in stream order, so the index is
// positional within its group and reconstructed at read time. The
// navigation, per-tx hashing, and event grouping live in the SDK
// (ingest.ExtractLedgerEvents — one TxProcessing walk yields hash + events
// together). This function adds only the RPC-specific Payload shape, the
// Stage→(TxIdx, OpIdx) cursor-sentinel mapping, and the cursor ordering.
func LCMViewToPayloads(lcm xdr.LedgerCloseMetaView) ([]Payload, error) {
	// The SDK extractor handles every LCM version; the V0 sentinel is
	// RPC-specific policy (distinguish "can't have events" from "had
	// none"), so the discriminator is read here.
	disc, err := lcmVersion(lcm)
	if err != nil {
		return nil, err
	}
	if disc == 0 {
		return nil, ErrV0Unsupported
	}
	ledgerSeq, err := lcm.LedgerSequence()
	if err != nil {
		return nil, err
	}
	ledgerClosedAt, err := lcm.LedgerCloseTime()
	if err != nil {
		return nil, err
	}

	txEvents, err := ingest.ExtractLedgerEvents(lcm)
	if err != nil {
		return nil, err
	}
	at := func(i int) (uint32, xdr.Hash) {
		return uint32(i) + 1, xdr.Hash(txEvents[i].Hash) //nolint:gosec // 1-based, matching ingest reader's tx.Index
	}
	// Every top-level TransactionEvent emits exactly one payload (in whichever
	// pass matches its stage) and every per-op event one more, so the total is
	// known up front — preallocate to avoid the append growth.
	payloads := make([]Payload, 0, countPayloads(txEvents))
	// Pass 1 — BeforeAllTxs across the whole ledger (cursor (0, 0); the
	// per-event index is the apply-order position within the group).
	for i := range txEvents {
		applyIdx, txHash := at(i)
		payloads, err = appendStageEventPayloads(payloads, txEvents[i].TransactionEvents,
			xdr.TransactionEventStageTransactionEventStageBeforeAllTxs,
			txHash, applyIdx, ledgerSeq, ledgerClosedAt)
		if err != nil {
			return nil, err
		}
	}
	// Pass 2 — per tx: op events, then the tx's AfterTx events (whose
	// (txIdx, OperationMask) cursor sorts after every op event of the tx).
	for i := range txEvents {
		applyIdx, txHash := at(i)
		for opIdx, opEvents := range txEvents[i].OperationEvents {
			for _, evRaw := range opEvents {
				payloads = append(payloads, Payload{
					TxHash:             txHash,
					LedgerSequence:     ledgerSeq,
					TxIdx:              applyIdx,
					OpIdx:              uint32(opIdx),
					LedgerClosedAt:     ledgerClosedAt,
					ContractEventBytes: evRaw,
				})
			}
		}
		payloads, err = appendStageEventPayloads(payloads, txEvents[i].TransactionEvents,
			xdr.TransactionEventStageTransactionEventStageAfterTx,
			txHash, applyIdx, ledgerSeq, ledgerClosedAt)
		if err != nil {
			return nil, err
		}
	}
	// Pass 3 — AfterAllTxs across the whole ledger (cursor
	// (TransactionMask, 0), the ledger tail).
	for i := range txEvents {
		applyIdx, txHash := at(i)
		payloads, err = appendStageEventPayloads(payloads, txEvents[i].TransactionEvents,
			xdr.TransactionEventStageTransactionEventStageAfterAllTxs,
			txHash, applyIdx, ledgerSeq, ledgerClosedAt)
		if err != nil {
			return nil, err
		}
	}
	return payloads, nil
}

// countPayloads sums the per-tx top-level event + per-op event counts — the
// exact number of payloads LCMViewToPayloads emits across its three passes,
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

// lcmVersion reads the LedgerCloseMeta union discriminator off the view.
func lcmVersion(lcm xdr.LedgerCloseMetaView) (int32, error) {
	dv, err := lcm.V()
	if err != nil {
		return 0, fmt.Errorf("events: LCM.V: %w", err)
	}
	disc, err := dv.Value()
	if err != nil {
		return 0, fmt.Errorf("events: LCM.V value: %w", err)
	}
	return disc, nil
}

// appendStageEventPayloads emits the V4 top-level TransactionEvents whose
// Stage equals wantStage, skipping the other (known) stages — LCMViewToPayloads
// calls it once per stage per its cursor-ordered pass structure. An UNKNOWN
// stage errors in every pass (via StageSentinels), so a new protocol stage can
// never be silently dropped by the stage filter. Stage and the inner
// ContractEvent bytes are both read through the generated zero-copy view
// accessors — no UnmarshalBinary, no re-encode. This matters: under CAP-67
// every protocol-23+ transaction emits 1-2 fee TransactionEvents, so top-level
// events scale with tx count, not per-ledger. The emitted ContractEventBytes
// ALIAS the LCM view buffer, the same lifetime contract as the per-operation
// events.
func appendStageEventPayloads(
	dst []Payload, txEventRaws [][]byte, wantStage xdr.TransactionEventStage,
	txHash xdr.Hash, applyIdx, ledgerSeq uint32, ledgerClosedAt int64,
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
			ContractEventBytes: evRaw,
		})
	}
	return dst, nil
}
