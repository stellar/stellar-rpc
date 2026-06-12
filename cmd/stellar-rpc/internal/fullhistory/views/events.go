package views

import (
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
)

// ErrV0Unsupported is returned by ExtractEvents on an LCM with discriminator
// V0. V0 (pre-Soroban) ledgers carry no contract events at all, so the events
// extractor has nothing to produce; it signals callers to skip / fall back
// (events.LCMToPayloads) rather than emit an empty result that could be
// confused with a genuinely event-free V1+ ledger.
var ErrV0Unsupported = errors.New("views: LCM V0 carries no contract events (caller should skip / fall back)")

// ExtractEvents walks a zero-copy LedgerCloseMetaView and returns one
// events.Payload per emitted contract event, in the same chronological order as
// the parsed path (events.LCMToPayloads): per-tx in apply order; within each tx,
// top-level TransactionEvents first (V4 only, dispatched on Stage to the
// (TxIdx, OpIdx) cursor sentinels), then per-operation events in (op, event)
// order. ALL ContractEventBytes — top-level and per-op — alias the view buffer
// (zero-copy); callers copy what they retain. Returns ErrV0Unsupported for V0
// LCMs.
//
// The per-event index is NOT computed here (it is positional and reconstructed
// at read time); the navigation, per-tx hashing, and event grouping live in
// the SDK (ingest.ExtractLedgerEvents — one TxProcessing walk yields hash +
// events together). This function adds only the RPC-specific Payload shape and
// the Stage→(TxIdx, OpIdx) cursor-sentinel mapping.
func ExtractEvents(lcm xdr.LedgerCloseMetaView) ([]events.Payload, error) {
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
	var payloads []events.Payload
	for i := range txEvents {
		applyIdx := uint32(i) + 1 // 1-based, matching ingest reader's tx.Index //nolint:gosec
		txHash := xdr.Hash(txEvents[i].Hash)

		payloads, err = appendTopLevelEventPayloads(
			payloads, txEvents[i].TransactionEvents, txHash, applyIdx, ledgerSeq, ledgerClosedAt)
		if err != nil {
			return nil, err
		}
		for opIdx, opEvents := range txEvents[i].OperationEvents {
			for _, evRaw := range opEvents {
				payloads = append(payloads, events.Payload{
					TxHash:             txHash,
					LedgerSequence:     ledgerSeq,
					TxIdx:              applyIdx,
					OpIdx:              uint32(opIdx), //nolint:gosec // op count fits uint32
					LedgerClosedAt:     ledgerClosedAt,
					ContractEventBytes: evRaw,
				})
			}
		}
	}
	return payloads, nil
}

// lcmVersion reads the LedgerCloseMeta union discriminator off the view.
func lcmVersion(lcm xdr.LedgerCloseMetaView) (int32, error) {
	dv, err := lcm.V()
	if err != nil {
		return 0, fmt.Errorf("views: LCM.V: %w", err)
	}
	disc, err := dv.Value()
	if err != nil {
		return 0, fmt.Errorf("views: LCM.V value: %w", err)
	}
	return disc, nil
}

// appendTopLevelEventPayloads emits the V4 top-level TransactionEvents.
// Stage (which maps to the (TxIdx, OpIdx) cursor sentinels via
// events.StageSentinels) and the inner ContractEvent bytes are both read
// through the generated zero-copy view accessors — no UnmarshalBinary, no
// re-encode. This matters: under CAP-67 every protocol-23+ transaction emits
// 1-2 fee TransactionEvents, so top-level events scale with tx count, not
// per-ledger. The emitted ContractEventBytes ALIAS the LCM view buffer, the
// same lifetime contract as the per-operation events.
func appendTopLevelEventPayloads(
	dst []events.Payload, txEventRaws [][]byte, txHash xdr.Hash, applyIdx, ledgerSeq uint32, ledgerClosedAt int64,
) ([]events.Payload, error) {
	for _, raw := range txEventRaws {
		tev := xdr.TransactionEventView(raw)
		stageView, err := tev.Stage()
		if err != nil {
			return nil, fmt.Errorf("views: tx event Stage: %w", err)
		}
		stage, err := stageView.Value()
		if err != nil {
			return nil, fmt.Errorf("views: tx event Stage value: %w", err)
		}
		txIdx, opIdx, err := events.StageSentinels(stage, applyIdx)
		if err != nil {
			return nil, fmt.Errorf("views: %w", err)
		}
		evView, err := tev.Event()
		if err != nil {
			return nil, fmt.Errorf("views: tx event Event: %w", err)
		}
		evRaw, err := evView.Raw()
		if err != nil {
			return nil, fmt.Errorf("views: tx ContractEvent.Raw: %w", err)
		}
		dst = append(dst, events.Payload{
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
