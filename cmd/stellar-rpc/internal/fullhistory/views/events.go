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
// at read time); the structural navigation and event grouping live in the SDK
// (ingest.TransactionEventsFromMeta). This function adds only the RPC-specific
// Payload shape and the Stage→(TxIdx, OpIdx) cursor-sentinel mapping.
func ExtractEvents(lcm xdr.LedgerCloseMetaView) ([]events.Payload, error) {
	d, err := ingest.DispatchLedgerCloseMetaView(lcm)
	if err != nil {
		return nil, err
	}
	if d.Version == 0 {
		return nil, ErrV0Unsupported
	}
	ledgerSeq, ledgerClosedAt, err := d.Header()
	if err != nil {
		return nil, err
	}

	var payloads []events.Payload
	applyIdx := uint32(0)
	for tx, iterErr := range d.TxProcessing() {
		if iterErr != nil {
			return nil, fmt.Errorf("views: TxProcessing iter: %w", iterErr)
		}
		applyIdx++ // 1-based, matching ingest reader's tx.Index

		txHash, err := ingest.TxProcessingHash(tx)
		if err != nil {
			return nil, err
		}
		metaView, err := tx.TxApplyProcessing()
		if err != nil {
			return nil, fmt.Errorf("views: TxApplyProcessing: %w", err)
		}
		tev, err := ingest.TransactionEventsFromMeta(metaView)
		if err != nil {
			return nil, err
		}

		payloads, err = appendTopLevelEventPayloads(payloads, tev.TransactionEvents, txHash, applyIdx, ledgerSeq, ledgerClosedAt)
		if err != nil {
			return nil, err
		}
		for opIdx, opEvents := range tev.OperationEvents {
			for _, evRaw := range opEvents {
				payloads = append(payloads, events.Payload{
					TxHash:             txHash,
					LedgerSequence:     ledgerSeq,
					TxIdx:              applyIdx,
					OpIdx:              uint32(opIdx), //nolint:gosec // op index bounded by protocol limits
					LedgerClosedAt:     ledgerClosedAt,
					ContractEventBytes: evRaw,
				})
			}
		}
	}

	return payloads, nil
}

// appendTopLevelEventPayloads emits the V4 top-level TransactionEvents.
// Stage (which maps to the (TxIdx, OpIdx) cursor sentinels via
// events.StageSentinels) and the inner ContractEvent bytes are both read
// through the generated zero-copy view accessors — no UnmarshalBinary, no
// re-encode. This matters: under CAP-67 every protocol-23+ transaction emits
// 1-2 fee TransactionEvents, so top-level events scale with tx count, not
// per-ledger. The emitted ContractEventBytes ALIAS the LCM view buffer, the
// same lifetime contract as the per-operation events.
func appendTopLevelEventPayloads(dst []events.Payload, txEventRaws [][]byte, txHash xdr.Hash, applyIdx, ledgerSeq uint32, ledgerClosedAt int64) ([]events.Payload, error) {
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
