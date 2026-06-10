package views

import (
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
)

// ErrV0Unsupported is returned by ExtractEvents on an LCM with
// discriminator V0. V0 (pre-Soroban) ledgers carry no contract events at
// all, so the events extractor has nothing to produce; it signals callers
// to skip / fall back (events.LCMToPayloads) rather than emit an empty
// result that could be confused with a genuinely event-free V1+ ledger.
// (This sentinel is NOT about apply order: the package treats TxProcessing
// order as apply order for every version, and ExtractTransactions derives
// V0 ApplicationOrder directly from the TxProcessing index.)
var ErrV0Unsupported = errors.New("views: LCM V0 carries no contract events (caller should skip / fall back)")

// ExtractEvents walks a zero-copy LedgerCloseMetaView and returns one
// events.Payload per emitted contract event, in the same chronological
// order as the parsed path (events.LCMToPayloads). ContractEventBytes
// aliases the view buffer (zero-copy); callers must copy what they
// retain. Returns ErrV0Unsupported for V0 LCMs.
//
// The produced Payloads carry only ContractEventBytes (the raw event
// XDR) and the scalar metadata fields — term keys are NOT precomputed
// here; downstream derives them via events.TermsForBytes on the bytes
// (this matches the current events.Payload API on this branch and the
// hot store's IngestLedgerEvents, which already calls TermsForBytes).
//
// Ordering matches LCMToPayloads exactly: per-tx in apply order; within
// each tx, top-level TransactionEvents first (V4 only, dispatched on
// Stage with ledger-wide before/after counters), then per-operation
// events in (op, event) order.
//
// V3 events are emitted whenever SorobanMeta is present, relying on the
// package's trusted-input invariant (see doc.go) rather than the paired
// envelope.
//
// Supported shapes:
//
//   - LCM V1, V2 (apply order = TxProcessing array order)
//   - TransactionMeta V0, V1, V2 (no events, skipped; V0 is legacy
//     pre-Soroban meta that the SDK reference path rejects but full-history
//     backfill must tolerate)
//   - TransactionMeta V3 (SorobanMeta.Events as op-0 events; no
//     top-level TransactionEvents in V3)
//   - TransactionMeta V4 (top-level Events + Operations[i].Events)
func ExtractEvents(lcm xdr.LedgerCloseMetaView) ([]events.Payload, error) {
	d, err := dispatchLCM(lcm)
	if err != nil {
		return nil, err
	}
	if d.disc == 0 {
		return nil, ErrV0Unsupported
	}

	ledgerSeq, ledgerClosedAt, err := readLedgerHeader(d.header)
	if err != nil {
		return nil, err
	}

	// Ledger-wide event-index counters for V4 top-level TransactionEvents.
	// These span the WHOLE ledger (NOT reset per-tx) — matches the
	// struct path's beforeIndex/afterIndex in LCMToPayloads. The
	// per-tx AfterTx counter (txAfterIndex) lives inside the loop body.
	state := txWalkState{
		ledgerSeq:      ledgerSeq,
		ledgerClosedAt: ledgerClosedAt,
	}

	var payloads []events.Payload
	applyIdx := uint32(0)
	for tx, iterErr := range d.tp {
		if iterErr != nil {
			return nil, fmt.Errorf("views: TxProcessing iter: %w", iterErr)
		}
		applyIdx++ // 1-based, matching ingest reader's tx.Index

		txHash, err := readTxHash(tx)
		if err != nil {
			return nil, err
		}
		state.txHash = txHash
		state.applyIdx = applyIdx

		payloads, err = payloadsFromTxView(tx, &state, payloads)
		if err != nil {
			return nil, err
		}
	}

	return payloads, nil
}

// txWalkState bundles the per-ledger and per-tx state threaded through
// payloadsFromTxView. The before/after counters span the whole ledger;
// txHash and applyIdx are reset each iteration of the TxProcessing
// loop in ExtractEvents.
type txWalkState struct {
	ledgerSeq      uint32
	ledgerClosedAt int64
	txHash         xdr.Hash
	applyIdx       uint32 // 1-based, matches ingest reader's tx.Index
	beforeIndex    uint32 // ledger-wide BeforeAllTxs event counter
	afterIndex     uint32 // ledger-wide AfterAllTxs event counter
}

// payloadsFromTxView dispatches on TransactionMeta version and emits
// the events for one tx. state holds per-ledger and per-tx counters;
// V4 top-level events update state.beforeIndex / afterIndex in place
// so subsequent txs continue from where this one left off.
func payloadsFromTxView(tx txResultMetaView, state *txWalkState, dst []events.Payload) ([]events.Payload, error) {
	metaView, err := tx.TxApplyProcessing()
	if err != nil {
		return nil, fmt.Errorf("views: TxApplyProcessing: %w", err)
	}
	vView, err := metaView.V()
	if err != nil {
		return nil, fmt.Errorf("views: meta.V: %w", err)
	}
	v, err := vView.Value()
	if err != nil {
		return nil, fmt.Errorf("views: meta.V value: %w", err)
	}

	switch v {
	case 0, 1, 2:
		// V0 (legacy pre-Soroban, Operations only), V1, V2 carry no contract
		// events. NB: the struct reference path (LCMToPayloads via the SDK's
		// GetTransactionEvents) actually ERRORS on meta V0 ("unsupported
		// TransactionMeta version: 0") — full-history backfills from genesis and
		// must tolerate legacy V0 meta, so this path is deliberately more
		// permissive than the reference and treats V0 as event-free.
		return dst, nil
	case 3:
		return payloadsFromV3SorobanMeta(metaView, state, dst)
	case 4:
		return payloadsFromV4Meta(metaView, state, dst)
	default:
		return nil, fmt.Errorf("views: unsupported TransactionMeta V=%d", v)
	}
}

// payloadsFromV3SorobanMeta extracts V3 events. Per
// ingest.LedgerTransaction.GetTransactionEvents case 3, only soroban
// txs emit events; the SorobanMeta optional is the carrier. The struct
// path emits SorobanMeta.Events as op-0 events with EventIdx running
// 0..N-1. V3 has no top-level TransactionEvents, so the ledger-wide
// before/after counters in state are not touched.
//
// Events are emitted whenever SorobanMeta is present, under the
// trusted-input invariant in doc.go ("SorobanMeta present ⟺ soroban tx");
// there is no envelope in hand here to re-check, unlike the read path's
// gateV3ContractEvents.
func payloadsFromV3SorobanMeta(metaView xdr.TransactionMetaView, state *txWalkState, dst []events.Payload) ([]events.Payload, error) {
	v3, err := metaView.V3()
	if err != nil {
		return nil, fmt.Errorf("views: meta V3: %w", err)
	}
	smOpt, err := v3.SorobanMeta()
	if err != nil {
		return nil, fmt.Errorf("views: SorobanMeta opt: %w", err)
	}
	sm, present, err := smOpt.Unwrap()
	if err != nil {
		return nil, fmt.Errorf("views: SorobanMeta unwrap: %w", err)
	}
	if !present {
		return dst, nil
	}

	eventsView, err := sm.Events()
	if err != nil {
		return nil, fmt.Errorf("views: SorobanMeta.Events: %w", err)
	}

	payloads := dst
	eventIdx := uint32(0)
	for evView, evErr := range eventsView.Iter() {
		if evErr != nil {
			return nil, fmt.Errorf("views: V3 event iter: %w", evErr)
		}
		evRaw, err := evView.Raw()
		if err != nil {
			return nil, fmt.Errorf("views: V3 ContractEvent.Raw: %w", err)
		}
		payloads = append(payloads, events.Payload{
			TxHash:             state.txHash,
			LedgerSequence:     state.ledgerSeq,
			TxIdx:              state.applyIdx,
			OpIdx:              0,
			LedgerClosedAt:     state.ledgerClosedAt,
			EventIdx:           eventIdx,
			ContractEventBytes: evRaw,
		})
		eventIdx++
	}
	return payloads, nil
}

// payloadsFromV4Meta extracts V4 events:
//
//  1. Top-level TransactionEvents first, dispatched on Stage:
//     BeforeAllTxs -> uses ledger-wide state.beforeIndex
//     AfterAllTxs  -> uses ledger-wide state.afterIndex
//     AfterTx      -> uses per-tx counter (reset here)
//  2. Then per-operation contract events in (op, event) order.
//
// Order matches LCMToPayloads / db.InsertEvents and the (TxIdx, OpIdx,
// EventIdx) sentinels match the struct path's encoding.
//
//nolint:cyclop,funlen // linear V4 pipeline: top-level Events (dispatched on Stage) -> per-op events
func payloadsFromV4Meta(metaView xdr.TransactionMetaView, state *txWalkState, dst []events.Payload) ([]events.Payload, error) {
	v4, err := metaView.V4()
	if err != nil {
		return nil, fmt.Errorf("views: meta V4: %w", err)
	}

	payloads := dst

	// Top-level TransactionEvents.
	txEventsView, err := v4.Events()
	if err != nil {
		return nil, fmt.Errorf("views: V4 Events: %w", err)
	}
	txAfterIndex := uint32(0)
	for tevView, tevErr := range txEventsView.Iter() {
		if tevErr != nil {
			return nil, fmt.Errorf("views: V4 tx event iter: %w", tevErr)
		}
		stageView, err := tevView.Stage()
		if err != nil {
			return nil, fmt.Errorf("views: V4 tx event Stage: %w", err)
		}
		stage, err := stageView.Value()
		if err != nil {
			return nil, fmt.Errorf("views: V4 tx event Stage value: %w", err)
		}
		evView, err := tevView.Event()
		if err != nil {
			return nil, fmt.Errorf("views: V4 tx event Event: %w", err)
		}
		evRaw, err := evView.Raw()
		if err != nil {
			return nil, fmt.Errorf("views: V4 tx ContractEvent.Raw: %w", err)
		}
		var txIdx, opIdx, eventIdx uint32
		switch stage {
		case xdr.TransactionEventStageTransactionEventStageBeforeAllTxs:
			txIdx, opIdx, eventIdx = 0, 0, state.beforeIndex
			state.beforeIndex++
		case xdr.TransactionEventStageTransactionEventStageAfterAllTxs:
			txIdx, opIdx, eventIdx = uint32(toid.TransactionMask), 0, state.afterIndex
			state.afterIndex++
		case xdr.TransactionEventStageTransactionEventStageAfterTx:
			txIdx, opIdx, eventIdx = state.applyIdx, uint32(toid.OperationMask), txAfterIndex
			txAfterIndex++
		default:
			return nil, fmt.Errorf("views: unhandled tx event stage %v", stage)
		}
		payloads = append(payloads, events.Payload{
			TxHash:             state.txHash,
			LedgerSequence:     state.ledgerSeq,
			TxIdx:              txIdx,
			OpIdx:              opIdx,
			LedgerClosedAt:     state.ledgerClosedAt,
			EventIdx:           eventIdx,
			ContractEventBytes: evRaw,
		})
	}

	// Per-operation contract events.
	opsView, err := v4.Operations()
	if err != nil {
		return nil, fmt.Errorf("views: V4 Operations: %w", err)
	}
	opIdx := uint32(0)
	for opView, opErr := range opsView.Iter() {
		if opErr != nil {
			return nil, fmt.Errorf("views: V4 op iter: %w", opErr)
		}
		opEventsView, err := opView.Events()
		if err != nil {
			return nil, fmt.Errorf("views: V4 op.Events: %w", err)
		}
		eventIdx := uint32(0)
		for evView, evErr := range opEventsView.Iter() {
			if evErr != nil {
				return nil, fmt.Errorf("views: V4 op event iter: %w", evErr)
			}
			evRaw, err := evView.Raw()
			if err != nil {
				return nil, fmt.Errorf("views: V4 op ContractEvent.Raw: %w", err)
			}
			payloads = append(payloads, events.Payload{
				TxHash:             state.txHash,
				LedgerSequence:     state.ledgerSeq,
				TxIdx:              state.applyIdx,
				OpIdx:              opIdx,
				LedgerClosedAt:     state.ledgerClosedAt,
				EventIdx:           eventIdx,
				ContractEventBytes: evRaw,
			})
			eventIdx++
		}
		opIdx++
	}
	return payloads, nil
}
