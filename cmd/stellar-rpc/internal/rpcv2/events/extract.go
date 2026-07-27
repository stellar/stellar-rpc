package events

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// PayloadShaper shapes a per-transaction event feed — one
// ingest.LedgerTransactionEvents at a time, in apply order — into one Payload per emitted
// contract event, in ASCENDING getEvents cursor order — the order the SQLite
// path serves (ORDER BY id ASC in sqlitedb/event.go). The event store serves in write
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
// The shaper accumulates the three cursor groups in per-group buckets as Add
// streams the transactions in apply order, and Finish concatenates them —
// the same sequence a three-pass shaping of the whole-ledger slice produced,
// from ONE walk over the ledger.
//
// ALL ContractEventBytes — top-level and per-op — alias the walked view's
// buffer (zero-copy); callers copy what they retain.
//
// Each payload's EventIdx is the event's position within its (txIdx, opIdx)
// cursor group — an op event's index within its operation, a stage event's
// index within its stage group (BeforeAllTxs/AfterAllTxs counted ledger-wide
// in tx-apply order, AfterTx counted per transaction). This is the same
// assignment the SQLite path makes (sqlitedb/event.go), so the trailing
// <TOID>-<eventIdx> component of the v1 getEvents ID matches across backends.
// The navigation, per-tx hashing, and event grouping live in the SDK
// (ingest.ExtractLedgerEvents — one TxProcessing walk yields hash + events
// together). The shaper adds only the RPC-specific Payload shape, the
// Stage→(TxIdx, OpIdx) cursor-sentinel mapping, EventIdx, and the cursor
// ordering.
//
// Feeding per transaction (rather than re-walking a view) lets a caller
// that also needs the paired tx hashes — the hot ingest path and the cold
// materializer — feed BOTH txhash and events from ONE ExtractLedgerEvents
// walk. ledgerSeq and ledgerClosedAt are the view's
// header values (cheap reads, not a walk).
type PayloadShaper struct {
	ledgerSeq      uint32
	ledgerClosedAt int64
	// The three cursor groups, in their concatenation order. mid holds group
	// 2 (per-tx op events + AfterTx); before/afterAll hold the ledger-wide
	// stage groups.
	before   []Payload
	mid      []Payload
	afterAll []Payload
	// Ledger-wide eventIdx counters for the BeforeAllTxs/AfterAllTxs groups
	// (AfterTx counts per transaction, so its counter lives in Add).
	beforeIdx   uint32
	afterAllIdx uint32
}

// NewPayloadShaper returns a shaper for one ledger's stream. ledgerSeq and
// ledgerClosedAt are stamped into every payload.
func NewPayloadShaper(ledgerSeq uint32, ledgerClosedAt int64) PayloadShaper {
	return PayloadShaper{ledgerSeq: ledgerSeq, ledgerClosedAt: ledgerClosedAt}
}

// Begin presizes the buckets from the ledger's transaction count. Under
// CAP-67 every protocol-23+
// transaction emits its fee event into the BeforeAllTxs group, so before
// scales with the tx count; mid gets at least the refund events plus every
// op event (growth beyond txCount is amortized append). AfterAllTxs events
// are rare, so that bucket allocates on first use.
func (s *PayloadShaper) Begin(txCount int) {
	s.before = make([]Payload, 0, txCount)
	s.mid = make([]Payload, 0, txCount)
}

// Add shapes one transaction's events into the buckets. txIdx is the tx's
// dense apply-order
// index. An UNKNOWN top-level event stage errors here (via StageSentinels
// inside appendStageEventPayloads), so a new protocol stage can never be
// silently dropped.
func (s *PayloadShaper) Add(txIdx int, ev ingest.LedgerTransactionEvents) error {
	applyIdx := uint32(txIdx) + 1 //nolint:gosec // 1-based, matching ingest reader's tx.Index
	txHash := xdr.Hash(ev.Hash)
	var err error
	// Group 1 — BeforeAllTxs (cursor (0, 0); eventIdx is the apply-order
	// position within the group, so the counter spans the whole ledger).
	s.before, err = appendStageEventPayloads(s.before, ev.TransactionEvents,
		xdr.TransactionEventStageTransactionEventStageBeforeAllTxs,
		txHash, applyIdx, s.ledgerSeq, s.ledgerClosedAt, &s.beforeIdx)
	if err != nil {
		return err
	}
	// Group 2 — the tx's op events, then its AfterTx events (whose
	// (txIdx, OperationMask) cursor sorts after every op event of the tx).
	for opIdx, opEvents := range ev.OperationEvents {
		// eventIdx is the event's position within its operation.
		for evIdx, evRaw := range opEvents {
			s.mid = append(s.mid, Payload{
				TxHash:             txHash,
				LedgerSequence:     s.ledgerSeq,
				TxIdx:              applyIdx,
				OpIdx:              uint32(opIdx),
				LedgerClosedAt:     s.ledgerClosedAt,
				EventIdx:           uint32(evIdx),
				ContractEventBytes: evRaw,
			})
		}
	}
	// AfterTx cursor (txIdx, OperationMask) is per-transaction, so eventIdx
	// resets for each tx.
	var afterTxIdx uint32
	s.mid, err = appendStageEventPayloads(s.mid, ev.TransactionEvents,
		xdr.TransactionEventStageTransactionEventStageAfterTx,
		txHash, applyIdx, s.ledgerSeq, s.ledgerClosedAt, &afterTxIdx)
	if err != nil {
		return err
	}
	// Group 3 — AfterAllTxs (cursor (TransactionMask, 0), the ledger tail;
	// eventIdx counts ledger-wide in apply order, like BeforeAllTxs).
	s.afterAll, err = appendStageEventPayloads(s.afterAll, ev.TransactionEvents,
		xdr.TransactionEventStageTransactionEventStageAfterAllTxs,
		txHash, applyIdx, s.ledgerSeq, s.ledgerClosedAt, &s.afterAllIdx)
	return err
}

// Finish returns the ledger's payloads in cursor order: the BeforeAllTxs
// group, the per-tx groups, the AfterAllTxs group. When both ledger-wide
// stage groups are empty (every pre-CAP-67 ledger) the middle bucket IS the
// result — no copy. The shaper is spent after Finish.
func (s *PayloadShaper) Finish() []Payload {
	if len(s.before) == 0 && len(s.afterAll) == 0 {
		return s.mid
	}
	out := make([]Payload, 0, len(s.before)+len(s.mid)+len(s.afterAll))
	out = append(out, s.before...)
	out = append(out, s.mid...)
	return append(out, s.afterAll...)
}

// appendStageEventPayloads emits the V4 top-level TransactionEvents whose
// Stage equals wantStage, skipping the other (known) stages — PayloadShaper.Add
// calls it once per stage per its cursor-ordered group structure. An UNKNOWN
// stage errors in every group (via StageSentinels), so a new protocol stage can
// never be silently dropped by the stage filter. Stage and the inner
// ContractEvent bytes are both read through the generated zero-copy view
// accessors — no UnmarshalBinary, no re-encode. This matters: under CAP-67
// every protocol-23+ transaction emits 1-2 fee TransactionEvents, so top-level
// events scale with tx count, not per-ledger. The emitted ContractEventBytes
// ALIAS the LCM view buffer, the same lifetime contract as the per-operation
// events.
//
// eventIdx points at the caller's per-group counter: ledger-wide for the
// BeforeAllTxs/AfterAllTxs groups, per-transaction for AfterTx. It is read for
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
		// here, in whichever group sees it first.
		txIdx, opIdx, err := store.StageSentinels(stage, applyIdx)
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
