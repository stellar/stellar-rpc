package events

import (
	"errors"
	"fmt"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ErrLCMV0Unsupported is returned by LCMToPayloadsFromRaw on an LCM
// with discriminator V0. V0's apply order requires hash-XOR-prevHash
// sorting that the view machinery doesn't expose cheaply. Callers
// handling V0 LCMs should fall back to LCMToPayloads (struct path).
var ErrLCMV0Unsupported = errors.New("events: LCM V0 not supported by view path (apply order requires sort)")

// LCMToPayloadsFromRaw is a view-based counterpart to LCMToPayloads
// that produces the same []Payload from raw LedgerCloseMeta wire bytes
// without calling lcm.UnmarshalBinary. It walks xdr.LedgerCloseMetaView
// directly, extracts each event's bytes via .Raw() (zero-copy slice
// into the input buffer), and hashes each topic's .Raw() bytes inline
// to populate Terms — eliminating every XDR Marshal/Unmarshal call on
// the ingest hot path (lcm.UnmarshalBinary, ContractEvent.MarshalBinary,
// topic.MarshalBinary).
//
// The produced Payloads carry ContractEventBytes (the raw event XDR)
// and Terms (precomputed term keys). Downstream Payload.Marshal and
// HotStore.IngestLedgerEvents use those cached fields to skip the
// re-marshaling work the struct-based path does.
//
// Ordering matches LCMToPayloads exactly: per-tx in apply order; within
// each tx, top-level TransactionEvents first (V4 only, dispatched on
// Stage with ledger-wide before/after counters), then per-operation
// events in (op, event) order. Cross-checked against the struct path
// in ingest_view_test.go.
//
// Supported shapes:
//
//   - LCM V1, V2 (apply order = TxProcessing array order)
//   - TransactionMeta V1, V2 (no events, skipped)
//   - TransactionMeta V3 (SorobanMeta.Events as op-0 events; no
//     top-level TransactionEvents in V3)
//   - TransactionMeta V4 (top-level Events + Operations[i].Events)
//
// LCM V0 is not supported: its apply order requires hash-XOR-prevHash
// sorting that the view machinery doesn't expose cheaply. Callers
// handling V0 should fall back to the struct-based LCMToPayloads.
//
// passphrase is accepted for API symmetry with LCMToPayloads but
// ignored — the view path reads tx hashes directly from
// TxProcessing[i].Result.TransactionHash rather than recomputing.
//
func LCMToPayloadsFromRaw(passphrase string, raw []byte) ([]Payload, error) {
	return LCMToPayloadsFromRawInto(passphrase, raw, nil)
}

// LCMToPayloadsFromRawInto is LCMToPayloadsFromRaw that appends into dst
// (reusing its capacity) rather than allocating a fresh slice — so a
// caller ingesting many ledgers avoids a per-ledger []Payload
// allocation. The returned slice aliases dst, and (as with
// LCMToPayloadsFromRaw) each Payload's ContractEventBytes aliases raw;
// both are valid only until the buffer is reused / raw is overwritten.
//
//nolint:cyclop,funlen // linear pipeline: dispatch LCM V → open header + TxProcessing → walk
func LCMToPayloadsFromRawInto(passphrase string, raw []byte, dst []Payload) ([]Payload, error) {
	_ = passphrase // unused: tx hashes come from TxProcessing, not recomputed

	lcmView := xdr.LedgerCloseMetaView(raw)
	dv, err := lcmView.V()
	if err != nil {
		return nil, fmt.Errorf("events: view LCM.V: %w", err)
	}
	disc, err := dv.Value()
	if err != nil {
		return nil, fmt.Errorf("events: view LCM.V value: %w", err)
	}

	var (
		headerView   xdr.LedgerHeaderHistoryEntryView
		txProcessing func() (txProcessingIterable, error)
	)
	switch disc {
	case 0:
		return nil, ErrLCMV0Unsupported
	case 1:
		v1, err := lcmView.V1()
		if err != nil {
			return nil, fmt.Errorf("events: view LCM V1: %w", err)
		}
		headerView, err = v1.LedgerHeader()
		if err != nil {
			return nil, fmt.Errorf("events: view V1 LedgerHeader: %w", err)
		}
		txProcessing = func() (txProcessingIterable, error) {
			tp, err := v1.TxProcessing()
			return v1TxProcessingIter{tp}, err
		}
	case 2:
		v2, err := lcmView.V2()
		if err != nil {
			return nil, fmt.Errorf("events: view LCM V2: %w", err)
		}
		headerView, err = v2.LedgerHeader()
		if err != nil {
			return nil, fmt.Errorf("events: view V2 LedgerHeader: %w", err)
		}
		txProcessing = func() (txProcessingIterable, error) {
			tp, err := v2.TxProcessing()
			return v2TxProcessingIter{tp}, err
		}
	default:
		return nil, fmt.Errorf("events: unknown LCM V=%d", disc)
	}

	ledgerSeq, ledgerClosedAt, err := readLedgerHeader(headerView)
	if err != nil {
		return nil, err
	}

	tp, err := txProcessing()
	if err != nil {
		return nil, fmt.Errorf("events: view TxProcessing: %w", err)
	}

	// Ledger-wide event-index counters for V4 top-level TransactionEvents.
	// These span the WHOLE ledger (NOT reset per-tx) — matches the
	// struct path's beforeIndex/afterIndex in LCMToPayloads. The
	// per-tx AfterTx counter (txAfterIndex) lives inside the loop body.
	state := txWalkState{
		ledgerSeq:      ledgerSeq,
		ledgerClosedAt: ledgerClosedAt,
	}

	payloads := dst[:0]
	applyIdx := uint32(0)
	for tx, iterErr := range tp.iter() {
		if iterErr != nil {
			return nil, fmt.Errorf("events: view TxProcessing iter: %w", iterErr)
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
// loop in LCMToPayloadsFromRaw.
type txWalkState struct {
	ledgerSeq      uint32
	ledgerClosedAt int64
	txHash         xdr.Hash
	applyIdx       uint32 // 1-based, matches ingest reader's tx.Index
	beforeIndex    uint32 // ledger-wide BeforeAllTxs event counter
	afterIndex     uint32 // ledger-wide AfterAllTxs event counter
}

// txProcessingIterable abstracts over the two SDK TxProcessing array
// view types (V1/V2 LCM use distinct generated views) so the dispatch
// switch above can share the iteration body. Both yield a
// txResultMetaView with the methods we need.
type txProcessingIterable interface {
	iter() func(yield func(txResultMetaView, error) bool)
}

// txResultMetaView is the view-side interface satisfied by both
// TransactionResultMetaView (V0/V1 LCM TxProcessing element) and
// TransactionResultMetaV1View (V2 LCM TxProcessing element).
type txResultMetaView interface {
	Result() (xdr.TransactionResultPairView, error)
	TxApplyProcessing() (xdr.TransactionMetaView, error)
}

type v1TxProcessingIter struct {
	tp xdr.LedgerCloseMetaV1TxProcessingView
}

func (v v1TxProcessingIter) iter() func(yield func(txResultMetaView, error) bool) {
	return func(yield func(txResultMetaView, error) bool) {
		for elem, err := range v.tp.Iter() {
			if !yield(elem, err) {
				return
			}
		}
	}
}

type v2TxProcessingIter struct {
	tp xdr.LedgerCloseMetaV2TxProcessingView
}

func (v v2TxProcessingIter) iter() func(yield func(txResultMetaView, error) bool) {
	return func(yield func(txResultMetaView, error) bool) {
		for elem, err := range v.tp.Iter() {
			if !yield(elem, err) {
				return
			}
		}
	}
}

// readLedgerHeader extracts (LedgerSequence, LedgerCloseTime) from a
// LedgerHeaderHistoryEntryView via the same path xdr.LedgerCloseMeta's
// LedgerSequence/LedgerCloseTime helpers use, but staying view-side.
func readLedgerHeader(header xdr.LedgerHeaderHistoryEntryView) (uint32, int64, error) {
	inner, err := header.Header()
	if err != nil {
		return 0, 0, fmt.Errorf("events: view Header.Header: %w", err)
	}
	seqView, err := inner.LedgerSeq()
	if err != nil {
		return 0, 0, fmt.Errorf("events: view LedgerSeq: %w", err)
	}
	seqVal, err := seqView.Value()
	if err != nil {
		return 0, 0, fmt.Errorf("events: view LedgerSeq value: %w", err)
	}
	scp, err := inner.ScpValue()
	if err != nil {
		return 0, 0, fmt.Errorf("events: view ScpValue: %w", err)
	}
	ctView, err := scp.CloseTime()
	if err != nil {
		return 0, 0, fmt.Errorf("events: view CloseTime: %w", err)
	}
	ctVal, err := ctView.Value()
	if err != nil {
		return 0, 0, fmt.Errorf("events: view CloseTime value: %w", err)
	}
	return uint32(seqVal), int64(ctVal), nil //nolint:gosec,unconvert // bounded
}

// readTxHash extracts the 32-byte TransactionHash from a TxProcessing
// entry view (TransactionResultPair.TransactionHash).
func readTxHash(tx txResultMetaView) (xdr.Hash, error) {
	var h xdr.Hash
	rp, err := tx.Result()
	if err != nil {
		return h, fmt.Errorf("events: view Result: %w", err)
	}
	hv, err := rp.TransactionHash()
	if err != nil {
		return h, fmt.Errorf("events: view TransactionHash: %w", err)
	}
	hb, err := hv.Value()
	if err != nil {
		return h, fmt.Errorf("events: view TransactionHash value: %w", err)
	}
	copy(h[:], hb)
	return h, nil
}

// payloadsFromTxView dispatches on TransactionMeta version and emits
// the events for one tx. state holds per-ledger and per-tx counters;
// V4 top-level events update state.beforeIndex / afterIndex in place
// so subsequent txs continue from where this one left off.
// payloadsFromTxView appends one tx's event payloads to dst (reusing its
// capacity) and returns the grown slice, so the per-ledger accumulator is
// threaded through without a per-tx slice allocation.
func payloadsFromTxView(tx txResultMetaView, state *txWalkState, dst []Payload) ([]Payload, error) {
	metaView, err := tx.TxApplyProcessing()
	if err != nil {
		return nil, fmt.Errorf("events: view TxApplyProcessing: %w", err)
	}
	vView, err := metaView.V()
	if err != nil {
		return nil, fmt.Errorf("events: view meta.V: %w", err)
	}
	v, err := vView.Value()
	if err != nil {
		return nil, fmt.Errorf("events: view meta.V value: %w", err)
	}

	switch v {
	case 1, 2:
		return dst, nil
	case 3:
		return payloadsFromV3SorobanMeta(metaView, state, dst)
	case 4:
		return payloadsFromV4Meta(metaView, state, dst)
	default:
		return nil, fmt.Errorf("events: unsupported TransactionMeta V=%d", v)
	}
}

// payloadsFromV3SorobanMeta extracts V3 events. Per ingest.LedgerTransaction.
// GetTransactionEvents case 3, only soroban txs emit events; the
// SorobanMeta optional is the carrier. The struct path emits
// SorobanMeta.Events as op-0 events with EventIdx running 0..N-1.
// V3 has no top-level TransactionEvents, so the ledger-wide before/
// after counters in state are not touched.
func payloadsFromV3SorobanMeta(metaView xdr.TransactionMetaView, state *txWalkState, dst []Payload) ([]Payload, error) {
	v3, err := metaView.V3()
	if err != nil {
		return nil, fmt.Errorf("events: view meta V3: %w", err)
	}
	smOpt, err := v3.SorobanMeta()
	if err != nil {
		return nil, fmt.Errorf("events: view SorobanMeta opt: %w", err)
	}
	sm, present, err := smOpt.Unwrap()
	if err != nil {
		return nil, fmt.Errorf("events: view SorobanMeta unwrap: %w", err)
	}
	if !present {
		return dst, nil
	}

	eventsView, err := sm.Events()
	if err != nil {
		return nil, fmt.Errorf("events: view SorobanMeta.Events: %w", err)
	}

	payloads := dst
	eventIdx := uint32(0)
	for evView, evErr := range eventsView.Iter() {
		if evErr != nil {
			return nil, fmt.Errorf("events: view V3 event iter: %w", evErr)
		}
		evRaw, terms, err := readContractEventViewBytesAndTerms(evView)
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, Payload{
			TxHash:             state.txHash,
			LedgerSequence:     state.ledgerSeq,
			TxIdx:              state.applyIdx,
			OpIdx:              0,
			LedgerClosedAt:     state.ledgerClosedAt,
			EventIdx:           eventIdx,
			ContractEventBytes: evRaw,
			Terms:              terms,
		})
		eventIdx++
	}
	return payloads, nil
}

// payloadsFromV4Meta extracts V4 events:
//
//  1. Top-level TransactionEvents first, dispatched on Stage:
//     BeforeAllTxs → uses ledger-wide state.beforeIndex
//     AfterAllTxs  → uses ledger-wide state.afterIndex
//     AfterTx      → uses per-tx counter (reset here)
//  2. Then per-operation contract events in (op, event) order.
//
// Order matches LCMToPayloads / db.InsertEvents and the (TxIdx, OpIdx,
// EventIdx) sentinels match the struct path's encoding.
//
//nolint:cyclop,funlen // linear V4 pipeline: top-level Events (dispatched on Stage) → per-op events
func payloadsFromV4Meta(metaView xdr.TransactionMetaView, state *txWalkState, dst []Payload) ([]Payload, error) {
	v4, err := metaView.V4()
	if err != nil {
		return nil, fmt.Errorf("events: view meta V4: %w", err)
	}

	payloads := dst

	// Top-level TransactionEvents.
	txEventsView, err := v4.Events()
	if err != nil {
		return nil, fmt.Errorf("events: view V4 Events: %w", err)
	}
	txAfterIndex := uint32(0)
	for tevView, tevErr := range txEventsView.Iter() {
		if tevErr != nil {
			return nil, fmt.Errorf("events: view V4 tx event iter: %w", tevErr)
		}
		stageView, err := tevView.Stage()
		if err != nil {
			return nil, fmt.Errorf("events: view V4 tx event Stage: %w", err)
		}
		stage, err := stageView.Value()
		if err != nil {
			return nil, fmt.Errorf("events: view V4 tx event Stage value: %w", err)
		}
		evView, err := tevView.Event()
		if err != nil {
			return nil, fmt.Errorf("events: view V4 tx event Event: %w", err)
		}
		evRaw, terms, err := readContractEventViewBytesAndTerms(evView)
		if err != nil {
			return nil, err
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
			return nil, fmt.Errorf("events: unhandled tx event stage %v", stage)
		}
		payloads = append(payloads, Payload{
			TxHash:             state.txHash,
			LedgerSequence:     state.ledgerSeq,
			TxIdx:              txIdx,
			OpIdx:              opIdx,
			LedgerClosedAt:     state.ledgerClosedAt,
			EventIdx:           eventIdx,
			ContractEventBytes: evRaw,
			Terms:              terms,
		})
	}

	// Per-operation contract events.
	opsView, err := v4.Operations()
	if err != nil {
		return nil, fmt.Errorf("events: view V4 Operations: %w", err)
	}
	opIdx := uint32(0)
	for opView, opErr := range opsView.Iter() {
		if opErr != nil {
			return nil, fmt.Errorf("events: view V4 op iter: %w", opErr)
		}
		opEventsView, err := opView.Events()
		if err != nil {
			return nil, fmt.Errorf("events: view V4 op.Events: %w", err)
		}
		eventIdx := uint32(0)
		for evView, evErr := range opEventsView.Iter() {
			if evErr != nil {
				return nil, fmt.Errorf("events: view V4 op event iter: %w", evErr)
			}
			evRaw, terms, err := readContractEventViewBytesAndTerms(evView)
			if err != nil {
				return nil, err
			}
			payloads = append(payloads, Payload{
				TxHash:             state.txHash,
				LedgerSequence:     state.ledgerSeq,
				TxIdx:              state.applyIdx,
				OpIdx:              opIdx,
				LedgerClosedAt:     state.ledgerClosedAt,
				EventIdx:           eventIdx,
				ContractEventBytes: evRaw,
				Terms:              terms,
			})
			eventIdx++
		}
		opIdx++
	}
	return payloads, nil
}

// readContractEventViewBytesAndTerms extracts the raw XDR bytes of a
// ContractEvent view via .Raw() (zero-copy slice into the source LCM
// buffer) and computes its term keys by hashing each topic's .Raw()
// bytes inline — no MarshalBinary anywhere on the path.
//
//nolint:cyclop // linear walk: ContractEvent → ContractID term → Body.V0.Topics terms
func readContractEventViewBytesAndTerms(ev xdr.ContractEventView) ([]byte, []TermKey, error) {
	raw, err := ev.Raw()
	if err != nil {
		return nil, nil, fmt.Errorf("events: view ContractEvent.Raw: %w", err)
	}

	var terms []TermKey

	// Contract ID term (optional).
	cidOpt, err := ev.ContractId()
	if err != nil {
		return nil, nil, fmt.Errorf("events: view ContractId opt: %w", err)
	}
	cidView, present, err := cidOpt.Unwrap()
	if err != nil {
		return nil, nil, fmt.Errorf("events: view ContractId unwrap: %w", err)
	}
	if present {
		cid, err := cidView.Value()
		if err != nil {
			return nil, nil, fmt.Errorf("events: view ContractId value: %w", err)
		}
		terms = append(terms, ComputeTermKey(cid, FieldContractID))
	}

	// Topic terms (Body.V0.Topics). Only Body discriminator V=0 has
	// topics; other variants emit no topic terms.
	body, err := ev.Body()
	if err != nil {
		return nil, nil, fmt.Errorf("events: view ContractEvent.Body: %w", err)
	}
	bodyV, err := body.V()
	if err != nil {
		return nil, nil, fmt.Errorf("events: view Body.V: %w", err)
	}
	bodyVVal, err := bodyV.Value()
	if err != nil {
		return nil, nil, fmt.Errorf("events: view Body.V value: %w", err)
	}
	if bodyVVal != 0 {
		return raw, terms, nil
	}
	v0, err := body.V0()
	if err != nil {
		return nil, nil, fmt.Errorf("events: view Body.V0: %w", err)
	}
	topicsArr, err := v0.Topics()
	if err != nil {
		return nil, nil, fmt.Errorf("events: view Body.V0.Topics: %w", err)
	}
	count, err := topicsArr.Count()
	if err != nil {
		return nil, nil, fmt.Errorf("events: view Topics.Count: %w", err)
	}
	for i := 0; i < count && i < protocol.MaxTopicCount; i++ {
		topic, err := topicsArr.At(i)
		if err != nil {
			return nil, nil, fmt.Errorf("events: view topic[%d]: %w", i, err)
		}
		topicRaw, err := topic.Raw()
		if err != nil {
			return nil, nil, fmt.Errorf("events: view topic[%d].Raw: %w", i, err)
		}
		terms = append(terms, ComputeTermKey(topicRaw, topicField(i)))
	}
	return raw, terms, nil
}
