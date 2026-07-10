package events_test

import (
	"fmt"
	"path"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
)

const testPassphrase = "Test SDF Network ; September 2015"

// lcmViewToPayloads is the view→payloads convenience the production
// LCMViewToPayloads wrapper used to provide before #836 deleted it as test-only:
// the header reads plus the single ExtractLedgerEvents walk, fed to
// PayloadsFromLedgerEvents. The cursor-contract tests exercise it from raw LCM
// bytes; production now walks once at a higher level (hot IngestLedger, cold
// ColdService) and calls PayloadsFromLedgerEvents directly.
func lcmViewToPayloads(lcm xdr.LedgerCloseMetaView) ([]events.Payload, error) {
	seq, err := lcm.LedgerSequence()
	if err != nil {
		return nil, err
	}
	closedAt, err := lcm.LedgerCloseTime()
	if err != nil {
		return nil, err
	}
	txEvents, err := ingest.ExtractLedgerEvents(lcm)
	if err != nil {
		return nil, err
	}
	return events.PayloadsFromLedgerEvents(txEvents, seq, closedAt)
}

// buildContractEvent returns a ContractEvent with a contractID and a
// single symbol topic.
func buildContractEvent(topic string) xdr.ContractEvent {
	var contractID xdr.ContractId
	contractID[0] = 0xab
	contractID[1] = 0xcd
	sym := xdr.ScSymbol(topic)
	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			},
		},
	}
}

func transactionResult() xdr.TransactionResult {
	opResults := []xdr.OperationResult{}
	return xdr.TransactionResult{
		FeeCharged: 100,
		Result: xdr.TransactionResultResult{
			Code:    xdr.TransactionResultCodeTxSuccess,
			Results: &opResults,
		},
	}
}

// txMetaWithOpEvents wraps the given operation-event slices (one outer
// slice per operation, inner slice = events for that op) into a
// TransactionMetaV4 with no transaction-level events.
func txMetaWithOpEvents(opEvents [][]xdr.ContractEvent) xdr.TransactionMeta {
	ops := make([]xdr.OperationMetaV2, len(opEvents))
	for i, evs := range opEvents {
		ops[i] = xdr.OperationMetaV2{Events: evs}
	}
	return xdr.TransactionMeta{
		V:  4,
		V4: &xdr.TransactionMetaV4{Operations: ops},
	}
}

// txMetaWithStagedEvents builds a TransactionMetaV4 carrying the given
// transaction-level events (with Stage) and no operation events.
func txMetaWithStagedEvents(stageEvents []xdr.TransactionEvent) xdr.TransactionMeta {
	return xdr.TransactionMeta{
		V:  4,
		V4: &xdr.TransactionMetaV4{Events: stageEvents},
	}
}

// txMetaWithV3SorobanEvents builds a TransactionMetaV3 whose SorobanMeta
// carries evs as op-0 events. A nil evs slice with a present SorobanMeta
// exercises the zero-events case; pass a nil meta separately for the
// absent-SorobanMeta case.
func txMetaWithV3SorobanEvents(evs []xdr.ContractEvent) xdr.TransactionMeta {
	return xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			SorobanMeta: &xdr.SorobanTransactionMeta{
				Events:      evs,
				ReturnValue: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		},
	}
}

// stagedTxAllStages builds a V4 tx emitting one event in each of the three
// top-level stages (BeforeAllTxs, AfterAllTxs, AfterTx), labeled so the
// differential can tell events of distinct txs apart.
func stagedTxAllStages(label string) xdr.TransactionMeta {
	return txMetaWithStagedEvents([]xdr.TransactionEvent{
		{Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, Event: buildContractEvent(label + "-before")},
		{Stage: xdr.TransactionEventStageTransactionEventStageAfterAllTxs, Event: buildContractEvent(label + "-after")},
		{Stage: xdr.TransactionEventStageTransactionEventStageAfterTx, Event: buildContractEvent(label + "-aftertx")},
	})
}

// cap67FeeTx builds the CAP-67 fee/op/refund shape: a BeforeAllTxs fee event,
// one op event, and an AfterTx fee-refund event. The before-fee/refund pair
// straddling the op event is what distinguishes cursor order from per-tx
// chronological order.
func cap67FeeTx(label string) xdr.TransactionMeta {
	return xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			Events: []xdr.TransactionEvent{
				{Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, Event: buildContractEvent(label + "-fee")},
				{Stage: xdr.TransactionEventStageTransactionEventStageAfterTx, Event: buildContractEvent(label + "-refund")},
			},
			Operations: []xdr.OperationMetaV2{
				{Events: []xdr.ContractEvent{buildContractEvent(label + "-op")}},
			},
		},
	}
}

func buildLCM(t testing.TB, ledgerSeq uint32, closeTimestamp int64, txMetas []xdr.TransactionMeta) xdr.LedgerCloseMeta {
	t.Helper()
	return buildLCMVersion(t, 2, ledgerSeq, closeTimestamp, txMetas)
}

// buildTxEnvelopeAndHash constructs a deterministic soroban-shaped tx
// envelope and its hash under testPassphrase. Shared by every LCM-version
// builder so the differential arms differ only in the LCM/TxSet shape.
func buildTxEnvelopeAndHash(t testing.TB) (xdr.TransactionEnvelope, xdr.Hash) {
	t.Helper()
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
				Ext: xdr.TransactionExt{
					V:           1,
					SorobanData: &xdr.SorobanTransactionData{},
				},
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, testPassphrase)
	require.NoError(t, err)
	return envelope, hash
}

// buildLCMVersion builds either an LCM V1 (LedgerCloseMetaV1 with
// TransactionResultMeta — the *non*-V1 result-meta type — exercising
// LCMViewToPayloads case 1) or an LCM V2 (LedgerCloseMetaV2 with
// TransactionResultMetaV1, case 2). Both use a GeneralizedTransactionSet
// (V1) which is already in apply order, so the SQLite oracle's
// LedgerTransactionReader and the view path agree on ordering.
func buildLCMVersion(
	t testing.TB, version int32, ledgerSeq uint32, closeTimestamp int64, txMetas []xdr.TransactionMeta,
) xdr.LedgerCloseMeta {
	t.Helper()

	phases := make([]xdr.TransactionPhase, 0, len(txMetas))
	v2Processing := make([]xdr.TransactionResultMetaV1, 0, len(txMetas))
	v1Processing := make([]xdr.TransactionResultMeta, 0, len(txMetas))

	for _, meta := range txMetas {
		envelope, hash := buildTxEnvelopeAndHash(t)
		result := xdr.TransactionResultPair{
			TransactionHash: hash,
			Result:          transactionResult(),
		}
		v2Processing = append(v2Processing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: meta,
			Result:            result,
		})
		v1Processing = append(v1Processing, xdr.TransactionResultMeta{
			TxApplyProcessing: meta,
			Result:            result,
		})
		comp := []xdr.TxSetComponent{{
			Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
				Txs: []xdr.TransactionEnvelope{envelope},
			},
		}}
		phases = append(phases, xdr.TransactionPhase{V: 0, V0Components: &comp})
	}

	header := xdr.LedgerHeaderHistoryEntry{
		Header: xdr.LedgerHeader{
			ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimestamp)},
			LedgerSeq: xdr.Uint32(ledgerSeq),
		},
	}
	txSet := xdr.GeneralizedTransactionSet{
		V:       1,
		V1TxSet: &xdr.TransactionSetV1{Phases: phases},
	}

	switch version {
	case 1:
		return xdr.LedgerCloseMeta{
			V: 1,
			V1: &xdr.LedgerCloseMetaV1{
				LedgerHeader: header,
				TxSet:        txSet,
				TxProcessing: v1Processing,
			},
		}
	case 2:
		return xdr.LedgerCloseMeta{
			V: 2,
			V2: &xdr.LedgerCloseMetaV2{
				LedgerHeader: header,
				TxSet:        txSet,
				TxProcessing: v2Processing,
			},
		}
	default:
		t.Fatalf("buildLCMVersion: unsupported version %d", version)
		return xdr.LedgerCloseMeta{}
	}
}

// TestExtractEvents_MatchesSQLite is the cross-backend differential: the
// view-based LCMViewToPayloads must emit, for the same xdr.LedgerCloseMeta, the
// SAME event sequence the legacy SQLite backend serves — insert via
// db's InsertEvents, read back through the GetEvents cursor-ordered query
// (ORDER BY id ASC), and assert the view path's emission order and per-event
// fields (TxHash, cursor (TxIdx, OpIdx), LedgerSequence, LedgerClosedAt, and
// the inner ContractEvent wire bytes) match position-for-position. SQLite is
// the production getEvents backend, so its cursor-ascending read order IS
// the contract the view path's emission order must reproduce.
func TestExtractEvents_MatchesSQLite(t *testing.T) {
	t.Run("empty-ledger", func(t *testing.T) {
		lcm := buildLCM(t, 4000, 1_700_000_100, nil)
		assertViewMatchesSQLite(t, lcm)
	})

	t.Run("v4-op-events-only", func(t *testing.T) {
		evA := buildContractEvent("alpha")
		evB := buildContractEvent("beta")
		evC := buildContractEvent("gamma")
		// Two txs, each with two ops carrying a single event.
		metas := []xdr.TransactionMeta{
			txMetaWithOpEvents([][]xdr.ContractEvent{{evA}, {evB}}),
			txMetaWithOpEvents([][]xdr.ContractEvent{{evC}}),
		}
		lcm := buildLCM(t, 4001, 1_700_000_111, metas)
		assertViewMatchesSQLite(t, lcm)
	})

	t.Run("v4-top-level-stages-ledger-wide-counters", func(t *testing.T) {
		// Three txs, each emitting a BeforeAllTxs + AfterAllTxs + AfterTx
		// event. SQLite's per-event cursor counters are ledger-wide for
		// BeforeAllTxs/AfterAllTxs (increment across txs) and per-tx for
		// AfterTx; the differential's positional per-group index check
		// verifies the view emission reproduces exactly that.
		lcm := buildLCM(t, 4002, 1_700_000_222, []xdr.TransactionMeta{
			stagedTxAllStages("a"), stagedTxAllStages("b"), stagedTxAllStages("c"),
		})
		assertViewMatchesSQLite(t, lcm)
	})

	t.Run("v4-mixed-top-level-and-op-events", func(t *testing.T) {
		// Tx with two top-level events (one BeforeAllTxs, one AfterTx)
		// AND two ops each with their own event. In cursor order the
		// BeforeAllTxs event (0, 0) comes first, then the op events
		// (1, op), then the AfterTx event (1, OperationMask).
		evIn := buildContractEvent("opA")
		evOut := buildContractEvent("opB")
		mixed := xdr.TransactionMeta{
			V: 4,
			V4: &xdr.TransactionMetaV4{
				Events: []xdr.TransactionEvent{
					{Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, Event: buildContractEvent("pre")},
					{Stage: xdr.TransactionEventStageTransactionEventStageAfterTx, Event: buildContractEvent("post")},
				},
				Operations: []xdr.OperationMetaV2{
					{Events: []xdr.ContractEvent{evIn}},
					{Events: []xdr.ContractEvent{evOut}},
				},
			},
		}
		lcm := buildLCM(t, 4003, 1_700_000_333, []xdr.TransactionMeta{mixed})
		assertViewMatchesSQLite(t, lcm)
	})

	t.Run("v4-cap67-fee-and-refund-inversion", func(t *testing.T) {
		// The CAP-67 shape that distinguishes cursor order from per-tx
		// chronological order: 2+ txs, each with a before-fee event
		// (BeforeAllTxs), an op event, and a fee-refund event (AfterTx).
		// Chronologically the ledger runs B1 B2 op1 R1 op2 R2 with each
		// tx's B adjacent to its op — but in ascending cursor order ALL
		// the (0, 0) B events precede every op event, and each tx's R
		// sorts at (txIdx, OperationMask) after that tx's ops. A per-tx
		// emitter (tx's top-level events then its ops) interleaves these
		// and CANNOT match the SQLite read order; this fixture is the
		// regression guard for exactly that inversion.
		lcm := buildLCM(t, 4007, 1_700_000_777, []xdr.TransactionMeta{
			cap67FeeTx("t1"), cap67FeeTx("t2"),
		})
		// Non-vacuity guard: the fixture must actually produce the six
		// events (2 txs x fee + op + refund) whose cursor order differs
		// from chronological order.
		raw, err := lcm.MarshalBinary()
		require.NoError(t, err)
		payloads, err := lcmViewToPayloads(xdr.LedgerCloseMetaView(raw))
		require.NoError(t, err)
		require.Len(t, payloads, 6)
		assertViewMatchesSQLite(t, lcm)
	})

	t.Run("v1-and-v2-meta-skip", func(t *testing.T) {
		// V1/V2 meta carry no events. Both paths should produce zero
		// payloads.
		metas := []xdr.TransactionMeta{
			{V: 1, V1: &xdr.TransactionMetaV1{}},
			{V: 2, V2: &xdr.TransactionMetaV2{}},
		}
		lcm := buildLCM(t, 4004, 1_700_000_444, metas)
		assertViewMatchesSQLite(t, lcm)
	})

	t.Run("v3-soroban-meta-events", func(t *testing.T) {
		// V3 meta carries events via SorobanMeta.Events; the struct
		// path emits them as op-0 events. Two txs verify the per-tx
		// ordering and applyIdx incrementing; a third tx with an
		// empty SorobanMeta.Events list checks the zero-events case
		// stays a no-op on both sides.
		evA := buildContractEvent("v3-alpha")
		evB := buildContractEvent("v3-beta")
		evC := buildContractEvent("v3-gamma")
		metas := []xdr.TransactionMeta{
			txMetaWithV3SorobanEvents([]xdr.ContractEvent{evA, evB}),
			txMetaWithV3SorobanEvents([]xdr.ContractEvent{evC}),
			txMetaWithV3SorobanEvents(nil),
		}
		lcm := buildLCM(t, 4005, 1_700_000_555, metas)
		assertViewMatchesSQLite(t, lcm)
	})

	t.Run("v3-soroban-meta-absent", func(t *testing.T) {
		// V3 meta with SorobanMeta unset: both paths emit zero payloads.
		metas := []xdr.TransactionMeta{
			{V: 3, V3: &xdr.TransactionMetaV3{SorobanMeta: nil}},
		}
		lcm := buildLCM(t, 4006, 1_700_000_666, metas)
		assertViewMatchesSQLite(t, lcm)
	})
}

// TestExtractEvents_V0NoPayloads asserts a V0 (pre-Soroban) LCM yields zero
// payloads with no error (the SDK extractor handles V0 directly).
func TestExtractEvents_V0NoPayloads(t *testing.T) {
	lcm := xdr.LedgerCloseMeta{V: 0, V0: &xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{LedgerSeq: 7},
		},
	}}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	payloads, err := lcmViewToPayloads(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	require.Empty(t, payloads)
}

// TestExtractEvents_V1DifferentialArm exercises the LCM V1 dispatch arm
// (case 1, TransactionResultMetaView) of LCMViewToPayloads, which the V2-only
// differential tests never run. V1 uses LedgerCloseMetaV1 + the non-V1
// TransactionResultMeta result-meta type — the whole point of this case.
func TestExtractEvents_V1DifferentialArm(t *testing.T) {
	// Event-bearing mix: a V4 tx with a top-level event + two op events,
	// and a V3 soroban tx, so the V1 arm walks real ContractEvent bytes.
	evA := buildContractEvent("v1-opA")
	evB := buildContractEvent("v1-opB")
	mixed := xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			Events: []xdr.TransactionEvent{
				{Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, Event: buildContractEvent("v1-pre")},
			},
			Operations: []xdr.OperationMetaV2{
				{Events: []xdr.ContractEvent{evA}},
				{Events: []xdr.ContractEvent{evB}},
			},
		},
	}
	metas := []xdr.TransactionMeta{
		mixed,
		txMetaWithV3SorobanEvents([]xdr.ContractEvent{buildContractEvent("v1-soroban")}),
	}
	lcm := buildLCMVersion(t, 1, 4101, 1_700_000_999, metas)
	require.Equal(t, int32(1), lcm.V, "fixture must be LCM V1")
	assertViewMatchesSQLite(t, lcm)
}

// TestExtractEvents_EmissionOrderCursorAscending pins the emission-order
// invariant directly on the payload slice: LCMViewToPayloads emits a ledger's
// payloads non-decreasing in (TxIdx, OpIdx) — ascending getEvents cursor
// order. The fixture MUST carry V4 stage events across multiple txs (each tx
// here emits BeforeAllTxs + AfterTx + AfterAllTxs events AND two op events):
// without stage events, per-tx chronological order and cursor order
// coincide and the invariant would be vacuously true. With them, a per-tx
// emitter would interleave the (0, 0) and (TransactionMask, 0) sentinel
// groups between op-event groups and fail the scan.
func TestExtractEvents_EmissionOrderCursorAscending(t *testing.T) {
	makeTx := func(label string) xdr.TransactionMeta {
		return xdr.TransactionMeta{
			V: 4,
			V4: &xdr.TransactionMetaV4{
				Events: []xdr.TransactionEvent{
					{
						Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs,
						Event: buildContractEvent(label + "-fee"),
					},
					{
						Stage: xdr.TransactionEventStageTransactionEventStageAfterTx,
						Event: buildContractEvent(label + "-refund"),
					},
					{
						Stage: xdr.TransactionEventStageTransactionEventStageAfterAllTxs,
						Event: buildContractEvent(label + "-post"),
					},
				},
				Operations: []xdr.OperationMetaV2{
					{Events: []xdr.ContractEvent{buildContractEvent(label + "-op0")}},
					{Events: []xdr.ContractEvent{buildContractEvent(label + "-op1")}},
				},
			},
		}
	}
	lcm := buildLCM(t, 4501, 1_700_002_222, []xdr.TransactionMeta{makeTx("t1"), makeTx("t2")})
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	payloads, err := lcmViewToPayloads(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	require.Len(t, payloads, 10, "2 txs x (3 stage events + 2 op events)")

	// The generic invariant: (TxIdx, OpIdx) never decreases.
	for i := 1; i < len(payloads); i++ {
		prev, cur := payloads[i-1], payloads[i]
		sorted := cur.TxIdx > prev.TxIdx ||
			(cur.TxIdx == prev.TxIdx && cur.OpIdx >= prev.OpIdx)
		assert.True(t, sorted,
			"payload %d key (%d, %d) sorts before payload %d key (%d, %d): emission is not cursor-ascending",
			i, cur.TxIdx, cur.OpIdx, i-1, prev.TxIdx, prev.OpIdx)
	}

	// And the exact expected cursor-key sequence, making the non-vacuity
	// of the fixture explicit: both txs' BeforeAllTxs events lead at
	// (0, 0), each tx's AfterTx event follows its own op events at
	// (txIdx, OperationMask), and both AfterAllTxs events close the
	// ledger at (TransactionMask, 0).
	type key struct{ tx, op uint32 }
	wantKeys := []key{
		{0, 0}, {0, 0}, // t1-fee, t2-fee (BeforeAllTxs, tx apply order)
		{1, 0}, {1, 1}, {1, uint32(toid.OperationMask)}, // t1 ops, then t1-refund
		{2, 0}, {2, 1}, {2, uint32(toid.OperationMask)}, // t2 ops, then t2-refund
		{uint32(toid.TransactionMask), 0}, {uint32(toid.TransactionMask), 0}, // t1-post, t2-post
	}
	gotKeys := make([]key, 0, len(payloads))
	for _, p := range payloads {
		gotKeys = append(gotKeys, key{p.TxIdx, p.OpIdx})
	}
	assert.Equal(t, wantKeys, gotKeys, "cursor-key emission sequence")

	// The two (0, 0) payloads carry DIFFERENT tx hashes: the sentinel
	// groups mix events of distinct transactions, which is why emission
	// order (not per-tx grouping) is the contract.
	assert.NotEqual(t, payloads[0].TxHash, payloads[1].TxHash,
		"BeforeAllTxs group must span both transactions")
}

// TestExtractEvents_AliasesViewBuffer asserts the zero-copy contract:
// the returned ContractEventBytes alias the input view buffer rather than
// being a copy. Verified two ways: (1) the slice's data pointer lies
// within [raw, raw+len(raw)); (2) mutating raw after extraction changes
// the bytes the payload sees.
func TestExtractEvents_AliasesViewBuffer(t *testing.T) {
	lcm := buildLCM(t, 4201, 1_700_001_111, []xdr.TransactionMeta{
		txMetaWithOpEvents([][]xdr.ContractEvent{{buildContractEvent("aliasme")}}),
	})
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	payloads, err := lcmViewToPayloads(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	require.Len(t, payloads, 1)
	require.NotEmpty(t, payloads[0].ContractEventBytes)

	// (1) Pointer-containment: the event bytes must point inside raw.
	rawBase := uintptr(unsafe.Pointer(unsafe.SliceData(raw)))
	rawEnd := rawBase + uintptr(len(raw))
	evBase := uintptr(unsafe.Pointer(unsafe.SliceData(payloads[0].ContractEventBytes)))
	assert.GreaterOrEqual(t, evBase, rawBase, "event bytes start before view buffer")
	assert.Less(t, evBase, rawEnd, "event bytes start past view buffer end")

	// (2) Mutation-observation: flip a byte in the aliased region and
	// confirm the payload slice reflects it (proving it is not a copy).
	off := evBase - rawBase
	before := raw[off]
	raw[off] ^= 0xFF
	assert.Equal(t, raw[off], payloads[0].ContractEventBytes[0],
		"payload bytes did not track mutation of raw — not aliased")
	assert.NotEqual(t, before, payloads[0].ContractEventBytes[0])
}

// TestExtractEvents_LegacyMetaV0 asserts that a legacy TransactionMeta V0
// (pre-Soroban, Operations only) carries no contract events and is skipped
// rather than erroring — the SQLite oracle's SDK reader path rejects V0
// meta, so this cannot be a differential case, but full-history backfills
// from genesis and must tolerate it. A V0 meta mixed with a
// V4-event-bearing tx must yield only the V4 tx's events, in order.
func TestExtractEvents_LegacyMetaV0(t *testing.T) {
	ev := buildContractEvent("after-v0")
	metas := []xdr.TransactionMeta{
		{V: 0, Operations: &[]xdr.OperationMeta{}}, // legacy, no events
		txMetaWithOpEvents([][]xdr.ContractEvent{{ev}}),
	}
	lcm := buildLCM(t, 4301, 1_700_001_222, metas)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	payloads, err := lcmViewToPayloads(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err, "V0 meta must be skipped, not error")
	// Only the second (V4) tx emits an event; the V0 tx contributes none.
	require.Len(t, payloads, 1)
	assert.Equal(t, uint32(2), payloads[0].TxIdx, "the sole event belongs to the 2nd (V4) tx")
}

// sqliteEventRow is one event as the legacy SQLite backend serves it back:
// the parsed getEvents cursor, the tx hash, the ledger close time, and the
// inner ContractEvent re-marshaled to its raw XDR (the DB stores a
// DiagnosticEvent wrapper; payloads carry the bare ContractEvent bytes, and
// XDR encoding is canonical, so re-marshaling the inner event yields the
// exact bytes the view path must emit).
type sqliteEventRow struct {
	cursor             protocol.Cursor
	txHash             xdr.Hash
	ledgerCloseTime    int64
	contractEventBytes []byte
}

// sqliteEventRows ingests lcm into a fresh temp-file SQLite DB via the
// production write path (db's InsertEvents) and reads every event of that
// ledger back through the production cursor-ordered read path (db's
// GetEvents, ORDER BY id ASC). The returned slice is therefore the
// cursor-ascending event sequence the live getEvents backend would serve —
// the reference the view path is differentially tested against.
func sqliteEventRows(t *testing.T, lcm xdr.LedgerCloseMeta) []sqliteEventRow {
	t.Helper()
	ctx := t.Context()
	logger := log.DefaultLogger

	testDB, err := db.OpenSQLiteDB(path.Join(t.TempDir(), "events.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testDB.Close()) })

	writer := db.NewReadWriter(logger, testDB, interfaces.MakeNoOpDeamon(),
		1_000_000 /* retention window: keep everything */, testPassphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)
	require.NoError(t, write.LedgerWriter().InsertLedger(lcm))
	require.NoError(t, write.EventWriter().InsertEvents(lcm))
	require.NoError(t, write.Commit(lcm, nil))

	seq := lcm.LedgerSequence()
	cursorRange := protocol.CursorRange{
		Start: protocol.Cursor{Ledger: seq},
		End:   protocol.Cursor{Ledger: seq + 1},
	}
	var rows []sqliteEventRow
	reader := db.NewEventReader(logger, testDB, testPassphrase)
	err = reader.GetEvents(ctx, cursorRange, nil, nil, nil,
		func(event xdr.DiagnosticEvent, cur protocol.Cursor, closeTime int64, txHash *xdr.Hash) bool {
			evBytes, merr := event.Event.MarshalBinary()
			require.NoError(t, merr)
			rows = append(rows, sqliteEventRow{
				cursor:             cur,
				txHash:             *txHash,
				ledgerCloseTime:    closeTime,
				contractEventBytes: evBytes,
			})
			return true
		})
	require.NoError(t, err, "SQLite GetEvents")
	return rows
}

// assertViewMatchesSQLite is the differential oracle: it marshals lcm, runs
// the view path (LCMViewToPayloads) on the bytes and the legacy SQLite
// path (db InsertEvents → GetEvents) on the struct, and asserts the view's
// emission sequence equals the SQLite cursor-ascending read sequence
// position-for-position — tx hash, (TxIdx, OpIdx) cursor key, ledger
// metadata, and the inner ContractEvent wire bytes. It asserts each view
// payload's stored EventIdx equals SQLite's per-event cursor index (the
// <TOID>-<eventIdx> backward-compat guarantee), and additionally that the same
// value is recoverable positionally (counter resetting on (TxIdx, OpIdx) key
// change), proving the contiguous-group emission contract.
func assertViewMatchesSQLite(t *testing.T, lcm xdr.LedgerCloseMeta) {
	t.Helper()

	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	viewPayloads, err := lcmViewToPayloads(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err, "view path LCMViewToPayloads")

	rows := sqliteEventRows(t, lcm)
	require.Len(t, viewPayloads, len(rows),
		"event count differs (sqlite=%d view=%d)", len(rows), len(viewPayloads))

	var groupIdx uint32
	for i := range rows {
		s := rows[i]
		v := viewPayloads[i]
		ctx := func(field string) string {
			return fmt.Sprintf("%s mismatch at event %d (sqlite cursor %s)", field, i, s.cursor.String())
		}

		assert.Equal(t, s.txHash, v.TxHash, ctx("TxHash"))
		assert.Equal(t, s.cursor.Ledger, v.LedgerSequence, ctx("LedgerSequence"))
		assert.Equal(t, s.cursor.Tx, v.TxIdx, ctx("TxIdx"))
		assert.Equal(t, s.cursor.Op, v.OpIdx, ctx("OpIdx"))
		assert.Equal(t, s.ledgerCloseTime, v.LedgerClosedAt, ctx("LedgerClosedAt"))

		// The view payload's raw ContractEvent bytes must be
		// wire-identical to the (canonical) re-marshaling of the inner
		// ContractEvent SQLite stored inside its DiagnosticEvent wrapper.
		assert.Equal(t, s.contractEventBytes, v.ContractEventBytes, ctx("ContractEventBytes"))

		// The stored per-event index must equal SQLite's per-event cursor
		// index — this is the <TOID>-<eventIdx> backward-compat guarantee.
		assert.Equal(t, s.cursor.Event, v.EventIdx, ctx("stored EventIdx"))

		// And it must still equal the positional per-group reconstruction
		// (counter resetting on (TxIdx, OpIdx) key change), proving the
		// contiguous-group emission contract that lets the index also be
		// recovered at read time.
		if i > 0 && (viewPayloads[i-1].TxIdx != v.TxIdx || viewPayloads[i-1].OpIdx != v.OpIdx) {
			groupIdx = 0
		}
		assert.Equal(t, s.cursor.Event, groupIdx, ctx("per-group event index"))
		groupIdx++
	}
}

// BenchmarkExtractEvents measures the view extractor over a
// representative event-bearing LCM (V4 mixed top-level + per-op events).
func BenchmarkExtractEvents(b *testing.B) {
	view := buildBenchEventsView(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := lcmViewToPayloads(view); err != nil {
			b.Fatal(err)
		}
	}
}

func buildBenchEventsView(tb testing.TB) xdr.LedgerCloseMetaView {
	tb.Helper()
	metas := make([]xdr.TransactionMeta, 0, 8)
	for t := range 8 {
		ops := make([][]xdr.ContractEvent, 4)
		for o := range ops {
			ops[o] = []xdr.ContractEvent{
				buildContractEvent(fmt.Sprintf("t%d-o%d-a", t, o)),
				buildContractEvent(fmt.Sprintf("t%d-o%d-b", t, o)),
			}
		}
		metas = append(metas, txMetaWithOpEvents(ops))
	}
	lcm := buildLCM(tb, 5001, 1_700_001_000, metas)
	raw, err := lcm.MarshalBinary()
	if err != nil {
		tb.Fatal(err)
	}
	return xdr.LedgerCloseMetaView(raw)
}

// TestExtractEvents_TopLevelEventsAliasViewBuffer extends the zero-copy
// contract to V4 TOP-LEVEL TransactionEvents: their ContractEventBytes must
// alias the input view buffer (sliced past the 4-byte Stage discriminant via
// the generated view accessors), not be a decode+re-marshal copy.
func TestExtractEvents_TopLevelEventsAliasViewBuffer(t *testing.T) {
	lcm := buildLCM(t, 4202, 1_700_001_112, []xdr.TransactionMeta{
		txMetaWithStagedEvents([]xdr.TransactionEvent{
			{Stage: xdr.TransactionEventStageTransactionEventStageAfterTx, Event: buildContractEvent("staged-alias")},
		}),
	})
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	payloads, err := lcmViewToPayloads(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	require.Len(t, payloads, 1)
	require.NotEmpty(t, payloads[0].ContractEventBytes)

	rawBase := uintptr(unsafe.Pointer(unsafe.SliceData(raw)))
	rawEnd := rawBase + uintptr(len(raw))
	evBase := uintptr(unsafe.Pointer(unsafe.SliceData(payloads[0].ContractEventBytes)))
	assert.GreaterOrEqual(t, evBase, rawBase, "top-level event bytes must alias the view buffer")
	assert.Less(t, evBase, rawEnd, "top-level event bytes must alias the view buffer")
}
