package views_test

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/views"
)

const testPassphrase = "Test SDF Network ; September 2015"

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

func transactionResult(success bool) xdr.TransactionResult {
	code := xdr.TransactionResultCodeTxBadSeq
	if success {
		code = xdr.TransactionResultCodeTxSuccess
	}
	opResults := []xdr.OperationResult{}
	return xdr.TransactionResult{
		FeeCharged: 100,
		Result: xdr.TransactionResultResult{
			Code:    code,
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
// ExtractEvents/ExtractTxHashes case 1) or an LCM V2 (LedgerCloseMetaV2
// with TransactionResultMetaV1, case 2). Both use a GeneralizedTransactionSet
// (V1) which is already in apply order, so the struct-path
// LedgerTransactionReader and the view path agree on ordering.
func buildLCMVersion(t testing.TB, version int32, ledgerSeq uint32, closeTimestamp int64, txMetas []xdr.TransactionMeta) xdr.LedgerCloseMeta {
	t.Helper()

	phases := make([]xdr.TransactionPhase, 0, len(txMetas))
	v2Processing := make([]xdr.TransactionResultMetaV1, 0, len(txMetas))
	v1Processing := make([]xdr.TransactionResultMeta, 0, len(txMetas))

	for _, meta := range txMetas {
		envelope, hash := buildTxEnvelopeAndHash(t)
		result := xdr.TransactionResultPair{
			TransactionHash: hash,
			Result:          transactionResult(true),
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

// buildLCMV0 builds an LCM V0 (LedgerCloseMetaV0) using a plain
// TransactionSet (NOT a GeneralizedTransactionSet) with PreviousLedgerHash
// set, and TransactionResultMeta in TxProcessing. ExtractEvents rejects V0
// (ErrV0Unsupported), but ExtractTxHashes supports it: TxProcessing is in
// apply order so hash extraction needs no sort. PreviousLedgerHash must be
// set so the struct-path LedgerTransactionReader can verify the tx set.
func buildLCMV0(t testing.TB, ledgerSeq uint32, closeTimestamp int64, txMetas []xdr.TransactionMeta) xdr.LedgerCloseMeta {
	t.Helper()

	var prevHash xdr.Hash
	prevHash[0] = 0x99
	envelopes := make([]xdr.TransactionEnvelope, 0, len(txMetas))
	txProcessing := make([]xdr.TransactionResultMeta, 0, len(txMetas))

	for _, meta := range txMetas {
		envelope, hash := buildTxEnvelopeAndHash(t)
		envelopes = append(envelopes, envelope)
		txProcessing = append(txProcessing, xdr.TransactionResultMeta{
			TxApplyProcessing: meta,
			Result: xdr.TransactionResultPair{
				TransactionHash: hash,
				Result:          transactionResult(true),
			},
		})
	}

	return xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimestamp)},
					LedgerSeq: xdr.Uint32(ledgerSeq),
				},
			},
			TxSet: xdr.TransactionSet{
				PreviousLedgerHash: prevHash,
				Txs:                envelopes,
			},
			TxProcessing: txProcessing,
		},
	}
}

// TestExtractEvents_MatchesStructPath cross-checks that the view-based
// ExtractEvents produces the same per-event Payload metadata (TxHash,
// LedgerSequence, TxIdx, OpIdx, LedgerClosedAt, EventIdx) AND the same
// wire bytes as the struct-based events.LCMToPayloads, for every
// supported meta shape.
func TestExtractEvents_MatchesStructPath(t *testing.T) {
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
		assertViewMatchesStruct(t, lcm)
	})

	t.Run("v4-top-level-stages-ledger-wide-counters", func(t *testing.T) {
		// Three txs, each emitting a BeforeAllTxs + AfterAllTxs +
		// AfterTx event so we can verify ledger-wide before/after
		// counters increment across txs (not per-tx) AND AfterTx
		// resets per-tx.
		makeStaged := func(label string) xdr.TransactionMeta {
			return txMetaWithStagedEvents([]xdr.TransactionEvent{
				{Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, Event: buildContractEvent(label + "-before")},
				{Stage: xdr.TransactionEventStageTransactionEventStageAfterAllTxs, Event: buildContractEvent(label + "-after")},
				{Stage: xdr.TransactionEventStageTransactionEventStageAfterTx, Event: buildContractEvent(label + "-aftertx")},
			})
		}
		lcm := buildLCM(t, 4002, 1_700_000_222, []xdr.TransactionMeta{
			makeStaged("a"), makeStaged("b"), makeStaged("c"),
		})
		assertViewMatchesStruct(t, lcm)
	})

	t.Run("v4-mixed-top-level-and-op-events", func(t *testing.T) {
		// Tx with two top-level events (one BeforeAllTxs, one AfterTx)
		// AND two ops each with their own event. The struct path
		// emits top-level first, then per-op — view must match.
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
		assertViewMatchesStruct(t, lcm)
	})

	t.Run("v1-and-v2-meta-skip", func(t *testing.T) {
		// V1/V2 meta carry no events. Both paths should produce zero
		// payloads.
		metas := []xdr.TransactionMeta{
			{V: 1, V1: &xdr.TransactionMetaV1{}},
			{V: 2, V2: &xdr.TransactionMetaV2{}},
		}
		lcm := buildLCM(t, 4004, 1_700_000_444, metas)
		assertViewMatchesStruct(t, lcm)
	})

	t.Run("v3-soroban-meta-events", func(t *testing.T) {
		// V3 meta carries events via SorobanMeta.Events; the struct
		// path emits them as op-0 events. Two txs verify the per-tx
		// EventIdx reset and applyIdx incrementing; a third tx with an
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
		assertViewMatchesStruct(t, lcm)
	})

	t.Run("v3-soroban-meta-absent", func(t *testing.T) {
		// V3 meta with SorobanMeta unset: both paths emit zero payloads.
		metas := []xdr.TransactionMeta{
			{V: 3, V3: &xdr.TransactionMetaV3{SorobanMeta: nil}},
		}
		lcm := buildLCM(t, 4006, 1_700_000_666, metas)
		assertViewMatchesStruct(t, lcm)
	})
}

// TestExtractEvents_V0Unsupported asserts a V0 LCM returns the sentinel.
func TestExtractEvents_V0Unsupported(t *testing.T) {
	lcm := xdr.LedgerCloseMeta{V: 0, V0: &xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{LedgerSeq: 7},
		},
	}}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	_, err = views.ExtractEvents(xdr.LedgerCloseMetaView(raw))
	require.ErrorIs(t, err, views.ErrV0Unsupported)
}

// TestExtractEvents_V1DifferentialArm exercises the LCM V1 dispatch arm
// (case 1, TransactionResultMetaView) of ExtractEvents, which the V2-only
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
	assertViewMatchesStruct(t, lcm)
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

	payloads, err := views.ExtractEvents(xdr.LedgerCloseMetaView(raw))
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

// TestExtractEvents_NegativePaths covers the error returns of the view
// extractor. The cheapest is an unsupported TransactionMeta version
// (V0 union) hitting the unsupported-meta-version path.
func TestExtractEvents_NegativePaths(t *testing.T) {
	tests := []struct {
		name    string
		metas   []xdr.TransactionMeta
		wantErr string
	}{
		{
			name: "unsupported-meta-version-v0",
			metas: []xdr.TransactionMeta{
				{V: 0, Operations: &[]xdr.OperationMeta{}},
			},
			wantErr: "unsupported TransactionMeta V=0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lcm := buildLCM(t, 4301, 1_700_001_222, tc.metas)
			raw, err := lcm.MarshalBinary()
			require.NoError(t, err)
			_, err = views.ExtractEvents(xdr.LedgerCloseMetaView(raw))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// assertViewMatchesStruct marshals lcm, runs both paths against the
// same bytes, and asserts they agree on every Payload field including
// ContractEventBytes (which both paths now populate: struct via
// MarshalBinary, view via .Raw()).
func assertViewMatchesStruct(t *testing.T, lcm xdr.LedgerCloseMeta) {
	t.Helper()

	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	structPayloads, err := events.LCMToPayloads(testPassphrase, lcm)
	require.NoError(t, err, "struct path LCMToPayloads")

	viewPayloads, err := views.ExtractEvents(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err, "view path ExtractEvents")

	require.Len(t, viewPayloads, len(structPayloads),
		"payload count differs (struct=%d view=%d)", len(structPayloads), len(viewPayloads))

	for i := range structPayloads {
		s := structPayloads[i]
		v := viewPayloads[i]
		ctx := func(field string) string {
			return fmt.Sprintf("%s mismatch at payload %d", field, i)
		}

		assert.Equal(t, s.TxHash, v.TxHash, ctx("TxHash"))
		assert.Equal(t, s.LedgerSequence, v.LedgerSequence, ctx("LedgerSequence"))
		assert.Equal(t, s.TxIdx, v.TxIdx, ctx("TxIdx"))
		assert.Equal(t, s.OpIdx, v.OpIdx, ctx("OpIdx"))
		assert.Equal(t, s.LedgerClosedAt, v.LedgerClosedAt, ctx("LedgerClosedAt"))
		assert.Equal(t, s.EventIdx, v.EventIdx, ctx("EventIdx"))

		// Both paths populate ContractEventBytes; they must be
		// wire-identical (struct path MarshalBinary vs view .Raw()).
		assert.Equal(t, s.ContractEventBytes, v.ContractEventBytes,
			ctx("ContractEventBytes (struct marshal vs view .Raw)"))

		// And the term keys derived from those bytes must match, since
		// downstream derives terms via events.TermsForBytes.
		sTerms, err := events.TermsForBytes(s.ContractEventBytes)
		require.NoError(t, err, ctx("struct TermsForBytes"))
		vTerms, err := events.TermsForBytes(v.ContractEventBytes)
		require.NoError(t, err, ctx("view TermsForBytes"))
		assert.Equal(t, sTerms, vTerms, ctx("Terms"))
	}
}

// BenchmarkExtractEvents measures the view extractor over a
// representative event-bearing LCM (V4 mixed top-level + per-op events).
func BenchmarkExtractEvents(b *testing.B) {
	view := buildBenchEventsView(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := views.ExtractEvents(view); err != nil {
			b.Fatal(err)
		}
	}
}

func buildBenchEventsView(tb testing.TB) xdr.LedgerCloseMetaView {
	tb.Helper()
	var metas []xdr.TransactionMeta
	for t := 0; t < 8; t++ {
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
