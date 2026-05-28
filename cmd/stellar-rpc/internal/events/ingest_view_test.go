package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// TestLCMToPayloadsFromRaw_MatchesStructPath cross-checks that the
// view-based LCMToPayloadsFromRaw produces the same per-event Payload
// metadata (TxHash, LedgerSequence, TxIdx, OpIdx, LedgerClosedAt,
// EventIdx) AND the same wire bytes + term keys as the struct-based
// LCMToPayloads, for every supported meta shape:
//
//   - V3 meta with SorobanMeta + soroban-envelope (op-0 events)
//   - V4 meta with operation-only events
//   - V4 meta with top-level transaction events spanning all three
//     Stage variants (BeforeAllTxs/AfterAllTxs/AfterTx) + ledger-wide
//     before/after counters across multiple txs
//   - V4 meta with mixed top-level + per-op events
//
// The view path populates ContractEventBytes + Terms; the struct path
// populates ContractEvent. The comparison reconstructs the struct
// path's expected ContractEventBytes (via MarshalBinary) and expected
// Terms (via TermsFor) so both paths can be compared on the SAME
// representation. If they ever diverge, the view materializer is
// silently producing the wrong bytes — which would corrupt cold
// artifacts when the bench writes them.
func TestLCMToPayloadsFromRaw_MatchesStructPath(t *testing.T) {
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
		// path emits them as op-0 events through GetContractEvents
		// (which routes through SorobanMeta.Events). The view path
		// walks SorobanMeta.Events directly with TxIdx=applyIdx,
		// OpIdx=0, EventIdx running 0..N-1. Two txs to verify the
		// per-tx EventIdx reset (not a ledger-wide counter) and to
		// exercise applyIdx incrementing across txs. A third tx with
		// an empty SorobanMeta.Events list checks the zero-events
		// case stays a no-op on both sides.
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
		// V3 meta with SorobanMeta unset: the struct path's
		// GetContractEventsForOperation returns (nil, nil), so
		// LCMToPayloads emits zero payloads. The view path unwraps
		// the SorobanMeta optional, sees absent, returns zero
		// payloads. Both must agree.
		metas := []xdr.TransactionMeta{
			{V: 3, V3: &xdr.TransactionMetaV3{SorobanMeta: nil}},
		}
		lcm := buildLCM(t, 4006, 1_700_000_666, metas)
		assertViewMatchesStruct(t, lcm)
	})
}

// assertViewMatchesStruct marshals lcm, runs both paths against the
// same bytes, and asserts they agree on every Payload field except
// ContractEvent vs ContractEventBytes (which encode the same XDR).
func assertViewMatchesStruct(t *testing.T, lcm xdr.LedgerCloseMeta) {
	t.Helper()

	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	structPayloads, err := LCMToPayloads(testPassphrase, lcm)
	require.NoError(t, err, "struct path LCMToPayloads")

	viewPayloads, err := LCMToPayloadsFromRaw(testPassphrase, raw)
	require.NoError(t, err, "view path LCMToPayloadsFromRaw")

	require.Len(t, viewPayloads, len(structPayloads),
		"payload count differs (struct=%d view=%d)", len(structPayloads), len(viewPayloads))

	for i := range structPayloads {
		s := structPayloads[i]
		v := viewPayloads[i]
		ctx := func(field string) string {
			return field + " mismatch at payload " + itoa(i)
		}

		assert.Equal(t, s.TxHash, v.TxHash, ctx("TxHash"))
		assert.Equal(t, s.LedgerSequence, v.LedgerSequence, ctx("LedgerSequence"))
		assert.Equal(t, s.TxIdx, v.TxIdx, ctx("TxIdx"))
		assert.Equal(t, s.OpIdx, v.OpIdx, ctx("OpIdx"))
		assert.Equal(t, s.LedgerClosedAt, v.LedgerClosedAt, ctx("LedgerClosedAt"))
		assert.Equal(t, s.EventIdx, v.EventIdx, ctx("EventIdx"))

		// View path: ContractEventBytes is the .Raw() slice;
		// ContractEvent is the zero value.
		// Struct path: ContractEvent is populated; ContractEventBytes
		// is nil. Compare on the wire bytes from both sides.
		structBytes, err := s.ContractEvent.MarshalBinary()
		require.NoError(t, err, ctx("struct ContractEvent.MarshalBinary"))
		assert.Equal(t, structBytes, v.ContractEventBytes,
			ctx("ContractEventBytes (struct marshal vs view .Raw)"))

		// Terms: struct path doesn't populate Terms (caller derives
		// via TermsFor). Reconstruct the expected Terms and compare.
		expectedTerms, err := TermsFor(s.ContractEvent)
		require.NoError(t, err, ctx("TermsFor"))
		gotTerms, ok := v.TermKeys()
		require.True(t, ok, ctx("TermKeys present"))
		assert.Equal(t, expectedTerms, gotTerms, ctx("Terms"))
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
