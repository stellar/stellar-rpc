package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
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

func buildLCM(t *testing.T, ledgerSeq uint32, closeTimestamp int64, txMetas []xdr.TransactionMeta) xdr.LedgerCloseMeta {
	t.Helper()

	phases := make([]xdr.TransactionPhase, 0, len(txMetas))
	txProcessing := make([]xdr.TransactionResultMetaV1, 0, len(txMetas))

	for _, meta := range txMetas {
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

		txProcessing = append(txProcessing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: meta,
			Result: xdr.TransactionResultPair{
				TransactionHash: hash,
				Result:          transactionResult(true),
			},
		})
		comp := []xdr.TxSetComponent{{
			Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
				Txs: []xdr.TransactionEnvelope{envelope},
			},
		}}
		phases = append(phases, xdr.TransactionPhase{V: 0, V0Components: &comp})
	}

	return xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimestamp)},
					LedgerSeq: xdr.Uint32(ledgerSeq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: phases},
			},
			TxProcessing: txProcessing,
		},
	}
}

// payloadTerm pairs a TermKey with the chunk-relative event ID the
// events writer would assign in payload order. Built by
// termsFromPayloads to make the LCMToPayloads tests' assertions
// concrete.
type payloadTerm struct {
	Key     TermKey
	EventID uint32
}

// termsFromPayloads is the test-side analog of what
// Writer does internally: derive term keys from each
// payload's ContractEvent using TermsFor and pair them with
// payload-order event IDs.
func termsFromPayloads(t *testing.T, payloads []Payload) []payloadTerm {
	t.Helper()
	var out []payloadTerm
	for i, p := range payloads {
		keys, err := TermsFor(p.ContractEvent)
		require.NoError(t, err)
		for _, key := range keys {
			out = append(out, payloadTerm{Key: key, EventID: uint32(i)})
		}
	}
	return out
}

func TestLCMToPayloads_EmptyLedger(t *testing.T) {
	lcm := buildLCM(t, 100, 1_700_000_000, nil)

	payloads, err := LCMToPayloads(testPassphrase, lcm)
	require.NoError(t, err)
	assert.Empty(t, payloads)
}

func TestLCMToPayloads_SingleOpEvent(t *testing.T) {
	ev := buildContractEvent("transfer")
	txMeta := txMetaWithOpEvents([][]xdr.ContractEvent{{ev}})
	lcm := buildLCM(t, 200, 1_700_001_000, []xdr.TransactionMeta{txMeta})

	payloads, err := LCMToPayloads(testPassphrase, lcm)
	require.NoError(t, err)
	require.Len(t, payloads, 1)

	p := payloads[0]
	assert.Equal(t, uint32(200), p.LedgerSequence)
	assert.Equal(t, uint32(1), p.TxIdx, "operation events use tx.Index (1-indexed)")
	assert.Equal(t, uint32(0), p.OpIdx)
	assert.Equal(t, uint32(0), p.EventIdx)
	assert.Equal(t, int64(1_700_001_000), p.LedgerClosedAt)
	require.NotNil(t, p.ContractEvent.ContractId)
	assert.Equal(t, byte(0xab), p.ContractEvent.ContractId[0])

	// TermsFor: one term for the contract ID + one for topic0 = 2 total.
	terms := termsFromPayloads(t, payloads)
	require.Len(t, terms, 2)
	assert.Equal(t, uint32(0), terms[0].EventID)
	assert.Equal(t, uint32(0), terms[1].EventID)

	expectedContractTerm := ComputeTermKey(p.ContractEvent.ContractId[:], FieldContractID)
	topicBytes, err := p.ContractEvent.Body.V0.Topics[0].MarshalBinary()
	require.NoError(t, err)
	expectedTopicTerm := ComputeTermKey(topicBytes, FieldTopic0)

	assert.Contains(t, []TermKey{terms[0].Key, terms[1].Key}, expectedContractTerm)
	assert.Contains(t, []TermKey{terms[0].Key, terms[1].Key}, expectedTopicTerm)
}

func TestLCMToPayloads_MultipleOpsAssignIncreasingEventIDs(t *testing.T) {
	ev1 := buildContractEvent("op0-event0")
	ev2 := buildContractEvent("op1-event0")
	ev3 := buildContractEvent("op1-event1")
	txMeta := txMetaWithOpEvents([][]xdr.ContractEvent{{ev1}, {ev2, ev3}})
	lcm := buildLCM(t, 300, 1_700_002_000, []xdr.TransactionMeta{txMeta})

	payloads, err := LCMToPayloads(testPassphrase, lcm)
	require.NoError(t, err)
	require.Len(t, payloads, 3)

	// (OpIdx, EventIdx) tuples by event order.
	assert.Equal(t, uint32(0), payloads[0].OpIdx)
	assert.Equal(t, uint32(0), payloads[0].EventIdx)
	assert.Equal(t, uint32(1), payloads[1].OpIdx)
	assert.Equal(t, uint32(0), payloads[1].EventIdx)
	assert.Equal(t, uint32(1), payloads[2].OpIdx)
	assert.Equal(t, uint32(1), payloads[2].EventIdx)

	// Each event yields two terms (contractID + 1 topic) via TermsFor —
	// so we expect EventIDs 0,0,1,1,2,2 in order.
	terms := termsFromPayloads(t, payloads)
	require.Len(t, terms, 6)
	for i := range terms {
		assert.Equal(t, uint32(i/2), terms[i].EventID, "term %d", i)
	}
}

func TestLCMToPayloads_EventWithoutContractIDOnlyEmitsTopicTerms(t *testing.T) {
	sym := xdr.ScSymbol("only-topic")
	ev := xdr.ContractEvent{
		// ContractId left nil — exercises the nil-guard branch.
		Type: xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			},
		},
	}
	txMeta := txMetaWithOpEvents([][]xdr.ContractEvent{{ev}})
	lcm := buildLCM(t, 400, 1_700_003_000, []xdr.TransactionMeta{txMeta})

	payloads, err := LCMToPayloads(testPassphrase, lcm)
	require.NoError(t, err)
	require.Len(t, payloads, 1)
	assert.Nil(t, payloads[0].ContractEvent.ContractId)
	require.Len(t, termsFromPayloads(t, payloads), 1, "no contract ID → only topic term emitted")
}

func TestLCMToPayloads_MultipleTopicsHashWithDistinctFields(t *testing.T) {
	// An event with two topics must produce two distinct TermKeys —
	// the field byte must differ even if topic bytes happened to.
	sym := xdr.ScSymbol("same")
	ev := xdr.ContractEvent{
		Type: xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{
					{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
					{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
				},
				Data: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			},
		},
	}
	txMeta := txMetaWithOpEvents([][]xdr.ContractEvent{{ev}})
	lcm := buildLCM(t, 500, 1_700_004_000, []xdr.TransactionMeta{txMeta})

	payloads, err := LCMToPayloads(testPassphrase, lcm)
	require.NoError(t, err)
	terms := termsFromPayloads(t, payloads)
	require.Len(t, terms, 2)
	assert.NotEqual(t, terms[0].Key, terms[1].Key,
		"same value in different fields must produce different term keys")
}

func TestLCMToPayloads_TopicCountClippedToMax(t *testing.T) {
	// MaxTopicCount=4. An event with 6 topics produces 4 topic terms
	// (plus 1 contractID term).
	syms := make([]xdr.ScSymbol, 6)
	topics := make([]xdr.ScVal, 6)
	for i := range syms {
		syms[i] = xdr.ScSymbol("t")
		topics[i] = xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &syms[i]}
	}
	var cid xdr.ContractId
	cid[0] = 0xfe
	ev := xdr.ContractEvent{
		ContractId: &cid,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V:  0,
			V0: &xdr.ContractEventV0{Topics: topics, Data: topics[0]},
		},
	}
	txMeta := txMetaWithOpEvents([][]xdr.ContractEvent{{ev}})
	lcm := buildLCM(t, 600, 1_700_005_000, []xdr.TransactionMeta{txMeta})

	payloads, err := LCMToPayloads(testPassphrase, lcm)
	require.NoError(t, err)
	// 1 contract ID term + 4 topic terms (5th and 6th dropped).
	assert.Len(t, termsFromPayloads(t, payloads), 5)
}

func TestLCMToPayloads_MultipleTxsPreserveTxIndices(t *testing.T) {
	tx0Event := buildContractEvent("tx0")
	tx1Event := buildContractEvent("tx1")
	lcm := buildLCM(t, 700, 1_700_006_000, []xdr.TransactionMeta{
		txMetaWithOpEvents([][]xdr.ContractEvent{{tx0Event}}),
		txMetaWithOpEvents([][]xdr.ContractEvent{{tx1Event}}),
	})

	payloads, err := LCMToPayloads(testPassphrase, lcm)
	require.NoError(t, err)
	require.Len(t, payloads, 2)
	// tx.Index from the ingest reader is 1-indexed.
	assert.Equal(t, uint32(1), payloads[0].TxIdx)
	assert.Equal(t, uint32(2), payloads[1].TxIdx)
	// Each tx carries its own TxHash.
	assert.NotEqual(t, payloads[0].TxHash, payloads[1].TxHash)
}

func TestLCMToPayloads_TxLevelEventsUseCursorSentinels(t *testing.T) {
	// Construct an LCM with one tx that emits one BeforeAllTxs event,
	// one AfterTx event, and one AfterAllTxs event. Each stage must
	// use the right sentinel for TxIdx / OpIdx.
	contractEvent := func(tag string) xdr.ContractEvent {
		sym := xdr.ScSymbol(tag)
		return xdr.ContractEvent{
			Type: xdr.ContractEventTypeContract,
			Body: xdr.ContractEventBody{
				V: 0,
				V0: &xdr.ContractEventV0{
					Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}},
					Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
				},
			},
		}
	}
	stageEvents := []xdr.TransactionEvent{
		{Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, Event: contractEvent("pre")},
		{Stage: xdr.TransactionEventStageTransactionEventStageAfterTx, Event: contractEvent("afterTx")},
		{Stage: xdr.TransactionEventStageTransactionEventStageAfterAllTxs, Event: contractEvent("post")},
	}
	txMeta := txMetaWithStagedEvents(stageEvents)
	lcm := buildLCM(t, 800, 1_700_007_000, []xdr.TransactionMeta{txMeta})

	payloads, err := LCMToPayloads(testPassphrase, lcm)
	require.NoError(t, err)
	require.Len(t, payloads, 3)

	// BeforeAllTxs: Tx=0, Op=0, Event=0
	assert.Equal(t, uint32(0), payloads[0].TxIdx)
	assert.Equal(t, uint32(0), payloads[0].OpIdx)
	assert.Equal(t, uint32(0), payloads[0].EventIdx)
	// AfterTx: Tx=tx.Index (1), Op=OperationMask, Event=0
	assert.Equal(t, uint32(1), payloads[1].TxIdx)
	assert.Equal(t, uint32(toid.OperationMask), payloads[1].OpIdx)
	assert.Equal(t, uint32(0), payloads[1].EventIdx)
	// AfterAllTxs: Tx=TransactionMask, Op=0, Event=0
	assert.Equal(t, uint32(toid.TransactionMask), payloads[2].TxIdx)
	assert.Equal(t, uint32(0), payloads[2].OpIdx)
	assert.Equal(t, uint32(0), payloads[2].EventIdx)
}
