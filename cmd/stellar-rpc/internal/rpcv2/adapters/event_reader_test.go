package adapters

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

func eventReaderFixture(t *testing.T) (*EventReader, *query.Registry, *catalog.Catalog) {
	t.Helper()
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	return NewEventReader(r), r, cat
}

func TestGetEvents_MatchAllAscending(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	evA := contractEventFixture(0xab, "transfer")
	evB := contractEventFixture(0xcd, "mint")
	evC := contractEventFixture(0xab, "burn")
	first := testChunk.FirstLedger()
	lcm1, txs1 := lcmWithTxs(t, first, txSpec{events: []xdr.ContractEvent{evA, evB}})
	lcm2, txs2 := lcmWithTxs(t, first+1, txSpec{events: []xdr.ContractEvent{evC}})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm1, lcm2)
	r.SetLatestLedger(first+1, closeTimeFor(first+1))

	var got []scannedEvent
	err := reader.GetEvents(context.Background(), wholeWindow(first, first+1),
		nil, nil, nil, collectInto(&got))
	require.NoError(t, err)

	require.Len(t, got, 3)
	assert.Equal(t, protocol.Cursor{Ledger: first, Tx: 1, Op: 0, Event: 0}, got[0].cursor)
	assert.Equal(t, protocol.Cursor{Ledger: first, Tx: 1, Op: 0, Event: 1}, got[1].cursor)
	assert.Equal(t, protocol.Cursor{Ledger: first + 1, Tx: 1, Op: 0, Event: 0}, got[2].cursor)
	assert.Equal(t, evA, got[0].event.Event)
	assert.Equal(t, evB, got[1].event.Event)
	assert.Equal(t, evC, got[2].event.Event)
	assert.Equal(t, closeTimeFor(first), got[0].closeTime)
	assert.Equal(t, closeTimeFor(first+1), got[2].closeTime)
	assert.Equal(t, txs1[0].hash, got[0].txHash)
	assert.Equal(t, txs2[0].hash, got[2].txHash)
}

func TestGetEvents_ScanFunctionFalseStopsEarly(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	first := testChunk.FirstLedger()
	lcm1, _ := lcmWithTxs(t, first, txSpec{events: []xdr.ContractEvent{
		contractEventFixture(0xab, "transfer"), contractEventFixture(0xab, "mint"),
	}})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm1)
	r.SetLatestLedger(first, closeTimeFor(first))

	calls := 0
	err := reader.GetEvents(context.Background(), wholeWindow(first, first), nil, nil, nil,
		func(xdr.DiagnosticEvent, protocol.Cursor, int64, *xdr.Hash) bool {
			calls++
			return false
		})
	require.NoError(t, err, "an early stop is not an error, matching v1")
	assert.Equal(t, 1, calls)
}

func TestGetEvents_ResumeSkipsAtAndBeforeStart(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	first := testChunk.FirstLedger()
	lcm1, _ := lcmWithTxs(t, first, txSpec{events: []xdr.ContractEvent{
		contractEventFixture(0xab, "a"), contractEventFixture(0xab, "b"), contractEventFixture(0xab, "c"),
	}})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm1)
	r.SetLatestLedger(first, closeTimeFor(first))

	// The handler resumes with the last returned cursor's Event incremented, so
	// Start itself is inclusive: resuming after event 0 must emit exactly 1 and 2.
	var got []scannedEvent
	err := reader.GetEvents(context.Background(), protocol.CursorRange{
		Start: protocol.Cursor{Ledger: first, Tx: 1, Op: 0, Event: 1},
		End:   protocol.Cursor{Ledger: first + 1},
	}, nil, nil, nil, collectInto(&got))
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, uint32(1), got[0].cursor.Event)
	assert.Equal(t, uint32(2), got[1].cursor.Event)
}

func TestGetEvents_ContractAndTopicFiltersCross(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	first := testChunk.FirstLedger()
	lcm1, _ := lcmWithTxs(t, first, txSpec{events: []xdr.ContractEvent{
		contractEventFixture(0xab, "transfer"),
		contractEventFixture(0xab, "mint"),
		contractEventFixture(0xcd, "transfer"),
		contractEventFixture(0xcd, "burn"),
	}})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm1)
	r.SetLatestLedger(first, closeTimeFor(first))

	var contractAB xdr.ContractId
	contractAB[0] = 0xab
	transferSym := xdr.ScSymbol("transfer")
	transferVal := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &transferSym}
	topicBytes := mustMarshal(t, &transferVal)

	// contract IN {ab} AND (topic1 = "transfer") — v1's WHERE shape.
	var got []scannedEvent
	err := reader.GetEvents(context.Background(), wholeWindow(first, first),
		[][]byte{contractAB[:]},
		store.TopicFilters{{store.TopicCondition{Column: 1, Value: topicBytes}}},
		nil, collectInto(&got))
	require.NoError(t, err)
	require.Len(t, got, 1, "only 0xab's transfer matches both dimensions")
	assert.Equal(t, contractAB, *got[0].event.Event.ContractId)
	assert.Equal(t, transferSym, *got[0].event.Event.Body.V0.Topics[0].Sym)

	// Topic-only filter: both transfers, either contract.
	got = nil
	err = reader.GetEvents(context.Background(), wholeWindow(first, first),
		nil, store.TopicFilters{{store.TopicCondition{Column: 1, Value: topicBytes}}},
		nil, collectInto(&got))
	require.NoError(t, err)
	assert.Len(t, got, 2)
}

func TestGetEvents_EventTypeFilter(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	first := testChunk.FirstLedger()
	systemEvent := contractEventFixture(0xab, "sys")
	systemEvent.Type = xdr.ContractEventTypeSystem
	lcm1, _ := lcmWithTxs(t, first, txSpec{events: []xdr.ContractEvent{
		contractEventFixture(0xab, "transfer"), systemEvent,
	}})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm1)
	r.SetLatestLedger(first, closeTimeFor(first))

	var got []scannedEvent
	err := reader.GetEvents(context.Background(), wholeWindow(first, first),
		nil, nil, []int{int(xdr.ContractEventTypeSystem)}, collectInto(&got))
	require.NoError(t, err)
	require.Len(t, got, 1, "the store does not index event type; the adapter must filter it")
	assert.Equal(t, xdr.ContractEventTypeSystem, got[0].event.Event.Type)
}

func TestGetEvents_InSuccessfulContractCallHardcodedTrue(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	first := testChunk.FirstLedger()
	feeEvent := xdr.TransactionEvent{
		Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs,
		Event: contractEventFixture(0xfe, "fee"),
	}
	lcm1, _ := lcmWithTxs(t, first,
		txSpec{failed: true, txEvents: []xdr.TransactionEvent{feeEvent}},
		txSpec{events: []xdr.ContractEvent{contractEventFixture(0xab, "transfer")}},
	)
	seedHotChunkLCMs(t, cat, r, testChunk, lcm1)
	r.SetLatestLedger(first, closeTimeFor(first))

	var got []scannedEvent
	err := reader.GetEvents(context.Background(), wholeWindow(first, first),
		nil, nil, nil, collectInto(&got))
	require.NoError(t, err)
	require.Len(t, got, 2)

	// The failed transaction's fee event still lands at cursor group (0, 0)...
	assert.Equal(t, protocol.Cursor{Ledger: first, Tx: 0, Op: 0, Event: 0}, got[0].cursor)
	// ...and, pending the payload-format rework that stores the success bit,
	// every event reads true — even this one (see the emitter's TODO).
	assert.True(t, got[0].event.InSuccessfulContractCall)
	assert.True(t, got[1].event.InSuccessfulContractCall)
}

// TestGetEvents_PagingAcrossTheChunkSeam is the ticket's canonical test: page a
// limit-2 client across the chunk 5 / chunk 6 border and require strictly
// increasing cursors, no duplicates, no page longer than the limit, and full
// coverage. A missing resume skip or a per-part limit bug fails here with a
// clean 200, which is why this exists.
func TestGetEvents_PagingAcrossTheChunkSeam(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	c0, c1 := testChunk, testChunk+1
	ab := func(topic string) xdr.ContractEvent { return contractEventFixture(0xab, topic) }
	other := contractEventFixture(0xcd, "noise")

	lcm1, _ := lcmWithTxs(t, c0.FirstLedger(), txSpec{events: []xdr.ContractEvent{ab("a"), other, ab("b")}})
	lcm2, _ := lcmWithTxs(t, c0.FirstLedger()+1, txSpec{events: []xdr.ContractEvent{ab("c")}})
	seedHotChunkLCMs(t, cat, r, c0, lcm1, lcm2)
	lcm3, _ := lcmWithTxs(t, c1.FirstLedger(), txSpec{events: []xdr.ContractEvent{other, ab("d")}})
	lcm4, _ := lcmWithTxs(t, c1.FirstLedger()+1, txSpec{events: []xdr.ContractEvent{ab("e")}})
	seedHotChunkLCMs(t, cat, r, c1, lcm3, lcm4)
	r.SetLatestLedger(c1.FirstLedger()+1, closeTimeFor(c1.FirstLedger()+1))

	var contractAB xdr.ContractId
	contractAB[0] = 0xab
	window := protocol.CursorRange{
		Start: protocol.Cursor{Ledger: c0.FirstLedger()},
		End:   protocol.Cursor{Ledger: c1.FirstLedger() + 2},
	}

	const limit = 2
	var all []scannedEvent
	start := window.Start
	for page := range 4 {
		var got []scannedEvent
		err := reader.GetEvents(context.Background(),
			protocol.CursorRange{Start: start, End: window.End},
			[][]byte{contractAB[:]}, nil, nil, collectLimit(&got, limit))
		require.NoError(t, err)
		require.LessOrEqual(t, len(got), limit, "page %d over limit", page)
		all = append(all, got...)
		if len(got) < limit {
			break
		}
		// The handler resumes from the last returned cursor, event index +1.
		start = got[len(got)-1].cursor
		start.Event++
	}

	require.Len(t, all, 5, "pages must cover every match exactly once")
	topics := make([]string, 0, len(all))
	for i, s := range all {
		topics = append(topics, string(*s.event.Event.Body.V0.Topics[0].Sym))
		if i > 0 {
			assert.Positive(t, s.cursor.Cmp(all[i-1].cursor), "cursors strictly increasing across pages")
		}
		assert.Equal(t, contractAB, *s.event.Event.ContractId)
	}
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, topics)
}

func TestGetEvents_BelowFloorIsRangeError(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	first := testChunk.FirstLedger()
	lcm1, _ := lcmWithTxs(t, first, txSpec{events: []xdr.ContractEvent{contractEventFixture(0xab, "a")}})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm1)
	r.SetLatestLedger(first, closeTimeFor(first))

	var rangeErr *query.RangeError
	err := reader.GetEvents(context.Background(), wholeWindow(2, first),
		nil, nil, nil, collectInto(&[]scannedEvent{}))
	require.ErrorAs(t, err, &rangeErr)
	assert.Equal(t, uint32(2), rangeErr.Requested)
}

func TestGetEvents_EndLedgerIsExclusive(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	first := testChunk.FirstLedger()
	lcm1, _ := lcmWithTxs(t, first, txSpec{events: []xdr.ContractEvent{contractEventFixture(0xab, "in")}})
	lcm2, _ := lcmWithTxs(t, first+1, txSpec{events: []xdr.ContractEvent{contractEventFixture(0xab, "out")}})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm1, lcm2)
	r.SetLatestLedger(first+1, closeTimeFor(first+1))

	var got []scannedEvent
	err := reader.GetEvents(context.Background(), protocol.CursorRange{
		Start: protocol.Cursor{Ledger: first},
		End:   protocol.Cursor{Ledger: first + 1},
	}, nil, nil, nil, collectInto(&got))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, first, got[0].cursor.Ledger)

	got = nil
	err = reader.GetEvents(context.Background(), protocol.CursorRange{
		Start: protocol.Cursor{Ledger: first},
		End:   protocol.Cursor{Ledger: first},
	}, nil, nil, nil, collectInto(&got))
	require.NoError(t, err, "an empty window is valid and empty, not an error")
	assert.Empty(t, got)
}

func TestGetEvents_ColdChunkServes(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	require.NoError(t, cat.FlipHotReady(999)) // acquisition needs a ready live chunk
	first := testChunk.FirstLedger()
	lcm1, txs1 := lcmWithTxs(t, first, txSpec{events: []xdr.ContractEvent{
		contractEventFixture(0xab, "a"), contractEventFixture(0xcd, "noise"),
	}})
	lcm2, _ := lcmWithTxs(t, first+1, txSpec{events: []xdr.ContractEvent{
		contractEventFixture(0xab, "b"),
	}})
	seedFrozenEventChunk(t, cat, testChunk, lcm1, lcm2)
	r.SetLatestLedger(first+1, closeTimeFor(first+1))

	var contractAB xdr.ContractId
	contractAB[0] = 0xab
	var got []scannedEvent
	err := reader.GetEvents(context.Background(), wholeWindow(first, first+1),
		[][]byte{contractAB[:]}, nil, nil, collectInto(&got))
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, txs1[0].hash, got[0].txHash)
	assert.Equal(t, first+1, got[1].cursor.Ledger)
}

func TestGetEvents_V1LedgerCloseMetaHasNoEvents(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	first := testChunk.FirstLedger()
	raw, _ := lcmV1WithClassicTx(t, first)
	seedHotChunkLCMs(t, cat, r, testChunk, raw)
	r.SetLatestLedger(first, closeTimeFor(first))

	var got []scannedEvent
	err := reader.GetEvents(context.Background(), wholeWindow(first, first),
		nil, nil, nil, collectInto(&got))
	require.NoError(t, err)
	assert.Empty(t, got, "a classic pre-Soroban ledger scans clean: no events, no error")
}

// TestGetEvents_BatchSmallerThanOneLedger pins paging inside a single ledger:
// with a 2-candidate batch, the 5-event ledger arrives over several pages
// (QueryPage's NextStart resumes mid-ledger), and the pages must cover every
// event exactly once — no duplicates, no gaps.
func TestGetEvents_BatchSmallerThanOneLedger(t *testing.T) {
	reader, r, cat := eventReaderFixture(t)
	reader.scanBatch = 2
	first := testChunk.FirstLedger()
	dense := []xdr.ContractEvent{
		contractEventFixture(0xab, "a"), contractEventFixture(0xab, "b"),
		contractEventFixture(0xab, "c"), contractEventFixture(0xab, "d"),
		contractEventFixture(0xab, "e"),
	}
	lcm1, _ := lcmWithTxs(t, first, txSpec{events: dense})
	lcm2, _ := lcmWithTxs(t, first+1, txSpec{events: []xdr.ContractEvent{
		contractEventFixture(0xab, "f"), contractEventFixture(0xab, "g"),
	}})
	seedHotChunkLCMs(t, cat, r, testChunk, lcm1, lcm2)
	r.SetLatestLedger(first+1, closeTimeFor(first+1))

	var contractAB xdr.ContractId
	contractAB[0] = 0xab
	for name, filter := range map[string][][]byte{"filtered": {contractAB[:]}, "match-all": nil} {
		t.Run(name, func(t *testing.T) {
			var got []scannedEvent
			err := reader.GetEvents(context.Background(), wholeWindow(first, first+1),
				filter, nil, nil, collectInto(&got))
			require.NoError(t, err)
			require.Len(t, got, 7, "every event exactly once despite batch re-scans")
			topics := make([]string, 0, len(got))
			for _, s := range got {
				topics = append(topics, string(*s.event.Event.Body.V0.Topics[0].Sym))
			}
			assert.Equal(t, []string{"a", "b", "c", "d", "e", "f", "g"}, topics)
		})
	}
}
