package serve

import (
	"strconv"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// The events seam fixture reuses the ledger seam constants (coldFirst..hotLast in
// ledger_reader_test.go): a frozen cold chunk 0 covering ledgers
// [coldFirst, coldLast] and a live hot chunk 1 covering [hotFirst, hotLast].
// Contract A emits an event at coldFirst, coldLast (cold) and hotFirst (hot);
// contract B at coldFirst+1 (cold) and hotLast (hot). A contract-A filter thus
// matches 3 events straddling the cold→hot seam.
func contractID(b byte) xdr.ContractId {
	var c xdr.ContractId
	c[0] = b
	return c
}

func contractStrkey(t *testing.T, c xdr.ContractId) string {
	t.Helper()
	s, err := strkey.Encode(strkey.VersionByteContract, c[:])
	require.NoError(t, err)
	return s
}

type eventsFixture struct {
	reg       *Registry
	layout    geometry.Layout
	contractA string
	contractB string
	contractC string // never emitted — the empty-match case
}

func buildEventsSeamFixture(t *testing.T) eventsFixture {
	t.Helper()
	cat, layout := newTestCatalog(t)
	require.NoError(t, cat.PinEarliestLedger(coldFirst))

	cA, cB := contractID(0xAA), contractID(0xBB)
	evA1, _ := contractEventXDR(t, cA, "topic-a", "a1")
	evB, _ := contractEventXDR(t, cB, "topic-b", "b1")
	evA2, _ := contractEventXDR(t, cA, "topic-a", "a2")
	evA3, _ := contractEventXDR(t, cA, "topic-a", "a3")
	evB2, _ := contractEventXDR(t, cB, "topic-b", "b2")

	// Cold chunk 0: A@coldFirst, B@coldFirst+1, A@coldLast.
	coldSeqs := []uint32{coldFirst, coldFirst + 1, coldLast}
	coldEvents := map[uint32][]xdr.ContractEvent{
		coldFirst:     {evA1},
		coldFirst + 1: {evB},
		coldLast:      {evA2},
	}
	// A cold ledger pack + cold events pack for chunk 0, both frozen.
	var raws [][]byte
	for seq := uint32(coldFirst); seq <= coldLast; seq++ {
		raws = append(raws, lcmWithCloseTime(t, seq, int64(seq)*10))
	}
	writeColdLedgerRun(t, layout, chunk.ID(0), coldFirst, raws)
	freezeLedgers(t, cat, chunk.ID(0))
	writeColdEventsChunk(t, layout, chunk.ID(0), coldFirst, coldSeqs, coldEvents)
	freezeEvents(t, cat, chunk.ID(0))

	// Hot chunk 1: A@hotFirst, B@hotLast.
	hotSeqs := []uint32{hotFirst, hotLast}
	hotEvents := map[uint32][]xdr.ContractEvent{
		hotFirst: {evA3},
		hotLast:  {evB2},
	}
	db1 := openHotEventChunk(t, cat, layout, chunk.ID(1), hotSeqs, hotEvents)
	t.Cleanup(func() { _ = db1.Close() })

	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(hotLast))
	r.HotOpened(chunk.ID(1), db1)

	return eventsFixture{
		reg:       r,
		layout:    layout,
		contractA: contractStrkey(t, cA),
		contractB: contractStrkey(t, cB),
		contractC: contractStrkey(t, contractID(0xCC)),
	}
}

func getEventsRequest(t *testing.T, params string) *jrpc2.Request {
	t.Helper()
	requests, err := jrpc2.ParseRequests([]byte(
		`{"jsonrpc":"2.0","id":1,"method":"getEvents","params":` + params + `}`,
	))
	require.NoError(t, err)
	require.Len(t, requests, 1)
	return requests[0].ToRequest()
}

func callGetEvents(t *testing.T, fx eventsFixture, params string) protocol.GetEventsResponse {
	t.Helper()
	h := NewGetEventsHandler(silentLogger(), fx.reg, fx.layout, 10000, 100)
	res, err := h(t.Context(), getEventsRequest(t, params))
	require.NoError(t, err)
	resp, ok := res.(protocol.GetEventsResponse)
	require.True(t, ok, "handler returns a GetEventsResponse")
	return resp
}

// A contract-A filter matches all three A events across the cold→hot seam in one
// page, in ascending cursor order.
func TestGetEvents_FilterAcrossSeam(t *testing.T) {
	fx := buildEventsSeamFixture(t)
	resp := callGetEvents(t, fx, `{"startLedger":`+itoa(coldFirst)+
		`,"filters":[{"contractIds":["`+fx.contractA+`"]}],"pagination":{"limit":10}}`)

	require.Len(t, resp.Events, 3, "all three contract-A events across the seam")
	assert.EqualValues(t, coldFirst, resp.Events[0].Ledger)
	assert.EqualValues(t, coldLast, resp.Events[1].Ledger)
	assert.EqualValues(t, hotFirst, resp.Events[2].Ledger)
	for _, e := range resp.Events {
		assert.Equal(t, fx.contractA, e.ContractID, "only contract-A events matched")
		assert.Equal(t, protocol.EventTypeContract, e.EventType)
		assert.NotEmpty(t, e.TopicXDR, "base64 topic encoding by default")
		assert.NotEmpty(t, e.ValueXDR)
	}
	assert.EqualValues(t, hotLast, resp.LatestLedger)
	assert.EqualValues(t, coldFirst, resp.OldestLedger)
}

// A limited page truncates to the limit and sets the cursor to the last returned
// event; the follow-up cursor page resumes exclusively.
func TestGetEvents_PaginationResumesExclusively(t *testing.T) {
	fx := buildEventsSeamFixture(t)

	page1 := callGetEvents(t, fx, `{"startLedger":`+itoa(coldFirst)+
		`,"filters":[{"contractIds":["`+fx.contractA+`"]}],"pagination":{"limit":2}}`)
	require.Len(t, page1.Events, 2, "limit truncates the page")
	assert.EqualValues(t, coldFirst, page1.Events[0].Ledger)
	assert.EqualValues(t, coldLast, page1.Events[1].Ledger)
	assert.Equal(t, page1.Events[1].ID, page1.Cursor, "cursor is the last returned event when limit reached")

	page2 := callGetEvents(t, fx, `{"filters":[{"contractIds":["`+fx.contractA+
		`"]}],"pagination":{"cursor":"`+page1.Cursor+`","limit":2}}`)
	require.Len(t, page2.Events, 1, "the remaining A event resumes after the cursor")
	assert.EqualValues(t, hotFirst, page2.Events[0].Ledger)
	assert.NotEqual(t, page1.Cursor, page2.Events[0].ID, "resume is exclusive of the prior cursor")
}

// A filter matching nothing returns an empty page whose cursor advances to the
// end of the scan window (endLedger-1), so the client makes forward progress.
func TestGetEvents_EmptyMatchAdvancesCursor(t *testing.T) {
	fx := buildEventsSeamFixture(t)
	resp := callGetEvents(t, fx, `{"startLedger":`+itoa(coldFirst)+
		`,"filters":[{"contractIds":["`+fx.contractC+`"]}],"pagination":{"limit":10}}`)

	assert.Empty(t, resp.Events, "contract C never emitted")
	// endLedger = min(latest+1, startLedger+scanLimit); here latest+1 = hotLast+1.
	wantCursor := protocol.MaxCursor
	wantCursor.Ledger = hotLast // endLedger-1
	assert.Equal(t, wantCursor.String(), resp.Cursor, "empty page advances the cursor to the window end")
	assert.EqualValues(t, hotLast, resp.LatestLedger)
	assert.EqualValues(t, coldFirst, resp.OldestLedger)
}

// A startLedger below the oldest served ledger is rejected with the available
// range surfaced (mirrors v1's InvalidRequest).
func TestGetEvents_OutOfRangeStartErrors(t *testing.T) {
	fx := buildEventsSeamFixture(t)
	h := NewGetEventsHandler(silentLogger(), fx.reg, fx.layout, 10000, 100)

	_, err := h(t.Context(), getEventsRequest(t,
		`{"startLedger":`+itoa(coldFirst-1)+`,"pagination":{"limit":10}}`))
	require.Error(t, err)
	rpcErr, ok := err.(*jrpc2.Error)
	require.True(t, ok, "a jrpc2.Error carrying the range")
	assert.Contains(t, rpcErr.Message, "ledger range")
}

// The json format renders topics/value as decoded JSON instead of base64 XDR.
func TestGetEvents_JSONFormat(t *testing.T) {
	fx := buildEventsSeamFixture(t)
	resp := callGetEvents(t, fx, `{"startLedger":`+itoa(coldFirst)+
		`,"filters":[{"contractIds":["`+fx.contractA+`"]}],"xdrFormat":"json","pagination":{"limit":10}}`)

	require.NotEmpty(t, resp.Events)
	for _, e := range resp.Events {
		assert.NotEmpty(t, e.TopicJSON, "json topic encoding")
		assert.NotEmpty(t, e.ValueJSON)
		assert.Empty(t, e.TopicXDR, "no base64 topics in json mode")
	}
}

// A match-all page (no filters) returns every event across both tiers in cursor
// order.
func TestGetEvents_NoFilterMatchesAll(t *testing.T) {
	fx := buildEventsSeamFixture(t)
	resp := callGetEvents(t, fx, `{"startLedger":`+itoa(coldFirst)+`,"pagination":{"limit":10}}`)

	require.Len(t, resp.Events, 5, "all five events across cold+hot")
	ledgers := make([]uint32, len(resp.Events))
	for i, e := range resp.Events {
		ledgers[i] = uint32(e.Ledger)
	}
	assert.Equal(t, []uint32{coldFirst, coldFirst + 1, coldLast, hotFirst, hotLast}, ledgers,
		"ascending cursor order across the seam")
}

// itoa is a tiny helper so the JSON request literals stay readable.
func itoa(v int) string {
	return strconv.Itoa(v)
}
