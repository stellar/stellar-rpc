package eventsapi

// The v1-parity harness, the shim's acceptance oracle: the same LCM fixtures
// are ingested into an rpcv1 sqlite store and into a v2 hot chunk, the shared
// v1 handler and the shim each serve over an in-memory JSON-RPC pipe, and
// responses are compared field for field, cursors included. The one
// deliberate divergence: inSuccessfulContractCall (deprecated, dropped by the
// shim per getevents-v1-shim-brief.md) is stripped from both sides before the
// diff. While getEventsV1 is the prep stub, every shim comparison skips
// itself; TestV1ParityHarness_V1SideServes runs regardless, so the harness
// itself stays verified.

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

const (
	parityChunk        = chunk.ID(5)
	parityMaxLimit     = 1000
	parityDefaultLimit = 100
	// parityTermBudget has headroom over the worst legal v1 expansion; the
	// production default is the brief's open sizing decision.
	parityTermBudget = 200
)

// parityLCMs is the shared fixture set, ingested identically on both sides:
//
//	F+0: contract A "a0" topics(xfer), "a1" topics(xfer, alice)  [one tx, one op]
//	F+1: contract B "b0" topics(mint)
//	F+2: no events
//	F+3: contract A "a2" topics(burn, alice, extra)
func parityLCMs(t *testing.T) [][]byte {
	t.Helper()
	a := xdr.ContractId(testContractRaw(0xAA))
	b := xdr.ContractId(testContractRaw(0xBB))
	first := parityChunk.FirstLedger()
	return [][]byte{
		rpcv2test.EventsLCMBytes(t, first,
			rpcv2test.SymbolContractEvent(a, "a0", "xfer"),
			rpcv2test.SymbolContractEvent(a, "a1", "xfer", "alice")),
		rpcv2test.EventsLCMBytes(t, first+1,
			rpcv2test.SymbolContractEvent(b, "b0", "mint")),
		rpcv2test.ZeroTxLCMBytes(t, first+2),
		rpcv2test.EventsLCMBytes(t, first+3,
			rpcv2test.SymbolContractEvent(a, "a2", "burn", "alice", "extra")),
	}
}

func newLocalClient(t *testing.T, h jrpc2.Handler) *jrpc2.Client {
	t.Helper()
	local := server.NewLocal(handler.Map{protocol.GetEventsMethodName: h}, nil)
	t.Cleanup(func() { _ = local.Client.Close() })
	return local.Client
}

// newV1Client seeds an rpcv1 sqlite store from lcms and serves the shared v1
// handler over it. The passphrase must be the fixtures': their tx hashes are
// computed against pubnet, and v1 ingestion recomputes them.
func newV1Client(t *testing.T, lcms [][]byte) *jrpc2.Client {
	t.Helper()
	ctx := context.Background()
	db, err := sqlitedb.OpenSQLiteDB(filepath.Join(t.TempDir(), "v1.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	logger := rpcv2test.SilentLogger()
	rw := sqlitedb.NewReadWriter(
		logger, db, host.MakeNoOpDaemon(), 1_000_000, network.PublicNetworkPassphrase)
	for _, raw := range lcms {
		var lcm xdr.LedgerCloseMeta
		require.NoError(t, lcm.UnmarshalBinary(raw))
		wtx, err := rw.NewTx(ctx)
		require.NoError(t, err)
		require.NoError(t, wtx.LedgerWriter().InsertLedger(lcm))
		require.NoError(t, wtx.EventWriter().InsertEvents(lcm))
		require.NoError(t, wtx.Commit(lcm, map[string]time.Duration{}))
	}
	h := methods.NewGetEventsHandler(logger,
		sqlitedb.NewEventReader(logger, db, network.PublicNetworkPassphrase),
		parityMaxLimit, parityDefaultLimit,
		sqlitedb.NewLedgerReader(db))
	return newLocalClient(t, h)
}

// newShimClient seeds a v2 hot chunk from the same lcms and serves the shim
// with a pinned read view, the way wrapAdapterRequest does in production.
func newShimClient(t *testing.T, lcms [][]byte) *jrpc2.Client {
	t.Helper()
	logger := rpcv2test.SilentLogger()
	cat, _ := rpcv2test.OpenTestCatalogWith(t, geometry.ChunksPerTxhashIndex, logger)
	r := query.NewRegistry(cat, geometry.NewRetention(0, parityChunk))
	rpcv2test.SeedHotChunkLCMs(t, cat, parityChunk,
		func(db *hotchunk.DB) { r.PublishHandle(parityChunk, db) }, lcms...)
	r.SetLatestLedger(parityChunk.FirstLedger()+uint32(len(lcms))-1, 0)
	view, err := r.NewReadView()
	require.NoError(t, err)
	t.Cleanup(view.Release)
	base := NewV1Handler(Limits{
		TermBudget:   parityTermBudget,
		MaxLimit:     parityMaxLimit,
		DefaultLimit: parityDefaultLimit,
	})
	return newLocalClient(t, func(ctx context.Context, req *jrpc2.Request) (any, error) {
		return base(adapters.WithView(ctx, view), req)
	})
}

func callGetEvents(
	t *testing.T, c *jrpc2.Client, req protocol.GetEventsRequest,
) (json.RawMessage, *jrpc2.Error) {
	t.Helper()
	var raw json.RawMessage
	err := c.CallResult(context.Background(), protocol.GetEventsMethodName, req, &raw)
	if err == nil {
		return raw, nil
	}
	var jerr *jrpc2.Error
	require.ErrorAs(t, err, &jerr)
	return nil, jerr
}

// normalizedResponse parses a response for comparison, stripping the one
// field the shim deliberately drops (from both sides, so the diff is blind to
// which mechanism the drop uses).
func normalizedResponse(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	if evs, ok := m["events"].([]any); ok {
		for _, e := range evs {
			if em, ok := e.(map[string]any); ok {
				delete(em, "inSuccessfulContractCall")
			}
		}
	}
	return m
}

func skipWhileStubbed(t *testing.T, jerr *jrpc2.Error) {
	t.Helper()
	if jerr != nil && strings.Contains(jerr.Message, "prep stub") {
		t.Skip("shim core pending (see the TODO plan in get_events_v1.go)")
	}
}

func requireParity(t *testing.T, v1c, shimc *jrpc2.Client, req protocol.GetEventsRequest) {
	t.Helper()
	r1, e1 := callGetEvents(t, v1c, req)
	r2, e2 := callGetEvents(t, shimc, req)
	skipWhileStubbed(t, e2)
	if e1 != nil {
		require.NotNil(t, e2, "v1 errored (%v) but the shim served", e1)
		assert.Equal(t, e1.Code, e2.Code)
		assert.Equal(t, e1.Message, e2.Message)
		return
	}
	require.Nil(t, e2, "v1 served but the shim errored: %v", e2)
	assert.Equal(t, normalizedResponse(t, r1), normalizedResponse(t, r2))
}

// TestV1ParityHarness_V1SideServes runs today, before the shim exists: it
// proves the seeding, the in-memory server, and the cursor round-trip work,
// so a parity failure later indicts the shim and not the harness.
func TestV1ParityHarness_V1SideServes(t *testing.T) {
	lcms := parityLCMs(t)
	c := newV1Client(t, lcms)
	first := parityChunk.FirstLedger()

	raw, jerr := callGetEvents(t, c, protocol.GetEventsRequest{StartLedger: first})
	require.Nil(t, jerr)
	var resp protocol.GetEventsResponse
	require.NoError(t, json.Unmarshal(raw, &resp))
	require.Len(t, resp.Events, 4)
	assert.Equal(t, int32(first), resp.Events[0].Ledger) //nolint:gosec // fixture ledgers are small
	assert.Equal(t, first, resp.OldestLedger)
	assert.Equal(t, first+3, resp.LatestLedger)

	// Short page: the cursor is the window's end, MaxCursor at the tip.
	wantCursor := protocol.MaxCursor
	wantCursor.Ledger = first + 3
	assert.Equal(t, wantCursor.String(), resp.Cursor)

	// The window-end cursor round-trips: at the tip it stays put, empty page.
	var cur protocol.Cursor
	require.NoError(t, json.Unmarshal([]byte(strconv.Quote(resp.Cursor)), &cur))
	raw2, jerr2 := callGetEvents(t, c, protocol.GetEventsRequest{
		Pagination: &protocol.PaginationOptions{Cursor: &cur},
	})
	require.Nil(t, jerr2)
	var resp2 protocol.GetEventsResponse
	require.NoError(t, json.Unmarshal(raw2, &resp2))
	assert.Empty(t, resp2.Events)
	assert.Equal(t, resp.Cursor, resp2.Cursor)
}

//nolint:funlen // one table, one case per v1 behavior
func TestGetEventsV1Parity(t *testing.T) {
	lcms := parityLCMs(t)
	v1c := newV1Client(t, lcms)
	shimc := newShimClient(t, lcms)
	first := parityChunk.FirstLedger()

	contractA := testContractStrkey(t, 0xAA)
	contractB := testContractStrkey(t, 0xBB)
	seg := func(sym string) protocol.SegmentFilter {
		v, _ := symbolScVal(t, sym)
		return protocol.SegmentFilter{ScVal: &v}
	}
	wild := func(w string) protocol.SegmentFilter {
		return protocol.SegmentFilter{Wildcard: &w}
	}

	for name, req := range map[string]protocol.GetEventsRequest{
		"no filters, whole window": {StartLedger: first},
		"endLedger is exclusive":   {StartLedger: first, EndLedger: first + 1},
		"endLedger at the start ledger is an empty page": {
			StartLedger: first + 1, EndLedger: first + 1},
		"endLedger below the start ledger is legal and empty": {
			StartLedger: first + 1, EndLedger: first},
		"contract id": {StartLedger: first, Filters: []protocol.EventFilter{
			{ContractIDs: []string{contractA}}}},
		"two contract ids": {StartLedger: first, Filters: []protocol.EventFilter{
			{ContractIDs: []string{contractA, contractB}}}},
		"type contract": {StartLedger: first, Filters: []protocol.EventFilter{
			{EventType: protocol.EventTypeSet{protocol.EventTypeContract: nil}}}},
		"type set of both stored types": {StartLedger: first, Filters: []protocol.EventFilter{
			{EventType: protocol.EventTypeSet{
				protocol.EventTypeContract: nil, protocol.EventTypeSystem: nil}}}},
		"type diagnostic is rejected": {StartLedger: first, Filters: []protocol.EventFilter{
			{EventType: protocol.EventTypeSet{protocol.EventTypeDiagnostic: nil}}}},
		"one-segment topic matches only one-topic events": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{{seg("xfer")}}}}},
		"trailing ** relaxes the arity": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{{seg("xfer"), wild("**")}}}}},
		"star matches any value at its position": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{{wild("*"), seg("alice")}}}}},
		"star alone matches every one-topic event": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{{wild("*")}}}}},
		"filters are OR-ed": {StartLedger: first, Filters: []protocol.EventFilter{
			{ContractIDs: []string{contractB}},
			{Topics: []protocol.TopicFilter{{seg("burn"), wild("**")}}}}},
		"fields within a filter are AND-ed": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{ContractIDs: []string{contractA},
					Topics: []protocol.TopicFilter{{seg("xfer"), wild("**")}}}}},
		"limit fills the page and mints the last id": {
			StartLedger: first, Pagination: &protocol.PaginationOptions{Limit: 2}},
		"start above the tip errors":   {StartLedger: first + 100},
		"start below the floor errors": {StartLedger: first - 1},
		"json format":                  {StartLedger: first, Format: protocol.FormatJSON},
	} {
		t.Run(name, func(t *testing.T) { requireParity(t, v1c, shimc, req) })
	}
}

// TestGetEventsV1Parity_PaginationChain drains the window one event per page
// through both backends, comparing every page, the mid-ledger resume after
// F+0's first event included.
func TestGetEventsV1Parity_PaginationChain(t *testing.T) {
	lcms := parityLCMs(t)
	v1c := newV1Client(t, lcms)
	shimc := newShimClient(t, lcms)

	req := protocol.GetEventsRequest{
		StartLedger: parityChunk.FirstLedger(),
		Pagination:  &protocol.PaginationOptions{Limit: 1},
	}
	const maxPages = 7 // 4 events + the empty window-end page, with margin
	for page := range maxPages {
		r1, e1 := callGetEvents(t, v1c, req)
		r2, e2 := callGetEvents(t, shimc, req)
		skipWhileStubbed(t, e2)
		require.Nil(t, e1, "page %d", page)
		require.Nil(t, e2, "page %d", page)
		n1, n2 := normalizedResponse(t, r1), normalizedResponse(t, r2)
		require.Equal(t, n1, n2, "page %d", page)

		events, _ := n1["events"].([]any)
		cursorStr, _ := n1["cursor"].(string)
		require.NotEmpty(t, cursorStr, "page %d", page)
		if len(events) == 0 {
			return // both sides drained the window in lockstep
		}
		var cur protocol.Cursor
		require.NoError(t, json.Unmarshal([]byte(strconv.Quote(cursorStr)), &cur))
		req = protocol.GetEventsRequest{
			Pagination: &protocol.PaginationOptions{Cursor: &cur, Limit: 1},
		}
	}
	t.Fatalf("window did not drain within %d pages", maxPages)
}
