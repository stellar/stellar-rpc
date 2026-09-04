package eventsapi

// The v1-parity harness, the shim's acceptance oracle: the same LCM fixtures
// are ingested into an rpcv1 sqlite store and into a v2 hot chunk, the shared
// v1 handler and the shim each serve over an in-memory JSON-RPC pipe, and
// responses are compared field for field, cursors included.
// TestV1ParityHarness_V1SideServes exercises the v1 side alone, so a parity
// failure indicts the shim and not the harness.

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
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
	// parityTermBudget has headroom over the worst legal v1 expansion,
	// like the production default (config.DefaultGetEventsV1TermBudget).
	parityTermBudget = 200
)

// parityCloseTime is ledger F+i's close time: distinct and nonzero, so a
// close-time wiring bug on either side diffs instead of comparing 0 == 0.
func parityCloseTime(i int) int64 { return 1_700_000_000 + int64(i)*7 }

// parityLCMs is the shared fixture set, ingested identically on both sides:
//
//	F+0: contract A "a0" topics(xfer), "a1" topics(xfer, alice)  [one tx, one op]
//	F+1: contract B "b0" topics(mint)
//	F+2: no events
//	F+3: contract A "a2" topics(burn, alice, extra)
//	F+4: two transactions carrying transaction-level events in all three
//	     stages — the sentinel-id surface (store.StageSentinels) the other
//	     ledgers never touch: tx 1 (success) fee, one op event, refund;
//	     tx 2 (failed) fee, unlock, no operations
func parityLCMs(t *testing.T) [][]byte {
	t.Helper()
	a := xdr.ContractId(testContractRaw(0xAA))
	b := xdr.ContractId(testContractRaw(0xBB))
	first := parityChunk.FirstLedger()
	return [][]byte{
		rpcv2test.EventsLCMBytesAt(t, first, parityCloseTime(0),
			rpcv2test.SymbolContractEvent(a, "a0", "xfer"),
			rpcv2test.SymbolContractEvent(a, "a1", "xfer", "alice")),
		rpcv2test.EventsLCMBytesAt(t, first+1, parityCloseTime(1),
			rpcv2test.SymbolContractEvent(b, "b0", "mint")),
		rpcv2test.ZeroTxLCMBytesAt(t, first+2, parityCloseTime(2)),
		rpcv2test.EventsLCMBytesAt(t, first+3, parityCloseTime(3),
			rpcv2test.SymbolContractEvent(a, "a2", "burn", "alice", "extra")),
		multiTxLCMBytes(t, first+4, parityCloseTime(4), []parityTx{
			{
				txEvents: []xdr.TransactionEvent{
					stageEvent(xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, "fee1"),
					stageEvent(xdr.TransactionEventStageTransactionEventStageAfterTx, "refund1"),
				},
				opEvents: []xdr.ContractEvent{rpcv2test.SymbolContractEvent(a, "c0", "call")},
			},
			{
				failed: true,
				txEvents: []xdr.TransactionEvent{
					stageEvent(xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, "fee2"),
					stageEvent(xdr.TransactionEventStageTransactionEventStageAfterAllTxs, "unlock"),
				},
			},
		}),
	}
}

// parityTx is one transaction of the sentinel fixture ledger. A failed
// transaction carries fee events and no operations, the shape the ledger
// stream actually produces.
type parityTx struct {
	txEvents []xdr.TransactionEvent
	opEvents []xdr.ContractEvent
	failed   bool
}

func stageEvent(stage xdr.TransactionEventStage, label string) xdr.TransactionEvent {
	return xdr.TransactionEvent{
		Stage: stage,
		Event: rpcv2test.SymbolContractEvent(xdr.ContractId(testContractRaw(0xFE)), label, label),
	}
}

// multiTxLCMBytes builds one ledger holding txs in apply order, each with
// top-level transaction events and operation events — the shapes
// EventsLCMBytesAt cannot express (it hard-codes one successful transaction
// of op events).
func multiTxLCMBytes(t *testing.T, seq uint32, closeTime int64, txs []parityTx) []byte {
	t.Helper()
	envelopes := make([]xdr.TransactionEnvelope, 0, len(txs))
	processing := make([]xdr.TransactionResultMetaV1, 0, len(txs))
	for _, tx := range txs {
		meta := xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{Events: tx.txEvents}}
		if len(tx.opEvents) > 0 {
			meta.V4.Operations = []xdr.OperationMetaV2{{Events: tx.opEvents}}
		}
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
		hash, err := network.HashTransactionInEnvelope(envelope, network.PublicNetworkPassphrase)
		require.NoError(t, err)
		code := xdr.TransactionResultCodeTxSuccess
		if tx.failed {
			code = xdr.TransactionResultCodeTxFailed
		}
		opResults := []xdr.OperationResult{}
		envelopes = append(envelopes, envelope)
		processing = append(processing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: meta,
			Result: xdr.TransactionResultPair{
				TransactionHash: hash,
				Result: xdr.TransactionResult{
					FeeCharged: 100,
					Result: xdr.TransactionResultResult{
						Code:    code,
						Results: &opResults,
					},
				},
			},
		})
	}
	return rpcv2test.V2LCMBytes(t, seq, closeTime, envelopes, processing)
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
	// The tip stamp is what latestLedgerCloseTime serves; it must be the
	// last fixture's real close time or the parity diff catches it.
	r.SetLatestLedger(parityChunk.FirstLedger()+uint32(len(lcms))-1,
		query.CloseTimeAt(parityCloseTime(len(lcms)-1)))
	view, err := r.NewReadView()
	require.NoError(t, err)
	t.Cleanup(view.Release)
	base := NewV1Handler(Limits{
		TermBudget:   parityTermBudget,
		MaxLimit:     parityMaxLimit,
		DefaultLimit: parityDefaultLimit,
	}, logger)
	return newLocalClient(t, func(ctx context.Context, req *jrpc2.Request) (any, error) {
		return base(query.WithView(ctx, view), req)
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

// normalizedResponse parses a response into the comparable form.
func normalizedResponse(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	return m
}

func requireParity(t *testing.T, v1c, shimc *jrpc2.Client, req protocol.GetEventsRequest) {
	t.Helper()
	r1, e1 := callGetEvents(t, v1c, req)
	r2, e2 := callGetEvents(t, shimc, req)
	if e1 != nil {
		require.NotNil(t, e2, "v1 errored (%v) but the shim served", e1)
		assert.Equal(t, e1.Code, e2.Code)
		assert.Equal(t, e1.Message, e2.Message)
		return
	}
	require.Nil(t, e2, "v1 served but the shim errored: %v", e2)
	assert.Equal(t, normalizedResponse(t, r1), normalizedResponse(t, r2))
}

// v1Response's short-page cursor claims everything through endLedger-1 was
// scanned, which holds only while one pager call can cover a whole v1
// window. getEventsV1's truncation guard turns a violation into a loud
// error; this pairing keeps the guard unreachable. It pins against the
// window the pager actually applies, not against the constant that window
// happens to derive from, so narrowing one without the other fails here.
func TestV1WindowFitsThePagerScanWindow(t *testing.T) {
	assert.LessOrEqual(t, uint32(methods.LedgerScanLimit), query.MaxScanLedgers)
}

// TestV1ParityHarness_V1SideServes exercises the v1 side alone: it
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
	require.Len(t, resp.Events, 9, "4 op events + 5 sentinel-ledger events")
	assert.Equal(t, int32(first), resp.Events[0].Ledger)
	assert.Equal(t, first, resp.OldestLedger)
	assert.Equal(t, first+4, resp.LatestLedger)
	assert.Equal(t, parityCloseTime(0), resp.OldestLedgerCloseTime)
	assert.Equal(t, parityCloseTime(4), resp.LatestLedgerCloseTime)

	// Short page: the cursor is the window's end, MaxCursor at the tip.
	wantCursor := protocol.MaxCursor
	wantCursor.Ledger = first + 4
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
	// windowEnd is the cursor a short page hands back: MaxCursor's tx and
	// op sentinels over one ledger. Sending it back is what a caught-up
	// poller does on every request.
	windowEnd := func(l uint32) *protocol.Cursor {
		c := protocol.MaxCursor
		c.Ledger = l
		return &c
	}

	for name, req := range map[string]protocol.GetEventsRequest{
		"no filters, whole window": {StartLedger: first},
		"endLedger is exclusive":   {StartLedger: first, EndLedger: first + 1},
		"endLedger at the start ledger is an empty page": {
			StartLedger: first + 1, EndLedger: first + 1,
		},
		"endLedger below the start ledger is legal and empty": {
			StartLedger: first + 1, EndLedger: first,
		},
		"contract id": {StartLedger: first, Filters: []protocol.EventFilter{
			{ContractIDs: []string{contractA}},
		}},
		"two contract ids": {StartLedger: first, Filters: []protocol.EventFilter{
			{ContractIDs: []string{contractA, contractB}},
		}},
		"type contract": {StartLedger: first, Filters: []protocol.EventFilter{
			{EventType: protocol.EventTypeSet{protocol.EventTypeContract: nil}},
		}},
		"type set of both stored types": {StartLedger: first, Filters: []protocol.EventFilter{
			{EventType: protocol.EventTypeSet{
				protocol.EventTypeContract: nil, protocol.EventTypeSystem: nil,
			}},
		}},
		"type diagnostic is rejected": {StartLedger: first, Filters: []protocol.EventFilter{
			{EventType: protocol.EventTypeSet{protocol.EventTypeDiagnostic: nil}},
		}},
		"one-segment topic matches only one-topic events": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{{seg("xfer")}}},
			},
		},
		"trailing ** relaxes the arity": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{{seg("xfer"), wild("**")}}},
			},
		},
		"star matches any value at its position": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{{wild("*"), seg("alice")}}},
			},
		},
		"star alone matches every one-topic event": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{{wild("*")}}},
			},
		},
		"filters are OR-ed": {StartLedger: first, Filters: []protocol.EventFilter{
			{ContractIDs: []string{contractB}},
			{Topics: []protocol.TopicFilter{{seg("burn"), wild("**")}}},
		}},
		"fields within a filter are AND-ed": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{
					ContractIDs: []string{contractA},
					Topics:      []protocol.TopicFilter{{seg("xfer"), wild("**")}},
				},
			},
		},
		"limit fills the page and mints the last id": {
			StartLedger: first, Pagination: &protocol.PaginationOptions{Limit: 2},
		},
		"limit exactly at the event count still mints the last id": {
			StartLedger: first, Pagination: &protocol.PaginationOptions{Limit: 9},
		},
		"crafted cursor with max event index wraps like v1": {
			// v1's resume increment wraps MaxUint32 to 0 and rescans the
			// cursor's whole (tx, op) group; the shim must reproduce it.
			Pagination: &protocol.PaginationOptions{Cursor: &protocol.Cursor{
				Ledger: first, Tx: 1, Op: 0, Event: math.MaxUint32,
			}},
		},
		"crafted cursor below the ledger's first event": {
			Pagination: &protocol.PaginationOptions{Cursor: &protocol.Cursor{Ledger: first}},
		},
		"crafted cursor at the first event's exact id": {
			Pagination: &protocol.PaginationOptions{Cursor: &protocol.Cursor{
				Ledger: first, Tx: 1, Op: 0, Event: 0,
			}},
		},
		"crafted cursor in the empty ledger": {
			Pagination: &protocol.PaginationOptions{Cursor: &protocol.Cursor{Ledger: first + 2}},
		},
		"window-end cursor mid-range resumes past its ledger": {
			Pagination: &protocol.PaginationOptions{Cursor: windowEnd(first)},
		},
		"window-end cursor at the tip stays put": {
			Pagination: &protocol.PaginationOptions{Cursor: windowEnd(first + 4)},
		},
		"window-end cursor keeps its filter": {
			Pagination: &protocol.PaginationOptions{Cursor: windowEnd(first)},
			Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{{seg("burn"), wild("**")}}},
			},
		},
		"crafted cursor inside the sentinel fee group": {
			Pagination: &protocol.PaginationOptions{Cursor: &protocol.Cursor{
				Ledger: first + 4, Tx: 0, Op: 0, Event: 0,
			}},
		},
		"start above the tip errors":   {StartLedger: first + 100},
		"start below the floor errors": {StartLedger: first - 1},
		"bad contract id is rejected by validation": {
			StartLedger: first, Filters: []protocol.EventFilter{
				{ContractIDs: []string{"CNOTVALID"}},
			},
		},
		"json format": {StartLedger: first, Format: protocol.FormatJSON},
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
	const maxPages = 13 // 9 events + the empty window-end page, with margin
	drained := 0
	for page := range maxPages {
		r1, e1 := callGetEvents(t, v1c, req)
		r2, e2 := callGetEvents(t, shimc, req)
		require.Nil(t, e1, "page %d", page)
		require.Nil(t, e2, "page %d", page)
		n1, n2 := normalizedResponse(t, r1), normalizedResponse(t, r2)
		require.Equal(t, n1, n2, "page %d", page)

		events, _ := n1["events"].([]any)
		cursorStr, _ := n1["cursor"].(string)
		require.NotEmpty(t, cursorStr, "page %d", page)
		if len(events) == 0 {
			// Both sides drained the window in lockstep. Assert what was
			// drained: an empty first page would otherwise pass without
			// ever exercising a resume.
			assert.Equal(t, 9, drained, "the chain must deliver every fixture event")
			return
		}
		drained += len(events)
		var cur protocol.Cursor
		require.NoError(t, json.Unmarshal([]byte(strconv.Quote(cursorStr)), &cur))
		req = protocol.GetEventsRequest{
			Pagination: &protocol.PaginationOptions{Cursor: &cur, Limit: 1},
		}
	}
	t.Fatalf("window did not drain within %d pages", maxPages)
}
