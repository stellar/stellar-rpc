package integrationtest

import (
	"encoding/json"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	client "github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

// One increment leaves three events: fee, the contract's own, fee refund.
const eventsPerInvoke = 3

type eventsFixture struct {
	contractID  string         // strkey, as the API reports it
	contractRaw xdr.ContractId // the same id, as invocations need it
	ledgers     []uint32       // one per increment, ascending
	txHashes    []string       // one per increment, same order
}

func (f *eventsFixture) first() uint32 { return f.ledgers[0] }

func (f *eventsFixture) increment(test *infrastructure.Test) {
	resp := test.InvokeHostFunc(f.contractRaw, "increment")
	f.ledgers = append(f.ledgers, resp.Ledger)
	f.txHashes = append(f.txHashes, resp.TransactionHash)
}
func (f *eventsFixture) last() uint32 { return f.ledgers[len(f.ledgers)-1] }

// Each call waits for inclusion, so the increments land in distinct ledgers.
func deployAndIncrement(t *testing.T, test *infrastructure.Test, n int) *eventsFixture {
	rpc := test.GetRPCLient()
	test.UploadEventsContract()

	creationOp := infrastructure.CreateCreateEventsContractOperation(test.MasterAccount().GetAccountID())
	params := infrastructure.PreflightTransactionParams(t, rpc,
		infrastructure.CreateTransactionParams(test.MasterAccount(), creationOp),
	)
	tx, err := txnbuild.NewTransaction(params)
	require.NoError(t, err)
	infrastructure.SendSuccessfulTransaction(t, rpc, test.MasterKey(), tx)

	preimage := creationOp.HostFunction.MustCreateContractV2().ContractIdPreimage
	rawID := infrastructure.GetContractID(
		t,
		test.MasterAccount().GetAccountID(),
		preimage.MustFromAddress().Salt,
		infrastructure.StandaloneNetworkPassphrase,
	)
	fx := &eventsFixture{
		contractID:  strkey.MustEncode(strkey.VersionByteContract, rawID[:]),
		contractRaw: xdr.ContractId(rawID),
	}

	for range n {
		fx.increment(test)
	}
	for i := 1; i < n; i++ {
		require.Greater(t, fx.ledgers[i], fx.ledgers[i-1], "increments must land in distinct ledgers")
	}
	return fx
}

// drain follows the cursor until the status is not HAS_MORE.
func drain(t *testing.T, rpc *client.Client, req protocol.GetEventsV2Request,
) ([]protocol.EventInfoV2, protocol.GetEventsV2Response) {
	const maxPages = 200
	descending := req.Order == protocol.OrderDescending
	var all []protocol.EventInfoV2
	for page := range maxPages {
		resp, err := rpc.GetEventsV2(t.Context(), req)
		require.NoError(t, err, "page %d", page)
		if req.Limit != nil {
			require.LessOrEqual(t, len(resp.Events), int(*req.Limit), "page %d", page)
		}
		if page > 0 {
			prev := all[len(all)-1].Ledger
			for _, e := range resp.Events {
				if descending {
					require.LessOrEqual(t, e.Ledger, prev, "page %d: descending order broken", page)
				} else {
					require.GreaterOrEqual(t, e.Ledger, prev, "page %d: ascending order broken", page)
				}
			}
		}
		all = append(all, resp.Events...)
		if resp.ScanStatus != protocol.ScanStatusHasMore {
			return all, resp
		}
		require.NotEmpty(t, resp.Cursor, "page %d: HAS_MORE without a cursor", page)
		req = protocol.GetEventsV2Request{Cursor: resp.Cursor, Limit: req.Limit, Format: req.Format}
	}
	t.Fatalf("still HAS_MORE after %d pages", maxPages)
	return nil, protocol.GetEventsV2Response{}
}

// The fee and refund events belong to the native asset contract, so only the
// contract id tells the fixture's own event apart.
func requireFixtureEvents(t *testing.T, fx *eventsFixture, events []protocol.EventInfoV2) {
	require.Len(t, events, eventsPerInvoke*len(fx.ledgers))
	ids := make(map[string]struct{}, len(events))
	fromContract := 0
	for _, e := range events {
		_, dup := ids[e.ID]
		require.False(t, dup, "duplicate event id %s", e.ID)
		ids[e.ID] = struct{}{}
		assert.Contains(t, fx.ledgers, uint32(e.Ledger))
		assert.Contains(t, fx.txHashes, e.TransactionHash)
		assert.Equal(t, protocol.EventTypeContract, e.EventType)
		assert.NotEmpty(t, e.ContractID)
		if e.ContractID == fx.contractID {
			fromContract++
		}
	}
	require.Equal(t, len(fx.ledgers), fromContract, "one event per increment from the fixture's contract")
}

func eventIDs(events []protocol.EventInfoV2) []string {
	ids := make([]string, 0, len(events))
	for _, e := range events {
		ids = append(ids, e.ID)
	}
	return ids
}

func reversed(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[len(s)-1-i] = v
	}
	return out
}

func TestGetEventsV2AscendingDrainToTheTip(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	rpc := test.GetRPCLient()
	fx := deployAndIncrement(t, test, 3)

	events, last := drain(t, rpc, protocol.GetEventsV2Request{
		MinLedger: fx.first(),
		Limit:     new(uint(1)),
	})
	requireFixtureEvents(t, fx, events)
	for i := 1; i < len(events); i++ {
		assert.LessOrEqual(t, events[i-1].Ledger, events[i].Ledger, "ascending order")
	}

	assert.Equal(t, protocol.ScanStatusWaitingForLedgers, last.ScanStatus)
	assert.NotEmpty(t, last.Cursor)
	assert.GreaterOrEqual(t, last.LatestLedger, fx.last())
	assert.LessOrEqual(t, last.OldestLedger, fx.first())

	again, err := rpc.GetEventsV2(t.Context(), protocol.GetEventsV2Request{Cursor: last.Cursor})
	require.NoError(t, err)
	assert.Empty(t, again.Events)
}

func TestGetEventsV2ClosedRangeBothOrders(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	rpc := test.GetRPCLient()
	fx := deployAndIncrement(t, test, 3)

	asc, err := rpc.GetEventsV2(t.Context(), protocol.GetEventsV2Request{
		MinLedger: fx.first(),
		MaxLedger: fx.last(),
	})
	require.NoError(t, err)
	requireFixtureEvents(t, fx, asc.Events)
	assert.Equal(t, protocol.ScanStatusComplete, asc.ScanStatus)
	assert.Empty(t, asc.Cursor, "a complete query carries no cursor")

	desc, err := rpc.GetEventsV2(t.Context(), protocol.GetEventsV2Request{
		MinLedger: fx.first(),
		MaxLedger: fx.last(),
		Order:     protocol.OrderDescending,
	})
	require.NoError(t, err)
	assert.Equal(t, protocol.ScanStatusComplete, desc.ScanStatus)
	assert.Empty(t, desc.Cursor)
	assert.Equal(t, reversed(eventIDs(asc.Events)), eventIDs(desc.Events),
		"descending is the exact reverse of ascending")

	paged, last := drain(t, rpc, protocol.GetEventsV2Request{
		MinLedger: fx.first(),
		MaxLedger: fx.last(),
		Order:     protocol.OrderDescending,
		Limit:     new(uint(2)),
	})
	assert.Equal(t, protocol.ScanStatusComplete, last.ScanStatus)
	assert.Equal(t, eventIDs(desc.Events), eventIDs(paged))
}

func TestGetEventsV2FiltersOverTheWire(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	rpc := test.GetRPCLient()
	fx := deployAndIncrement(t, test, 2)

	rng := protocol.GetEventsV2Request{MinLedger: fx.first(), MaxLedger: fx.last()}
	call := func(t *testing.T, req protocol.GetEventsV2Request) protocol.GetEventsV2Response {
		resp, err := rpc.GetEventsV2(t.Context(), req)
		require.NoError(t, err)
		assert.Equal(t, protocol.ScanStatusComplete, resp.ScanStatus)
		return resp
	}
	contractOnly := func(resp protocol.GetEventsV2Response) []protocol.EventInfoV2 {
		var out []protocol.EventInfoV2
		for _, e := range resp.Events {
			if e.ContractID == fx.contractID {
				out = append(out, e)
			}
		}
		return out
	}

	// Filter with the topic the node itself emitted, in both encodings.
	unfiltered := call(t, rng)
	requireFixtureEvents(t, fx, unfiltered.Events)
	sample := contractOnly(unfiltered)[0]
	require.NotEmpty(t, sample.TopicXDR)
	require.Empty(t, sample.TopicJSON, "base64 output carries no JSON topics")
	topic0XDR := sample.TopicXDR[0]

	rngJSON := rng
	rngJSON.Format = protocol.FormatJSON
	asJSON := call(t, rngJSON)
	sampleJSON := contractOnly(asJSON)[0]
	require.Equal(t, sample.ID, sampleJSON.ID)
	require.NotEmpty(t, sampleJSON.TopicJSON)
	require.Empty(t, sampleJSON.TopicXDR, "JSON output carries no base64 topics")
	topic0JSON := sampleJSON.TopicJSON[0]

	wantIDs := eventIDs(contractOnly(unfiltered))
	require.Len(t, wantIDs, len(fx.ledgers))

	t.Run("contract id", func(t *testing.T) {
		req := rng
		req.Filters = []protocol.EventFilterV2{{ContractID: fx.contractID}}
		assert.Equal(t, wantIDs, eventIDs(call(t, req).Events))
	})

	t.Run("contract id and type", func(t *testing.T) {
		req := rng
		req.Filters = []protocol.EventFilterV2{{ContractID: fx.contractID, EventType: protocol.EventTypeContract}}
		assert.Equal(t, wantIDs, eventIDs(call(t, req).Events))
	})

	t.Run("topic0 as base64", func(t *testing.T) {
		req := rng
		req.Filters = []protocol.EventFilterV2{{
			ContractID: fx.contractID,
			Topic0:     json.RawMessage(`"` + topic0XDR + `"`),
		}}
		assert.Equal(t, wantIDs, eventIDs(call(t, req).Events))
	})

	// The spec lists json as an xdrInputFormat; this server does not serve it.
	t.Run("topic0 as JSON is rejected", func(t *testing.T) {
		req := rng
		req.XDRInputFormat = protocol.FormatJSON
		req.Filters = []protocol.EventFilterV2{{ContractID: fx.contractID, Topic0: topic0JSON}}
		_, err := rpc.GetEventsV2(t.Context(), req)
		var rpcErr *jrpc2.Error
		require.ErrorAs(t, err, &rpcErr)
		assert.Equal(t, jrpc2.InvalidParams, rpcErr.Code)
		var data protocol.InvalidParamsErrorData
		require.NoError(t, json.Unmarshal(rpcErr.Data, &data))
		assert.Equal(t, protocol.ErrorReasonInvalidParams, data.Reason)
	})

	t.Run("topic0 that matches nothing", func(t *testing.T) {
		nothing, err := xdr.MarshalBase64(xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: new(xdr.ScSymbol)})
		require.NoError(t, err)
		req := rng
		req.Filters = []protocol.EventFilterV2{{
			ContractID: fx.contractID,
			Topic0:     json.RawMessage(`"` + nothing + `"`),
		}}
		assert.Empty(t, call(t, req).Events)
	})

	t.Run("filters are OR-ed", func(t *testing.T) {
		req := rng
		req.Filters = []protocol.EventFilterV2{
			{ContractID: fx.contractID},
			{EventType: protocol.EventTypeContract},
		}
		assert.Equal(t, eventIDs(unfiltered.Events), eventIDs(call(t, req).Events))
	})
}

// Same node, real Core output. The unit parity harness uses synthetic data.
func TestGetEventsV2MatchesV1(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	rpc := test.GetRPCLient()
	fx := deployAndIncrement(t, test, 3)

	for _, format := range []string{"", protocol.FormatJSON} {
		t.Run("format="+format, func(t *testing.T) {
			v1, err := rpc.GetEvents(t.Context(), protocol.GetEventsRequest{
				StartLedger: fx.first(),
				EndLedger:   fx.last() + 1, // v1 endLedger is exclusive
				Pagination:  &protocol.PaginationOptions{Limit: 1000},
				Format:      format,
			})
			require.NoError(t, err)
			v2, err := rpc.GetEventsV2(t.Context(), protocol.GetEventsV2Request{
				MinLedger: fx.first(),
				MaxLedger: fx.last(),
				Format:    format,
			})
			require.NoError(t, err)
			requireFixtureEvents(t, fx, v2.Events)

			require.Len(t, v2.Events, len(v1.Events))
			for i := range v1.Events {
				assert.Equal(t, protocol.EventInfoV2(v1.Events[i]), v2.Events[i], "event %d", i)
			}
			assert.Equal(t, v1.LatestLedger, v2.LatestLedger, "latestLedger")
			assert.Equal(t, v1.OldestLedger, v2.OldestLedger, "oldestLedger")
		})
	}
}

// Page to the tip, submit, resume from the tip cursor. The union of the pages
// must equal a closed range read: nothing lost at the tip, nothing twice.
func TestGetEventsV2PagingWhileTipMoves(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	rpc := test.GetRPCLient()
	fx := deployAndIncrement(t, test, 1)

	seen := make([]protocol.EventInfoV2, 0, 16)
	req := protocol.GetEventsV2Request{MinLedger: fx.first(), Limit: new(uint(1))}
	const rounds = 3
	for round := range rounds {
		got, last := drain(t, rpc, req)
		seen = append(seen, got...)
		require.Equal(t, protocol.ScanStatusWaitingForLedgers, last.ScanStatus, "round %d", round)
		require.NotEmpty(t, last.Cursor, "round %d", round)

		idle, err := rpc.GetEventsV2(t.Context(), protocol.GetEventsV2Request{Cursor: last.Cursor})
		require.NoError(t, err)
		assert.Empty(t, idle.Events, "round %d: idle tip cursor served events", round)
		assert.NotEmpty(t, idle.Cursor, "round %d: idle tip cursor lost the cursor", round)

		if round < rounds-1 {
			fx.increment(test)
		}
		req = protocol.GetEventsV2Request{Cursor: idle.Cursor, Limit: new(uint(1))}
	}

	requireFixtureEvents(t, fx, seen)
	final, err := rpc.GetEventsV2(t.Context(), protocol.GetEventsV2Request{MinLedger: fx.first(), MaxLedger: fx.last()})
	require.NoError(t, err)
	assert.Equal(t, eventIDs(final.Events), eventIDs(seen), "paging to the tip disagrees with a closed range read")
}
