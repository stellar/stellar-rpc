package rpcv2

import (
	"context"
	"encoding/json"
	"math"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

func defaultsConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.ParseConfig(nil)
	require.NoError(t, err)
	return cfg
}

// seedServingRegistry seeds one hot-ready chunk-0 ledger (seq 2) and returns a
// registry serving it.
func seedServingRegistry(t *testing.T) *query.Registry {
	t.Helper()
	cat, _ := testCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, 0))
	const c = chunk.ID(0)
	rpcv2test.SeedHotChunkLCMs(t, cat, c, func(d *hotchunk.DB) {
		r.PublishHandle(c, d)
	}, rpcv2test.ZeroTxLCMBytes(t, chunk.FirstLedgerSeq))
	r.SetLatestLedger(chunk.FirstLedgerSeq, query.CloseTimeAt(time.Now().Unix()))
	return r
}

func testHandlerParams(t *testing.T, r *query.Registry) handlerParams {
	t.Helper()
	return handlerParams{
		daemon:            host.MakeNoOpDaemon(),
		logger:            silentLogger(),
		metrics:           observability.NopMetrics{},
		registry:          r,
		ledgerReader:      adapters.NewLedgerReader(),
		transactionReader: adapters.NewTransactionReader("test passphrase", nil),
		feeWindows:        feewindow.NewFeeWindows(10, 10),
		networkPassphrase: "test passphrase",
		retentionWindow:   1,
	}
}

func newTestRPCServer(t *testing.T, r *query.Registry) string {
	t.Helper()
	handler := newJSONRPCHandler(defaultsConfig(t), testHandlerParams(t, r))
	srv := httptest.NewServer(handler)
	// No handler teardown: the mount owns no goroutine and no connection.
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestJSONRPCHandler_GetEventsIsExplicitlyNotImplemented(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	out := rpcv2test.PostRPC(t, url, "getEvents", `{"startLedger":2}`)
	require.NotNil(t, out.Error)
	assert.EqualValues(t, jrpc2.MethodNotFound, out.Error.Code)
	assert.Contains(t, out.Error.Message, "#774")
}

// The registered handler answers over the real server, so this covers
// the wiring the eventsapi tests cannot: the method name in the table,
// and the wrapper's read view reaching a handler that is not an adapter.
func TestJSONRPCHandler_ServesGetEventsV2(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	out := rpcv2test.PostRPC(t, url, "getEventsV2",
		`{"minLedger":2,"maxLedger":2}`)
	require.Nil(t, out.Error)
	var result struct {
		Events       []json.RawMessage `json:"events"`
		Cursor       string            `json:"cursor"`
		ScanStatus   string            `json:"scanStatus"`
		LatestLedger uint32            `json:"latestLedger"`
	}
	require.NoError(t, json.Unmarshal(out.Result, &result))
	assert.Empty(t, result.Events, "the fixture ledger carries no events")
	assert.Equal(t, protocol.ScanStatusComplete, result.ScanStatus)
	assert.Empty(t, result.Cursor)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), result.LatestLedger)
}

// An invalid request must reach the client as typed error data, not as a
// bare message.
func TestJSONRPCHandler_GetEventsV2ReportsTypedErrorData(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	out := rpcv2test.PostRPC(t, url, "getEventsV2", `{"minLedger":2,"limit":9999}`)
	require.NotNil(t, out.Error)
	assert.EqualValues(t, jrpc2.InvalidParams, out.Error.Code)
	var data struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(out.Error.Data, &data))
	assert.Equal(t, protocol.ErrorReasonInvalidParams, data.Reason)
}

// Every field is optional, so a typo would widen the query rather than fail.
func TestJSONRPCHandler_GetEventsV2RejectsUnknownFields(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	for name, params := range map[string]string{
		"top level":      `{"minLedger":2,"maxLedgor":2}`,
		"in a filter":    `{"minLedger":2,"filters":[{"topicc1":"AAAA"}]}`,
		"array params":   `[2]`,
		"negative limit": `{"minLedger":2,"limit":-1}`,
		"wrong type":     `{"minLedger":"abc"}`,
		"filter not obj": `{"minLedger":2,"filters":[7]}`,
	} {
		t.Run(name, func(t *testing.T) {
			out := rpcv2test.PostRPC(t, url, protocol.GetEventsV2MethodName, params)
			require.NotNil(t, out.Error)
			assert.EqualValues(t, jrpc2.InvalidParams, out.Error.Code)
			var data struct {
				Reason string `json:"reason"`
			}
			require.NoError(t, json.Unmarshal(out.Error.Data, &data))
			assert.Equal(t, protocol.ErrorReasonInvalidParams, data.Reason)
			assert.NotContains(t, out.Error.Message, "json:",
				"the decoder's own prefix is not the client's business")
			assert.NotContains(t, out.Error.Message, "protocol.",
				"a Go type name is not the client's business")
			assert.NotContains(t, out.Error.Message, "Go struct",
				"the decoder's phrasing is not the client's business")
		})
	}
}

func TestJSONRPCHandler_ServesLatestLedgerFromRegistry(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	out := rpcv2test.PostRPC(t, url, "getLatestLedger", `{}`)
	require.Nil(t, out.Error)
	var result struct {
		Sequence uint32 `json:"sequence"`
	}
	require.NoError(t, json.Unmarshal(out.Result, &result))
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), result.Sequence)
}

func TestJSONRPCHandler_HealthyOverFreshRegistryStamp(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	out := rpcv2test.PostRPC(t, url, "getHealth", `{}`)
	require.Nil(t, out.Error)
	var result struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(out.Result, &result))
	assert.Equal(t, "healthy", result.Status)
}

func TestJSONRPCHandler_HealthGatedUntilFirstCommit(t *testing.T) {
	r := seedServingRegistry(t)
	r.SeedLatestAtBoot(chunk.FirstLedgerSeq)
	url := newTestRPCServer(t, r)

	out := rpcv2test.PostRPC(t, url, "getHealth", `{}`)
	require.NotNil(t, out.Error)
	assert.EqualValues(t, jrpc2.InternalError, out.Error.Code)
	assert.Contains(t, out.Error.Message, "since this process started")

	r.SetLatestLedger(chunk.FirstLedgerSeq+1, query.CloseTimeAt(time.Now().Unix()))
	out = rpcv2test.PostRPC(t, url, "getHealth", `{}`)
	require.Nil(t, out.Error)
	var result struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(out.Result, &result))
	assert.Equal(t, "healthy", result.Status)
}

func TestWrapAdapterRequest_PanicReleasesSharedView(t *testing.T) {
	logger, buf := capturingLogger()
	cat, _ := rpcv2test.OpenTestCatalogWith(t, testCPI, logger)
	r := query.NewRegistry(cat, geometry.NewRetention(0, 0))
	rpcv2test.SeedHotChunkLCMs(t, cat, chunk.ID(0),
		func(d *hotchunk.DB) { r.PublishHandle(chunk.ID(0), d) },
		rpcv2test.ZeroTxLCMBytes(t, chunk.FirstLedgerSeq))
	r.SetLatestLedger(chunk.FirstLedgerSeq, query.CloseTimeAt(time.Now().Unix()))
	reader := adapters.NewLedgerReader()

	wrapped := wrapAdapterRequest(func(ctx context.Context, _ *jrpc2.Request) (any, error) {
		_, err := reader.GetLatestLedgerSequence(ctx)
		require.NoError(t, err)
		panic("handler panic after acquiring the shared view")
	}, r)

	assert.Panics(t, func() { _, _ = wrapped(context.Background(), nil) })

	require.NoError(t, cat.Close())
	assert.NotContains(t, buf.String(), "unreleased snapshot",
		"the deferred release must run during the panic unwind")
}

func TestDeriveLifecycleGrace_DefaultsGive55Seconds(t *testing.T) {
	cfg := defaultsConfig(t)
	// Defaults: 25s global cap dominates the 5/10/15s per-method budgets.
	assert.Equal(t, 55*time.Second, deriveLifecycleGrace(cfg.Service))
}

func TestDeriveLifecycleGrace_TracksARaisedMethodBudget(t *testing.T) {
	cfg := defaultsConfig(t)
	long := 2 * time.Minute
	cfg.Service.Methods.SimulateTransaction.MaxExecutionDuration = &long
	assert.Equal(t, long+graceMargin, deriveLifecycleGrace(cfg.Service))
}

// The mount answers an unknown method with jrpc2's own method-not-found frame:
// code, message and data are exactly what a client saw when jrpc2's server
// resolved the method, so replacing that server changed no byte here.
//
// It also drives the trigger of the library leak documented below many times
// over. Under the wire framing an unknown method never reaches a jrpc2.Server —
// it never reaches a handler at all — so no in-flight entry can be stranded and
// no attacker-chosen method name can reach the per-method metric labels.
func TestJSONRPCHandler_UnknownMethodWireShapeUnchanged(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))
	before := runtime.NumGoroutine()

	const unknownCalls = 500
	for range unknownCalls {
		out := rpcv2test.PostRPC(t, url, "noSuchMethod", `{"padding":"xxxxxxxxxxxxxxxx"}`)
		require.NotNil(t, out.Error)
		require.Nil(t, out.Result)
		require.EqualValues(t, jrpc2.MethodNotFound, out.Error.Code)
		require.Equal(t, "method not found", out.Error.Message)
		require.JSONEq(t, `"noSuchMethod"`, string(out.Error.Data))
	}

	// Generous: the HTTP server's own per-connection goroutines come and go.
	// A per-call goroutine leak over 500 calls would blow past this.
	assert.Less(t, runtime.NumGoroutine(), before+50,
		"unknown-method calls must not accumulate goroutines")
}

// The upstream jrpc2 defect, kept as a self-contained reproduction for the
// planned filing against creachadair/jrpc2 v1.3.3.
//
// Server.checkAndAssignLocked calls setContext (server.go:365-375) BEFORE it
// resolves a handler (348-351), so by the time Assign returns nil the server
// has already recorded the request's CancelFunc in Server.used. Release happens
// only in deliver() (295-299), and only for a response with no rsp.err — which
// responses() sets precisely when the task was never assigned a handler
// (800-802). So every unknown-method CALL carrying an id strands its map entry,
// its CancelFunc, and through that context the *Request and its params, for the
// life of the process. Remotely triggerable from garbage method names, and
// unbounded.
//
// Server.used is unexported, but it has one exported consequence: a second
// request reusing a still-registered id is rejected as a duplicate. That makes
// id reuse a direct, deterministic probe — no reflection, no fork.
//
// Nothing in this repo's serving path constructs a jrpc2.Server any more, so
// this is no longer a mitigation we carry; it is evidence. If this test ever
// stops failing in the way it asserts, the library has fixed the leak and both
// this test and the upstream ticket can go.
func TestJRPC2ServerStrandsUnknownMethodInFlightEntries(t *testing.T) {
	// Two calls, same id, unknown method: the second is refused as a duplicate
	// id because the first one's entry was never released.
	cli, srv := channel.Direct()
	table := handler.Map{
		"getHealth": func(context.Context, *jrpc2.Request) (any, error) { return "ok", nil },
	}
	server := jrpc2.NewServer(table, &jrpc2.ServerOptions{DisableBuiltin: true}).Start(srv)
	t.Cleanup(func() {
		require.NoError(t, cli.Close())
		require.NoError(t, server.Wait())
	})

	errs := make([]*jrpc2.Error, 0, 2)
	for range 2 {
		require.NoError(t, cli.Send([]byte(`{"jsonrpc":"2.0","id":1,"method":"noSuchMethod"}`)))
		raw, err := cli.Recv()
		require.NoError(t, err)
		var rsp struct {
			Error *jrpc2.Error `json:"error"`
		}
		require.NoError(t, json.Unmarshal(raw, &rsp))
		require.NotNil(t, rsp.Error, "an unknown method must answer an error")
		errs = append(errs, rsp.Error)
	}

	assert.Equal(t, jrpc2.MethodNotFound, errs[0].Code)
	assert.Equal(t, jrpc2.InvalidRequest, errs[1].Code,
		"if this passes as MethodNotFound, jrpc2 fixed the leak: retire the upstream ticket")
	assert.Equal(t, "duplicate request ID", errs[1].Message)
}

// A budget near the top of the range must not wrap the grace negative:
// lifecycle.WithLifecycleDefaults reads a non-positive Grace as unset and
// substitutes 5 minutes, which would pair the narrowest grace with the widest
// budget. Config validates only a minimum, so nothing upstream stops this.
func TestDeriveLifecycleGrace_SaturatesInsteadOfWrapping(t *testing.T) {
	cfg := defaultsConfig(t)
	huge := time.Duration(math.MaxInt64)
	cfg.Service.MaxRequestExecutionDuration = &huge

	grace := deriveLifecycleGrace(cfg.Service)
	assert.Positive(t, grace, "the derived grace wrapped negative")
	assert.Equal(t, time.Duration(math.MaxInt64), grace)
}
