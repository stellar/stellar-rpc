package rpcv2

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
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
	r.SetLatestLedger(chunk.FirstLedgerSeq, time.Now().Unix())
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
	t.Cleanup(func() {
		srv.Close()
		handler.Close()
	})
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

func TestWrapAdapterRequest_PanicReleasesSharedView(t *testing.T) {
	logger, buf := capturingLogger()
	cat, _ := rpcv2test.OpenTestCatalogWith(t, testCPI, logger)
	r := query.NewRegistry(cat, geometry.NewRetention(0, 0))
	rpcv2test.SeedHotChunkLCMs(t, cat, chunk.ID(0),
		func(d *hotchunk.DB) { r.PublishHandle(chunk.ID(0), d) },
		rpcv2test.ZeroTxLCMBytes(t, chunk.FirstLedgerSeq))
	r.SetLatestLedger(chunk.FirstLedgerSeq, time.Now().Unix())
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
