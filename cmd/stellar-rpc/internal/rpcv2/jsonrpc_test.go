package rpcv2

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
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

// servingRegistry seeds one hot-ready chunk-0 ledger (seq 2) and returns a
// registry serving it, plus the hot DB so tests can close it underneath a read.
func servingRegistry(t *testing.T) (*query.Registry, *hotchunk.DB) {
	t.Helper()
	cat, _ := testCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, 0))
	const c = chunk.ID(0)
	var db *hotchunk.DB
	rpcv2test.SeedHotChunkLCMs(t, cat, c, func(d *hotchunk.DB) {
		db = d
		r.PublishHandle(c, d)
	}, rpcv2test.ZeroTxLCMBytes(t, chunk.FirstLedgerSeq))
	r.SetLatestLedger(chunk.FirstLedgerSeq, time.Now().Unix())
	return r, db
}

func seedServingRegistry(t *testing.T) *query.Registry {
	t.Helper()
	r, _ := servingRegistry(t)
	return r
}

func testHandlerParams(t *testing.T, r *query.Registry) handlerParams {
	t.Helper()
	return handlerParams{
		daemon:            host.MakeNoOpDaemon(),
		logger:            silentLogger(),
		handlerMetrics:    jsonrpc.NewHandlerMetrics("test", prometheus.NewRegistry()),
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

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

func postRPC(t *testing.T, url, method, params string) rpcResponse {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":` + params + `}`
	resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body))) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	var out rpcResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func TestJSONRPCHandler_GetEventsIsExplicitlyNotImplemented(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	out := postRPC(t, url, "getEvents", `{"startLedger":2}`)
	require.NotNil(t, out.Error)
	assert.EqualValues(t, jrpc2.MethodNotFound, out.Error.Code)
	assert.Contains(t, out.Error.Message, "#774")
}

func TestJSONRPCHandler_GetEventsV2IsExplicitlyNotImplemented(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	out := postRPC(t, url, "getEventsV2", `{"startLedger":2}`)
	require.NotNil(t, out.Error)
	assert.EqualValues(t, jrpc2.MethodNotFound, out.Error.Code)
	assert.Contains(t, out.Error.Message, "#774")
}

func TestJSONRPCHandler_ServesLatestLedgerFromRegistry(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	out := postRPC(t, url, "getLatestLedger", `{}`)
	require.Nil(t, out.Error)
	var result struct {
		Sequence uint32 `json:"sequence"`
	}
	require.NoError(t, json.Unmarshal(out.Result, &result))
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), result.Sequence)
}

func TestJSONRPCHandler_HealthyOverFreshRegistryStamp(t *testing.T) {
	url := newTestRPCServer(t, seedServingRegistry(t))

	out := postRPC(t, url, "getHealth", `{}`)
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
