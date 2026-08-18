package rpcv2

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// servingRegistry seeds one hot-ready chunk-0 ledger (seq 2) and returns a
// registry serving it, plus the hot DB so tests can close it underneath a read.
func servingRegistry(t *testing.T) (*query.Registry, *hotchunk.DB) {
	t.Helper()
	cat, _ := testCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, 0))
	const c = chunk.ID(0)
	db, err := hotchunk.Open(cat.Layout().HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	rpcv2test.IngestLedger(t, db, chunk.FirstLedgerSeq, rpcv2test.ZeroTxLCMBytes(t, chunk.FirstLedgerSeq))
	require.NoError(t, cat.FlipHotReady(c))
	r.PublishHandle(c, db)
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
		metrics:           observability.NopMetrics{},
		ledgerReader:      adapters.NewLedgerReader(r),
		transactionReader: adapters.NewTransactionReader(r, "test passphrase"),
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

type storeClosedCounter struct {
	observability.NopMetrics

	served atomic.Int32
}

func (c *storeClosedCounter) StoreClosedServed() { c.served.Add(1) }

// flatteningHandler mimics what every shared handler does with an adapter
// error: replace it with a generic jrpc2 internal error, losing the chain.
func flatteningHandler(t *testing.T, call func(ctx context.Context) error) jrpc2.Handler {
	t.Helper()
	return func(ctx context.Context, _ *jrpc2.Request) (any, error) {
		err := call(ctx)
		require.Error(t, err)
		return nil, &jrpc2.Error{Code: jrpc2.InternalError, Message: "could not read stores"}
	}
}

func TestMapAdapterErrors_UnavailableChunkBecomesRetryable(t *testing.T) {
	cat, _ := testCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, 0))
	// Hot-ready in the catalog but no published handle: resolveTier finds no
	// serving store and the adapter surfaces query.ErrUnavailable.
	require.NoError(t, cat.FlipHotReady(chunk.ID(0)))
	r.SetLatestLedger(chunk.FirstLedgerSeq, 0)
	reader := adapters.NewLedgerReader(r)

	wrapped := mapAdapterErrors(flatteningHandler(t, func(ctx context.Context) error {
		_, _, err := reader.GetLedger(ctx, chunk.FirstLedgerSeq)
		return err
	}), observability.NopMetrics{})

	_, err := wrapped(context.Background(), nil)
	var rpcErr *jrpc2.Error
	require.ErrorAs(t, err, &rpcErr)
	assert.EqualValues(t, errCodeTemporarilyUnavailable, rpcErr.Code)
}

func TestMapAdapterErrors_StoreClosedIsRetryableAndCounted(t *testing.T) {
	r, db := servingRegistry(t)
	require.NoError(t, db.Close())
	reader := adapters.NewLedgerReader(r)
	metrics := &storeClosedCounter{}

	wrapped := mapAdapterErrors(flatteningHandler(t, func(ctx context.Context) error {
		_, _, err := reader.GetLedger(ctx, chunk.FirstLedgerSeq)
		return err
	}), metrics)

	_, err := wrapped(context.Background(), nil)
	var rpcErr *jrpc2.Error
	require.ErrorAs(t, err, &rpcErr)
	assert.EqualValues(t, errCodeTemporarilyUnavailable, rpcErr.Code)
	assert.Equal(t, int32(1), metrics.served.Load())
}

func TestMapAdapterErrors_UnmarkedErrorPassesThrough(t *testing.T) {
	inner := &jrpc2.Error{Code: jrpc2.InternalError, Message: "disk on fire"}
	wrapped := mapAdapterErrors(func(context.Context, *jrpc2.Request) (any, error) {
		return nil, inner
	}, observability.NopMetrics{})

	_, err := wrapped(context.Background(), nil)
	require.Same(t, error(inner), err)
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

func TestRetentionLedgers(t *testing.T) {
	assert.Equal(t, uint32(math.MaxUint32), retentionLedgers(0), "full history reports the widest window")
	assert.Equal(t, uint32(30_000), retentionLedgers(3))
	assert.Equal(t, uint32(math.MaxUint32), retentionLedgers(math.MaxUint32), "overflow clamps")
}
