package jsonrpc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
)

// A client disconnect cancels the handler context, except for sendTransaction,
// which keeps its duration limit.
func TestWrapWithLimiters_ClientDisconnect(t *testing.T) {
	for _, test := range []struct {
		method       string
		wantCanceled bool
	}{
		{protocol.SendTransactionMethodName, false},
		{protocol.GetHealthMethodName, true},
	} {
		t.Run(test.method, func(t *testing.T) {
			ctx, disconnect := context.WithCancel(t.Context())
			var deadline bool
			h := wrapWithLimiters(HandlerSpec{
				MethodName: test.method,
				Handler: func(hctx context.Context, _ *jrpc2.Request) (any, error) {
					disconnect()
					_, deadline = hctx.Deadline()
					return nil, hctx.Err()
				},
				QueueLimit:           1,
				RequestDurationLimit: time.Minute,
			}, host.MakeNoOpDaemon(), log.DefaultLogger)

			req := (&jrpc2.ParsedRequest{ID: "1", Method: test.method}).ToRequest()
			_, err := h(ctx, req)
			if test.wantCanceled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			assert.True(t, deadline, "duration limit not applied")
		})
	}
}

// newOneAtATimeHandler returns a handler that runs one request at a time. Its
// method "wait" returns once release is closed, and "ok" returns at once.
func newOneAtATimeHandler(t *testing.T, waiting chan<- struct{}, release <-chan struct{}) (Handler, *logtest.Hook) {
	logger, logs := testLogger()
	spec := func(name string, h jrpc2.Handler) HandlerSpec {
		return HandlerSpec{MethodName: name, Handler: h, QueueLimit: 10, RequestDurationLimit: time.Minute}
	}
	h := NewHandler(Params{
		Daemon: host.MakeNoOpDaemon(),
		Logger: logger,
		Specs: []HandlerSpec{
			spec("wait", handler.New(func(context.Context) (any, error) {
				waiting <- struct{}{}
				<-release
				return 1, nil
			})),
			spec("ok", handler.New(func(context.Context) (any, error) { return 2, nil })),
		},
		GlobalQueueLimit:    1,
		GlobalDurationLimit: time.Minute,
		MaxRequestBatchSize: testMaxBatchSize,
	})
	t.Cleanup(h.Close)
	return h, logs
}

func runningRequests(t *testing.T, h Handler) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, h.bridge.requests.gauge.Write(&m))
	return m.GetGauge().GetValue()
}

// A request whose body is still arriving does not count toward the global
// limit.
func TestNewHandler_CountsARequestOnceItsBodyIsRead(t *testing.T) {
	h, _ := newOneAtATimeHandler(t, nil, nil)
	body, sender := io.Pipe()
	slow := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", body)
	slow.Header.Set("Content-Type", contentTypeJSON)
	slowRec := httptest.NewRecorder()
	served := make(chan struct{})
	go func() {
		h.ServeHTTP(slowRec, slow)
		close(served)
	}()
	_, err := io.WriteString(sender, `{"jsonrpc":"2.0","id":1,`)
	require.NoError(t, err)

	rec := postBridge(t, h, `{"jsonrpc":"2.0","id":2,"method":"ok"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"jsonrpc":"2.0","id":2,"result":2}`, rec.Body.String())

	_, err = io.WriteString(sender, `"method":"ok"}`)
	require.NoError(t, err)
	require.NoError(t, sender.Close())
	<-served
	require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":2}`, slowRec.Body.String())
}

// A request over the global limit gets a 503 with no body, and reaching the
// limit is logged once while it lasts.
func TestNewHandler_RejectsRequestsOverTheGlobalLimit(t *testing.T) {
	waiting, release := make(chan struct{}, 1), make(chan struct{})
	h, logs := newOneAtATimeHandler(t, waiting, release)
	running := make(chan *httptest.ResponseRecorder, 1)
	go func() { running <- postBridge(t, h, `{"jsonrpc":"2.0","id":1,"method":"wait"}`) }()
	<-waiting
	require.InDelta(t, 1, runningRequests(t, h), 0)

	for range 2 {
		// Not valid JSON: a request over the limit is rejected before it is parsed.
		rec := postBridge(t, h, `not json`)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Empty(t, rec.Body.String())
	}
	var limited []string
	for _, entry := range logs.AllEntries() {
		if entry.Level == logrus.InfoLevel && strings.HasPrefix(entry.Message, "Backlog queue limiter") {
			limited = append(limited, entry.Message)
		}
	}
	require.Equal(t, []string{
		"Backlog queue limiter reached the queue limit of 1 executing concurrent http requests.",
	}, limited)

	close(release)
	require.Equal(t, http.StatusOK, (<-running).Code)
	require.InDelta(t, 0, runningRequests(t, h), 0)
	require.Equal(t, http.StatusOK, postBridge(t, h, `{"jsonrpc":"2.0","id":3,"method":"ok"}`).Code)
}

// registryDaemon is a no-op daemon that keeps one metrics registry.
type registryDaemon struct {
	*host.NoOpDaemon

	registry *prometheus.Registry
}

func (d registryDaemon) MetricsRegistry() *prometheus.Registry { return d.registry }

// NewHandler registers the global and per-method limiter metrics, and a second
// handler on the same registry counts on the same ones.
func TestNewHandler_RegistersTheLimiterMetrics(t *testing.T) {
	daemon := registryDaemon{NoOpDaemon: host.MakeNoOpDaemon(), registry: prometheus.NewRegistry()}
	params := Params{
		Daemon: daemon,
		Logger: log.DefaultLogger,
		Specs: []HandlerSpec{{
			MethodName:           protocol.GetHealthMethodName,
			Handler:              handler.New(func(context.Context) (any, error) { return "ok", nil }),
			QueueLimit:           1,
			RequestDurationLimit: time.Minute,
		}},
		GlobalQueueLimit:    1,
		GlobalDurationLimit: time.Minute,
	}
	first, second := NewHandler(params), NewHandler(params)
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)
	require.Same(t, first.bridge.requests.gauge, second.bridge.requests.gauge)

	families, err := daemon.registry.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}
	prefix := daemon.MetricsNamespace() + "_network_"
	require.ElementsMatch(t, []string{
		prefix + "global_inflight_requests",
		prefix + "global_request_execution_duration_threshold_warning",
		prefix + "global_request_execution_duration_threshold_limit",
		prefix + "global_total_response_bytes_limit",
		prefix + "get_health_inflight_requests",
		prefix + "get_health_execution_threshold_warning",
		prefix + "get_health_execution_threshold_limit",
	}, names)
}

// The request metric labels an error with its JSON-RPC code, or as "error"
// when it has none.
func TestDecorateHandlersLabelsErrors(t *testing.T) {
	logger, logs := testLogger()
	h := decorateHandlers(host.MakeNoOpDaemon(), logger, handler.Map{
		"plain": handler.New(func(context.Context) (any, error) { return nil, errors.New("boom") }),
		"coded": handler.New(func(context.Context) (any, error) { return nil, network.ErrPreflightQueueFull }),
	})
	for method, want := range map[string]string{"plain": "error", "coded": "error_code__32004"} {
		_, _ = h[method](t.Context(), (&jrpc2.ParsedRequest{ID: "1", Method: method}).ToRequest())
		assert.Equal(t, want, logs.LastEntry().Data["status"], method)
	}
}
