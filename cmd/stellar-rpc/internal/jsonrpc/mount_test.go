package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
)

// The framing is only correct WHERE IT IS MOUNTED: inside the shared chain.
// NewHandler builds one, so these cover both daemons.

func mountLogger() *log.Entry {
	l := log.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}

// newMountedHandler assembles the production chain and serves it over HTTP.
func newMountedHandler(t *testing.T, globalLimit time.Duration, specs []HandlerSpec) string {
	t.Helper()
	url, _ := newMountedHandlerAndHandle(t, globalLimit, specs)
	return url
}

// newMountedHandlerAndHandle also returns the mount, for lifetime tests.
func newMountedHandlerAndHandle(
	t *testing.T, globalLimit time.Duration, specs []HandlerSpec,
) (string, Handler) {
	t.Helper()
	handler := NewHandler(Params{
		Daemon:                host.MakeNoOpDaemon(),
		Logger:                mountLogger(),
		Specs:                 specs,
		GlobalQueueLimit:      100,
		GlobalDurationWarning: globalLimit / 2,
		GlobalDurationLimit:   globalLimit,
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL, handler
}

func postMounted(t *testing.T, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return res.StatusCode, string(raw)
}

// eventually is what the parking handlers below return once released.
const eventually = "eventually"

// fastSpec is every mount below's ordinary method.
func fastSpec() HandlerSpec {
	return HandlerSpec{
		MethodName: "fast",
		Handler: func(context.Context, *jrpc2.Request) (any, error) {
			return map[string]int{"n": 7}, nil
		},
		QueueLimit:           10,
		RequestDurationLimit: time.Minute,
	}
}

//nolint:testifylint // byte-exact wire pins; JSONEq would ignore key order and escaping
func TestMount_ServesThroughTheRealMiddlewareChain(t *testing.T) {
	// No parking handler: it would hold the only permit on one CPU.
	url := newMountedHandler(t, time.Minute, []HandlerSpec{fastSpec()})

	t.Run("a normal call gets its framed body, id escaped as the bridge escaped it", func(t *testing.T) {
		status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":"a<b","method":"fast"}`)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, `{"jsonrpc":"2.0","id":"a\u003cb","result":{"n":7}}`, body)
	})

	t.Run("an unknown method is method-not-found, not a 500", func(t *testing.T) {
		status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":3,"method":"nope"}`)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t,
			`{"jsonrpc":"2.0","id":3,"error":{"code":-32601,"message":"method not found","data":"nope"}}`, body)
	})

	t.Run("a batch answers in input order through the whole chain", func(t *testing.T) {
		status, body := postMounted(t, url,
			`[{"jsonrpc":"2.0","id":1,"method":"fast"},{"jsonrpc":"1.0","id":2,"method":"fast"}]`)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, `[{"jsonrpc":"2.0","id":1,"result":{"n":7}},`+
			`{"jsonrpc":"2.0","id":2,"error":{"code":-32600,"message":"invalid version marker"}}]`, body)
	})

	t.Run("a body over the 512KB cap is rejected by the shared MaxBytesHandler", func(t *testing.T) {
		padding := strings.Repeat("x", maxHTTPRequestSize+1)
		status, body := postMounted(t, url,
			`{"jsonrpc":"2.0","id":1,"method":"fast","params":{"pad":"`+padding+`"}}`)
		assert.Equal(t, http.StatusInternalServerError, status)
		assert.Contains(t, body, "request body too large")
	})
}

// A client that hangs up mid-call does not cancel the work.
func TestMount_HandlerContextIsNotTheRequestContext(t *testing.T) {
	type outcome struct {
		deadline bool
		err      error
	}
	seen := make(chan outcome, 1)

	url := newMountedHandler(t, time.Minute, []HandlerSpec{{
		MethodName: "inspect",
		Handler: func(ctx context.Context, _ *jrpc2.Request) (any, error) {
			_, hasDeadline := ctx.Deadline()
			seen <- outcome{deadline: hasDeadline, err: ctx.Err()}
			return "ok", nil
		},
		QueueLimit: 10,
		// No per-method budget, so nothing between Background and the handler
		// adds a deadline of its own.
		RequestDurationLimit: network.RequestDurationLimiterNoLimit,
	}})

	status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":1,"method":"inspect"}`)
	require.Equal(t, http.StatusOK, status)
	//nolint:testifylint // byte-exact wire pin; JSONEq would ignore key order and escaping
	require.Equal(t, `{"jsonrpc":"2.0","id":1,"result":"ok"}`, body)

	got := <-seen
	assert.False(t, got.deadline, "the handler context must not carry the HTTP request's deadline")
	assert.NoError(t, got.err)
}

// A frame the mount produces must be what a JSON-RPC client parses.
func TestMount_FramesParseAsJSONRPC(t *testing.T) {
	url := newMountedHandler(t, time.Minute, []HandlerSpec{{
		MethodName:           "fast",
		Handler:              func(context.Context, *jrpc2.Request) (any, error) { return []int{1, 2, 3}, nil },
		QueueLimit:           10,
		RequestDurationLimit: time.Minute,
	}})

	status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":"x","method":"fast"}`)
	require.Equal(t, http.StatusOK, status)

	var frame struct {
		Version string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  []int           `json:"result"`
		Error   *jrpc2.Error    `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &frame))
	assert.Equal(t, "2.0", frame.Version)
	assert.JSONEq(t, `"x"`, string(frame.ID))
	assert.Equal(t, []int{1, 2, 3}, frame.Result)
	assert.Nil(t, frame.Error)
}

// The slow-client decoupler, on its own mount: a parked handler holds a permit.
//
//nolint:testifylint // byte-exact wire pins; JSONEq would ignore key order and escaping
func TestMount_ASlowCallGets504AndTheMountRecovers(t *testing.T) {
	const limit = 500 * time.Millisecond
	blocked := make(chan struct{})
	returned := make(chan struct{}, 1)

	url := newMountedHandler(t, limit, []HandlerSpec{fastSpec(), {
		MethodName: "slow",
		Handler: func(ctx context.Context, _ *jrpc2.Request) (any, error) {
			defer func() { returned <- struct{}{} }()
			select {
			case <-blocked:
			case <-ctx.Done():
			}
			return eventually, nil
		},
		QueueLimit: 10,
		// Longer than the global limit, so the HTTP limiter answers.
		RequestDurationLimit: time.Minute,
	}})

	start := time.Now()
	status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":1,"method":"slow"}`)
	assert.Equal(t, http.StatusGatewayTimeout, status)
	assert.Empty(t, body, "the duration limiter answers before the framing writes anything")
	assert.Less(t, time.Since(start), 30*time.Second)

	// The handler still holds its permit (DELTA (g)), so join it first.
	close(blocked)
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("the slow handler never returned after its channel was closed")
	}

	status, body = postMounted(t, url, `{"jsonrpc":"2.0","id":2,"method":"fast"}`)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, `{"jsonrpc":"2.0","id":2,"result":{"n":7}}`, body)
}

// ONE deadline for the whole request: calls that each fit must fit together.
func TestMount_ABatchOfSlowCallsFitsTheOneHTTPDeadline(t *testing.T) {
	const (
		globalLimit = time.Second
		perCall     = 400 * time.Millisecond
		elements    = 4
	)
	if runtime.GOMAXPROCS(0) < elements {
		t.Skip("needs at least one permit per batch element; the wire bound is GOMAXPROCS")
	}

	url := newMountedHandler(t, globalLimit, []HandlerSpec{{
		MethodName: "slow",
		Handler: func(context.Context, *jrpc2.Request) (any, error) {
			time.Sleep(perCall)
			return "done", nil
		},
		QueueLimit:           10,
		RequestDurationLimit: time.Minute,
	}})

	body := make([]string, elements)
	want := make([]string, elements)
	for i := range elements {
		body[i] = fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"slow"}`, i+1)
		want[i] = fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"done"}`, i+1)
	}

	status, got := postMounted(t, url, "["+strings.Join(body, ",")+"]")

	// Serial dispatch needs elements*perCall, past globalLimit.
	require.Equal(t, http.StatusOK, status,
		"a batch of %d %v calls did not fit a %v budget: the elements ran serially", elements, perCall, globalLimit)
	assert.Equal(t, "["+strings.Join(want, ",")+"]", got)
}

// Which layer catches a handler panic decides what the client sees.
func TestMount_HandlerPanics(t *testing.T) {
	boom := func(context.Context, *jrpc2.Request) (any, error) { panic("handler exploded") }
	url := newMountedHandler(t, time.Minute, []HandlerSpec{{
		MethodName:           "boomBudgeted",
		Handler:              boom,
		QueueLimit:           10,
		RequestDurationLimit: time.Minute,
	}, {
		MethodName: "boomUnbudgeted",
		Handler:    boom,
		QueueLimit: 10,
		// The only configuration in which a panic reaches the framing.
		RequestDurationLimit: network.RequestDurationLimiterNoLimit,
	}, {
		MethodName:           "fast",
		Handler:              func(context.Context, *jrpc2.Request) (any, error) { return map[string]int{"n": 7}, nil },
		QueueLimit:           10,
		RequestDurationLimit: time.Minute,
	}})

	stillServing := func(t *testing.T, id string) {
		t.Helper()
		status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":`+id+`,"method":"fast"}`)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, `{"jsonrpc":"2.0","id":`+id+`,"result":{"n":7}}`, body)
	}

	t.Run("a method with a duration budget answers -32003; the panic never reaches the framing", func(t *testing.T) {
		status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":1,"method":"boomBudgeted"}`)
		assert.Equal(t, http.StatusOK, status)
		//nolint:testifylint // byte-exact wire pin; JSONEq would ignore key order and escaping
		assert.Equal(t, `{"jsonrpc":"2.0","id":1,"error":{"code":-32003,`+
			`"message":"[-32003] request failed to process due to internal issue"}}`, body)
		stillServing(t, "2")
	})

	t.Run("a method with no budget fails the request with 500 and no body", func(t *testing.T) {
		status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":3,"method":"boomUnbudgeted"}`)
		assert.Equal(t, http.StatusInternalServerError, status)
		assert.Empty(t, body, "the limiter's 500 discards the response buffer rather than writing a partial one")
		stillServing(t, "4")
	})

	t.Run("a panicking batch element fails its batch, not the process", func(t *testing.T) {
		status, body := postMounted(t, url,
			`[{"jsonrpc":"2.0","id":5,"method":"fast"},{"jsonrpc":"2.0","id":6,"method":"boomUnbudgeted"}]`)
		assert.Equal(t, http.StatusInternalServerError, status)
		assert.Empty(t, body)
		stillServing(t, "7")
	})
}

// DELTA (g): the deadline ends the DISPATCH, not the work.
func TestMount_AStartedElementSurvivesTheDeadline(t *testing.T) {
	const limit = 300 * time.Millisecond
	finished := make(chan error, 1)

	url := newMountedHandler(t, limit, []HandlerSpec{{
		MethodName: "slow",
		Handler: func(ctx context.Context, _ *jrpc2.Request) (any, error) {
			time.Sleep(3 * limit) // well past the HTTP deadline
			finished <- ctx.Err()
			return eventually, nil
		},
		QueueLimit:           10,
		RequestDurationLimit: time.Minute,
	}})

	start := time.Now()
	status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":1,"method":"slow"}`)
	answered := time.Since(start)

	require.Equal(t, http.StatusGatewayTimeout, status)
	assert.Empty(t, body)
	assert.Less(t, answered, 3*limit, "the 504 waited for the handler instead of abandoning it")

	select {
	case err := <-finished:
		assert.NoError(t, err, "the HTTP deadline reached a handler that had already started")
	case <-time.After(10 * time.Second):
		t.Fatal("a started handler never finished: the run-to-completion guarantee is gone")
	}
}

// The deadline answers the client and leaves the handler running; only
// Shutdown ends it.
func TestMount_ShutdownEndsWhatTheDeadlineDidNot(t *testing.T) {
	const limit = 300 * time.Millisecond
	entered := make(chan struct{}, 1)
	observed := make(chan error, 1)

	url, handler := newMountedHandlerAndHandle(t, limit, []HandlerSpec{{
		MethodName: "watch",
		Handler: func(ctx context.Context, _ *jrpc2.Request) (any, error) {
			entered <- struct{}{}
			<-ctx.Done()
			observed <- ctx.Err()
			return eventually, nil
		},
		QueueLimit:           10,
		RequestDurationLimit: time.Minute,
	}})

	status, _ := postMounted(t, url, `{"jsonrpc":"2.0","id":1,"method":"watch"}`)
	require.Equal(t, http.StatusGatewayTimeout, status)
	<-entered

	select {
	case err := <-observed:
		t.Fatalf("the HTTP deadline canceled the handler context (%v); it must outlive its request", err)
	case <-time.After(2 * limit):
	}

	// Bounded, so a regression fails here instead of hanging the package.
	drainCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, handler.Shutdown(drainCtx))
	select {
	case err := <-observed:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not end a handler the deadline had abandoned")
	}
}

// A handler that ignores cancellation and outruns its per-method budget is
// answered -32001 and ABANDONED: the duration limiter returns without joining
// it, so its wire permit is back long before Shutdown starts. Draining the
// bound alone would prove nothing about it.
func TestMount_ShutdownJoinsATimedOutHandler(t *testing.T) {
	const budget = 200 * time.Millisecond

	// stuckMount answers one request, whose handler then parks on release.
	stuckMount := func(t *testing.T, release <-chan struct{}, done *atomic.Bool) Handler {
		t.Helper()
		url, handler := newMountedHandlerAndHandle(t, time.Minute, []HandlerSpec{{
			MethodName: "stuck",
			Handler: func(context.Context, *jrpc2.Request) (any, error) {
				<-release
				done.Store(true)
				return "late", nil
			},
			QueueLimit:           10,
			RequestDurationLimit: budget,
		}})
		status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":1,"method":"stuck"}`)
		require.Equal(t, http.StatusOK, status)
		require.Contains(t, body, "-32001", "the method budget did not fire")
		return handler
	}

	t.Run("reports the straggler when the drain budget runs out", func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		var done atomic.Bool
		handler := stuckMount(t, release, &done)

		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		err := handler.Shutdown(ctx)

		require.Error(t, err, "the drain claimed success while an abandoned handler was still running")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.False(t, done.Load())
	})

	t.Run("returns only once the straggler has finished", func(t *testing.T) {
		release := make(chan struct{})
		var done atomic.Bool
		handler := stuckMount(t, release, &done)

		time.AfterFunc(2*budget, func() { close(release) })
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		require.NoError(t, handler.Shutdown(ctx))
		assert.True(t, done.Load(), "Shutdown returned before the abandoned handler finished")
	})
}

// The wiring invariant both daemons now hold: connections dead BEFORE the
// drain. DELTA (g) gates an element's start on the request's context, so a
// live connection mid-batch keeps authorizing starts and the drain chases a
// moving target. Closing the connections kills those contexts first.
func TestMount_ClosedConnectionsStopTheDispatchBeforeTheDrain(t *testing.T) {
	weight := runtime.GOMAXPROCS(0)
	elements := 4 * weight
	entered := make(chan struct{}, elements)
	release := make(chan struct{})
	var started atomic.Int64

	handler := NewHandler(Params{
		Daemon: host.MakeNoOpDaemon(), Logger: mountLogger(), GlobalQueueLimit: 100,
		GlobalDurationWarning: time.Minute, GlobalDurationLimit: time.Minute,
		Specs: []HandlerSpec{{
			MethodName: "hold",
			Handler: func(context.Context, *jrpc2.Request) (any, error) {
				started.Add(1)
				entered <- struct{}{}
				<-release
				return "done", nil
			},
			QueueLimit:           1000,
			RequestDurationLimit: time.Minute,
		}},
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	body := make([]string, elements)
	for i := range body {
		body[i] = fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"hold"}`, i+1)
	}
	go func() {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL,
			strings.NewReader("["+strings.Join(body, ",")+"]"))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if res, derr := http.DefaultClient.Do(req); derr == nil {
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}
	}()

	// Bound saturated: the dispatch loop is parked in Acquire with most of the
	// batch still queued behind it.
	for range weight {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			close(release)
			t.Fatal("the batch never saturated the bound")
		}
	}

	srv.CloseClientConnections() // the request contexts die with them
	time.Sleep(250 * time.Millisecond)
	atClose := started.Load()
	close(release)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, handler.Shutdown(ctx), "the drain did not converge after the connections closed")

	t.Logf("started %d of %d elements at a bound of %d", started.Load(), elements, weight)
	assert.Equal(t, atClose, started.Load(), "elements started after the connections were closed")
	assert.LessOrEqual(t, atClose, int64(weight+1),
		"the dispatch kept going past the bound while its connection was dead")
}

// stableRegistryDaemon is MakeNoOpDaemon with a registry that does not change
// between calls. NoOpDaemon hands out a fresh one per call so tests can
// register repeatedly, which also makes registration unobservable.
type stableRegistryDaemon struct {
	*host.NoOpDaemon

	registry *prometheus.Registry
}

func (d stableRegistryDaemon) MetricsRegistry() *prometheus.Registry { return d.registry }

// Six metric families were built and never registered between #804 (2023) and
// the commit that added this test, so the per-method and global limiter
// signals did not exist on /metrics at all — including
// <method>_inflight_requests, which is the count of live handler bodies and
// the operator's only view of the population the drain waits for.
func TestMount_LimiterMetricsAreRegistered(t *testing.T) {
	daemon := stableRegistryDaemon{host.MakeNoOpDaemon(), prometheus.NewRegistry()}
	NewHandler(Params{
		Daemon: daemon, Logger: mountLogger(), GlobalQueueLimit: 100,
		GlobalDurationWarning: time.Second, GlobalDurationLimit: time.Minute,
		Specs: []HandlerSpec{{
			MethodName:           "getHealth",
			Handler:              func(context.Context, *jrpc2.Request) (any, error) { return "ok", nil },
			QueueLimit:           10,
			RequestDurationLimit: time.Minute,
		}},
	})

	families, err := daemon.registry.Gather()
	require.NoError(t, err)
	got := make(map[string]bool, len(families))
	for _, f := range families {
		got[f.GetName()] = true
	}

	ns := host.PrometheusNamespace + "_" + subsystemNetwork + "_"
	for _, name := range []string{
		ns + "get_health_inflight_requests",
		ns + "get_health_execution_threshold_warning",
		ns + "get_health_execution_threshold_limit",
		ns + "global_inflight_requests",
		ns + "global_request_execution_duration_threshold_warning",
		ns + "global_request_execution_duration_threshold_limit",
	} {
		assert.True(t, got[name], "%s was built but never registered", name)
	}

	// Registering twice must reuse rather than panic: the mount is rebuildable.
	assert.NotPanics(t, func() {
		NewHandler(Params{
			Daemon: daemon, Logger: mountLogger(), GlobalQueueLimit: 100,
			GlobalDurationWarning: time.Second, GlobalDurationLimit: time.Minute,
			Specs: []HandlerSpec{{
				MethodName:           "getHealth",
				Handler:              func(context.Context, *jrpc2.Request) (any, error) { return "ok", nil },
				QueueLimit:           10,
				RequestDurationLimit: time.Minute,
			}},
		})
	})
}
