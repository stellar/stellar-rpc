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
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
)

// The framing is only correct if it is correct WHERE IT IS MOUNTED: inside the
// shared chain (cors -> 512KB cap -> request-duration limiter and its buffered
// response writer -> global backlog limiter -> framing), not on its own. The
// duration limiter is the slow-client decoupler — it answers at the deadline
// and returns while the handler goroutine is still live, which is only safe
// because the framing wrote into a buffer and not the socket. These tests drive
// the assembled mount over real HTTP and pin both halves of that: a slow
// handler still gets 504, a normal one still gets its body.
//
// NewHandler builds one mount, so these cover both daemons: v1 and v2 differ
// only in which HandlerSpecs they hand it.

func mountLogger() *log.Entry {
	l := log.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}

// newMountedHandler assembles the production chain over specs and serves it
// over real HTTP.
func newMountedHandler(t *testing.T, globalLimit time.Duration, specs []HandlerSpec) string {
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
	// No handler teardown: the mount owns no goroutine and no connection.
	t.Cleanup(srv.Close)
	return srv.URL
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

// fastSpec answers immediately, and is every mount below's ordinary method.
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
	// No parking handler here: one would hold a wire permit for as long as it
	// parks, and the bound is GOMAXPROCS — one, on a single-CPU runner — so
	// every subtest after it would 504. The 504 path has its own mount below.
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

// Handlers see a context derived from context.Background, exactly as jrpc2's
// ServerOptions.NewContext default gave them: a client that hangs up mid-call
// does not cancel the work. Deriving from the http.Request's context would
// change that for every method including sendTransaction, and is deliberately
// not part of this change.
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

// A frame the mount produces must be exactly what a JSON-RPC client parses, not
// merely something that looks like it. This is the belt to the byte-pins' braces.
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

// The slow-client decoupler, on a mount of its own. A parked handler holds its
// wire permit until it returns, and that bound is GOMAXPROCS — one on a
// single-CPU runner — so this cannot share a mount with tests that need a
// permit of their own. Releasing and joining the handler is the second half of
// the test, not bookkeeping: "the mount survives a 504" is only meaningful once
// the abandoned handler has finished.
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
			return "eventually", nil
		},
		QueueLimit: 10,
		// Longer than the global limit, so the HTTP-level limiter is the one
		// that answers and the 504 path is the one under test.
		RequestDurationLimit: time.Minute,
	}})

	start := time.Now()
	status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":1,"method":"slow"}`)
	assert.Equal(t, http.StatusGatewayTimeout, status)
	assert.Empty(t, body, "the duration limiter answers before the framing writes anything")
	assert.Less(t, time.Since(start), 30*time.Second)

	// The client has its 504; the handler goroutine is still live and still
	// holds its permit. That is the documented shape (DELTA (g)), so the mount
	// is only expected to serve again once it has actually finished.
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

// The regression that made batch dispatch concurrent, at the altitude where it
// bites: the mount enforces ONE deadline for the whole HTTP request, so a
// serial batch sums its elements' latencies against it. Four calls that each
// fit comfortably inside the budget must still fit inside it together.
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

	// Serial dispatch needs elements*perCall, which is past globalLimit, and
	// the duration limiter answers 504 with an empty body.
	require.Equal(t, http.StatusOK, status,
		"a batch of %d %v calls did not fit a %v budget: the elements ran serially", elements, perCall, globalLimit)
	assert.Equal(t, "["+strings.Join(want, ",")+"]", got)
}

// The panic path, pinned at the mount, because which layer catches a handler
// panic decides what the client sees and there are two candidate layers in the
// chain.
//
// jrpc2 recovered nothing: under the bridge a panic from a method with no
// duration budget unwound on a goroutine the jrpc2.Server owned and took the
// PROCESS with it. Here the chain's own recover answers 500 and the mount
// keeps serving — and a batch element's panic has to be relayed to the serving
// goroutine to get that, since nothing above a worker recovers.
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
		// The only configuration in which a panic reaches the framing at all.
		// No method either daemon registers is built this way.
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
