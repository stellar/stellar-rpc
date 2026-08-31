package jsonrpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

//nolint:testifylint // byte-exact wire pins; JSONEq would ignore key order and escaping
func TestMount_ServesThroughTheRealMiddlewareChain(t *testing.T) {
	const limit = 500 * time.Millisecond
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })

	url := newMountedHandler(t, limit, []HandlerSpec{{
		MethodName: "fast",
		Handler: func(context.Context, *jrpc2.Request) (any, error) {
			return map[string]int{"n": 7}, nil
		},
		QueueLimit:           10,
		RequestDurationLimit: time.Minute,
	}, {
		MethodName: "slow",
		Handler: func(ctx context.Context, _ *jrpc2.Request) (any, error) {
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

	t.Run("a normal call gets its framed body, id escaped as the bridge escaped it", func(t *testing.T) {
		status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":"a<b","method":"fast"}`)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, `{"jsonrpc":"2.0","id":"a\u003cb","result":{"n":7}}`, body)
	})

	t.Run("a slow call still gets the 504 path with no body", func(t *testing.T) {
		start := time.Now()
		status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":1,"method":"slow"}`)
		assert.Equal(t, http.StatusGatewayTimeout, status)
		assert.Empty(t, body, "the duration limiter answers before the framing writes anything")
		assert.Less(t, time.Since(start), 30*time.Second)
	})

	t.Run("the mount keeps serving after a timed-out request", func(t *testing.T) {
		status, body := postMounted(t, url, `{"jsonrpc":"2.0","id":2,"method":"fast"}`)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, `{"jsonrpc":"2.0","id":2,"result":{"n":7}}`, body)
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
