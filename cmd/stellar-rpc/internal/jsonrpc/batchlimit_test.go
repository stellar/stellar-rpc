package jsonrpc

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creachadair/jrpc2/handler"
	"github.com/stretchr/testify/require"
)

func TestBridge_RequestLimits(t *testing.T) {
	var calls atomic.Int32
	b := newBridge(handler.Map{
		"getHealth": handler.New(func(context.Context) (string, error) {
			calls.Add(1)
			return "ok", nil
		}),
	}, 3, unlimited)
	t.Cleanup(b.Close)
	serve := func(method, contentType, body string) int {
		return serveBridge(t, b, bridgeCase{method: method, contentType: contentType, body: body}).Code
	}

	call := `{"jsonrpc":"2.0","id":1,"method":"getHealth"}`
	notification := `{"jsonrpc":"2.0","method":"getHealth"}`
	batch := func(elems ...string) string { return "[" + strings.Join(elems, ",") + "]" }
	notifications := batch(notification, notification, notification, notification)
	ok, exceeds := `"result":"ok"`, "exceeds the limit of 3 calls"
	for _, tc := range []struct {
		name  string
		body  string
		code  int
		want  map[string]int // substring -> occurrences in the response
		calls int32          // handlers run
	}{
		{"single call", call, http.StatusOK, map[string]int{ok: 1}, 1},
		{"batch at the limit", batch(call, call, call), http.StatusOK, map[string]int{ok: 3}, 3},
		{"batch over the limit", batch(call, call, call, call), http.StatusOK, map[string]int{exceeds: 1}, 0},
		{"notifications over the limit", notifications, http.StatusOK, map[string]int{exceeds: 1}, 0},
		{"malformed element over the limit", batch(call, call, call, "1"), http.StatusOK, map[string]int{exceeds: 1}, 0},
		{"truncated batch over the limit", "[" + strings.Repeat(call+",", 4), http.StatusOK, map[string]int{exceeds: 1}, 0},
		{"empty batch", "[]", http.StatusOK, map[string]int{"empty batch": 1}, 0},
		{"notification", notification, http.StatusNoContent, nil, 1},
		{"notification in a batch", batch(call, notification), http.StatusOK, map[string]int{ok: 1}, 2},
		{"null id", `{"jsonrpc":"2.0","id":null,"method":"getHealth"}`, http.StatusNoContent, nil, 1},
		{"malformed element", batch(call, "1"), http.StatusOK, map[string]int{ok: 1}, 1},
		{"null element", batch(call, "null"), http.StatusOK, map[string]int{ok: 1}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			rec := postBridge(t, b, tc.body)
			require.Equal(t, tc.code, rec.Code)
			if tc.code == http.StatusNoContent {
				require.Empty(t, rec.Body.String())
			}
			for s, n := range tc.want {
				require.Equal(t, n, strings.Count(rec.Body.String(), s), "%q in %s", s, rec.Body.String())
			}
			require.Equal(t, tc.calls, calls.Load(), "calls dispatched")
		})
	}

	overLimit := postBridge(t, b, batch(call, call, call, call)).Body.String()
	require.True(t, strings.HasPrefix(overLimit, "["), "an oversized batch gets a batch response: %s", overLimit)

	require.Equal(t, http.StatusMethodNotAllowed, serve(http.MethodGet, contentTypeJSON, call))
	require.Equal(t, http.StatusUnsupportedMediaType, serve(http.MethodPost, "text/plain", call))
	require.Equal(t, http.StatusUnsupportedMediaType, serve(http.MethodPost, contentTypeJSON+"; charset=latin1", call))
	// Charset names are case-insensitive, though jhttp.Bridge accepted only lowercase.
	require.Equal(t, http.StatusOK, serve(http.MethodPost, contentTypeJSON+"; charset=UTF-8", call))
}

// A notification runs like a call: on the request's context, which ends at
// the duration limit, and before the bridge answers.
func TestBridge_NotificationRunsUnderTheLimits(t *testing.T) {
	logger, _ := testLogger()
	var timeouts counter
	var ran, hadDeadline atomic.Bool
	limits := durationLimits{limit: time.Second / 20, warnings: &counter{}, timeouts: &timeouts, logger: logger}
	b := newBridge(handler.Map{
		"quick": handler.New(func(ctx context.Context) error {
			_, ok := ctx.Deadline()
			hadDeadline.Store(ok)
			ran.Store(true)
			return nil
		}),
		"slow": handler.New(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}),
	}, testMaxBatchSize, limits)
	t.Cleanup(b.Close)

	rec := postBridge(t, b, `{"jsonrpc":"2.0","method":"quick"}`)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.True(t, ran.Load(), "the bridge answers after the notification ran")
	require.True(t, hadDeadline.Load(), "the notification runs on the request's bounded context")

	rec = postBridge(t, b, `{"jsonrpc":"2.0","method":"slow"}`)
	require.Equal(t, http.StatusGatewayTimeout, rec.Code)
	require.Equal(t, int64(1), timeouts.n.Load())
}
