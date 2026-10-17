package network

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stretchr/testify/require"
)

func TestJSONRPCRequestLimits(t *testing.T) {
	var calls atomic.Int32
	bridge := jhttp.NewBridge(handler.Map{
		"getHealth": handler.New(func(context.Context) (string, error) {
			calls.Add(1)
			return "ok", nil
		}),
	}, &jhttp.BridgeOptions{ParseRequest: MakeJSONRPCRequestParser(3)})
	t.Cleanup(func() { require.NoError(t, bridge.Close()) })
	server := RequireJSONPost(bridge)
	serve := func(method, contentType, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), method, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", contentType)
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		return rec
	}

	call := `{"jsonrpc":"2.0","id":1,"method":"getHealth"}`
	notification := `{"jsonrpc":"2.0","method":"getHealth"}`
	batch := func(elems ...string) string { return "[" + strings.Join(elems, ",") + "]" }
	ok, exceeds, notify := `"result":"ok"`, "exceeds the limit of 3 calls", "notifications are not supported"
	for _, tc := range []struct {
		name string
		body string
		want map[string]int // substring -> occurrences in the response
	}{
		{"single call", call, map[string]int{ok: 1}},
		{"batch at the limit", batch(call, call, call), map[string]int{ok: 3}},
		{"batch over the limit", batch(call, call, call, call), map[string]int{exceeds: 1}},
		{"malformed element over the limit", batch(call, call, call, "1"), map[string]int{exceeds: 1}},
		{"truncated batch over the limit", "[" + strings.Repeat(call+",", 4), map[string]int{exceeds: 1}},
		{"empty batch", "[]", map[string]int{"empty batch": 1}},
		{"notification", notification, map[string]int{notify: 1}},
		{"notification in a batch", batch(call, notification), map[string]int{ok: 1, notify: 1}},
		{"null id", `{"jsonrpc":"2.0","id":null,"method":"getHealth"}`, map[string]int{notify: 1}},
		{"malformed element", batch(call, "1"), map[string]int{ok: 1, notify: 0}},
		{"null element", batch(call, "null"), map[string]int{ok: 1, notify: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			rec := serve(http.MethodPost, "application/json", tc.body)
			require.Equal(t, http.StatusOK, rec.Code)
			for s, n := range tc.want {
				require.Equal(t, n, strings.Count(rec.Body.String(), s), "%q in %s", s, rec.Body.String())
			}
			require.Equal(t, int32(tc.want[ok]), calls.Load(), "calls dispatched")
		})
	}

	overLimit := serve(http.MethodPost, "application/json", batch(call, call, call, call)).Body.String()
	require.True(t, strings.HasPrefix(overLimit, "["), "an oversized batch gets a batch response: %s", overLimit)

	require.Equal(t, http.StatusMethodNotAllowed, serve(http.MethodGet, "application/json", call).Code)
	require.Equal(t, http.StatusUnsupportedMediaType, serve(http.MethodPost, "text/plain", call).Code)
	latin1 := "application/json; charset=latin1"
	require.Equal(t, http.StatusUnsupportedMediaType, serve(http.MethodPost, latin1, call).Code)
	require.Equal(t, http.StatusOK, serve(http.MethodPost, "application/json; charset=UTF-8", call).Code)
}
