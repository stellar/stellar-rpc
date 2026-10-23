package jsonrpc

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

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
			rec := postBridge(t, b, tc.body)
			require.Equal(t, http.StatusOK, rec.Code)
			for s, n := range tc.want {
				require.Equal(t, n, strings.Count(rec.Body.String(), s), "%q in %s", s, rec.Body.String())
			}
			require.Equal(t, int32(tc.want[ok]), calls.Load(), "calls dispatched")
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
