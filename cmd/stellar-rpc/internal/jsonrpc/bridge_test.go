package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stretchr/testify/require"
)

// bridgeMethods is the method table the bridge tests serve. Its raw result is
// compact and free of HTML characters, so jhttp.Bridge re-encodes it to the
// bytes the bridge passes through.
func bridgeMethods() handler.Map {
	return handler.Map{
		"echo": handler.New(func(_ context.Context, r *jrpc2.Request) (any, error) {
			var v any
			if err := r.UnmarshalParams(&v); err != nil {
				return nil, err
			}
			return v, nil
		}),
		"html": handler.New(func(context.Context) (any, error) {
			return map[string]string{"s": "<b>&</b>"}, nil
		}),
		"raw": handler.New(func(context.Context) (any, error) {
			return json.RawMessage(`{"pre":"rendered"}`), nil
		}),
		"rawEmpty": handler.New(func(context.Context) (any, error) {
			return json.RawMessage(nil), nil
		}),
		"ptrErr": handler.New(func(context.Context) (any, error) {
			return nil, &jrpc2.Error{Code: jrpc2.InvalidParams, Message: "<bad>", Data: json.RawMessage(`{"k":1}`)}
		}),
		"valErr": handler.New(func(context.Context) (any, error) {
			return nil, jrpc2.Error{Code: jrpc2.InternalError, Message: "val"}
		}),
		"plainErr": handler.New(func(context.Context) (any, error) {
			return nil, errors.New("boom")
		}),
		"canceled": handler.New(func(context.Context) (any, error) {
			return nil, context.Canceled
		}),
		"unmarshalable": handler.New(func(context.Context) (any, error) {
			return make(chan int), nil
		}),
	}
}

type bridgeCase struct {
	name, method, contentType, body string
}

func bridgeCases() []bridgeCase {
	call := func(name, body string) bridgeCase {
		return bridgeCase{name: name, method: http.MethodPost, contentType: contentTypeJSON, body: body}
	}
	const echo = `{"jsonrpc":"2.0","id":1,"method":"echo","params":[1]}`
	return []bridgeCase{
		call("call", `{"jsonrpc":"2.0","id":1,"method":"echo","params":[1,"a",{"b":null}]}`),
		call("null result", `{"jsonrpc":"2.0","id":1,"method":"echo"}`),
		call("html in result", `{"jsonrpc":"2.0","id":1,"method":"html"}`),
		call("raw result", `{"jsonrpc":"2.0","id":"r","method":"raw"}`),
		call("empty raw result", `{"jsonrpc":"2.0","id":"r","method":"rawEmpty"}`),
		call("pointer error", `{"jsonrpc":"2.0","id":1,"method":"ptrErr"}`),
		call("value error", `{"jsonrpc":"2.0","id":1,"method":"valErr"}`),
		call("plain error", `{"jsonrpc":"2.0","id":1,"method":"plainErr"}`),
		call("canceled error", `{"jsonrpc":"2.0","id":1,"method":"canceled"}`),
		call("unmarshalable result", `{"jsonrpc":"2.0","id":1,"method":"unmarshalable"}`),
		call("notification", `{"jsonrpc":"2.0","method":"echo","params":[1]}`),
		call("notification to unknown method", `{"jsonrpc":"2.0","method":"nope"}`),
		call("notification with empty method", `{"jsonrpc":"2.0","method":""}`),
		call("notification with bad params", `{"jsonrpc":"2.0","method":"echo","params":"s"}`),
		call("unknown method", `{"jsonrpc":"2.0","id":1,"method":"nope"}`),
		call("empty method", `{"jsonrpc":"2.0","id":1,"method":""}`),
		call("bad params", `{"jsonrpc":"2.0","id":1,"method":"echo","params":"s"}`),
		call("bad version", `{"jsonrpc":"1.0","id":1,"method":"echo"}`),
		call("html id", `{"jsonrpc":"2.0","id":"<b>&`+"\u2028"+`","method":"echo","params":[]}`),
		call("html id on invalid request", `{"jsonrpc":"2.0","id":"<b>&","method":"echo","params":"s"}`),
		call("null id", `{"jsonrpc":"2.0","id":null,"method":"echo","params":[]}`),
		call("number id", `{"jsonrpc":"2.0","id":1.5e3,"method":"echo","params":[]}`),
		call("batch", `[`+echo+`,`+
			`{"jsonrpc":"2.0","method":"echo","params":[2]},`+
			`{"jsonrpc":"2.0","id":2,"method":"nope"},`+
			`{"jsonrpc":"1.0","id":3,"method":"echo"},`+
			`{"jsonrpc":"2.0","id":"<i>","method":"ptrErr"},`+
			`{"jsonrpc":"2.0","id":4,"method":"valErr"},`+
			`{"jsonrpc":"2.0","id":5,"method":"raw"}]`),
		call("batch of one", `[`+echo+`]`),
		call("batch of notifications", `[{"jsonrpc":"2.0","method":"echo"},{"jsonrpc":"2.0","method":"nope"}]`),
		call("batch with invalid entries", `[1,`+echo+`,"x"]`),
		call("empty batch", `[]`),
		call("empty object", `{}`),
		call("invalid json", `{"jsonrpc":`),
		call("empty body", ``),
		call("not json", `hello`),
		{name: "get", method: http.MethodGet, contentType: contentTypeJSON, body: ``},
		{name: "put", method: http.MethodPut, contentType: contentTypeJSON, body: echo},
		{name: "text content type", method: http.MethodPost, contentType: "text/plain", body: echo},
		{name: "no content type", method: http.MethodPost, contentType: "", body: echo},
		{name: "json-rpc content type", method: http.MethodPost, contentType: "application/json-rpc", body: echo},
		{name: "charset utf-8", method: http.MethodPost, contentType: contentTypeJSON + "; charset=utf-8", body: echo},
		{name: "charset UTF8", method: http.MethodPost, contentType: contentTypeJSON + "; charset=UTF8", body: echo},
		{name: "charset latin1", method: http.MethodPost, contentType: contentTypeJSON + "; charset=latin1", body: echo},
	}
}

func serveBridge(t *testing.T, h http.Handler, tc bridgeCase) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), tc.method, "/", strings.NewReader(tc.body))
	if tc.contentType != "" {
		req.Header.Set("Content-Type", tc.contentType)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func postBridge(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	return serveBridge(t, h, bridgeCase{method: http.MethodPost, contentType: contentTypeJSON, body: body})
}

// The bridge answers every request with the status, headers and body that
// jhttp.Bridge, the transport it replaces, gives on upstream jrpc2.
func TestBridge_ParityWithJHTTP(t *testing.T) {
	methods := bridgeMethods()
	old := jhttp.NewBridge(methods, &jhttp.BridgeOptions{Server: &jrpc2.ServerOptions{DisableBuiltin: true}})
	t.Cleanup(func() { _ = old.Close() })
	b := newBridge(methods)
	t.Cleanup(b.Close)
	for _, tc := range bridgeCases() {
		t.Run(tc.name, func(t *testing.T) {
			want, got := serveBridge(t, old, tc), serveBridge(t, b, tc)
			require.Equal(t, want.Code, got.Code)
			require.Equal(t, want.Header(), got.Header())
			require.Equal(t, want.Body.String(), got.Body.String())
		})
	}
}

// A non-empty json.RawMessage result is written as is, with the Content-Length
// to match.
func TestBridge_RawResultPassthrough(t *testing.T) {
	raw := json.RawMessage("{\n  \"pretty\": \"<kept>\"\n}")
	b := newBridge(handler.Map{"raw": handler.New(func(context.Context) (any, error) { return raw, nil })})
	t.Cleanup(b.Close)
	rec := postBridge(t, b, `{"jsonrpc":"2.0","id":1,"method":"raw"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, `{"jsonrpc":"2.0","id":1,"result":`+string(raw)+`}`, rec.Body.String())
	require.Equal(t, strconv.Itoa(rec.Body.Len()), rec.Header().Get("Content-Length"))
}

// A handler runs on the HTTP request's context, so a client disconnect
// cancels it, and its error reaches the wire as a cancellation.
func TestBridge_RequestContext(t *testing.T) {
	ctx, disconnect := context.WithCancel(t.Context())
	b := newBridge(handler.Map{"m": handler.New(func(hctx context.Context) (any, error) {
		disconnect()
		return nil, hctx.Err()
	})})
	t.Cleanup(b.Close)
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"m"}`))
	req.Header.Set("Content-Type", contentTypeJSON)
	rec := httptest.NewRecorder()
	b.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t,
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"error":{"code":%d,"message":"context canceled"}}`,
			jrpc2.ErrorCode(context.Canceled)),
		rec.Body.String())
}

// Close cancels the requests in flight, waits for their handlers to return,
// and fails the requests after it, as jhttp.Bridge.Close did.
func TestBridge_CloseDrains(t *testing.T) {
	started, unwound := make(chan struct{}), make(chan struct{})
	b := newBridge(handler.Map{"slow": handler.New(func(hctx context.Context) (any, error) {
		close(started)
		<-hctx.Done()
		time.Sleep(50 * time.Millisecond) // unwinding takes a while
		close(unwound)
		return nil, hctx.Err()
	})})
	served := make(chan *httptest.ResponseRecorder, 1)
	go func() { served <- postBridge(t, b, `{"jsonrpc":"2.0","id":1,"method":"slow"}`) }()
	<-started
	b.Close()
	select {
	case <-unwound:
	default:
		t.Fatal("Close returned before the handler did")
	}
	require.Equal(t, http.StatusOK, (<-served).Code)

	rec := postBridge(t, b, `{"jsonrpc":"2.0","id":1,"method":"slow"}`)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, errBridgeClosed.Error()+"\n", rec.Body.String())
}
