package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
)

type parityParams struct {
	X int `json:"x"`
}

type parityResult struct {
	Y int `json:"y"`
}

// fixed is a handler that always answers v, err.
func fixed(v any, err error) jrpc2.Handler {
	return func(context.Context, *jrpc2.Request) (any, error) { return v, err }
}

// parityMethods covers every way a handler can answer: typed and untyped
// params, pre-rendered bytes, a nil result, and each error shape jrpc2
// converts differently.
func parityMethods() handler.Map {
	return handler.Map{
		"echo": func(_ context.Context, req *jrpc2.Request) (any, error) {
			var p parityParams
			if err := req.UnmarshalParams(&p); err != nil {
				return nil, err
			}
			return parityResult{Y: p.X}, nil
		},
		"typed": methods.NewHandler(func(_ context.Context, p parityParams) (parityResult, error) {
			return parityResult{Y: 2 * p.X}, nil
		}),
		"raw":         fixed(json.RawMessage(`{"pre":"rendered","n":[1,2,3]}`), nil),
		"rawNil":      fixed(json.RawMessage(nil), nil),
		"nilResult":   fixed(nil, nil),
		"errPtr":      fixed(nil, &jrpc2.Error{Code: -32001, Message: "ptr", Data: json.RawMessage(`{"k":1}`)}),
		"errValue":    fixed(nil, jrpc2.Error{Code: -32002, Message: "value"}),
		"errPlain":    fixed(nil, errors.New("plain")),
		"errWrapped":  fixed(nil, fmt.Errorf("wrapped: %w", &jrpc2.Error{Code: -32004, Message: "inner"})),
		"errCanceled": fixed(nil, context.Canceled),
		"errDeadline": fixed(nil, context.DeadlineExceeded),
	}
}

type parityCase struct {
	name        string
	httpMethod  string
	contentType string
	body        string
}

func parityCases() []parityCase {
	post := func(name, body string) parityCase {
		return parityCase{name: name, httpMethod: http.MethodPost, contentType: "application/json", body: body}
	}
	return []parityCase{
		post("call with numeric id", `{"jsonrpc":"2.0","id":1,"method":"echo","params":{"x":7}}`),
		post("call with string id", `{"jsonrpc":"2.0","id":"abc","method":"echo","params":{"x":7}}`),
		post("call with negative id", `{"jsonrpc":"2.0","id":-5,"method":"typed","params":{"x":3}}`),
		post("notification", `{"jsonrpc":"2.0","method":"echo","params":{"x":1}}`),
		post("null id is a notification", `{"jsonrpc":"2.0","id":null,"method":"echo","params":{"x":1}}`),
		post("batch keeps order",
			`[{"jsonrpc":"2.0","id":2,"method":"raw"},{"jsonrpc":"2.0","id":1,"method":"echo","params":{"x":9}}]`),
		post("batch of one", `[{"jsonrpc":"2.0","id":1,"method":"echo","params":{"x":9}}]`),
		post("batch with notification",
			`[{"jsonrpc":"2.0","method":"echo"},{"jsonrpc":"2.0","id":1,"method":"typed","params":{"x":1}}]`),
		post("batch of notifications", `[{"jsonrpc":"2.0","method":"echo"},{"jsonrpc":"2.0","method":"raw"}]`),
		post("batch with duplicate ids",
			`[{"jsonrpc":"2.0","id":1,"method":"raw"},{"jsonrpc":"2.0","id":1,"method":"nilResult"}]`),
		post("empty batch", `[]`),
		post("unknown method", `{"jsonrpc":"2.0","id":1,"method":"nope"}`),
		post("unknown method notification", `{"jsonrpc":"2.0","method":"nope"}`),
		post("missing method", `{"jsonrpc":"2.0","id":1}`),
		post("missing method notification", `{"jsonrpc":"2.0"}`),
		post("wrong version", `{"jsonrpc":"1.0","id":1,"method":"echo"}`),
		post("missing version", `{"id":1,"method":"echo"}`),
		post("bad id type", `{"jsonrpc":"2.0","id":{"a":1},"method":"echo"}`),
		post("unknown field", `{"jsonrpc":"2.0","id":1,"method":"echo","extra":1}`),
		post("params not an object", `{"jsonrpc":"2.0","id":1,"method":"echo","params":5}`),
		post("array params rejected by typed handler", `{"jsonrpc":"2.0","id":1,"method":"typed","params":[1]}`),
		post("invalid params", `{"jsonrpc":"2.0","id":1,"method":"typed","params":{"x":"str"}}`),
		post("raw passthrough", `{"jsonrpc":"2.0","id":1,"method":"raw"}`),
		post("nil raw result", `{"jsonrpc":"2.0","id":1,"method":"rawNil"}`),
		post("nil result", `{"jsonrpc":"2.0","id":1,"method":"nilResult"}`),
		post("pointer error with data", `{"jsonrpc":"2.0","id":1,"method":"errPtr"}`),
		post("value error", `{"jsonrpc":"2.0","id":1,"method":"errValue"}`),
		post("plain error", `{"jsonrpc":"2.0","id":1,"method":"errPlain"}`),
		post("wrapped error", `{"jsonrpc":"2.0","id":1,"method":"errWrapped"}`),
		post("canceled", `{"jsonrpc":"2.0","id":1,"method":"errCanceled"}`),
		post("deadline", `{"jsonrpc":"2.0","id":1,"method":"errDeadline"}`),
		post("error in batch", `[{"jsonrpc":"2.0","id":1,"method":"errPtr"},{"jsonrpc":"2.0","id":2,"method":"raw"}]`),
		post("not json", `{`),
		post("empty body", ``),
		post("scalar body", `42`),
		post("string body", `"hi"`),
		post("batch of scalars", `[1,2]`),
		{name: "GET", httpMethod: http.MethodGet, contentType: "application/json", body: ""},
		{name: "wrong content type", httpMethod: http.MethodPost, contentType: "text/plain", body: `{}`},
		{name: "missing content type", httpMethod: http.MethodPost, contentType: "", body: `{}`},
		{
			name: "utf-8 charset accepted", httpMethod: http.MethodPost,
			contentType: "application/json; charset=utf-8", body: `{"jsonrpc":"2.0","id":1,"method":"raw"}`,
		},
		{
			name: "other charset rejected", httpMethod: http.MethodPost,
			contentType: "application/json; charset=latin1", body: `{"jsonrpc":"2.0","id":1,"method":"raw"}`,
		},
	}
}

func record(t *testing.T, h http.Handler, tc parityCase) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), tc.httpMethod, "/", strings.NewReader(tc.body))
	if tc.contentType != "" {
		req.Header.Set("Content-Type", tc.contentType)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestDispatcherMatchesBridge pins the dispatcher to jhttp.Bridge's observable
// behavior: status, content type, and the JSON-RPC response (compared as JSON,
// since only the bridge re-compacts pre-rendered results).
func TestDispatcherMatchesBridge(t *testing.T) {
	m := parityMethods()
	bridge := jhttp.NewBridge(m, &jhttp.BridgeOptions{Server: &jrpc2.ServerOptions{DisableBuiltin: true}})
	t.Cleanup(func() { _ = bridge.Close() })
	dispatch := newDispatcher(m, log.New())

	for _, tc := range parityCases() {
		t.Run(tc.name, func(t *testing.T) {
			want, got := record(t, bridge, tc), record(t, dispatch, tc)
			require.Equal(t, want.Code, got.Code, "bridge body: %s\ndispatcher body: %s", want.Body, got.Body)
			assert.Equal(t, want.Header().Get("Content-Type"), got.Header().Get("Content-Type"))
			if strings.HasPrefix(want.Header().Get("Content-Type"), "application/json") && want.Body.Len() > 0 {
				assert.JSONEq(t, want.Body.String(), got.Body.String())
			} else {
				assert.Equal(t, want.Body.String(), got.Body.String())
			}
		})
	}
}

// A pre-rendered result reaches the wire byte for byte: that is the whole
// point of returning json.RawMessage from a handler.
func TestDispatcherWritesRawResultVerbatim(t *testing.T) {
	raw := json.RawMessage("{ \"spaced\" : [1, 2 ,3],\n\"tab\":\t\"<kept>\"}")
	m := handler.Map{"raw": fixed(raw, nil)}
	rec := record(t, newDispatcher(m, log.New()), parityCase{
		httpMethod: http.MethodPost, contentType: "application/json",
		body: `{"jsonrpc":"2.0","id":7,"method":"raw"}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, `{"jsonrpc":"2.0","id":7,"result":`+string(raw)+`}`, rec.Body.String())
}

// Every listed method is reachable and the response carries the request id.
func TestNewHandlerServesTheMethodTable(t *testing.T) {
	limits := fullLimitsByMethod()
	specs := limits.Apply(BuildHandlerSpecs(testHandlerDeps()))
	h := NewHandler(Params{
		Daemon: testHandlerDeps().Daemon, Logger: log.New(), Specs: specs,
		GlobalQueueLimit: 10, GlobalDurationWarning: time.Minute, GlobalDurationLimit: time.Minute,
	})
	t.Cleanup(h.Close)

	rec := record(t, h, parityCase{
		httpMethod: http.MethodPost, contentType: "application/json",
		body: `{"jsonrpc":"2.0","id":"req-1","method":"getEvents"}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"jsonrpc":"2.0","id":"req-1","result":"ok"}`, rec.Body.String())

	rec = record(t, h, parityCase{
		httpMethod: http.MethodPost, contentType: "application/json",
		body: `{"jsonrpc":"2.0","id":1,"method":"rpc.serverInfo"}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"code":-32601`)
}
