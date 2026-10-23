package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/http/mutil"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
)

// unlimited lets requests run as long as their handlers take.
var unlimited = durationLimits{limit: network.RequestDurationLimiterNoLimit, logger: log.DefaultLogger}

const testMaxBatchSize = 10

type counter struct{ n atomic.Int64 }

func (c *counter) Inc() { c.n.Add(1) }

// testLogger returns a quiet logger whose entries hook records.
func testLogger() (*log.Entry, *logtest.Hook) {
	hook := new(logtest.Hook)
	logger := log.New()
	logger.SetOutput(io.Discard)
	logger.SetLevel(logrus.DebugLevel)
	logger.AddHook(hook)
	return logger, hook
}

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
		call("batch at the limit", `[`+strings.Repeat(echo+`,`, testMaxBatchSize-1)+echo+`]`),
		call("batch over the limit", `[`+strings.Repeat(echo+`,`, testMaxBatchSize)+echo+`]`),
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
		{name: "charset utf8", method: http.MethodPost, contentType: contentTypeJSON + "; charset=utf8", body: echo},
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
// jhttp.Bridge, the transport it replaces, gives on upstream jrpc2: stock
// jhttp for a request its POST and content-type checks reject, and jhttp with
// parseRequests as its ParseRequest hook, which turns those checks off, for
// any other.
func TestBridge_ParityWithJHTTP(t *testing.T) {
	methods := bridgeMethods()
	server := &jrpc2.ServerOptions{DisableBuiltin: true}
	stock := jhttp.NewBridge(methods, &jhttp.BridgeOptions{Server: server})
	t.Cleanup(func() { _ = stock.Close() })
	hooked := jhttp.NewBridge(methods, &jhttp.BridgeOptions{
		Server: server,
		ParseRequest: func(r *http.Request) ([]*jrpc2.ParsedRequest, error) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			return parseRequests(body, testMaxBatchSize)
		},
	})
	t.Cleanup(func() { _ = hooked.Close() })
	b := newBridge(methods, testMaxBatchSize, unlimited)
	t.Cleanup(b.Close)
	for _, tc := range bridgeCases() {
		t.Run(tc.name, func(t *testing.T) {
			want := serveBridge(t, stock, tc)
			if want.Code != http.StatusMethodNotAllowed && want.Code != http.StatusUnsupportedMediaType {
				want = serveBridge(t, hooked, tc)
				want.Header().Set("Accept-Post", contentTypeJSON) // the hook turns this header off too
			}
			got := serveBridge(t, b, tc)
			require.Equal(t, want.Code, got.Code)
			require.Equal(t, want.Header(), got.Header())
			require.Equal(t, want.Body.String(), got.Body.String())
		})
	}
}

// windowRecorder records how many bytes are written under each write deadline.
type windowRecorder struct {
	*httptest.ResponseRecorder

	windows []int
}

func (r *windowRecorder) SetWriteDeadline(time.Time) error {
	r.windows = append(r.windows, 0)
	return nil
}

func (r *windowRecorder) Write(b []byte) (int, error) {
	if len(r.windows) == 0 {
		return 0, errors.New("write before any deadline")
	}
	r.windows[len(r.windows)-1] += len(b)
	return r.ResponseRecorder.Write(b)
}

// The bridge moves the write deadline before each responseChunkSize bytes of
// a response, however the response is split into parts.
func TestBridge_WriteSetsADeadlinePerChunk(t *testing.T) {
	result := json.RawMessage(`"` + strings.Repeat("x", 2*responseChunkSize-1) + `"`)
	b := newBridge(handler.Map{"big": handler.New(func(context.Context) (any, error) { return result, nil })},
		testMaxBatchSize, unlimited)
	t.Cleanup(b.Close)
	rec := &windowRecorder{ResponseRecorder: httptest.NewRecorder()}

	// The logging middleware wraps the writer, so the deadline has to reach through it.
	b.ServeHTTP(mutil.WrapWriter(rec), newBridgeRequest(t, `{"jsonrpc":"2.0","id":1,"method":"big"}`))

	require.Equal(t, `{"jsonrpc":"2.0","id":1,"result":`+string(result)+"}", rec.Body.String())
	require.Equal(t, []int{responseChunkSize, responseChunkSize, rec.Body.Len() - 2*responseChunkSize}, rec.windows)
}

func newBridgeRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", contentTypeJSON)
	return req
}

// A non-empty json.RawMessage result is written as is, with the Content-Length
// to match.
func TestBridge_RawResultPassthrough(t *testing.T) {
	raw := json.RawMessage("{\n  \"pretty\": \"<kept>\"\n}")
	b := newBridge(handler.Map{"raw": handler.New(func(context.Context) (any, error) { return raw, nil })},
		testMaxBatchSize, unlimited)
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
	})}, testMaxBatchSize, unlimited)
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
	})}, testMaxBatchSize, unlimited)
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

// A request that outlasts the global limit is answered 504 with no body, and
// one past the limit or the warning is counted and logged.
func TestBridge_DurationLimits(t *testing.T) {
	const ok = `{"jsonrpc":"2.0","id":1,"result":1}`
	for _, tc := range []struct {
		name                     string
		warning, limit, run      time.Duration
		wantCode                 int
		wantBody                 string
		wantWarnings, wantLimits int64
	}{
		{"limited", time.Second / 20, time.Second / 10, 10 * time.Second, http.StatusGatewayTimeout, "", 0, 1},
		{"not limited", 5 * time.Second, 10 * time.Second, time.Second / 10, http.StatusOK, ok, 0, 0},
		{"warned", time.Second / 10, 10 * time.Second, time.Second / 5, http.StatusOK, ok, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var warnings, limits counter
			logger, hook := testLogger()
			b := newBridge(handler.Map{"slow": handler.New(func(ctx context.Context) (any, error) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(tc.run):
					return 1, nil
				}
			})}, testMaxBatchSize, durationLimits{tc.warning, tc.limit, &warnings, &limits, logger})
			t.Cleanup(b.Close)
			rec := postBridge(t, b, `{"jsonrpc":"2.0","id":1,"method":"slow"}`)
			require.Equal(t, tc.wantCode, rec.Code)
			require.Equal(t, tc.wantBody, rec.Body.String())
			require.Equal(t, tc.wantWarnings, warnings.n.Load())
			require.Equal(t, tc.wantLimits, limits.n.Load())
			require.Len(t, hook.AllEntries(), int(tc.wantWarnings+tc.wantLimits))
		})
	}
}

// A handler panic answers just that call with -32003 and its stack is logged,
// whether the handler ran on the request's goroutine or on its own.
func TestBridge_Panic(t *testing.T) {
	methods := handler.Map{
		"panic": handler.New(func(context.Context) (any, error) { panic("test panic") }),
		"ok":    handler.New(func(context.Context) (any, error) { return 1, nil }),
	}
	failed := `{"jsonrpc":"2.0","id":1,"error":` +
		`{"code":-32003,"message":"[-32003] request failed to process due to internal issue"}}`
	for name, tc := range map[string]struct{ body, want string }{
		"single": {`{"jsonrpc":"2.0","id":1,"method":"panic"}`, failed},
		"batch element": {
			`[{"jsonrpc":"2.0","id":1,"method":"panic"},{"jsonrpc":"2.0","id":2,"method":"ok"}]`,
			`[` + failed + `,{"jsonrpc":"2.0","id":2,"result":1}]`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			logger, hook := testLogger()
			b := newBridge(methods, testMaxBatchSize,
				durationLimits{limit: network.RequestDurationLimiterNoLimit, logger: logger})
			t.Cleanup(b.Close)
			rec := postBridge(t, b, tc.body)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, tc.want, rec.Body.String())
			entries := hook.AllEntries()
			require.NotEmpty(t, entries)
			require.Equal(t, "test panic when calling panic", entries[0].Message)
			for _, e := range entries {
				require.Equal(t, logrus.WarnLevel, e.Level)
			}
		})
	}
}

// NewBridge serves with no duration limit: a handler's context has no deadline
// and carries the request context's values.
func TestNewBridge_UnboundedRequestContext(t *testing.T) {
	type key struct{}
	b := NewBridge(handler.Map{"m": handler.New(func(hctx context.Context) (any, error) {
		_, hasDeadline := hctx.Deadline()
		return map[string]any{"deadline": hasDeadline, "value": hctx.Value(key{})}, nil
	})}, log.DefaultLogger)
	t.Cleanup(b.Close)
	req := httptest.NewRequestWithContext(context.WithValue(t.Context(), key{}, "planted"), http.MethodPost, "/",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"m"}`))
	req.Header.Set("Content-Type", contentTypeJSON)
	rec := httptest.NewRecorder()
	b.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":{"deadline":false,"value":"planted"}}`, rec.Body.String())
}
