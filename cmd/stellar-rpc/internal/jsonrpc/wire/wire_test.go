package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every `want` in this file is a byte string, not a shape. They were captured
// from jrpc2 v1.3.3's jhttp.Bridge — the framing this package replaces — on
// 2026-08-30, except where a case is annotated with the delta that changes it
// deliberately. The point of pinning bytes rather than structure is that the
// error vocabulary these frames carry (errEmptyMethod, errNoSuchMethod,
// errEmptyBatch, "invalid request value", "invalid version marker", "extra
// fields in request", ...) is unexported in jrpc2 and transcribed here: only an
// exact-byte pin will notice if a library bump or a refactor moves one.

// held is what the handlers that park under the concurrency bound return once
// they are released.
const held = "done"

// testMethods is the table under test. notified counts calls to "note" so a
// notification's completion is observable.
//
//nolint:unparam // every literal here must match the fixed jrpc2.Handler signature
func testMethods(notified *atomic.Int64) map[string]jrpc2.Handler {
	return map[string]jrpc2.Handler{
		"echo": func(_ context.Context, r *jrpc2.Request) (any, error) {
			return map[string]any{"method": r.Method()}, nil
		},
		"note": func(context.Context, *jrpc2.Request) (any, error) {
			notified.Add(1)
			return "noted", nil
		},
		"nilResult": func(context.Context, *jrpc2.Request) (any, error) { return nil, nil },
		"plainError": func(context.Context, *jrpc2.Request) (any, error) {
			return nil, errors.New("something went wrong")
		},
		"jrpcError": func(context.Context, *jrpc2.Request) (any, error) {
			return nil, (&jrpc2.Error{Code: -32123, Message: "custom failure"}).WithData([]string{"a", "b"})
		},
		"wrappedJRPCError": func(context.Context, *jrpc2.Request) (any, error) {
			return nil, fmt.Errorf("while doing the thing: %w", &jrpc2.Error{Code: -32123, Message: "custom failure"})
		},
		"valueJRPCError": func(context.Context, *jrpc2.Request) (any, error) {
			// The repo's own limiter sentinels are jrpc2.Error VALUES; see
			// handlerError's comment for why that changes the wire message.
			return nil, jrpc2.Error{Code: -32001, Message: "request exceeded processing limit threshold"}
		},
		"contextCanceled": func(context.Context, *jrpc2.Request) (any, error) {
			return nil, fmt.Errorf("gave up: %w", context.Canceled)
		},
		"unmarshalable": func(context.Context, *jrpc2.Request) (any, error) {
			return map[string]any{"ch": make(chan int)}, nil
		},
	}
}

// post drives one request through the handler and returns status, headers, body.
func post(t *testing.T, h http.Handler, body string) (int, http.Header, string) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return res.StatusCode, res.Header, string(raw)
}

func newTestHandler(t *testing.T) (*Handler, *atomic.Int64) {
	t.Helper()
	var notified atomic.Int64
	return NewHandler(testMethods(&notified)), &notified
}

func TestWire_SingleRequests(t *testing.T) {
	h, _ := newTestHandler(t)

	for _, tc := range []struct {
		name string
		body string
		want string
	}{{
		name: "number id",
		body: `{"jsonrpc":"2.0","id":1,"method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":1,"result":{"method":"echo"}}`,
	}, {
		name: "string id",
		body: `{"jsonrpc":"2.0","id":"abc","method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":"abc","result":{"method":"echo"}}`,
	}, {
		// The id is echoed as raw bytes, never decoded and re-encoded: a
		// float64 round trip would render this 12345678901234567000.
		name: "integer id beyond float64 precision keeps every digit",
		body: `{"jsonrpc":"2.0","id":12345678901234567890,"method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":12345678901234567890,"result":{"method":"echo"}}`,
	}, {
		name: "fractional id keeps its exact text",
		body: `{"jsonrpc":"2.0","id":1.500,"method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":1.500,"result":{"method":"echo"}}`,
	}, {
		name: "exponent id keeps its exact text",
		body: `{"jsonrpc":"2.0","id":1e2,"method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":1e2,"result":{"method":"echo"}}`,
	}, {
		name: "escapes inside a string id are passed through untouched",
		body: `{"jsonrpc":"2.0","id":"a\"b\nc\u00e9","method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":"a\"b\nc\u00e9","result":{"method":"echo"}}`,
	}, {
		name: "whitespace around the id is not echoed",
		body: `{"jsonrpc":"2.0","id"  :  7  ,"method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":7,"result":{"method":"echo"}}`,
	}, {
		name: "a nil result is null, not omitted",
		body: `{"jsonrpc":"2.0","id":1,"method":"nilResult"}`,
		want: `{"jsonrpc":"2.0","id":1,"result":null}`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			status, header, got := post(t, h, tc.body)
			assert.Equal(t, http.StatusOK, status)
			assert.Equal(t, "application/json", header.Get("Content-Type"))
			assert.Equal(t, strconv.Itoa(len(tc.want)), header.Get("Content-Length"))
			assert.Equal(t, tc.want, got)
		})
	}
}

// The id is client-controlled and reflected. jrpc2 splices it into the response
// unescaped; today's responses are escaped only as a side effect of the
// compaction passes this package deletes. If this test ever fails because
// someone "simplified" appendID to a plain append, the mount has started
// reflecting arbitrary client bytes into a response with no nosniff header.
func TestWire_IDIsHTMLEscapedExactlyAsTheBridgeEscapedIt(t *testing.T) {
	h, _ := newTestHandler(t)

	for _, tc := range []struct{ name, body, want string }{{
		name: "angle brackets and ampersand",
		body: `{"jsonrpc":"2.0","id":"a<b&c>d","method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":"a\u003cb\u0026c\u003ed","result":{"method":"echo"}}`,
	}, {
		name: "U+2028 line separator",
		body: "{\"jsonrpc\":\"2.0\",\"id\":\"a\u2028b\",\"method\":\"echo\"}",
		want: `{"jsonrpc":"2.0","id":"a\u2028b","result":{"method":"echo"}}`,
	}, {
		name: "U+2029 paragraph separator",
		body: "{\"jsonrpc\":\"2.0\",\"id\":\"a\u2029b\",\"method\":\"echo\"}",
		want: `{"jsonrpc":"2.0","id":"a\u2029b","result":{"method":"echo"}}`,
	}, {
		name: "a script tag cannot escape the id",
		body: `{"jsonrpc":"2.0","id":"</script><script>alert(1)</script>","method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":"\u003c/script\u003e\u003cscript\u003e` +
			`alert(1)\u003c/script\u003e","result":{"method":"echo"}}`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			status, header, got := post(t, h, tc.body)
			assert.Equal(t, http.StatusOK, status)
			assert.Equal(t, tc.want, got)
			// The capacity hint must account for the escape, or the frame is
			// built with a reallocation.
			assert.Equal(t, strconv.Itoa(len(tc.want)), header.Get("Content-Length"))
		})
	}
}

func TestWire_ErrorMapping(t *testing.T) {
	h, _ := newTestHandler(t)

	for _, tc := range []struct{ name, body, want string }{{
		name: "a *jrpc2.Error passes through verbatim, data included",
		body: `{"jsonrpc":"2.0","id":1,"method":"jrpcError"}`,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32123,"message":"custom failure","data":["a","b"]}}`,
	}, {
		// jrpc2 asserts, it does not unwrap: a wrapped *Error keeps its code
		// but takes the wrapper's message. Pinned because it looks like a bug.
		name: "a WRAPPED *jrpc2.Error keeps the code and takes the wrapper message",
		body: `{"jsonrpc":"2.0","id":1,"method":"wrappedJRPCError"}`,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32123,` +
			`"message":"while doing the thing: [-32123] custom failure"}}`,
	}, {
		// The repo's limiter sentinels take this path; the doubled "[-32001] "
		// is today's wire text and is reproduced, not fixed, in this commit.
		name: "a jrpc2.Error VALUE keeps its code and doubles its code into the message",
		body: `{"jsonrpc":"2.0","id":1,"method":"valueJRPCError"}`,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32001,` +
			`"message":"[-32001] request exceeded processing limit threshold"}}`,
	}, {
		name: "a plain error becomes SystemError",
		body: `{"jsonrpc":"2.0","id":1,"method":"plainError"}`,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32098,"message":"something went wrong"}}`,
	}, {
		name: "a wrapped context.Canceled maps to jrpc2's -32097",
		body: `{"jsonrpc":"2.0","id":1,"method":"contextCanceled"}`,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32097,"message":"gave up: context canceled"}}`,
	}, {
		name: "an unmarshalable result becomes an internal error, not a truncated body",
		body: `{"jsonrpc":"2.0","id":1,"method":"unmarshalable"}`,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,` +
			`"message":"json: unsupported type: chan int"}}`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, got := post(t, h, tc.body)
			assert.Equal(t, http.StatusOK, status)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestWire_UnknownMethod(t *testing.T) {
	h, _ := newTestHandler(t)

	t.Run("a call gets jrpc2's own method-not-found frame", func(t *testing.T) {
		status, _, got := post(t, h, `{"jsonrpc":"2.0","id":1,"method":"noSuchMethod"}`)
		assert.Equal(t, http.StatusOK, status)
		//nolint:testifylint // byte-exact wire pin; JSONEq would ignore key order and escaping
		assert.Equal(t,
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found","data":"noSuchMethod"}}`,
			got)
	})

	t.Run("a notification is silent", func(t *testing.T) {
		status, _, got := post(t, h, `{"jsonrpc":"2.0","method":"noSuchMethod"}`)
		assert.Equal(t, http.StatusNoContent, status)
		assert.Empty(t, got)
	})
}

func TestWire_RequestLevelProtocolErrors(t *testing.T) {
	h, _ := newTestHandler(t)

	for _, tc := range []struct {
		name   string
		body   string
		status int
		want   string
	}{{
		// DELTA (a): the bridge answers 500 + the plaintext
		// "[-32700] invalid request value". This is the spec's answer.
		name:   "a malformed body is a -32700 frame over HTTP 200",
		body:   `{`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"invalid request value"}}`,
	}, {
		name:   "a body that is not JSON at all is the same -32700 frame",
		body:   `not json`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"invalid request value"}}`,
	}, {
		name:   "an empty body is the same -32700 frame",
		body:   `   `,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"invalid request value"}}`,
	}, {
		// DELTA (b): the bridge answers `[]` with 204.
		name:   "an empty batch is a single -32600 frame, not an array and not a 204",
		body:   `[]`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"empty request batch"}}`,
	}, {
		name:   "an empty method name on a call is -32600",
		body:   `{"jsonrpc":"2.0","id":1,"method":""}`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"empty method name"}}`,
	}, {
		// DELTA (e): the bridge answers 204 here, and only by accident — its
		// in-process client discards the server's frame because the id is
		// empty. An empty method name is a request-level error and is
		// reportable even for a notification.
		name:   "an empty method name on a NOTIFICATION is a -32600 frame with a null id",
		body:   `{"jsonrpc":"2.0","method":""}`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"empty method name"}}`,
	}, {
		// Parity, not a delta: a response-shaped message has no method, so it
		// degrades to the empty-method error rather than being "understood".
		name:   "a response-shaped message with a version marker degrades to empty method",
		body:   `{"jsonrpc":"2.0","id":1,"result":5}`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"empty method name"}}`,
	}, {
		name:   "a response-shaped message with no version marker fails on the version",
		body:   `{"result":5}`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid version marker"}}`,
	}, {
		name:   "a wrong version marker is -32600 and still echoes the id",
		body:   `{"jsonrpc":"1.0","id":2,"method":"echo"}`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":2,"error":{"code":-32600,"message":"invalid version marker"}}`,
	}, {
		// Parity: a parse/shape error is reported even for a notification.
		// This is what the bridge does today and what the spec requires.
		name:   "a parse error on a notification still answers",
		body:   `{"jsonrpc":"1.0","method":"echo"}`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid version marker"}}`,
	}, {
		name:   "extra fields are rejected and named",
		body:   `{"jsonrpc":"2.0","id":1,"method":"echo","bogus":1}`,
		status: http.StatusOK,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32600,` +
			`"message":"extra fields in request","data":["bogus"]}}`,
	}, {
		name:   "params must be an array or an object",
		body:   `{"jsonrpc":"2.0","id":1,"method":"echo","params":5}`,
		status: http.StatusOK,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32600,` +
			`"message":"parameters must be array or object"}}`,
	}, {
		name:   "an unusable id answers with a null id",
		body:   `{"jsonrpc":"2.0","id":{"a":1},"method":"echo"}`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid request ID"}}`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, got := post(t, h, tc.body)
			assert.Equal(t, tc.status, status)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestWire_Notifications(t *testing.T) {
	t.Run("an absent id is a notification and runs to completion before the 204", func(t *testing.T) {
		h, notified := newTestHandler(t)
		status, _, got := post(t, h, `{"jsonrpc":"2.0","method":"note"}`)
		assert.Equal(t, http.StatusNoContent, status)
		assert.Empty(t, got)
		assert.Equal(t, int64(1), notified.Load(),
			"DELTA (c): the work must be finished when the 204 is written")
	})

	t.Run("an explicit null id is a notification too", func(t *testing.T) {
		h, notified := newTestHandler(t)
		status, _, got := post(t, h, `{"jsonrpc":"2.0","id":null,"method":"note"}`)
		assert.Equal(t, http.StatusNoContent, status)
		assert.Empty(t, got)
		assert.Equal(t, int64(1), notified.Load())
	})

	t.Run("an all-notification batch 204s after every element has run", func(t *testing.T) {
		h, notified := newTestHandler(t)
		status, _, got := post(t, h,
			`[{"jsonrpc":"2.0","method":"note"},{"jsonrpc":"2.0","method":"note"},{"jsonrpc":"2.0","method":"note"}]`)
		assert.Equal(t, http.StatusNoContent, status)
		assert.Empty(t, got)
		assert.Equal(t, int64(3), notified.Load())
	})

	t.Run("a notification inside a batch produces no entry but still runs", func(t *testing.T) {
		h, notified := newTestHandler(t)
		status, _, got := post(t, h,
			`[{"jsonrpc":"2.0","method":"note"},{"jsonrpc":"2.0","id":9,"method":"echo"}]`)
		assert.Equal(t, http.StatusOK, status)
		//nolint:testifylint // byte-exact wire pin; JSONEq would ignore key order and escaping
		assert.Equal(t, `[{"jsonrpc":"2.0","id":9,"result":{"method":"echo"}}]`, got)
		assert.Equal(t, int64(1), notified.Load())
	})

	t.Run("a notification whose handler fails is silent", func(t *testing.T) {
		h, _ := newTestHandler(t)
		status, _, got := post(t, h, `{"jsonrpc":"2.0","method":"plainError"}`)
		assert.Equal(t, http.StatusNoContent, status)
		assert.Empty(t, got)
	})
}

func TestWire_Batches(t *testing.T) {
	h, _ := newTestHandler(t)

	for _, tc := range []struct{ name, body, want string }{{
		name: "a one-element batch stays an array",
		body: `[{"jsonrpc":"2.0","id":1,"method":"echo"}]`,
		want: `[{"jsonrpc":"2.0","id":1,"result":{"method":"echo"}}]`,
	}, {
		// DELTA (f): the bridge answers this
		// [invalid(id 2), valid(id 1), valid(id 3)].
		name: "invalid elements answer in place, not first",
		body: `[{"jsonrpc":"2.0","id":1,"method":"echo"},` +
			`{"jsonrpc":"1.0","id":2,"method":"echo"},` +
			`{"jsonrpc":"2.0","id":3,"method":"echo"}]`,
		want: `[{"jsonrpc":"2.0","id":1,"result":{"method":"echo"}},` +
			`{"jsonrpc":"2.0","id":2,"error":{"code":-32600,"message":"invalid version marker"}},` +
			`{"jsonrpc":"2.0","id":3,"result":{"method":"echo"}}]`,
	}, {
		name: "a non-object element answers in place with a null id",
		body: `[{"jsonrpc":"2.0","id":1,"method":"echo"},5]`,
		want: `[{"jsonrpc":"2.0","id":1,"result":{"method":"echo"}},` +
			`{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"request is not a JSON object"}}]`,
	}, {
		// Parity: the bridge virtualizes ids, so jrpc2's duplicate-id check is
		// unreachable today. Owning the framing must not make it reachable.
		name: "duplicate ids inside one batch are allowed",
		body: `[{"jsonrpc":"2.0","id":1,"method":"echo"},{"jsonrpc":"2.0","id":1,"method":"nilResult"}]`,
		want: `[{"jsonrpc":"2.0","id":1,"result":{"method":"echo"}},` +
			`{"jsonrpc":"2.0","id":1,"result":null}]`,
	}, {
		name: "mixed methods, errors and unknown names all answer in order",
		body: `[{"jsonrpc":"2.0","id":1,"method":"echo"},` +
			`{"jsonrpc":"2.0","id":2,"method":"nope"},` +
			`{"jsonrpc":"2.0","id":3,"method":"plainError"}]`,
		want: `[{"jsonrpc":"2.0","id":1,"result":{"method":"echo"}},` +
			`{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"method not found","data":"nope"}},` +
			`{"jsonrpc":"2.0","id":3,"error":{"code":-32098,"message":"something went wrong"}}]`,
	}, {
		name: "a nested array element is a parse error, still wrapped as a batch",
		body: `[[]]`,
		want: `[{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"request is not a JSON object"}}]`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			status, header, got := post(t, h, tc.body)
			assert.Equal(t, http.StatusOK, status)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, strconv.Itoa(len(tc.want)), header.Get("Content-Length"))
		})
	}
}

func TestWire_HTTPShell(t *testing.T) {
	h, _ := newTestHandler(t)

	do := func(method, contentType, body string) *http.Response {
		req := httptest.NewRequestWithContext(t.Context(), method, "/", strings.NewReader(body))
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Result()
	}

	t.Run("Accept-Post is advertised on a successful POST too", func(t *testing.T) {
		res := do(http.MethodPost, "application/json", `{"jsonrpc":"2.0","id":1,"method":"echo"}`)
		defer res.Body.Close()
		assert.Equal(t, http.StatusOK, res.StatusCode)
		assert.Equal(t, "application/json", res.Header.Get("Accept-Post"))
	})

	t.Run("GET is 405 with Accept-Post and no body", func(t *testing.T) {
		res := do(http.MethodGet, "", "")
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		assert.Equal(t, http.StatusMethodNotAllowed, res.StatusCode)
		assert.Equal(t, "application/json", res.Header.Get("Accept-Post"))
		assert.Empty(t, body)
	})

	for _, tc := range []struct {
		name, contentType, want string
	}{
		{"no content type", "", "content-type must be application/json\n"},
		{"wrong media type", "text/plain", "content-type must be application/json\n"},
		{"wrong charset", "application/json; charset=iso-8859-1", "invalid content-type charset\n"},
	} {
		t.Run(tc.name+" is 415", func(t *testing.T) {
			res := do(http.MethodPost, tc.contentType, `{"jsonrpc":"2.0","id":1,"method":"echo"}`)
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			assert.Equal(t, http.StatusUnsupportedMediaType, res.StatusCode)
			assert.Equal(t, tc.want, string(body))
		})
	}

	t.Run("an explicit utf-8 charset is accepted", func(t *testing.T) {
		res := do(http.MethodPost, "application/json; charset=utf-8", `{"jsonrpc":"2.0","id":1,"method":"echo"}`)
		defer res.Body.Close()
		assert.Equal(t, http.StatusOK, res.StatusCode)
	})

	t.Run("an unreadable body is 500 plus plaintext, not a JSON-RPC frame", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", errReader{})
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
		assert.Equal(t, "connection reset\n", string(body))
	})
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("connection reset") }

// A fat response must survive the envelope byte for byte. There is no way to
// assert "exactly one copy" from a test, so this asserts the two things that
// would break if the copy count changed for the wrong reason: the body still
// parses, and its result is byte-identical to an independent json.Marshal of
// the same value (so nothing re-compacted, re-escaped, or truncated it).
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_BigResponseIntegrity(t *testing.T) {
	const payloadBytes = 4 << 20
	payload := strings.Repeat("abcdefgh", payloadBytes/8)
	value := map[string]any{"blob": payload, "n": 12345}

	h := NewHandler(map[string]jrpc2.Handler{
		"fat": func(context.Context, *jrpc2.Request) (any, error) { return value, nil },
	})

	status, header, got := post(t, h, `{"jsonrpc":"2.0","id":"big","method":"fat"}`)
	require.Equal(t, http.StatusOK, status)

	oracle, err := json.Marshal(value)
	require.NoError(t, err)
	want := `{"jsonrpc":"2.0","id":"big","result":` + string(oracle) + `}`

	//nolint:testifylint // compare lengths, not values: assert.Len would dump megabytes on failure
	assert.Equal(t, len(want), len(got), "the framed body must be exactly the envelope plus the marshaled value")
	assert.Equal(t, want, got)
	assert.Equal(t, strconv.Itoa(len(want)), header.Get("Content-Length"))

	var envelope struct {
		Version string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(got), &envelope))
	assert.Equal(t, "2.0", envelope.Version)
	assert.JSONEq(t, `"big"`, string(envelope.ID))
	assert.Equal(t, oracle, []byte(envelope.Result))
}

// The concurrency bound is the design's own #1 hazard: without it, the number
// of fat marshals in flight is the HTTP backlog limit (5000), not the core
// count. jrpc2's Server held a semaphore of runtime.NumCPU() across handler and
// result marshal; this reproduces it, and this test fails if it is removed.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_SemaphoreBoundsConcurrentDispatch(t *testing.T) {
	limit := runtime.NumCPU()
	entered := make(chan struct{}, 4*limit)
	release := make(chan struct{})

	var inflight, peak atomic.Int64
	h := NewHandler(map[string]jrpc2.Handler{
		"hold": func(context.Context, *jrpc2.Request) (any, error) {
			now := inflight.Add(1)
			for {
				old := peak.Load()
				if now <= old || peak.CompareAndSwap(old, now) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			inflight.Add(-1)
			return held, nil
		},
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	callers := 2 * limit
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL,
				strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"hold"}`))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}()
	}

	// Wait for the bound to be saturated, then give the unbounded case ample
	// room to exceed it before looking.
	for range limit {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			close(release)
			wg.Wait()
			t.Fatalf("only %d of %d handlers started", peak.Load(), limit)
		}
	}
	time.Sleep(250 * time.Millisecond)
	observed := peak.Load()
	close(release)
	wg.Wait()

	assert.LessOrEqual(t, observed, int64(limit),
		"more than runtime.NumCPU() handlers ran at once: the semaphore is gone or its weight is wrong")
	assert.Equal(t, int64(limit), observed,
		"the bound was never reached, so this test would not notice its removal")
}

// The permit must be gone before any byte is handed to the ResponseWriter, or a
// slow reader holds a slot in a bound sized for CPUs. Proved structurally: the
// handler that writes is the one that has already released, so a request whose
// write blocks cannot stop the next request's handler from starting.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_SemaphoreIsReleasedBeforeTheWrite(t *testing.T) {
	started := make(chan struct{}, 1)
	h := NewHandler(map[string]jrpc2.Handler{
		"echo": func(context.Context, *jrpc2.Request) (any, error) {
			started <- struct{}{}
			return "ok", nil
		},
	})

	// Saturate the bound with writers that never drain, then prove a fresh
	// request still reaches its handler.
	blocked := make(chan struct{})
	var wg sync.WaitGroup
	for range runtime.NumCPU() {
		wg.Go(func() {
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/",
				strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"echo"}`))
			req.Header.Set("Content-Type", "application/json")
			h.ServeHTTP(blockingWriter{blocked: blocked}, req)
		})
	}
	for range runtime.NumCPU() {
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			close(blocked)
			wg.Wait()
			t.Fatal("the saturating requests never reached their handlers")
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		status, _, body := post(t, h, `{"jsonrpc":"2.0","id":2,"method":"echo"}`)
		assert.Equal(t, http.StatusOK, status)
		//nolint:testifylint // byte-exact wire pin; JSONEq would ignore key order and escaping
		assert.Equal(t, `{"jsonrpc":"2.0","id":2,"result":"ok"}`, body)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Error("a request could not be served while NumCPU writers were stalled: " +
			"the semaphore is being held across the write")
	}
	<-started
	close(blocked)
	wg.Wait()
}

// blockingWriter is a ResponseWriter whose Write never returns until blocked is
// closed, standing in for a client that stopped reading.
type blockingWriter struct {
	blocked chan struct{}
	header  http.Header
}

func (b blockingWriter) Header() http.Header {
	if b.header == nil {
		return http.Header{}
	}
	return b.header
}
func (b blockingWriter) Write(p []byte) (int, error) { <-b.blocked; return len(p), nil }
func (blockingWriter) WriteHeader(int)               {}

func TestAppendHTMLEscaped(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`"plain"`, `"plain"`},
		{`123`, `123`},
		{`"a<b"`, `"a\u003cb"`},
		{`"a>b"`, `"a\u003eb"`},
		{`"a&b"`, `"a\u0026b"`},
		{`"<>&"`, `"\u003c\u003e\u0026"`},
		{"\"\u2028\"", `"\u2028"`},
		{"\"\u2029\"", `"\u2029"`},
		{`"already \u003c escaped"`, `"already \u003c escaped"`},
	} {
		got := string(appendHTMLEscaped(nil, tc.in))
		assert.Equal(t, tc.want, got, "input %q", tc.in)
		assert.Equal(t, len(tc.want), idLen(tc.in), "idLen must predict the escaped length of %q", tc.in)
	}
	assert.Equal(t, len("null"), idLen(""))
}

// batchOf builds a batch body of n calls to method, with ids 1..n.
func batchOf(n int, method string) string {
	var b strings.Builder
	b.WriteByte('[')
	for i := 1; i <= n; i++ {
		if i > 1 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"jsonrpc":"2.0","id":%d,"method":%q}`, i, method)
	}
	b.WriteByte(']')
	return b.String()
}

// serveAsync runs one body through h on its own goroutine and returns the
// recorder plus a channel closed when ServeHTTP has returned. It exists so a
// test can look at a batch while it is still in flight; post() cannot, and its
// require calls would be on the wrong goroutine anyway.
func serveAsync(t *testing.T, h http.Handler, body string) (*httptest.ResponseRecorder, <-chan struct{}) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(rec, req)
	}()
	return rec, done
}

// Batch elements run concurrently, as jrpc2's dispatchLocked ran them. Serial
// dispatch sums a batch's latencies under the one HTTP deadline the mount
// enforces, so a batch of individually-fine calls 504s where each of them alone
// succeeds — the regression this test exists to catch.
//
// The proof is a barrier, not a stopwatch: every element must be inside its
// handler before any of them is allowed to return. Serial dispatch delivers
// exactly one.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_BatchElementsRunConcurrently(t *testing.T) {
	n := min(runtime.NumCPU(), 8)
	if n < 2 {
		t.Skip("a one-permit bound cannot show concurrency")
	}
	entered := make(chan struct{}, n)
	release := make(chan struct{})

	h := NewHandler(map[string]jrpc2.Handler{
		"hold": func(context.Context, *jrpc2.Request) (any, error) {
			entered <- struct{}{}
			<-release
			return held, nil
		},
	})
	rec, done := serveAsync(t, h, batchOf(n, "hold"))

	for k := range n {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			close(release)
			<-done
			t.Fatalf("only %d of %d batch elements were in flight at once: batch dispatch is serial", k, n)
		}
	}
	close(release)
	<-done

	var want strings.Builder
	want.WriteByte('[')
	for i := 1; i <= n; i++ {
		if i > 1 {
			want.WriteByte(',')
		}
		fmt.Fprintf(&want, `{"jsonrpc":"2.0","id":%d,"result":%q}`, i, held)
	}
	want.WriteByte(']')

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, want.String(), rec.Body.String())
	assert.Equal(t, strconv.Itoa(want.Len()), rec.Header().Get("Content-Length"))
}

// The stopwatch form of the test above, because "the batch took N times one
// call" is the user-visible symptom and is worth asserting in its own units.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_ABatchOfSlowCallsTakesAboutOneCallsTime(t *testing.T) {
	n := min(runtime.NumCPU(), 4)
	if n < 4 {
		t.Skip("needs at least 4 permits for the serial and concurrent times to separate")
	}
	const delay = 200 * time.Millisecond

	h := NewHandler(map[string]jrpc2.Handler{
		"slow": func(context.Context, *jrpc2.Request) (any, error) {
			time.Sleep(delay)
			return held, nil
		},
	})

	start := time.Now()
	status, _, _ := post(t, h, batchOf(n, "slow"))
	elapsed := time.Since(start)

	assert.Equal(t, http.StatusOK, status)
	assert.Less(t, elapsed, time.Duration(n)*delay/2,
		"a batch of %d %v calls took %v; serial dispatch would need %v", n, delay, elapsed, time.Duration(n)*delay)
}

// A batch borrows from the process-wide bound; it does not multiply it. Two
// separate things are pinned, because they fail separately:
//
//  1. no more than runtime.NumCPU() of a batch's handlers run at once, and
//  2. no more than that many GOROUTINES exist for them, which is only true
//     because the permit is taken on the serving goroutine BEFORE the worker
//     is started. Acquire inside the worker instead and this batch parks one
//     goroutine per element — which the 512KB body cap bounds at ~260,000.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_ABatchDoesNotMultiplyTheConcurrencyBound(t *testing.T) {
	limit := runtime.NumCPU()
	if limit < 2 {
		t.Skip("a one-permit bound cannot separate the two failure modes")
	}
	const oversubscribe = 16
	elements := oversubscribe * limit

	entered := make(chan struct{}, elements)
	release := make(chan struct{})
	var inflight, peak atomic.Int64

	h := NewHandler(map[string]jrpc2.Handler{
		"hold": func(context.Context, *jrpc2.Request) (any, error) {
			now := inflight.Add(1)
			for {
				old := peak.Load()
				if now <= old || peak.CompareAndSwap(old, now) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			inflight.Add(-1)
			return held, nil
		},
	})

	runtime.GC() // settle the goroutine count before sampling it
	before := runtime.NumGoroutine()
	rec, done := serveAsync(t, h, batchOf(elements, "hold"))

	for k := range limit {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			close(release)
			<-done
			t.Fatalf("only %d of %d permits were taken", k, limit)
		}
	}
	// Saturated: give an unbounded implementation ample room to overshoot.
	time.Sleep(250 * time.Millisecond)
	observedPeak := peak.Load()
	observedGoroutines := runtime.NumGoroutine()
	close(release)
	<-done

	assert.Equal(t, int64(limit), observedPeak,
		"a batch of %d elements ran %d handlers at once against a bound of %d", elements, observedPeak, limit)
	// The serving goroutine, this test's goroutine and the runtime's own make
	// up the slack; what must not appear is one goroutine per element.
	assert.Less(t, observedGoroutines, before+limit+16,
		"a batch of %d elements parked %d goroutines: the permit is being acquired inside the worker",
		elements, observedGoroutines-before)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// Input order (DELTA (f)) and notification compaction are properties of the
// response, not of the completion order. This batch finishes back to front.
//
//nolint:unparam // the handler literals must match the fixed jrpc2.Handler signature
func TestWire_BatchOrderAndCompactionSurviveOutOfOrderCompletion(t *testing.T) {
	var notified atomic.Int64
	h := NewHandler(map[string]jrpc2.Handler{
		"slowest": func(context.Context, *jrpc2.Request) (any, error) {
			time.Sleep(120 * time.Millisecond)
			return "slowest", nil
		},
		"slower": func(context.Context, *jrpc2.Request) (any, error) {
			time.Sleep(60 * time.Millisecond)
			return "slower", nil
		},
		"note": func(context.Context, *jrpc2.Request) (any, error) {
			time.Sleep(30 * time.Millisecond)
			notified.Add(1)
			return "noted", nil
		},
		"fast": func(context.Context, *jrpc2.Request) (any, error) { return "fast", nil },
	})

	status, header, got := post(t, h, `[{"jsonrpc":"2.0","id":1,"method":"slowest"},`+
		`{"jsonrpc":"2.0","method":"note"},`+
		`{"jsonrpc":"2.0","id":2,"method":"slower"},`+
		`{"jsonrpc":"1.0","id":3,"method":"fast"},`+
		`{"jsonrpc":"2.0","id":4,"method":"fast"},`+
		`{"jsonrpc":"2.0","id":4,"method":"nope"}]`)

	want := `[{"jsonrpc":"2.0","id":1,"result":"slowest"},` +
		`{"jsonrpc":"2.0","id":2,"result":"slower"},` +
		`{"jsonrpc":"2.0","id":3,"error":{"code":-32600,"message":"invalid version marker"}},` +
		`{"jsonrpc":"2.0","id":4,"result":"fast"},` +
		`{"jsonrpc":"2.0","id":4,"error":{"code":-32601,"message":"method not found","data":"nope"}}]`

	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, want, got)
	assert.Equal(t, strconv.Itoa(len(want)), header.Get("Content-Length"))
	assert.Equal(t, int64(1), notified.Load(), "the notification must have run before the response was written")
}

// jrpc2 never recovered a handler panic: under the bridge it unwound on a
// goroutine the server owned, and took the process with it. Here a panic
// unwinds out of ServeHTTP into httpRequestDurationLimiter's recover, which
// answers 500 — so a batch worker's panic must be RE-RAISED on the serving
// goroutine rather than escaping its own, and the permit it held must be gone
// either way.
//
//nolint:unparam // the handler literals must match the fixed jrpc2.Handler signature
func TestWire_APanicInABatchElementFailsTheRequestAndReleasesItsPermit(t *testing.T) {
	h := NewHandler(map[string]jrpc2.Handler{
		"boom": func(context.Context, *jrpc2.Request) (any, error) { panic("handler exploded") },
		"echo": func(_ context.Context, r *jrpc2.Request) (any, error) {
			return map[string]any{"method": r.Method()}, nil
		},
	})

	serve := func(body string) any {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			h.ServeHTTP(httptest.NewRecorder(), req)
		}()
		return recovered
	}

	t.Run("a single request's panic propagates unchanged", func(t *testing.T) {
		assert.Equal(t, "handler exploded", serve(`{"jsonrpc":"2.0","id":1,"method":"boom"}`))
	})

	t.Run("a batch element's panic is re-raised with its own stack", func(t *testing.T) {
		got := serve(`[{"jsonrpc":"2.0","id":1,"method":"echo"},{"jsonrpc":"2.0","id":2,"method":"boom"}]`)
		require.NotNil(t, got, "the batch must fail the request, not swallow the panic")
		rendered := fmt.Sprintf("%v", got)
		assert.Contains(t, rendered, "handler exploded")
		assert.Contains(t, rendered, "panicked on a batch worker goroutine")
		assert.Contains(t, rendered, "wire.TestWire_APanicInABatchElementFailsTheRequestAndReleasesItsPermit",
			"the relayed value must carry the stack the handler panicked on, not the relay's")
	})

	// Enough panics to exhaust the bound if any of them leaked its permit.
	for range runtime.NumCPU() + 1 {
		serve(`[{"jsonrpc":"2.0","id":1,"method":"boom"},{"jsonrpc":"2.0","id":2,"method":"boom"}]`)
	}

	served := make(chan string, 1)
	go func() {
		_, _, body := post(t, h, `{"jsonrpc":"2.0","id":9,"method":"echo"}`)
		served <- body
	}()
	select {
	case body := <-served:
		//nolint:testifylint // byte-exact wire pin; JSONEq would ignore key order and escaping
		assert.Equal(t, `{"jsonrpc":"2.0","id":9,"result":{"method":"echo"}}`, body)
	case <-time.After(10 * time.Second):
		t.Error("the handler stopped serving after panicking batches: every panic leaked its permit")
	}
}

// A body that is valid JSON but is not a request object or an array of them.
// jrpc2 parses it as a single element whose validation failed, so it answers
// with one bare frame — never an array — which is what the bridge did too.
func TestWire_ValidJSONThatIsNotARequest(t *testing.T) {
	h, _ := newTestHandler(t)

	for _, tc := range []struct{ name, body, want string }{{
		name: "a bare number",
		body: `5`,
		want: `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"request is not a JSON object"}}`,
	}, {
		name: "a bare string",
		body: `"hello"`,
		want: `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"request is not a JSON object"}}`,
	}, {
		name: "a bare boolean",
		body: `true`,
		want: `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"request is not a JSON object"}}`,
	}, {
		// null decodes into a nil map rather than failing, so it gets as far as
		// the version check. Pinned because it is the one scalar that does not
		// answer -32700.
		name: "a bare null",
		body: `null`,
		want: `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid version marker"}}`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			status, header, got := post(t, h, tc.body)
			assert.Equal(t, http.StatusOK, status)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, strconv.Itoa(len(tc.want)), header.Get("Content-Length"))
		})
	}
}

// Handlers used to run on a context jrpc2's Server built, carrying the inbound
// request and the server itself. They now run on a context.Background-derived
// one carrying neither, so jrpc2.InboundRequest returns nil and
// jrpc2.ServerFromContext PANICS. Neither value is fabricated here (see the
// package doc), which is only safe while nothing calls them — so this test
// fails the moment production code does.
func TestNoJRPC2ContextValueCallersInProductionCode(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	require.NoError(t, err)
	require.Equal(t, "cmd", filepath.Base(root), "this test walks cmd/; the package moved")

	banned := map[string]bool{"InboundRequest": true, "ServerFromContext": true}
	fset := token.NewFileSet()
	var callers []string

	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}
		// Selector expressions only: the package doc names both accessors, and
		// a comment is not a caller.
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || !banned[sel.Sel.Name] {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "jrpc2" {
				callers = append(callers, fmt.Sprintf("%s: jrpc2.%s", fset.Position(sel.Pos()), sel.Sel.Name))
			}
			return true
		})
		return nil
	}))

	assert.Empty(t, callers, "wire runs handlers on a context with none of jrpc2's server values: "+
		"InboundRequest returns nil and ServerFromContext panics. Take the request from the handler's "+
		"second argument instead")
}
