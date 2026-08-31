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

// Every `want` is a byte string captured from jrpc2 v1.3.3's jhttp.Bridge,
// except where annotated with a delta. Only an exact-byte pin notices a bump
// moving one of wire.go's transcribed error strings.

// held is what the parked handlers return once released.
const held = "done"

// recordPeak raises peak to now.
func recordPeak(peak *atomic.Int64, now int64) {
	for {
		old := peak.Load()
		if now <= old || peak.CompareAndSwap(old, now) {
			return
		}
	}
}

// testMethods is the table under test; notified observes a notification.
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
			// The limiter sentinels are jrpc2.Error VALUES; see handlerError.
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
	return NewHandler(testMethods(&notified), nil), &notified
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
		// A float64 round trip would render this 12345678901234567000.
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
		// The only isValidID branch no other case reaches.
		name: "a negative id keeps its sign",
		body: `{"jsonrpc":"2.0","id":-1,"method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":-1,"result":{"method":"echo"}}`,
	}, {
		name: "escapes inside a string id are passed through untouched",
		body: `{"jsonrpc":"2.0","id":"a\"b\nc\u00e9","method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":"a\"b\nc\u00e9","result":{"method":"echo"}}`,
	}, {
		name: "whitespace around the id is not echoed",
		body: `{"jsonrpc":"2.0","id"  :  7  ,"method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":7,"result":{"method":"echo"}}`,
	}, {
		// encoding/json's object decode keeps the last value for a key.
		name: "a repeated id key echoes the last one",
		body: `{"jsonrpc":"2.0","id":1,"id":2,"method":"echo"}`,
		want: `{"jsonrpc":"2.0","id":2,"result":{"method":"echo"}}`,
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

// If this fails because appendID was "simplified" to a plain append, the mount
// is reflecting arbitrary client bytes into a response with no nosniff header.
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
		// Every byte but the five is spliced raw, invalid UTF-8 included:
		// the boundary of the guarantee, not an oversight in it.
		name: "bytes that are not valid UTF-8 are spliced raw, as the bridge spliced them",
		body: "{\"jsonrpc\":\"2.0\",\"id\":\"a\xffb\",\"method\":\"echo\"}",
		want: "{\"jsonrpc\":\"2.0\",\"id\":\"a\xffb\",\"result\":{\"method\":\"echo\"}}",
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
			assert.Equal(t, strconv.Itoa(len(tc.want)), header.Get("Content-Length"))
		})
	}
}

// The frame's capacity must be its EXACT length: no assertion on the response
// can see that, but cap > len means the payload was re-copied.
func TestFrameIsExactlyOneAllocation(t *testing.T) {
	payload := []byte(`{"a":[1,2,3]}`)
	for _, id := range []string{
		``, `1`, `12345678901234567890`, `-1`, `1.500`,
		`"plain"`, `"a<b&c>d"`, `"</script>"`, "\"a\u2028b\"", "\"a\u2029b\"",
		`"` + strings.Repeat("<", 64*1024) + `"`,
		`"` + strings.Repeat("\u2028", 16*1024) + `"`,
	} {
		for _, key := range []string{resultKey, errorKey} {
			f := frame(id, key, payload)
			assert.Equal(t, len(f), cap(f),
				"frame(%.20q, %q, ...) reallocated: idLen under-predicts by %d bytes", id, key, cap(f)-len(f))
		}
	}
}

func TestWire_ErrorMapping(t *testing.T) {
	h, _ := newTestHandler(t)

	for _, tc := range []struct{ name, body, want string }{{
		name: "a *jrpc2.Error passes through verbatim, data included",
		body: `{"jsonrpc":"2.0","id":1,"method":"jrpcError"}`,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32123,"message":"custom failure","data":["a","b"]}}`,
	}, {
		// jrpc2 asserts, it does not unwrap. Pinned: it looks like a bug.
		name: "a WRAPPED *jrpc2.Error keeps the code and takes the wrapper message",
		body: `{"jsonrpc":"2.0","id":1,"method":"wrappedJRPCError"}`,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32123,` +
			`"message":"while doing the thing: [-32123] custom failure"}}`,
	}, {
		// The doubled "[-32001] " is reproduced, not fixed.
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
		// -32098, not -32603: a marshal failure takes the handler ladder.
		name: "a result that cannot be marshaled is a system error, not a truncated body",
		body: `{"jsonrpc":"2.0","id":1,"method":"unmarshalable"}`,
		want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32098,` +
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
		// DELTA (a).
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
		name:   "trailing bytes after a complete request are a parse error",
		body:   `{"jsonrpc":"2.0","id":1,"method":"echo"} trailing`,
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
		// DELTA (d). Reportable even for a notification.
		name:   "an empty method name on a NOTIFICATION is a -32600 frame with a null id",
		body:   `{"jsonrpc":"2.0","method":""}`,
		status: http.StatusOK,
		want:   `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"empty method name"}}`,
	}, {
		// No method, so it degrades rather than being "understood".
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
		// A parse/shape error is reported even for a notification.
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
		// DELTA (e).
		name: "invalid elements answer in place, not first",
		body: `[{"jsonrpc":"2.0","id":1,"method":"echo"},` +
			`{"jsonrpc":"1.0","id":2,"method":"echo"},` +
			`{"jsonrpc":"2.0","id":3,"method":"echo"}]`,
		want: `[{"jsonrpc":"2.0","id":1,"result":{"method":"echo"}},` +
			`{"jsonrpc":"2.0","id":2,"error":{"code":-32600,"message":"invalid version marker"}},` +
			`{"jsonrpc":"2.0","id":3,"result":{"method":"echo"}}]`,
	}, {
		// DELTA (e) again.
		name: "a non-object element answers in place with a null id",
		body: `[{"jsonrpc":"2.0","id":1,"method":"echo"},5]`,
		want: `[{"jsonrpc":"2.0","id":1,"result":{"method":"echo"}},` +
			`{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"request is not a JSON object"}}]`,
	}, {
		// Owning the framing must not make jrpc2's duplicate-id check reachable.
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

// A fat response must survive the envelope byte for byte.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_BigResponseIntegrity(t *testing.T) {
	const payloadBytes = 4 << 20
	payload := strings.Repeat("abcdefgh", payloadBytes/8)
	value := map[string]any{"blob": payload, "n": 12345}

	h := NewHandler(map[string]jrpc2.Handler{
		"fat": func(context.Context, *jrpc2.Request) (any, error) { return value, nil },
	}, nil)

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

// Without the bound, fat marshals in flight are the backlog limit, not cores.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_SemaphoreBoundsConcurrentDispatch(t *testing.T) {
	limit := runtime.GOMAXPROCS(0)
	entered := make(chan struct{}, 4*limit)
	release := make(chan struct{})

	var inflight, peak atomic.Int64
	h := NewHandler(map[string]jrpc2.Handler{
		"hold": func(context.Context, *jrpc2.Request) (any, error) {
			recordPeak(&peak, inflight.Add(1))
			entered <- struct{}{}
			<-release
			inflight.Add(-1)
			return held, nil
		},
	}, nil)
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

	// Saturate the bound, then give an unbounded case room to exceed it.
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
		"more handlers ran at once than the bound allows: the semaphore is gone or its weight is wrong")
	assert.Equal(t, int64(limit), observed,
		"the bound was never reached, so this test would not notice its removal")
}

// The permit must be gone before any byte reaches the ResponseWriter.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_SemaphoreIsReleasedBeforeTheWrite(t *testing.T) {
	started := make(chan struct{}, runtime.GOMAXPROCS(0)+1)
	h := NewHandler(map[string]jrpc2.Handler{
		"echo": func(context.Context, *jrpc2.Request) (any, error) {
			started <- struct{}{}
			return "ok", nil
		},
	}, nil)

	// Saturate with writers that never drain, then serve a fresh request.
	blocked := make(chan struct{})
	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/",
				strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"echo"}`))
			req.Header.Set("Content-Type", "application/json")
			h.ServeHTTP(&blockingWriter{blocked: blocked}, req)
		})
	}
	for range runtime.GOMAXPROCS(0) {
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			close(blocked)
			wg.Wait()
			t.Fatal("the saturating requests never reached their handlers")
		}
	}

	rec, done := serveAsync(t, h, `{"jsonrpc":"2.0","id":2,"method":"echo"}`)
	select {
	case <-done:
		assert.Equal(t, http.StatusOK, rec.Code)
		//nolint:testifylint // byte-exact wire pin; JSONEq would ignore key order and escaping
		assert.Equal(t, `{"jsonrpc":"2.0","id":2,"result":"ok"}`, rec.Body.String())
	case <-time.After(10 * time.Second):
		t.Error("a request could not be served while the bound's worth of writers were stalled: " +
			"the semaphore is being held across the write")
	}
	<-started
	close(blocked)
	wg.Wait()
}

// blockingWriter stands in for a client that stopped reading. Header must keep
// the map it hands out, or the writer swallows every header set on it.
type blockingWriter struct {
	blocked chan struct{}
	header  http.Header
}

func (b *blockingWriter) Header() http.Header {
	if b.header == nil {
		b.header = http.Header{}
	}
	return b.header
}
func (b *blockingWriter) Write(p []byte) (int, error) { <-b.blocked; return len(p), nil }
func (*blockingWriter) WriteHeader(int)               {}

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
	return jsonArray(n, func(id int) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q}`, id, method)
	})
}

// jsonArray joins n elements, built from ids 1..n, into a JSON array.
func jsonArray(n int, element func(id int) string) string {
	elems := make([]string, n)
	for i := range elems {
		elems[i] = element(i + 1)
	}
	return "[" + strings.Join(elems, ",") + "]"
}

// serveAsync runs one body on its own goroutine, to watch a batch in flight.
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

// A barrier, not a stopwatch: serial dispatch gets one element in flight.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_BatchElementsRunConcurrently(t *testing.T) {
	n := min(runtime.GOMAXPROCS(0), 8)
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
	}, nil)
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

	want := jsonArray(n, func(id int) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":%q}`, id, held)
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, want, rec.Body.String())
	assert.Equal(t, strconv.Itoa(len(want)), rec.Header().Get("Content-Length"))
}

// A batch borrows from the process-wide bound. The weight must bound concurrent
// HANDLERS and GOROUTINES both — the latter only because the permit is taken
// before the worker starts.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_ABatchDoesNotMultiplyTheConcurrencyBound(t *testing.T) {
	limit := runtime.GOMAXPROCS(0)
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
			recordPeak(&peak, inflight.Add(1))
			entered <- struct{}{}
			<-release
			inflight.Add(-1)
			return held, nil
		},
	}, nil)

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
	// Give an unbounded implementation room to overshoot.
	time.Sleep(250 * time.Millisecond)
	observedPeak := peak.Load()
	observedGoroutines := runtime.NumGoroutine()
	close(release)
	<-done

	assert.Equal(t, int64(limit), observedPeak,
		"a batch of %d elements ran %d handlers at once against a bound of %d", elements, observedPeak, limit)
	// The slack is the serving, test and runtime goroutines.
	assert.Less(t, observedGoroutines, before+limit+16,
		"a batch of %d elements parked %d goroutines: the permit is being acquired inside the worker",
		elements, observedGoroutines-before)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// DELTA (e) and compaction hold however the batch completes; this one
// finishes back to front.
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
	}, nil)

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

// A worker's panic must be RE-RAISED, not escape, and free its permit.
//
//nolint:unparam // the handler literals must match the fixed jrpc2.Handler signature
func TestWire_APanicInABatchElementFailsTheRequestAndReleasesItsPermit(t *testing.T) {
	h := NewHandler(map[string]jrpc2.Handler{
		"boom": func(context.Context, *jrpc2.Request) (any, error) { panic("handler exploded") },
		"echo": func(_ context.Context, r *jrpc2.Request) (any, error) {
			return map[string]any{"method": r.Method()}, nil
		},
	}, nil)

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
	for range runtime.GOMAXPROCS(0) + 1 {
		serve(`[{"jsonrpc":"2.0","id":1,"method":"boom"},{"jsonrpc":"2.0","id":2,"method":"boom"}]`)
	}

	rec, done := serveAsync(t, h, `{"jsonrpc":"2.0","id":9,"method":"echo"}`)
	select {
	case <-done:
		//nolint:testifylint // byte-exact wire pin; JSONEq would ignore key order and escaping
		assert.Equal(t, `{"jsonrpc":"2.0","id":9,"result":{"method":"echo"}}`, rec.Body.String())
	case <-time.After(10 * time.Second):
		t.Error("the handler stopped serving after panicking batches: every panic leaked its permit")
	}
}

// Valid JSON that is not a request object answers one bare frame.
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
		// null decodes into a nil map, so it reaches the version check: the
		// one scalar that does not answer -32700.
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

// Three jrpc2 accessors stopped working with the jrpc2.Server; see the doc.
func TestNoJRPC2ContextValueCallersInProductionCode(t *testing.T) {
	// Anchored on this file's compiled-in path, not the working directory.
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "cannot locate this test's own source file")
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "..", ".."))
	require.Equal(t, "cmd", filepath.Base(root), "this test walks cmd/; the package moved")

	// The accessor name, and the receiver that makes a hit a real call.
	receiverOf := map[string]string{
		"InboundRequest":    "jrpc2",
		"ServerFromContext": "jrpc2",
		"IsNotification":    "",
	}
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
		// Selector expressions only: a comment is not a caller.
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			recv, banned := receiverOf[sel.Sel.Name]
			if !banned {
				return true
			}
			if recv != "" {
				pkg, isIdent := sel.X.(*ast.Ident)
				if !isIdent || pkg.Name != recv {
					return true
				}
			}
			callers = append(callers, fmt.Sprintf("%s: %s", fset.Position(sel.Pos()), sel.Sel.Name))
			return true
		})
		return nil
	}))

	assert.Empty(t, callers, "no jrpc2.Server sits under these handlers any more, so InboundRequest "+
		"returns nil, ServerFromContext panics, and IsNotification is false even for a notification. "+
		"Take the request from the handler's second argument, and its notification-ness from an empty ID()")
}

// DELTA (f). rpcv2's getEventsV2 reads ParamString, so the bytes are contract.
//
//nolint:nilnil,unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_ParamsReachTheHandlerVerbatim(t *testing.T) {
	var seen string
	h := NewHandler(map[string]jrpc2.Handler{
		"params": func(_ context.Context, r *jrpc2.Request) (any, error) {
			seen = r.ParamString()
			return nil, nil
		},
	}, nil)

	for _, tc := range []struct{ name, body, want string }{{
		name: "by position",
		body: `{"jsonrpc":"2.0","id":1,"method":"params","params":[1,"two",null]}`,
		want: `[1,"two",null]`,
	}, {
		name: "by name, in the client's key order and with the client's whitespace",
		body: `{"jsonrpc":"2.0","id":1,"method":"params","params":{"b": 2,  "a":1}}`,
		want: `{"b": 2,  "a":1}`,
	}, {
		name: "characters the bridge's re-marshal would have escaped are not escaped",
		body: `{"jsonrpc":"2.0","id":1,"method":"params","params":{"a":"x<y&z"}}`,
		want: `{"a":"x<y&z"}`,
	}, {
		name: "absent params are the empty string",
		body: `{"jsonrpc":"2.0","id":1,"method":"params"}`,
		want: ``,
	}, {
		// jrpc2 reduces "null" params to nil, indistinguishable from absent.
		name: "an explicit null is reduced to absent",
		body: `{"jsonrpc":"2.0","id":1,"method":"params","params":null}`,
		want: ``,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			seen = "<unset>"
			status, _, _ := post(t, h, tc.body)
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, tc.want, seen)
		})
	}
}

// DELTA (g): a dead request stops feeding the bound; what started finishes.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_ADeadRequestStopsStartingElements(t *testing.T) {
	weight := runtime.GOMAXPROCS(0)
	elements := max(512, 4*weight)

	entered := make(chan struct{}, elements)
	release := make(chan struct{})
	var started, completed atomic.Int64

	h := NewHandler(map[string]jrpc2.Handler{
		"hold": func(context.Context, *jrpc2.Request) (any, error) {
			started.Add(1)
			entered <- struct{}{}
			<-release
			completed.Add(1)
			return held, nil
		},
	}, nil)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/", strings.NewReader(batchOf(elements, "hold")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(rec, req)
	}()

	// Saturate: the loop is now parked in Acquire, most of the batch to go.
	for k := range weight {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			cancel()
			close(release)
			<-done
			t.Fatalf("only %d of %d permits were taken", k, weight)
		}
	}

	// Kill the request, exactly as the limiter's 504 branch does.
	cancel()
	time.Sleep(250 * time.Millisecond) // ample room for the loop to notice
	atCancel := started.Load()

	// The serving goroutine joins what it started, so time from the release.
	close(release)
	unwindStart := time.Now()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the serving goroutine never unwound: the dispatch loop is not watching the request")
	}
	unwind := time.Since(unwindStart)
	t.Logf("started %d of %d elements at a bound of %d; unwound %v after the last one returned",
		started.Load(), elements, weight, unwind)

	assert.Equal(t, atCancel, started.Load(), "elements started after the request was already dead")
	assert.LessOrEqual(t, atCancel, int64(weight+1),
		"a dead request started more than the elements already holding permits (+1 for the Acquire race)")
	assert.Equal(t, atCancel, completed.Load(), "an element that had started did not run to completion")
	assert.Less(t, unwind, 5*time.Second, "the unwind waited on more than the work it had started")
	assert.Empty(t, rec.Body.String(),
		"a dead request is answered by the layer that killed it; the framing writes nothing")
}

// adversarialBatch fills the body cap with the cheapest element that still
// demands an answer.
func adversarialBatch(t *testing.T) (string, int) {
	t.Helper()
	const bodyCap = 512 * 1024
	n := (bodyCap - 2) / 2 // "5," per element, inside the brackets
	elems := make([]string, n)
	for i := range elems {
		elems[i] = "5"
	}
	body := "[" + strings.Join(elems, ",") + "]"
	require.LessOrEqual(t, len(body), bodyCap)
	return body, n
}

// DELTA (g) for the elements that never reach acquirePermit. Measured ABOVE
// the parse: the frames are discarded either way, so all that changes is
// whether they were built.
func TestWire_ADeadRequestStopsBuildingStaticErrorFrames(t *testing.T) {
	h, _ := newTestHandler(t)
	body, elements := adversarialBatch(t)

	measure := func(f func()) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	serve := func(ctx context.Context) (uint64, time.Duration, int) {
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		var elapsed time.Duration
		alloc := measure(func() {
			start := time.Now()
			h.ServeHTTP(rec, req)
			elapsed = time.Since(start)
		})
		return alloc, elapsed, rec.Body.Len()
	}

	// The floor both runs pay whatever happens.
	parseAlloc := measure(func() {
		parsed, err := jrpc2.ParseRequests([]byte(body))
		require.NoError(t, err)
		require.Len(t, parsed, elements)
	})

	liveAlloc, liveTime, liveLen := serve(t.Context())
	dead, cancel := context.WithCancel(t.Context())
	cancel()
	deadAlloc, deadTime, deadLen := serve(dead)

	mb := func(n float64) float64 { return n / (1 << 20) }
	liveFraming := float64(liveAlloc) - float64(parseAlloc)
	deadFraming := float64(deadAlloc) - float64(parseAlloc)
	t.Logf("%d elements, parse floor %.1fMB: live %.1fMB (%.1fMB framing) in %v -> %d body bytes; "+
		"dead %.1fMB (%.1fMB framing) in %v -> %d body bytes",
		elements, mb(float64(parseAlloc)),
		mb(float64(liveAlloc)), mb(liveFraming), liveTime, liveLen,
		mb(float64(deadAlloc)), mb(deadFraming), deadTime, deadLen)

	require.Positive(t, liveLen, "the control run must actually answer, or this proves nothing")
	require.Positive(t, liveFraming, "the control run framed nothing; the measurement is broken")
	assert.Zero(t, deadLen, "a dead request must not be answered by the framing")
	// The residual is dispatchAll's slot slice, allocated before it can know.
	assert.Less(t, deadFraming, liveFraming/5,
		"a dead request spent %.1fMB framing against a %.1fMB control: it is still answering its elements",
		mb(deadFraming), mb(liveFraming))
}

// Without Shutdown a straggler keeps reading a store its daemon has closed.
//
//nolint:unparam // the handler literals must match the fixed jrpc2.Handler signature
func TestWire_Shutdown(t *testing.T) {
	t.Run("cancels a running handler and returns once it is gone", func(t *testing.T) {
		entered := make(chan struct{}, 1)
		observed := make(chan error, 1)
		h := NewHandler(map[string]jrpc2.Handler{
			"watch": func(ctx context.Context, _ *jrpc2.Request) (any, error) {
				entered <- struct{}{}
				<-ctx.Done() // no request deadline reaches here; only Shutdown
				observed <- ctx.Err()
				return held, nil
			},
		}, nil)
		rec, done := serveAsync(t, h, `{"jsonrpc":"2.0","id":1,"method":"watch"}`)
		<-entered

		drained := make(chan error, 1)
		go func() { drained <- h.Shutdown(context.Background()) }()

		select {
		case err := <-observed:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("Shutdown did not cancel the running handler's context")
		}
		select {
		case err := <-drained:
			assert.NoError(t, err, "the drain must report success once its handlers are gone")
		case <-time.After(10 * time.Second):
			t.Fatal("Shutdown returned before, or never after, its handlers finished")
		}
		<-done
		assert.Equal(t, http.StatusOK, rec.Code, "the request in flight still gets its answer")
	})

	t.Run("reports a drain that runs out of time, and stays idempotent", func(t *testing.T) {
		entered := make(chan struct{}, 1)
		release := make(chan struct{})
		h := NewHandler(map[string]jrpc2.Handler{
			"stuck": func(context.Context, *jrpc2.Request) (any, error) {
				entered <- struct{}{}
				<-release // ignores cancellation, as a scan loop between checks does
				return held, nil
			},
		}, nil)
		_, done := serveAsync(t, h, `{"jsonrpc":"2.0","id":1,"method":"stuck"}`)
		<-entered

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		err := h.Shutdown(ctx)
		require.Error(t, err, "a straggler that outlasts the budget must be reported")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, err, h.Shutdown(t.Context()),
			"the first result must answer every later call, without re-taking a bound it cannot have")

		close(release)
		<-done
	})

	// Shutdown holds the bound, so a later request unwinds at its own
	// deadline (DELTA (g)) rather than hanging.
	t.Run("nothing new starts after a completed drain", func(t *testing.T) {
		var ran atomic.Int64
		h := NewHandler(map[string]jrpc2.Handler{
			"count": func(context.Context, *jrpc2.Request) (any, error) {
				ran.Add(1)
				return held, nil
			},
		}, nil)
		require.NoError(t, h.Shutdown(t.Context()))

		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/",
			strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"count"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Zero(t, ran.Load(), "a handler ran against resources that are being torn down")
		assert.Empty(t, rec.Body.String())
	})
}

// Static-error frames are the one answer that needs no handler, and a hostile
// body carries ~260,000 of them at ~23MB. Building them outside the bound let
// concurrent hostile bodies materialize that much each, so the deferred build
// takes a permit — proved here by holding every permit and showing a
// static-only batch cannot finish until one comes back.
//
//nolint:unparam // the handler literal must match the fixed jrpc2.Handler signature
func TestWire_StaticErrorFramesAreBuiltInsideTheBound(t *testing.T) {
	weight := runtime.GOMAXPROCS(0)
	entered := make(chan struct{}, weight)
	release := make(chan struct{})

	h := NewHandler(map[string]jrpc2.Handler{
		"hold": func(context.Context, *jrpc2.Request) (any, error) {
			entered <- struct{}{}
			<-release
			return held, nil
		},
	}, nil)

	var wg sync.WaitGroup
	for range weight {
		wg.Go(func() {
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/",
				strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"hold"}`))
			req.Header.Set("Content-Type", "application/json")
			h.ServeHTTP(httptest.NewRecorder(), req)
		})
	}
	for range weight {
		<-entered
	}

	// Every permit is held. This batch runs no handler at all, so before the
	// deferred build it answered immediately.
	rec, done := serveAsync(t, h, `[5,5,5]`)
	select {
	case <-done:
		close(release)
		wg.Wait()
		t.Fatal("a static-error batch was framed while every permit was held")
	case <-time.After(250 * time.Millisecond):
	}

	close(release)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the static-error batch never completed after a permit came back")
	}
	wg.Wait()

	one := `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"request is not a JSON object"}}`
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "["+one+","+one+","+one+"]", rec.Body.String())
}
