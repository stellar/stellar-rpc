package jsonrpc

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"sync"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"

	"github.com/stellar/go-stellar-sdk/support/log"
)

// dispatcher is the HTTP transport for the method table. It parses the
// JSON-RPC envelope with jrpc2's parser, calls each request's handler directly,
// and writes the response envelope itself. This replaces jhttp.Bridge, whose
// in-memory client/server round trip re-parsed every response twice and
// re-compacted it once — the dominant cost for MB-scale results. A
// json.RawMessage result is written verbatim, so a handler can serve
// pre-rendered bytes.
//
// HTTP semantics mirror jhttp.Bridge: POST with application/json only, 200 with
// the response object (an array for a batch), 204 when nothing calls for a
// response, and a plain-text 500 when the body is not JSON.
type dispatcher struct {
	methods handler.Map
	logger  *log.Entry
}

func newDispatcher(methods handler.Map, logger *log.Entry) *dispatcher {
	return &dispatcher{methods: methods, logger: logger}
}

// rpcResponse is one call's outcome: exactly one of result and err is set.
type rpcResponse struct {
	id     string // raw JSON of the request id, "null" when there is none
	result json.RawMessage
	err    *jrpc2.Error
}

func (d *dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Accept-Post", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	mediaType, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaType != "application/json" {
		http.Error(w, "content-type must be application/json", http.StatusUnsupportedMediaType)
		return
	}
	if cs, ok := params["charset"]; ok && cs != "utf-8" && cs != "utf8" {
		http.Error(w, "invalid content-type charset", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writePlainError(w, err)
		return
	}
	reqs, err := jrpc2.ParseRequests(body)
	if err != nil {
		writePlainError(w, err)
		return
	}
	d.serve(r.Context(), w, reqs)
}

// writePlainError is the bridge's answer to an unreadable or non-JSON body: a
// text 500 rather than a JSON-RPC parse error. Kept for wire parity.
func writePlainError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = io.WriteString(w, err.Error()+"\n")
}

func (d *dispatcher) serve(ctx context.Context, w http.ResponseWriter, reqs []*jrpc2.ParsedRequest) {
	rsps := d.dispatch(ctx, reqs)
	if len(rsps) == 0 {
		w.WriteHeader(http.StatusNoContent) // only notifications, or an empty batch
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	out := envelopeWriter{w: w}
	batch := reqs[0].Batch
	if batch {
		out.writeString("[")
	}
	for i := range rsps {
		if i > 0 {
			out.writeString(",")
		}
		out.response(&rsps[i])
	}
	if batch {
		out.writeString("]")
	}
	if out.err != nil {
		d.logger.WithError(out.err).Debug("jsonrpc: response write failed")
	}
}

// dispatch answers every request, running a batch's members concurrently as
// the jrpc2 server did.
func (d *dispatcher) dispatch(ctx context.Context, reqs []*jrpc2.ParsedRequest) []rpcResponse {
	rsps := make([]rpcResponse, len(reqs))
	answered := make([]bool, len(reqs))
	if len(reqs) == 1 {
		rsps[0], answered[0] = d.call(ctx, reqs[0])
	} else {
		var wg sync.WaitGroup
		for i, req := range reqs {
			wg.Go(func() { rsps[i], answered[i] = d.call(ctx, req) })
		}
		wg.Wait()
	}
	out := make([]rpcResponse, 0, len(rsps))
	for i := range rsps {
		if answered[i] {
			out = append(out, rsps[i])
		}
	}
	return out
}

// call runs one request and reports whether it produces a response. A
// notification (no id) is answered only for a parse or validation error the
// bridge would have reported under a null id.
func (d *dispatcher) call(ctx context.Context, req *jrpc2.ParsedRequest) (rpcResponse, bool) {
	isNotification := req.ID == ""
	rsp := rpcResponse{id: req.ID}
	if isNotification {
		rsp.id = "null"
	}
	if req.Error != nil {
		rsp.err = req.Error
		return rsp, true
	}
	if req.Method == "" {
		rsp.err = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty method name"}
		return rsp, !isNotification
	}
	h, ok := d.methods[req.Method]
	if !ok {
		rsp.err = (&jrpc2.Error{Code: jrpc2.MethodNotFound, Message: jrpc2.MethodNotFound.String()}).WithData(req.Method)
		return rsp, !isNotification
	}
	result, err := h(ctx, req.ToRequest())
	if isNotification {
		return rsp, false
	}
	if err == nil {
		rsp.result, err = encodeResult(result)
	}
	if err != nil {
		rsp.err = toRPCError(err)
	}
	return rsp, true
}

// encodeResult renders a handler result: pre-rendered bytes pass through,
// anything else is marshaled exactly as the jrpc2 server did.
func encodeResult(v any) (json.RawMessage, error) {
	if raw, ok := v.(json.RawMessage); ok && len(raw) > 0 {
		return raw, nil
	}
	return json.Marshal(v)
}

// toRPCError mirrors the jrpc2 server's conversion of a handler error: a
// *jrpc2.Error is sent as is; anything else keeps its code (jrpc2.ErrorCode
// resolves ErrCoder values and context errors) with Error() as the message.
func toRPCError(err error) *jrpc2.Error {
	if e, ok := err.(*jrpc2.Error); ok { //nolint:errorlint // the concrete-type check is what jrpc2 does
		return e
	}
	return &jrpc2.Error{Code: jrpc2.ErrorCode(err), Message: err.Error()}
}

// envelopeWriter writes response envelopes and keeps the first write failure:
// once the client is gone nothing further is worth writing.
type envelopeWriter struct {
	w   http.ResponseWriter
	err error
}

// The gosec taint warnings below are the request id echoed back: it is
// validated JSON from jrpc2.ParseRequests, sent as application/json.

func (e *envelopeWriter) write(p []byte) {
	if e.err == nil {
		_, e.err = e.w.Write(p) //nolint:gosec // G705, see above
	}
}

func (e *envelopeWriter) writeString(s string) {
	if e.err == nil {
		_, e.err = io.WriteString(e.w, s) //nolint:gosec // G705, see above
	}
}

func (e *envelopeWriter) response(rsp *rpcResponse) {
	e.writeString(`{"jsonrpc":"2.0","id":`)
	e.writeString(rsp.id)
	if rsp.err != nil {
		e.writeString(`,"error":`)
		e.write(marshalRPCError(rsp.err))
	} else {
		e.writeString(`,"result":`)
		e.write(rsp.result)
	}
	e.writeString("}")
}

// marshalRPCError renders the error object; only malformed Data can make that
// fail, in which case the code survives and the message says why.
func marshalRPCError(rpcErr *jrpc2.Error) []byte {
	out, err := json.Marshal(rpcErr)
	if err == nil {
		return out
	}
	out, err = json.Marshal(&jrpc2.Error{
		Code:    rpcErr.Code,
		Message: rpcErr.Message + " (error data not encodable: " + err.Error() + ")",
	})
	if err != nil {
		return []byte(`{"code":-32603,"message":"internal error"}`)
	}
	return out
}
