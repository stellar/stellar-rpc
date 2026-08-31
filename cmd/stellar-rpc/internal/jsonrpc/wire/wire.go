// Package wire frames JSON-RPC 2.0 over HTTP. Both mounts use it; it is the
// only framing this repo serves.
//
// It replaced jrpc2's jhttp.Bridge, a loopback that marshaled every response
// four times over. jrpc2 remains the parser and the vocabulary; this package
// owns the envelope: one json.Marshal, one hand-appended envelope, one Write.
//
// Three rules here are LOAD-BEARING, each with a plausible-looking
// "improvement" that silently reverses this package's reason to exist:
// json.Marshal + Write (never json.NewEncoder), hand-appended batch frames
// (never json.Marshal of a []json.RawMessage), and a batch worker's permit
// taken before its goroutine is started (never inside it).
//
// # The deltas
//
// Seven places answer differently from the bridge, argued at their sites:
//
//	(a) a malformed body is a -32700 frame over 200, not 500 + plaintext
//	(b) an empty batch `[]` is a -32600 frame, not a 204
//	(c) notifications have finished when their 204 is written
//	(d) an empty-method notification is answered rather than 204'd
//	(e) batch frames come back in input order
//	(f) params reach handlers verbatim rather than re-marshaled
//	(g) dispatch stops at the HTTP deadline; started elements still finish
//
// # Three jrpc2 accessors a handler must not use
//
// Handlers run on a SERVER-scoped context: derived from context.Background, so
// no request cancels it, and canceled by Handler.Shutdown. It carries none of
// the values jrpc2's Server.invoke attached, and none is fabricated back —
// jrpc2.InboundRequest(ctx) returns nil, jrpc2.ServerFromContext(ctx) panics,
// and (*jrpc2.Request).IsNotification() is false for EVERY request. The last
// is the dangerous one because it fails quietly: it tests id == nil, and
// ToRequest builds an empty-but-non-nil id for an absent one. Notification-ness
// is ParsedRequest.ID == "". Take the request from the handler's second
// argument. TestNoJRPC2ContextValueCallersInProductionCode fails on a caller.
package wire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/creachadair/jrpc2"
	"golang.org/x/sync/semaphore"
)

const (
	contentTypeJSON = "application/json"

	// framePrefix opens every response frame; the id follows immediately, then
	// resultKey or errorKey. Field order matches jrpc2's jmessage.toJSON.
	framePrefix = `{"jsonrpc":"2.0","id":`
	resultKey   = `,"result":`
	errorKey    = `,"error":`

	// nullID answers a request whose id was absent, null or unparseable.
	nullID = "null"
)

// Transcriptions of jrpc2's unexported error vocabulary (jrpc2/error.go). The
// cost is permanent: a `go get -u` moves the library's strings and leaves
// these frozen. The byte-exact tests are what notice.
var (
	errEmptyMethod         = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty method name"}
	errEmptyBatch          = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty request batch"}
	errInvalidRequestValue = &jrpc2.Error{Code: jrpc2.ParseError, Message: "invalid request value"}
)

// Handler serves JSON-RPC 2.0 over HTTP for one method table.
//
// Mounted INSIDE the shared middleware chain jsonrpc.NewHandler builds:
// cors -> MaxBytesHandler -> httpRequestDurationLimiter -> BacklogHTTPQLimiter
// -> Handler. The duration limiter is the slow-client decoupler — it answers a
// timed-out client with 504 while the handler is still live, safe only because
// nothing has reached the socket, and flushes after the per-method limiters
// and the view release, so a stalled client holds one flat []byte and nothing
// else. rpcv2's deriveLifecycleGrace relies on that deadline. Do not bypass or
// wrap it: bufferedResponseWriter has no Unwrap, Flusher or SetWriteDeadline.
type Handler struct {
	methods map[string]jrpc2.Handler

	// sem bounds concurrent handler+marshal work: acquired before dispatch,
	// released BEFORE the bytes reach the (buffered) ResponseWriter, so a slow
	// reader cannot hold a permit. ONE semaphore for the whole handler, so a
	// batch borrows from the process-wide budget instead of multiplying it.
	//
	// The weight is GOMAXPROCS, not jrpc2's NumCPU: since Go 1.25 GOMAXPROCS
	// follows the cgroup CPU quota and NumCPU does not, and this is what caps
	// the number of fat results marshaled at once. Matches the repo's
	// convention (rpcv2/backfill.DefaultWorkers).
	sem *semaphore.Weighted

	// weight is sem's size, kept because semaphore.Weighted does not expose
	// it and Shutdown drains by acquiring all of it.
	weight int64

	// root is the SERVER-scoped lifetime every handler runs on. No request
	// touches it; only Shutdown cancels it, which is what keeps a straggler
	// from reading a store its daemon has already closed.
	//
	//nolint:containedctx // a server's own lifetime, as http.Server holds one
	root     context.Context
	stopRoot context.CancelFunc

	drainOnce sync.Once
	drainErr  error
}

// NewHandler returns the JSON-RPC handler over methods, which is the
// decorated, limiter-wrapped table and is read-only from here on.
func NewHandler(methods map[string]jrpc2.Handler) *Handler {
	weight := int64(runtime.GOMAXPROCS(0))
	//nolint:gosec // G118: stopRoot is the handler's, called by Shutdown
	root, stopRoot := context.WithCancel(context.Background())
	return &Handler{
		methods:  methods,
		sem:      semaphore.NewWeighted(weight),
		weight:   weight,
		root:     root,
		stopRoot: stopRoot,
	}
}

// Shutdown cancels the context every handler runs on and waits for the running
// handlers, or for ctx to end. An error means a straggler is still touching
// whatever the caller is about to close.
//
// The wait is an acquire of the whole bound: every invocation happens between
// an acquirePermit and its Release, so holding every permit IS "no handler is
// running", with no second counter to keep in step. The permits are never
// given back — a drained handler stays drained, and a request that arrives
// anyway unwinds at its own deadline (DELTA (g)).
//
// Idempotent. Call it after the HTTP server has stopped accepting and before
// the resources the handlers read are closed.
func (h *Handler) Shutdown(ctx context.Context) error {
	h.stopRoot()
	h.drainOnce.Do(func() {
		if err := h.sem.Acquire(ctx, h.weight); err != nil {
			h.drainErr = fmt.Errorf("wire: draining handlers: %w", err)
		}
	})
	return h.drainErr
}

// ServeHTTP implements http.Handler. The HTTP shell is byte-identical to
// jhttp.Bridge.ServeHTTP: Accept-Post advertised unconditionally, non-POST 405
// with no body, wrong media type or charset 415 with jhttp's exact plaintext.
func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Accept-Post", contentTypeJSON)

	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	mt, params, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if mt != contentTypeJSON {
		http.Error(w, "content-type must be application/json", http.StatusUnsupportedMediaType)
		return
	} else if cs, ok := params["charset"]; ok && cs != "utf-8" && cs != "utf8" {
		http.Error(w, "invalid content-type charset", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		// A transport event, not a JSON-RPC one: no request to answer and no
		// id to echo. MaxBytesHandler takes this path for an oversized body.
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err.Error())
		return
	}

	// req.Context() governs the DISPATCH — whether another element may start
	// (DELTA (g)) — and nothing else. Handlers run on h.root; see invoke.
	h.serve(req.Context(), w, body)
}

// serve parses body, dispatches it, and writes the response. ctx is the HTTP
// request's, used only to decide whether to keep dispatching.
func (h *Handler) serve(ctx context.Context, w http.ResponseWriter, body []byte) {
	reqs, err := jrpc2.ParseRequests(body)
	if err != nil {
		// DELTA (a).
		writeFrames(w, false, [][]byte{errorFrame("", parseError(err))})
		return
	}
	if len(reqs) == 0 {
		// DELTA (b). A non-batch body always parses to exactly one element,
		// so an empty slice means the body was `[]`.
		writeFrames(w, false, [][]byte{errorFrame("", errEmptyBatch)})
		return
	}

	isBatch := reqs[0].Batch // a property of the body, carried on every element

	frames := h.dispatchAll(ctx, reqs)
	if ctx.Err() != nil {
		// DELTA (g): the request died mid-dispatch, so there is nobody to
		// answer. Falling through would have to choose between a partial
		// batch array and 204ing an abandoned batch as all-notification.
		return
	}
	if len(frames) == 0 {
		// Every element was a notification, and DELTA (c) — all of them have
		// finished, because dispatchAll joins every handler it started.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeFrames(w, isBatch, frames)
}

// dispatchAll answers every element of one parsed body and returns the frames
// in INPUT order (DELTA (e)), the elements that get no response compacted out.
// It returns only once every handler it started has, which is what lets
// serve's 204 mean "the notifications have run".
//
// A batch's elements run CONCURRENTLY: dispatched serially they sum their
// latencies under the ONE HTTP deadline the mount enforces, so a batch of
// individually-fine calls 504s where each call alone succeeds.
//
// LOAD-BEARING: the permit is acquired HERE, on the serving goroutine, and
// released by the worker. Acquire inside the worker instead and every element
// gets a goroutine parked on Acquire — and one body fits ~260,000 elements
// under the cap. Acquiring first makes the in-flight worker count the
// semaphore's weight, whatever the batch's length.
//
// Two contexts govern a dispatch and do not overlap: the REQUEST's decides
// whether an element may START, the server-scoped root whether a running
// handler may CONTINUE. DELTA (g) is the first of those:
//
//   - An element already inside its handler runs to completion; only Shutdown
//     cancels it. wg.Wait joins every worker that started, so the unwind costs
//     at most one element's duration.
//   - An element that never got a permit yields NO FRAME, not an error frame:
//     there is nobody to answer, and building answers for a departed reader is
//     the work this stops.
//   - An element that needs no handler never reaches the permit, so the loop
//     probes the request's Done channel per element too.
func (h *Handler) dispatchAll(ctx context.Context, reqs []*jrpc2.ParsedRequest) [][]byte {
	// One slot per element, by index, so response order is request order
	// whatever the completion order. A nil slot gets no answer.
	frames := make([][]byte, len(reqs))

	if len(reqs) == 1 {
		// A single-element body never leaves this goroutine. Not an
		// optimization: it is what lets its panic reach the limiter's recover
		// with its OWN stack, unwrapped by relayedPanic.
		frames[0] = h.dispatchOne(ctx, reqs[0])
	} else {
		h.dispatchBatch(ctx, reqs, frames)
	}
	return slices.DeleteFunc(frames, func(f []byte) bool { return f == nil })
}

// dispatchBatch runs the elements of a multi-element body into their own slots
// of frames. See dispatchAll for the permit ordering and the dead-request rule.
func (h *Handler) dispatchBatch(ctx context.Context, reqs []*jrpc2.ParsedRequest, frames [][]byte) {
	var wg sync.WaitGroup
	var raised atomic.Pointer[relayedPanic]

	// Read once: ctx.Err() takes a lock per call, a channel probe does not.
	dead := ctx.Done()

	for i, pr := range reqs {
		if isClosed(dead) {
			// DELTA (g) for the elements that never reach acquirePermit.
			break
		}
		method, frame := h.route(pr)
		if method == nil {
			frames[i] = frame // answered without a permit or a goroutine
			continue
		}
		if !h.acquirePermit(ctx) {
			break // the request died waiting for the bound
		}
		wg.Go(func() {
			// Released before the caller writes a byte, panics included.
			defer h.sem.Release(1)
			defer catchPanic(&raised)
			frames[i] = h.invoke(pr, method)
		})
	}
	wg.Wait()

	if p := raised.Load(); p != nil {
		// A panic on a worker has nothing above it to recover and would take
		// the process down. Re-raised here, once every sibling has finished,
		// it fails only this HTTP request. A safety net, not a live path:
		// every registered method carries a RequestDurationLimit, whose
		// limiter recovers a handler panic into -32003 first.
		panic(p)
	}
}

// dispatchOne answers one request on the calling goroutine, releasing its
// permit before returning so the caller writes without one, panics included.
func (h *Handler) dispatchOne(ctx context.Context, pr *jrpc2.ParsedRequest) []byte {
	method, frame := h.route(pr)
	if method == nil {
		return frame
	}
	if !h.acquirePermit(ctx) {
		return nil // DELTA (g): never started, so there is nothing to answer.
	}
	defer h.sem.Release(1)
	return h.invoke(pr, method)
}

// acquirePermit takes one permit from the shared bound, blocking until it gets
// one or ctx ends, and reports which happened. ctx is the HTTP request's — the
// one place a request's lifetime reaches into dispatch (DELTA (g)). x/sync
// gives back a permit won in the race, so false always means "did not run".
func (h *Handler) acquirePermit(ctx context.Context) bool {
	return h.sem.Acquire(ctx, 1) == nil
}

// isClosed reports whether c is closed, without blocking.
func isClosed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// route decides how one parsed request is answered WITHOUT running anything:
// it returns the handler to invoke, or a nil handler and the finished frame,
// which is itself nil for a request that gets no response at all.
//
// The order is load-bearing. Parse and shape errors answer with a frame even
// for a notification, as the spec requires; an empty method name is the same
// class and answers too (DELTA (d)); only then does notification-ness apply,
// and only to DISPATCH results, so an unknown method or a handler error on a
// notification is silent.
//
// Notification-ness is ParsedRequest.ID == ""; see the package doc.
func (h *Handler) route(pr *jrpc2.ParsedRequest) (jrpc2.Handler, []byte) {
	if pr.Error != nil {
		return nil, errorFrame(pr.ID, pr.Error)
	}
	if pr.Method == "" {
		return nil, errorFrame(pr.ID, errEmptyMethod)
	}
	// `method != nil` as well as ok: route says "run this" by returning a
	// non-nil handler, so a nil table entry must not be able to say it.
	if method, ok := h.methods[pr.Method]; ok && method != nil {
		return method, nil
	}
	if pr.ID == "" {
		return nil, nil // an unknown method on a notification is silent
	}
	// jrpc2's errNoSuchMethod frame, byte for byte. Resolved here, so no
	// handler runs and no metric is labeled: an attacker-chosen method name
	// cannot grow the per-method summary's cardinality.
	return nil, errorFrame(pr.ID, (&jrpc2.Error{
		Code:    jrpc2.MethodNotFound,
		Message: jrpc2.MethodNotFound.String(),
	}).WithData(pr.Method))
}

// invoke runs one handler and builds its frame. The caller holds the permit
// for the whole call and releases it before any byte is written.
func (h *Handler) invoke(pr *jrpc2.ParsedRequest, method jrpc2.Handler) []byte {
	// The SERVER's lifetime, never the http.Request's: a client that hangs up
	// mid-call still completes it, which sendTransaction depends on.
	ctx := h.root

	// DELTA (f): ToRequest hands over the params bytes the body carried, in
	// the client's key order and whitespace and with its own escapes. rpcv2's
	// getEventsV2 reads ParamString, so those raw bytes are contract.
	result, err := method(ctx, pr.ToRequest())
	if pr.ID == "" {
		// "The Server MUST NOT reply to a Notification, including those that
		// are within a batch request." The work is done, the answer is not
		// sent, and an error is dropped as jrpc2's invoke drops it.
		return nil
	}
	if err != nil {
		return errorFrame(pr.ID, handlerError(err))
	}

	// THE one marshal. LOAD-BEARING: json.Marshal, never
	// json.NewEncoder(w).Encode — Encode's deferred encodeStatePool.Put fires
	// after enc.w.Write, so N stalled clients pin N full-size pooled buffers,
	// and under GOEXPERIMENT=jsonv2 it copies through its own buffer,
	// reintroducing exactly the copy this package deletes. Marshal also lets
	// Content-Length be set.
	bits, err := json.Marshal(result)
	if err != nil {
		// Through handlerError, not a hand-built InternalError: jrpc2 runs a
		// marshal failure down the same ladder a handler error takes, which
		// answers -32098, never -32603.
		return errorFrame(pr.ID, handlerError(err))
	}
	return frame(pr.ID, resultKey, bits)
}

// relayedPanic is a handler panic caught on a batch worker so the serving
// goroutine can re-raise it, carrying the stack the handler panicked on —
// the recover that logs it runs elsewhere, where debug.Stack() shows the relay.
type relayedPanic struct {
	value any
	stack []byte
}

// String is what the limiter's "%v" of the recovered value prints.
func (p *relayedPanic) String() string {
	return fmt.Sprintf("%v (panicked on a batch worker goroutine)\n%s", p.value, p.stack)
}

// catchPanic must be deferred directly, so its recover is the worker's. The
// first panic wins; only one can be re-raised.
func catchPanic(raised *atomic.Pointer[relayedPanic]) {
	if v := recover(); v != nil {
		raised.CompareAndSwap(nil, &relayedPanic{value: v, stack: debug.Stack()})
	}
}

// handlerError maps a handler's error onto the wire error object, reproducing
// jrpc2's tasks.responses exactly, bare type assertion included: a WRAPPED
// *jrpc2.Error falls to the ErrorCode branch, keeping the code and taking the
// wrapper's message. Looks like a bug and is pinned as behavior: network's
// limiter sentinels are jrpc2.Error VALUES, so the assertion misses them and
// err.Error() — "[%d] %s" — supplies the message, giving those wire messages a
// redundant "[-32001] " prefix. They always have.
func handlerError(err error) *jrpc2.Error {
	if e, ok := err.(*jrpc2.Error); ok { //nolint:errorlint // jrpc2 asserts, it does not unwrap
		return e
	}
	if code := jrpc2.ErrorCode(err); code != jrpc2.NoError {
		return &jrpc2.Error{Code: code, Message: err.Error()}
	}
	return &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
}

// parseError recovers the *jrpc2.Error ParseRequests reports for a body that
// is not valid JSON. The fallback only guards a future library that returns
// something other than its errInvalidRequest.
func parseError(err error) *jrpc2.Error {
	if e, ok := err.(*jrpc2.Error); ok { //nolint:errorlint // matching jrpc2's own assertion style
		return e
	}
	return errInvalidRequestValue
}

// frame builds {"jsonrpc":"2.0","id":<id>,<key><payload>}. id is the request's
// raw id text, or "" for one with no usable id, which answers a null id.
//
// The capacity is the frame's exact byte length, so a frame is one allocation
// and one copy — which is why the arithmetic and the appends it predicts sit
// in the same six lines. No assertion on the RESPONSE can catch it being
// wrong: append grows silently and the bytes come out identical while the
// payload is re-copied. TestFrameIsExactlyOneAllocation pins it on cap.
func frame(id, key string, payload []byte) []byte {
	out := make([]byte, 0, len(framePrefix)+idLen(id)+len(key)+len(payload)+1)
	out = append(out, framePrefix...)
	out = appendID(out, id)
	out = append(out, key...)
	out = append(out, payload...)
	return append(out, '}')
}

// errorFrame builds the frame for one error object.
func errorFrame(id string, e *jrpc2.Error) []byte {
	bits, err := json.Marshal(e)
	if err != nil {
		// Only reachable if a handler hand-built an Error whose Data is not
		// valid JSON. Answer something well-formed rather than nothing; the
		// bridge hung the request until its deadline here.
		bits, err = json.Marshal(&jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()})
		if err != nil {
			bits = []byte(`{"code":-32603,"message":"internal error"}`)
		}
	}
	return frame(id, errorKey, bits)
}

// appendID writes the request's id back exactly as the client sent it —
// string, number or null — with one transformation that is NOT optional. The
// id is raw client bytes, escaped today only as a side effect of the
// compaction passes this package deletes, so splicing it plain would reflect
// up to 512KB of client-controlled bytes into a response with no nosniff
// header from a mount with fully open CORS.
//
// LOAD-BEARING: the escaping stdlib's appendCompact(escape=true) applies —
// <, >, & and U+2028/U+2029 — is applied here at the splice. "Simplifying"
// this to a plain append is a security regression, not a saving. Numbers pass
// through untouched, so no id loses a digit and no float round-trip happens.
func appendID(dst []byte, id string) []byte {
	if id == "" {
		return append(dst, nullID...)
	}
	return appendHTMLEscaped(dst, id)
}

// idLen is the number of bytes appendID will write for id.
func idLen(id string) int {
	if id == "" {
		return len(nullID)
	}
	n := len(id)
	for i := range len(id) {
		switch c := id[i]; {
		case c == '<' || c == '>' || c == '&':
			n += 5 // one byte becomes the six of \u003c
		case c == 0xE2 && i+2 < len(id) && id[i+1] == 0x80 && id[i+2]&^1 == 0xA8:
			n += 3 // three bytes become the six of \u2028
		}
	}
	return n
}

const hexDigits = "0123456789abcdef"

// appendHTMLEscaped is encoding/json's appendHTMLEscape, over a string, for the
// one field this package writes without routing through the encoder.
func appendHTMLEscaped(dst []byte, src string) []byte {
	start := 0
	for i := range len(src) {
		c := src[i]
		if c == '<' || c == '>' || c == '&' {
			dst = append(dst, src[start:i]...)
			dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
			start = i + 1
			continue
		}
		// U+2028 and U+2029 are E2 80 A8 and E2 80 A9.
		if c == 0xE2 && i+2 < len(src) && src[i+1] == 0x80 && src[i+2]&^1 == 0xA8 {
			dst = append(dst, src[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[src[i+2]&0xF])
			start = i + 3
		}
	}
	return append(dst, src[start:]...)
}

// writeFrames writes the response body: one frame bare, or the frames joined
// inside a JSON array when the request was a batch, a one-element batch
// included.
//
// LOAD-BEARING: the array is hand-appended. json.Marshal of a
// []json.RawMessage sends every frame through marshalerEncoder, whose
// unconditional appendCompact re-validates and re-copies the whole payload —
// the largest thing this package exists to delete, and an invisible mistake.
func writeFrames(w http.ResponseWriter, isBatch bool, frames [][]byte) {
	var body []byte
	if !isBatch && len(frames) == 1 {
		// The fat case: the frame the dispatcher already built, uncopied.
		body = frames[0]
	} else {
		// '[' + ']' is 2, and n frames carry n-1 commas: 1 + sum(len+1).
		size := 1
		for _, f := range frames {
			size += len(f) + 1
		}
		body = make([]byte, 0, size)
		body = append(body, '[')
		for i, f := range frames {
			if i > 0 {
				body = append(body, ',')
			}
			body = append(body, f...)
		}
		body = append(body, ']')
	}

	// One Write of the finished body, into the duration limiter's buffered
	// writer. The body is fully materialized, so Content-Length is set rather
	// than leaving the client to discover it from a chunked stream.
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	w.Write(body) //nolint:errcheck // nothing actionable remains once the body is committed
}
