// Package wire frames JSON-RPC 2.0 over HTTP; both mounts use it.
//
// Three things here look like they could be written more idiomatically, and
// must not be:
//
//   - Results are marshaled with json.Marshal and written with one Write.
//     Switching to json.NewEncoder(w).Encode re-adds a full copy of every
//     large response under GOEXPERIMENT=jsonv2, silently.
//   - Batch responses are assembled by appending frames between brackets.
//     json.Marshal of a []json.RawMessage makes the stdlib re-validate and
//     re-copy every frame — the multi-pass waste this package exists to
//     remove.
//   - A batch worker's semaphore permit is acquired before its goroutine is
//     started. Acquired inside the goroutine, every element parks a
//     goroutine on the semaphore, and one request body fits ~260,000
//     elements.
//
// # The deltas
//
//	(a) a malformed body is a -32700 frame over 200, not 500 + plaintext
//	(b) an empty batch `[]` is a -32600 frame, not a 204
//	(c) notifications have finished, or been answered-at-budget and abandoned
//	    per the limiter's contract, before the 204
//	(d) an empty-method notification is answered rather than 204'd
//	(e) batch frames come back in input order
//	(f) params reach handlers verbatim rather than re-marshaled
//	(g) dispatch stops at the HTTP deadline; started elements still finish
//
// Handlers run on a SERVER-scoped context, Background-derived, so a client's
// disconnect does not cancel a handler; the method budget does, and at
// teardown Handler.Shutdown does. Three jrpc2 accessors are unusable, and a
// guard test pins that: InboundRequest returns nil, ServerFromContext panics,
// and IsNotification is false for EVERY request — that one QUIETLY, so
// notification-ness is ParsedRequest.ID == "".
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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
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

// Transcribed from jrpc2's unexported vocabulary; byte-exact tests notice drift.
var (
	errEmptyMethod         = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty method name"}
	errEmptyBatch          = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty request batch"}
	errInvalidRequestValue = &jrpc2.Error{Code: jrpc2.ParseError, Message: "invalid request value"}
)

// Handler serves JSON-RPC 2.0 for one method table, mounted INSIDE the chain
// jsonrpc.NewHandler builds: cors -> MaxBytesHandler ->
// httpRequestDurationLimiter -> BacklogHTTPQLimiter -> Handler. Do not
// bypass or wrap the duration limiter: it is the slow-client decoupler, has no
// Unwrap/Flusher/SetWriteDeadline, and deriveLifecycleGrace needs its deadline.
type Handler struct {
	methods map[string]jrpc2.Handler

	// sem bounds handler+marshal work, released BEFORE the bytes reach the
	// (buffered) ResponseWriter so a slow reader cannot hold a permit. ONE for
	// the whole handler, so a batch borrows from the budget rather than
	// multiplying it. Weight is GOMAXPROCS (cgroup-aware), not NumCPU; it caps
	// concurrent fat marshals; see backfill.DefaultWorkers.
	//
	// It bounds DISPATCHES, not handler goroutines: a request that outruns its
	// per-method budget gets its permit back while its handler runs on, by
	// design (network.RPCRequestDurationLimiter, and the graceMargin that
	// exists for that gap), so goroutines executing handler code can exceed
	// this by the number of timed-out requests. Shutdown joins them, and what
	// BOUNDS them is the per-method backlog limiter nested inside the duration
	// limiter (jsonrpc.wrapWithLimiters), whose slot is held across the real
	// call: QueueLimit per method, ~10,700 aggregate at v2 defaults. Far too
	// loose to be an operative memory bound, and invisible from here.
	sem *semaphore.Weighted

	// weight is sem's size; Weighted does not expose it and Shutdown drains by
	// acquiring all of it.
	weight int64

	// root is the SERVER-scoped lifetime handlers run on. Only Shutdown cancels
	// it, which is what stops a straggler reading an already-closed store.
	//
	//nolint:containedctx // a server's own lifetime, as http.Server holds one
	root     context.Context
	stopRoot context.CancelFunc

	// liveHandlers is every handler execution under a budgeted method, not
	// just the abandoned ones (see network.RPCRequestDurationLimiter). An
	// abandoned one has given its permit back, so the bound cannot see it and
	// Shutdown joins the group separately.
	liveHandlers *network.LiveHandlers

	drainOnce sync.Once
	drainErr  error
}

// NewHandler returns the handler over methods, read-only from here on.
// liveHandlers must be the SAME group the method table's duration limiters
// count into — jsonrpc.NewHandler is the one place that has both.
func NewHandler(methods map[string]jrpc2.Handler, liveHandlers *network.LiveHandlers) *Handler {
	weight := int64(runtime.GOMAXPROCS(0))
	//nolint:gosec // G118: stopRoot is the handler's, called by Shutdown
	root, stopRoot := context.WithCancel(context.Background())
	if liveHandlers == nil {
		panic("wire: NewHandler needs the mount's LiveHandlers; " +
			"jsonrpc.NewHandler is the one place that builds it")
	}
	return &Handler{
		methods:      methods,
		sem:          semaphore.NewWeighted(weight),
		weight:       weight,
		root:         root,
		stopRoot:     stopRoot,
		liveHandlers: liveHandlers,
	}
}

// Shutdown cancels the root and waits for every handler this mount can still
// be running, in two steps, because two things can be running one: it acquires
// the whole bound, which IS "no dispatch is in flight" since every invocation
// holds a permit, and then joins the handlers a per-method duration limiter
// abandoned at its timeout, which gave their permits back long ago.
//
// THREE premises, none of them local. The caller closes its connections FIRST
// — DELTA (g) gates an element's start on the REQUEST's context, so a live
// connection keeps authorizing starts and the drain chases a moving target.
// The method table's limiters count into the same group this handler was
// built with. And the full-weight Acquire converges against an in-flight batch
// only because x/sync queues a full-weight waiter ahead of the Acquire(1)s
// behind it.
//
// The permits are never returned. An error means a straggler is still touching
// what the caller is about to close. Idempotent; call it after the HTTP server
// stops accepting and before the handlers' resources are closed.
func (h *Handler) Shutdown(ctx context.Context) error {
	h.stopRoot()
	h.drainOnce.Do(func() {
		if err := h.sem.Acquire(ctx, h.weight); err != nil {
			h.drainErr = fmt.Errorf("wire: draining handlers: %w", err)
			return
		}
		// Only now: every permit is held, so no wrapper is running and none
		// can launch another child behind the Wait.
		if err := joinLiveHandlers(ctx, h.liveHandlers); err != nil {
			h.drainErr = fmt.Errorf("wire: joining timed-out handlers: %w", err)
		}
	})
	return h.drainErr
}

// joinLiveHandlers waits for wg, or for ctx to end. The waiter outlives this call
// when ctx wins; it ends when the stragglers do.
func joinLiveHandlers(ctx context.Context, wg *network.LiveHandlers) error {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ServeHTTP implements http.Handler. The shell is byte-identical to
// jhttp.Bridge's: Accept-Post always, non-POST 405, bad media type 415.
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
		// A transport event: no request to answer and no id to echo.
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err.Error())
		return
	}

	// req.Context() governs the DISPATCH only (DELTA (g)); handlers run on
	// h.root.
	h.serve(req.Context(), w, body)
}

// serve parses, dispatches and writes. ctx is the HTTP request's, used only
// to decide whether to keep dispatching.
func (h *Handler) serve(ctx context.Context, w http.ResponseWriter, body []byte) {
	reqs, err := jrpc2.ParseRequests(body)
	if err != nil {
		// DELTA (a).
		writeFrames(w, false, [][]byte{errorFrame("", parseError(err))})
		return
	}
	if len(reqs) == 0 {
		// DELTA (b). A non-batch body parses to exactly one element.
		writeFrames(w, false, [][]byte{errorFrame("", errEmptyBatch)})
		return
	}

	isBatch := reqs[0].Batch // a property of the body, on every element

	frames := h.dispatchAll(ctx, reqs)
	if ctx.Err() != nil {
		// DELTA (g). Nobody to answer, and no honest answer to give.
		return
	}
	if len(frames) == 0 {
		// All notifications, and DELTA (c) — each has finished, or its
		// method budget expired and the limiter abandoned it. dispatchAll
		// joins the dispatches, not the limiter's children.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeFrames(w, isBatch, frames)
}

// dispatchAll answers every element in INPUT order (DELTA (e)), joining every
// handler it started.
//
// LOAD-BEARING: the permit is acquired HERE and released by the worker.
// Acquire inside the worker and every element parks a goroutine on Acquire —
// one body fits ~260,000 elements under the cap.
//
// Two contexts govern a dispatch: the REQUEST's decides whether an element may
// START (DELTA (g)), the root whether a running handler may CONTINUE. An
// element that never started yields NO FRAME, not an error one.
func (h *Handler) dispatchAll(ctx context.Context, reqs []*jrpc2.ParsedRequest) [][]byte {
	// By index, so response order is request order. A nil slot gets no answer.
	frames := make([][]byte, len(reqs))

	if len(reqs) == 1 {
		// Not an optimization: staying here lets a panic reach the limiter's
		// recover with its OWN stack.
		frames[0] = h.dispatchOne(ctx, reqs[0])
	} else {
		h.dispatchBatch(ctx, reqs, frames)
	}
	return slices.DeleteFunc(frames, func(f []byte) bool { return f == nil })
}

// dispatchBatch runs a multi-element body; see dispatchAll for the rules.
func (h *Handler) dispatchBatch(ctx context.Context, reqs []*jrpc2.ParsedRequest, frames [][]byte) {
	var wg sync.WaitGroup
	var raised atomic.Pointer[relayedPanic]

	// Read once: ctx.Err() takes a lock per call, a channel probe does not.
	dead := ctx.Done()

	// Errors answerable without a handler, by index, built below rather than
	// here: one body can carry ~260,000 of them and ~23MB of frames.
	var static []*jrpc2.Error

	for i, pr := range reqs {
		if isClosed(dead) {
			// DELTA (g) for the elements that never reach acquirePermit.
			break
		}
		method, rerr := h.route(pr)
		if method == nil {
			if rerr != nil {
				if static == nil {
					static = make([]*jrpc2.Error, len(reqs))
				}
				static[i] = rerr
			}
			continue // no permit and no goroutine spent on it
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

	// ONE permit for every static frame, not one per element: a hostile body
	// would otherwise put ~260,000 acquire/release cycles through a semaphore
	// every legitimate request shares. Safe to take here — this goroutine
	// holds nothing, so it cannot deadlock against the workers above. The
	// parse that precedes it is ~7x larger for the same body and is bounded by
	// the backlog limit times the body cap, not by this; that is pre-existing
	// and true of the bridge too.
	if static != nil && !isClosed(dead) && h.acquirePermit(ctx) {
		for i, rerr := range static {
			if rerr != nil {
				frames[i] = errorFrame(reqs[i].ID, rerr)
			}
		}
		h.sem.Release(1)
	}
	wg.Wait()

	if p := raised.Load(); p != nil {
		// Nothing above a worker recovers, so an escaping panic would take the
		// process down; re-raised here it fails only this request.
		panic(p)
	}
}

// dispatchOne answers one request here, releasing its permit before returning
// so the caller writes without one, panics included.
func (h *Handler) dispatchOne(ctx context.Context, pr *jrpc2.ParsedRequest) []byte {
	method, rerr := h.route(pr)
	if method == nil {
		if rerr == nil {
			return nil
		}
		// One frame, so it is built here rather than deferred as a batch's are.
		return errorFrame(pr.ID, rerr)
	}
	if !h.acquirePermit(ctx) {
		return nil // DELTA (g): never started, so there is nothing to answer.
	}
	defer h.sem.Release(1)
	return h.invoke(pr, method)
}

// acquirePermit takes one permit, blocking until it gets one or ctx ends.
// x/sync gives back a permit won in the race, so false means "did not run".
func (h *Handler) acquirePermit(ctx context.Context) bool {
	return h.sem.Acquire(ctx, 1) == nil
}

func isClosed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// route decides how one request is answered WITHOUT running anything: the
// handler, or a nil handler and the error to answer with (itself nil when the
// request gets no response). It returns the error rather than the frame so the
// caller can choose when to pay for the bytes — see dispatchBatch.
//
// The ORDER is load-bearing — parse and shape errors answer even for a
// notification, then an empty method (DELTA (d)), and only then does
// notification-ness apply, to DISPATCH results only.
func (h *Handler) route(pr *jrpc2.ParsedRequest) (jrpc2.Handler, *jrpc2.Error) {
	if pr.Error != nil {
		return nil, pr.Error
	}
	if pr.Method == "" {
		return nil, errEmptyMethod
	}
	// `method != nil` too: a nil table entry must not read as "run this".
	if method, ok := h.methods[pr.Method]; ok && method != nil {
		return method, nil
	}
	if pr.ID == "" {
		return nil, nil // an unknown method on a notification is silent
	}
	// Resolved here, so no metric is labeled: an attacker-chosen method name
	// cannot grow the summary's cardinality.
	return nil, (&jrpc2.Error{
		Code:    jrpc2.MethodNotFound,
		Message: jrpc2.MethodNotFound.String(),
	}).WithData(pr.Method)
}

// invoke runs one handler and frames it; the caller holds the permit.
func (h *Handler) invoke(pr *jrpc2.ParsedRequest, method jrpc2.Handler) []byte {
	// The SERVER's lifetime, so a client's disconnect does not cancel a
	// handler — the METHOD BUDGET does, via the WithTimeout the per-method
	// duration limiter derives from this. That is the property
	// sendTransaction depends on: nothing the caller does mid-call decides
	// whether the submission happens.
	ctx := h.root

	// DELTA (f): the params bytes go over verbatim, and rpcv2's getEventsV2
	// reads ParamString, so those raw bytes are contract.
	result, err := method(ctx, pr.ToRequest())
	if pr.ID == "" {
		// "The Server MUST NOT reply to a Notification, including those that
		// are within a batch request." Deliberately NOT joined here: waiting
		// for an over-budget notification would tie the 204 and its backlog
		// slot to straggler duration — the coupling the duration limiter
		// exists to break — for a difference no notification client can see.
		return nil
	}
	if err != nil {
		return errorFrame(pr.ID, handlerError(err))
	}

	// THE one marshal. LOAD-BEARING: never json.NewEncoder(w).Encode — its
	// pool Put fires after the Write, so N stalled clients pin N pooled
	// buffers, and under jsonv2 it copies through its own buffer.
	bits, err := json.Marshal(result)
	if err != nil {
		// Through handlerError: a marshal failure is -32098, never -32603.
		return errorFrame(pr.ID, handlerError(err))
	}
	return frame(pr.ID, resultKey, bits)
}

// relayedPanic carries a worker's panic and the stack it panicked on, not the
// relay's, to the serving goroutine.
type relayedPanic struct {
	value any
	stack []byte
}

// String is what the limiter's "%v" of the recovered value prints.
func (p *relayedPanic) String() string {
	return fmt.Sprintf("%v (panicked on a batch worker goroutine)\n%s", p.value, p.stack)
}

// catchPanic must be deferred directly, so its recover is the worker's.
func catchPanic(raised *atomic.Pointer[relayedPanic]) {
	if v := recover(); v != nil {
		raised.CompareAndSwap(nil, &relayedPanic{value: v, stack: debug.Stack()})
	}
}

// handlerError reproduces jrpc2's tasks.responses exactly, bare type assertion
// included. Pinned, not a bug: network's limiter sentinels are jrpc2.Error
// VALUES, so the assertion misses them and their message keeps its own
// "[-32001] " prefix.
func handlerError(err error) *jrpc2.Error {
	if e, ok := err.(*jrpc2.Error); ok { //nolint:errorlint // jrpc2 asserts, it does not unwrap
		return e
	}
	if code := jrpc2.ErrorCode(err); code != jrpc2.NoError {
		return &jrpc2.Error{Code: code, Message: err.Error()}
	}
	return &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
}

// parseError recovers the *jrpc2.Error ParseRequests reports for bad JSON.
func parseError(err error) *jrpc2.Error {
	if e, ok := err.(*jrpc2.Error); ok { //nolint:errorlint // matching jrpc2's own assertion style
		return e
	}
	return errInvalidRequestValue
}

// frame builds {"jsonrpc":"2.0","id":<id>,<key><payload>}; id is the raw id
// text, or "" for one with no usable id, which answers a null id. The capacity
// must be the frame's EXACT length — nothing about the response can catch it
// being wrong, so TestFrameIsExactlyOneAllocation pins it on cap.
func frame(id, key string, payload []byte) []byte {
	out := make([]byte, 0, len(framePrefix)+idLen(id)+len(key)+len(payload)+1)
	out = append(out, framePrefix...)
	out = appendID(out, id)
	out = append(out, key...)
	out = append(out, payload...)
	return append(out, '}')
}

func errorFrame(id string, e *jrpc2.Error) []byte {
	bits, err := json.Marshal(e)
	if err != nil {
		// Only reachable for an Error whose Data is not valid JSON.
		bits, err = json.Marshal(&jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()})
		if err != nil {
			bits = []byte(`{"code":-32603,"message":"internal error"}`)
		}
	}
	return frame(id, errorKey, bits)
}

// appendID writes the id back exactly as the client sent it, LOAD-BEARINGLY
// applying stdlib appendCompact(escape=true)'s escaping — <, >, & and
// U+2028/U+2029 — at the splice. "Simplifying" this to a plain append reflects
// up to 512KB of client-controlled bytes into a response with no nosniff
// header from a mount with open CORS. Numbers pass through untouched.
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

// appendHTMLEscaped is encoding/json's appendHTMLEscape, over a string.
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

// writeFrames writes the body: one frame bare, or the frames joined inside a
// JSON array for a batch, a one-element batch included. LOAD-BEARING: the
// array is hand-appended — json.Marshal of a []json.RawMessage sends every
// frame through marshalerEncoder, which re-copies the whole payload.
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

	// One Write; the body is materialized, so Content-Length is set.
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	w.Write(body) //nolint:errcheck // nothing actionable remains once the body is committed
}
