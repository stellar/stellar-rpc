// Package wire frames JSON-RPC 2.0 over HTTP. Both mounts use it; it is the
// only framing this repo serves.
//
// It replaced jrpc2's jhttp.Bridge, which is no longer built here. The bridge
// was a loopback, not a framing layer: jhttp.NewBridge builds a jrpc2.Server
// and a jrpc2.Client joined by an in-memory pipe, so every response was
// marshaled by the server, re-parsed by the client, and marshaled twice more
// by the bridge.
// In the 2026-08-30 fat-response profile that cost 10.34s of 18.10s of daemon
// CPU (57.1%) and 3.29GB of 9.36GB allocated (35%), for zero semantic work.
//
// jrpc2 is kept, demoted from a runtime to a parser and a vocabulary:
// ParseRequests does the parse and every shape check, and Request, Error,
// Code and ErrorCode remain the types on the handler contract. This package
// owns exactly one thing the bridge used to own four times over — the
// envelope: one json.Marshal of the handler's result, one hand-appended
// envelope around it, one Write.
//
// Three rules in here are load-bearing and are commented at their site,
// because each has a plausible-looking "improvement" that silently reverses
// this package's reason to exist: json.Marshal + Write (never
// json.NewEncoder), hand-appended batch frames (never json.Marshal of a
// []json.RawMessage), and a batch worker's permit taken before its goroutine
// is started (never inside it).
//
// # The deltas
//
// Seven places answer differently from the bridge. Each is argued where it is
// implemented; they are indexed here so the set is countable, because a
// lettered list whose letters are scattered is exactly the kind of thing that
// silently loses one.
//
//	(a) a malformed body is a -32700 frame over 200, not 500 + plaintext
//	(b) an empty batch `[]` is a -32600 frame, not a 204
//	(c) notifications have finished when their 204 is written
//	(d) an empty-method notification is answered rather than 204'd
//	(e) batch frames come back in input order
//	(f) params reach handlers verbatim rather than re-marshaled
//	(g) dispatch stops at the HTTP deadline; started elements still finish
//
// # What the jrpc2.Server took with it
//
// Handlers run on a context derived from context.Background, which is what
// jrpc2's ServerOptions.NewContext default gave them, so a client hangup still
// does not cancel work in flight. What it does NOT carry is the pair of values
// jrpc2's Server.invoke attached to every handler context: the inbound
// *jrpc2.Request (inboundRequestKey) and the *jrpc2.Server itself (serverKey).
// The delta those two values leave is exactly two accessors:
// jrpc2.InboundRequest(ctx) returns nil where it used to return the request,
// and jrpc2.ServerFromContext(ctx) panics where it used to return the server —
// it is documented to panic on a non-handler context, and this is one.
//
// Neither is fabricated here. There is no jrpc2.Server to hand back, a fake one
// would be a worse answer than a loud one, and a handler that wants its request
// already has it as its second argument.
//
// A third accessor is not about the context at all, and is worse than either,
// because it fails QUIETLY:
// (*jrpc2.Request).IsNotification() is now false for every request, including
// notifications. It tests id == nil, and ParsedRequest.ToRequest builds the id
// as fixID(json.RawMessage(p.ID)) from a string — for an absent id that is an
// empty-but-non-nil RawMessage, which fixID passes through because its isNull
// only matches the four bytes "null". Under the bridge the handler was reached
// through jrpc2's own server, which had the real nil. Notification-ness is
// ParsedRequest.ID == "" here, and route says so at its site.
//
// Nothing in this tree calls any of the three: grepping the names across cmd/
// finds only this paragraph and the test that enforces it, and
// TestNoJRPC2ContextValueCallersInProductionCode fails if that stops being
// true.
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

	// framePrefix opens every response frame. The id follows it immediately,
	// then either ,"result": or ,"error":. Field order matches
	// jrpc2's jmessage.toJSON, which is what the bridge emits today.
	framePrefix = `{"jsonrpc":"2.0","id":`
	resultKey   = `,"result":`
	errorKey    = `,"error":`

	// nullID is the id of a frame answering a request whose own id was
	// absent, null, or unparseable — jhttp's marshalError convention.
	nullID = "null"
)

// These are transcriptions of jrpc2's unexported error vocabulary
// (jrpc2/error.go: errEmptyMethod, errEmptyBatch, errInvalidRequest). They are
// the wire strings the bridge emits today and this mount keeps emitting;
// errNoSuchMethod is built at its one use site in dispatch.
//
// Transcribing an unexported vocabulary is a real, permanent cost: a
// `go get -u` of jrpc2 moves rpcv1's strings and leaves these frozen. That is
// accepted, not overlooked — see the wire-parity tests, which pin every one of
// these strings against the bytes measured from the bridge on 2026-08-30.
var (
	errEmptyMethod         = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty method name"}
	errEmptyBatch          = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty request batch"}
	errInvalidRequestValue = &jrpc2.Error{Code: jrpc2.ParseError, Message: "invalid request value"}
)

// Handler serves JSON-RPC 2.0 over HTTP for one method table.
//
// It is mounted INSIDE the shared middleware chain jsonrpc.NewHandler builds
// for both daemons, in the bridge's place:
// cors -> MaxBytesHandler -> httpRequestDurationLimiter -> BacklogHTTPQLimiter
// -> Handler. That is deliberate. The duration limiter's bufferedResponseWriter
// is the slow-client decoupler: it answers a timed-out client with 504 and
// returns while the handler goroutine is still live, which is only safe because
// nothing has reached the socket; and the buffer is written out after the
// per-method limiters have released and after wrapAdapterRequest's
// `defer view.Release()` has run, so a stalled client holds one flat []byte and
// nothing else — no semaphore, no backlog slot, no store view, no lock.
// rpcv2's deriveLifecycleGrace comment records that its daemon relies on that
// hard deadline. Do not bypass, wrap, or "replace it with a write deadline":
// bufferedResponseWriter has no Unwrap, Flusher, or SetWriteDeadline, so
// http.NewResponseController over it reports ErrNotSupported.
type Handler struct {
	methods map[string]jrpc2.Handler

	// sem bounds concurrent handler+marshal work exactly as jrpc2's Server.sem
	// did: acquired before dispatch, released after the envelope is built and
	// BEFORE the bytes are handed to the (buffered) ResponseWriter, so the
	// socket write stays outside the bound and a slow reader cannot hold a
	// permit.
	//
	// The weight is runtime.GOMAXPROCS(0), which is the one number in here
	// that deliberately does NOT reproduce jrpc2's — its ServerOptions
	// defaults Concurrency to runtime.NumCPU(). Since Go 1.25, and this module
	// is `go 1.26`, the default GOMAXPROCS is derived from the cgroup CPU
	// quota as well as from the machine, while NumCPU still reports the node's
	// CPUs and its affinity mask only. In a container limited to 2 CPUs on a
	// 64-core node the two differ by 32x, and this bound is what caps the
	// number of fat results being marshaled at once: 64 x 17MB rather than
	// 2 x 17MB is the difference between a bound and a decoration. The repo
	// already takes GOMAXPROCS as its parallelism convention — see
	// rpcv2/backfill.DefaultWorkers. It is read once, here, which is enough:
	// the quota that matters is in effect before the daemon binds a port.
	//
	// It is ONE semaphore for the whole handler, and a batch's elements take
	// their permits from it individually, just as jrpc2's Server.invoke did.
	// A batch therefore borrows from the process-wide budget instead of
	// multiplying it — see dispatchAll.
	sem *semaphore.Weighted
}

// NewHandler returns the JSON-RPC handler over methods. The map is the
// decorated, limiter-wrapped table the bridge would otherwise have been given;
// it is read-only from here on.
func NewHandler(methods map[string]jrpc2.Handler) *Handler {
	return &Handler{
		methods: methods,
		sem:     semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0))),
	}
}

// ServeHTTP implements http.Handler.
//
// The HTTP shell is byte-identical to jhttp.Bridge.ServeHTTP: Accept-Post is
// advertised unconditionally (it is set on 200 bodies too), non-POST is 405
// with no body, and a wrong media type or charset is 415 with jhttp's exact
// plaintext.
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
		// A body that could not be read is a transport event, not a JSON-RPC
		// one: there is no request to answer and no id to echo. This is the
		// path MaxBytesHandler takes for a body over the 512KB cap, and it
		// keeps the bridge's 500 + plaintext ("http: request body too large").
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err.Error())
		return
	}

	// req.Context() governs the DISPATCH — whether another element is allowed
	// to start (DELTA (g)) — and nothing else. The HANDLER context is still
	// context.Background, so a client hangup does not cancel work in flight;
	// see invoke, and the package doc for the two jrpc2 server values that
	// context no longer has.
	h.serve(req.Context(), w, body)
}

// serve parses body, dispatches it, and writes the response. ctx is the HTTP
// request's, used only to decide whether to keep dispatching.
func (h *Handler) serve(ctx context.Context, w http.ResponseWriter, body []byte) {
	reqs, err := jrpc2.ParseRequests(body)
	if err != nil {
		// DELTA (a): the bridge answers a malformed body with HTTP 500 and the
		// plaintext "[-32700] invalid request value", which is not a JSON-RPC
		// response at all. A parse failure is exactly what -32700 is for.
		writeFrames(w, false, [][]byte{errorFrame("", parseError(err))})
		return
	}
	if len(reqs) == 0 {
		// DELTA (b): the bridge answers `[]` with 204. The spec (and jrpc2's
		// own server) calls an empty batch an invalid request. A non-batch
		// body always parses to exactly one element, so an empty slice means
		// the body was `[]` and nothing else.
		writeFrames(w, false, [][]byte{errorFrame("", errEmptyBatch)})
		return
	}

	// Batch-ness is a property of the body, carried on every element.
	isBatch := reqs[0].Batch

	frames := h.dispatchAll(ctx, reqs)
	if ctx.Err() != nil {
		// DELTA (g): the request died while we were dispatching, so there is
		// nobody to answer — httpRequestDurationLimiter has already written
		// its 504 and returned, or the client is gone and its buffered body
		// would be discarded. Writing anything here would have to choose
		// between a partial batch array and mistaking an abandoned batch for
		// an all-notification one and 204ing it. Neither is a response; this
		// is not one either, and it is the honest one.
		return
	}
	if len(frames) == 0 {
		// Every element was a notification. DELTA (c): they have all run to
		// completion by the time this 204 is written, because dispatchAll
		// returns only after every handler it started has returned. The bridge
		// fires notifications at its in-process client and 204s immediately,
		// so today a notification is free to the caller and its work races the
		// response.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeFrames(w, isBatch, frames)
}

// dispatchAll answers every element of one parsed body and returns the frames
// in INPUT order (DELTA (e)), with the elements that get no response — the
// notifications — compacted out. It returns only once every handler it started
// has returned, which is what lets serve's 204 mean "the notifications have
// run" rather than "the notifications have been queued".
//
// A batch's elements run CONCURRENTLY, as jrpc2's dispatchLocked ran them.
// Dispatching them serially would sum their latencies under the ONE HTTP
// deadline the mount enforces (httpRequestDurationLimiter, 25s by default), so
// a batch of individually-fine calls would 504 where each call on its own
// succeeds, and would hold its global backlog slot for the sum rather than the
// max. It is not free: a worker costs about 2.6us in goroutine and closure,
// measured on this table, so a batch of instant calls is slower than a serial
// one and a batch of anything real is faster by the semaphore's weight. At a
// weight of 1 the elements serialize anyway and the workers are pure loss —
// accepted rather than special-cased, because a third dispatch path costs more
// than microseconds on a one-CPU deployment saves.
//
// The concurrency is the process-wide semaphore's, not a second budget: a batch
// borrows permits from the same bound a single request takes one from, so N
// elements cannot buy N times the handler parallelism. It also cannot buy N
// goroutines. The permit is acquired HERE, on the serving goroutine, and
// released by the worker; that ordering is the third load-bearing rule in this
// file. Acquire inside the worker instead and every element gets a goroutine
// parked on Acquire — and the body cap bounds elements only by their minimum
// size, so a 512KB body of `[5,5,5,...]` is ~260,000 of them. Acquiring first
// makes the in-flight worker count the semaphore's weight, whatever the batch's
// length; the batch's length still bounds the frames, which is where the
// (pre-existing, bridge-identical) response amplification lives.
//
// DELTA (g): the permit is acquired against the HTTP REQUEST's context, so
// this loop stops feeding the bound the moment the request is dead — when
// httpRequestDurationLimiter has answered 504, or the client has hung up.
// Without that it kept starting elements for ceil(elements/weight) x the
// per-method duration limit AFTER the 504, with the global backlog slot
// BacklogHTTPQLimiter holds sitting inside an abandoned goroutine. The bridge
// did unwind here (jhttp passed req.Context() to Client.Batch, whose
// waitComplete failed every pending response when the context ended), so this
// is parity restored, not a new behavior.
//
// The line it draws is between STARTED and QUEUED, and only queued work is
// canceled:
//
//   - An element already inside its handler runs to completion. Its context is
//     context.Background and nothing cancels it — that is the guarantee
//     sendTransaction depends on, and it is untouched. wg.Wait still joins
//     every worker that started, so the unwind costs at most one element's
//     duration, which is exactly what a single request has always cost.
//   - An element that never got a permit never runs, and produces no frame.
//     There is no answer to give it and nobody to give one to: the 504 is
//     already on the wire. serve writes nothing at all rather than assembling
//     a response out of the survivors.
//
// So a canceled CALL element yields no frame, not an error frame. Building
// one would allocate up to 22MB of answers for a batch whose reader is gone,
// which is the work this delta exists to stop.
func (h *Handler) dispatchAll(ctx context.Context, reqs []*jrpc2.ParsedRequest) [][]byte {
	// One slot per element, filled by index, so the response order is the
	// request order and does not depend on completion order. A nil slot is an
	// element that gets no response.
	frames := make([][]byte, len(reqs))

	if len(reqs) == 1 {
		// A single-element body — every non-batch request, including the fat
		// ones this package exists for — never leaves this goroutine. The
		// saving is a goroutine and a WaitGroup, which is noise beside a 17MB
		// marshal; what this branch really buys is that a single request's
		// panic reaches the limiter's recover with its OWN stack rather than
		// wrapped in a relayedPanic. Deleting it as premature optimization
		// would quietly change that.
		frames[0] = h.dispatchOne(ctx, reqs[0])
	} else {
		h.dispatchBatch(ctx, reqs, frames)
	}
	return slices.DeleteFunc(frames, func(f []byte) bool { return f == nil })
}

// dispatchBatch runs the elements of a multi-element body, writing each one's
// frame into its own slot of frames. See dispatchAll for why the permit is
// taken on this goroutine rather than in the worker, and for what a dead
// request leaves behind.
func (h *Handler) dispatchBatch(ctx context.Context, reqs []*jrpc2.ParsedRequest, frames [][]byte) {
	var wg sync.WaitGroup
	var raised atomic.Pointer[relayedPanic]

	for i, pr := range reqs {
		method, frame := h.route(pr)
		if method == nil {
			// Answered without running anything: a parse or shape error, an
			// empty method, an unknown method, or a silent notification. No
			// permit and no goroutine are spent on it.
			frames[i] = frame
			continue
		}
		if !h.acquirePermit(ctx) {
			// DELTA (g): the request is dead, so this element and every one
			// after it stays unstarted and unanswered. Breaking rather than
			// continuing is the point — routing the rest would build frames
			// for a reader that is already gone.
			break
		}
		//nolint:contextcheck // invoke's context is Background by design; see invoke
		wg.Go(func() {
			// Released once the frame is built and strictly before the caller
			// writes a byte, exactly as dispatchOne releases it — and released
			// on a panicking handler too, because it is deferred.
			defer h.sem.Release(1)
			defer catchPanic(&raised)
			frames[i] = h.invoke(pr, method)
		})
	}
	wg.Wait()

	if p := raised.Load(); p != nil {
		// A panic on a worker goroutine has nothing above it to recover, so it
		// would take the whole process down; the same handler bug on a single
		// request unwinds into httpRequestDurationLimiter's recover, which
		// answers 500 and logs the stack. Re-raising it here, after every
		// sibling has finished, keeps a batch element's panic inside the HTTP
		// request it belongs to instead of the process — the batch's other
		// frames go down with it, which is what the serial dispatch this
		// replaced did too. (No parity is lost:
		// jrpc2 ran batch elements on goroutines it did not recover either, so
		// under the bridge this bug class was a crash.) In this mount it is a
		// safety net and not a live path: every method both daemons register
		// carries a RequestDurationLimit, and network's RPCRequestDurationLimiter
		// recovers a handler panic into its own -32003 error before wire can
		// see it.
		panic(p)
	}
}

// dispatchOne answers one request on the calling goroutine, holding a permit
// across the handler and the marshal and releasing it before it returns the
// finished frame — so the caller writes with no permit held, and a panicking
// handler still releases on the way out.
func (h *Handler) dispatchOne(ctx context.Context, pr *jrpc2.ParsedRequest) []byte {
	method, frame := h.route(pr)
	if method == nil {
		return frame
	}
	if !h.acquirePermit(ctx) {
		return nil // DELTA (g): never started, so there is nothing to answer.
	}
	defer h.sem.Release(1)
	//nolint:contextcheck // invoke's context is Background by design; see invoke
	return h.invoke(pr, method)
}

// acquirePermit takes one permit from the shared bound, blocking until either
// it gets one or ctx ends, and reports which happened.
//
// ctx is the HTTP request's, never the handler's: this is the one place the
// request's lifetime reaches into dispatch (DELTA (g)). Acquire fails only on
// a canceled context and never keeps a permit it won in the race — x/sync
// checks Done before, after, and while waiting, releasing the tokens if it has
// to — so false here always means "this element did not run, and the request
// it belongs to is over".
func (h *Handler) acquirePermit(ctx context.Context) bool {
	return h.sem.Acquire(ctx, 1) == nil
}

// route decides how one parsed request is answered, WITHOUT running anything:
// it returns the handler to invoke, or a nil handler and the finished frame —
// which is itself nil for a request that gets no response at all.
//
// Order is load-bearing and is not jrpc2's stated order, because jrpc2's
// composite behavior is not its stated order either:
//
//  1. Parse and shape errors answer with a frame even for a notification —
//     that is what the bridge does today (it filters invalid elements before
//     it ever looks at notification-ness) and what the spec requires. jrpc2's
//     own responses() reports a notification's error only when its code is
//     ParseError or InvalidRequest; every error ParseRequests can attach to an
//     element carries one of those two codes, so reporting them all is the
//     same set, not a wider one.
//  2. An empty method name is the same class of request-level protocol error,
//     so it answers with a frame too. DELTA (d): today an empty-method
//     NOTIFICATION returns 204, and only by accident — jrpc2's server does
//     emit the InvalidRequest frame, but with an empty id, so the bridge's
//     in-process client discards it as a response to an unknown id and the
//     bridge sees zero results.
//  3. Only then is notification-ness applied, and only to DISPATCH results:
//     an unknown method or a handler error on a notification is silent.
//
// Notification-ness is decided on ParsedRequest.ID, never Request.IsNotification:
// ToRequest builds the id with fixID(json.RawMessage(p.ID)) from a string, so
// for an absent id it yields an empty-but-non-nil RawMessage and
// IsNotification (which tests id == nil) cannot be trusted.
func (h *Handler) route(pr *jrpc2.ParsedRequest) (jrpc2.Handler, []byte) {
	if pr.Error != nil {
		return nil, errorFrame(pr.ID, pr.Error)
	}
	if pr.Method == "" {
		return nil, errorFrame(pr.ID, errEmptyMethod)
	}
	// `ok && method != nil`: route says "run this" by returning a non-nil
	// handler, so a nil entry in the table must not be able to say it. The
	// mount cannot register one (handler.New never returns nil), and answering
	// -32601 for one is a better failure than dropping the element silently.
	if method, ok := h.methods[pr.Method]; ok && method != nil {
		return method, nil
	}
	if pr.ID == "" {
		return nil, nil // an unknown method on a notification is silent
	}
	// jrpc2's own errNoSuchMethod frame, byte for byte: code -32601, message
	// "method not found", data the method name. Resolving it here means no
	// jrpc2.Server ever sees the request, which is also why the library's
	// unknown-method in-flight-map leak is unreachable from this serving path.
	// No handler is invoked and no metric is labeled, so an attacker-chosen
	// method name cannot grow the per-method summary's cardinality either.
	return nil, errorFrame(pr.ID, (&jrpc2.Error{
		Code:    jrpc2.MethodNotFound,
		Message: jrpc2.MethodNotFound.String(),
	}).WithData(pr.Method))
}

// invoke runs one handler and builds its frame. The caller holds the permit
// for the whole call and releases it before any byte is written.
func (h *Handler) invoke(pr *jrpc2.ParsedRequest, method jrpc2.Handler) []byte {
	// context.Background, never the http.Request's context, and never a
	// timeout of our own: this reproduces jrpc2's ServerOptions.NewContext
	// default byte for byte. Handlers keep today's hangup semantics (a client
	// that disconnects mid-call still completes it, which matters for
	// sendTransaction) and today's deadline shape (the per-method duration
	// limiter derives its own timeout from this). Deriving from r.Context()
	// is a separate, deliberate decision that has not been taken. The two
	// values jrpc2's server used to attach to this context are gone with it;
	// see the package doc.
	ctx := context.Background()

	// ToRequest hands the handler the params bytes the body carried, in the
	// client's key order and whitespace and with the client's own escapes.
	// DELTA (f): the bridge re-marshaled them on the way in — Client.req ->
	// marshalParams -> json.Marshal of a json.RawMessage, which compacts and
	// HTML-escapes — so a handler used to see `{"a":"x\u003cy"}` where it now
	// sees `{"a": "x<y"}`. Both decode to the same value, and every handler
	// here decodes (including getEventsV2, the one that reads ParamString
	// rather than UnmarshalParams), so the only visible difference is that the
	// debug log's "params" field now shows what the client actually sent.
	result, err := method(ctx, pr.ToRequest())
	if pr.ID == "" {
		// "The Server MUST NOT reply to a Notification, including those that
		// are within a batch request." The work is done; the answer is not
		// sent. Errors are dropped exactly as jrpc2's invoke drops them.
		return nil
	}
	if err != nil {
		return errorFrame(pr.ID, handlerError(err))
	}

	// THE one marshal. json.Marshal, never json.NewEncoder(w).Encode:
	//   - Encode's deferred encodeStatePool.Put fires after enc.w.Write, so N
	//     stalled clients would pin N full-size pooled buffers; Marshal
	//     returns its state to the pool before the caller writes a byte.
	//   - under GOEXPERIMENT=jsonv2 (present in this toolchain, one env var
	//     away) Encoder.Encode marshals into its own buffer and then copies
	//     that into w — reintroducing precisely the copy this package deletes,
	//     byte-identically, with no test able to see it.
	// Marshal+Write is stable across both encoder generations and lets
	// Content-Length be set.
	bits, err := json.Marshal(result)
	if err != nil {
		// Through handlerError, not a hand-built InternalError: jrpc2's own
		// invoke returns the marshal failure as the task's error, so
		// tasks.responses runs it down the SAME ladder a handler error takes
		// and answers -32098 (SystemError), never -32603. Measured against the
		// live bridge, both for an unmarshalable value and for a MarshalJSON
		// that fails.
		return errorFrame(pr.ID, handlerError(err))
	}
	return frame(pr.ID, resultKey, bits)
}

// relayedPanic is a handler panic caught on a batch worker so the serving
// goroutine can re-raise it. It carries the stack the handler actually
// panicked on, because the recover that ends up logging it runs on a different
// goroutine and debug.Stack() there would show only the relay.
type relayedPanic struct {
	value any
	stack []byte
}

// String is what httpRequestDurationLimiter's "%v" of the recovered value
// prints, so the original panic and its stack land in the log.
func (p *relayedPanic) String() string {
	return fmt.Sprintf("%v (panicked on a batch worker goroutine)\n%s", p.value, p.stack)
}

// catchPanic must be deferred directly, so that its recover is the worker's.
// The first panic wins; later ones are dropped, exactly as only one panic can
// be re-raised.
func catchPanic(raised *atomic.Pointer[relayedPanic]) {
	if v := recover(); v != nil {
		raised.CompareAndSwap(nil, &relayedPanic{value: v, stack: debug.Stack()})
	}
}

// handlerError maps a handler's error onto the wire error object, reproducing
// jrpc2's tasks.responses exactly — including its bare type assertion. A
// WRAPPED *jrpc2.Error deliberately does not pass through verbatim: it falls to
// the ErrorCode branch, which keeps the code but takes the wrapper's message.
//
// One repo-local consequence worth naming, because it looks like a bug and is
// pinned as behavior: network.ErrRequestExceededProcessingLimitThreshold and
// ErrFailToProcessDueToInternalIssue are declared as jrpc2.Error VALUES, not
// pointers, so the assertion below misses them; ErrorCode's errors.As(ErrCoder)
// branch then supplies the code while err.Error() supplies the message — which
// for jrpc2.Error is "[%d] %s". The wire message therefore carries a redundant
// "[-32001] " prefix, and has for as long as the limiter has existed.
func handlerError(err error) *jrpc2.Error {
	if e, ok := err.(*jrpc2.Error); ok { //nolint:errorlint // jrpc2 asserts, it does not unwrap
		return e
	}
	if code := jrpc2.ErrorCode(err); code != jrpc2.NoError {
		return &jrpc2.Error{Code: code, Message: err.Error()}
	}
	return &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
}

// parseError recovers the *jrpc2.Error ParseRequests reports for a body that is
// not valid JSON. jrpc2 returns its unexported errInvalidRequest there, which
// is already the right object; the fallback only guards a future library that
// returns something else.
func parseError(err error) *jrpc2.Error {
	if e, ok := err.(*jrpc2.Error); ok { //nolint:errorlint // matching jrpc2's own assertion style
		return e
	}
	return errInvalidRequestValue
}

// frame builds {"jsonrpc":"2.0","id":<id>,<key><payload>}, where key is
// resultKey or errorKey. id is the request's raw id text, or "" for a request
// with no usable id, which answers with a null id per jhttp's marshalError.
//
// The capacity is the frame's exact byte length, so a frame is one allocation
// and one copy — which is why the arithmetic and the appends it predicts live
// in the same six lines. Getting it wrong is not a correctness bug and no
// assertion on the RESPONSE can see it: append grows silently and the bytes
// come out identical. It is an expensive bug, though — an append that outgrows
// its capacity reallocates and re-copies the whole payload, which on a fat
// getLedgers response is another 17MB moved. TestFrameIsExactlyOneAllocation
// pins it the only way it is observable, on cap.
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
		// valid JSON. Answer something well-formed rather than nothing — an
		// improvement, not parity: the bridge's server-side encode failed on
		// the same input, which killed its channel, so ServeHTTP never
		// returned and the request hung until the deadline.
		bits, err = json.Marshal(&jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()})
		if err != nil {
			bits = []byte(`{"code":-32603,"message":"internal error"}`)
		}
	}
	return frame(id, errorKey, bits)
}

// appendID writes the request's id back exactly as the client sent it —
// string, number, or null — with one transformation, which is not optional.
//
// The id text is raw client bytes: ParseRequests keeps the id as the
// json.RawMessage the body carried, and validates only that its first byte
// looks like a string or a number. jrpc2's own jmessage.toJSON splices those
// bytes into the response unescaped; today's responses are nonetheless escaped,
// but only as a side effect of the two compaction passes this package deletes.
// Delete them and stop escaping here and up to 512KB of client-controlled bytes
// are reflected verbatim into a response that carries no nosniff header, from a
// mount with fully open CORS.
//
// So the same escaping the stdlib's appendCompact(escape=true) applies is
// applied here, at the splice: <, >, & and U+2028/U+2029 inside the token. That
// makes the id byte-identical to what the bridge emits today, and it is the one
// place where hand-building the envelope would have been a security regression
// rather than a saving. Anyone "simplifying" this to a plain append is
// reintroducing that regression.
//
// Numbers pass through untouched — none of the five characters can occur in a
// JSON number — so a large integer id keeps every digit and a fractional id
// keeps its exact text. No float round-trip happens anywhere on this path.
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
// inside a JSON array when the request was a batch. A one-element batch stays
// an array, matching jhttp.encodeResponses.
//
// The array is hand-appended. json.Marshal of a []json.RawMessage would send
// every frame through marshalerEncoder, whose unconditional appendCompact
// re-validates and re-copies the whole payload — that pass is 4.64s of the
// profile and the single largest thing this package exists to delete. It is an
// easy and completely invisible mistake to make here.
func writeFrames(w http.ResponseWriter, isBatch bool, frames [][]byte) {
	var body []byte
	if !isBatch && len(frames) == 1 {
		// The single-frame case is the fat one, and it writes the frame the
		// dispatcher already built: no second copy of a 17MB body.
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
	// writer. Content-Length is set for the same reason jhttp.writeJSON sets
	// it: the body is fully materialized, so the client should not have to
	// discover its length from a chunked stream.
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	w.Write(body) //nolint:errcheck // nothing actionable remains once the body is committed
}
