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
// Deltas from the retired jhttp.Bridge, argued at their sites:
//
//	(a) a malformed body is a -32700 frame over 200, not 500 + plaintext
//	(b) an empty batch is a -32600 frame, not a 204
//	(c) a notification has finished before its 204, unless its budget expired
//	(d) an empty-method notification is answered, not 204'd
//	(e) batch frames come back in input order
//	(f) params reach handlers verbatim, not re-marshaled
//	(g) dispatch stops at the HTTP deadline; started elements finish
//
// Handlers run on a server-scoped context: no request cancels it, the method
// budget and Shutdown do. jrpc2.InboundRequest returns nil, ServerFromContext
// panics, and IsNotification is false for every request; notification-ness is
// ParsedRequest.ID == "". A guard test pins all three.
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

	// framePrefix opens every frame; the id follows, then resultKey or
	// errorKey. Field order matches jrpc2's jmessage.toJSON.
	framePrefix = `{"jsonrpc":"2.0","id":`
	resultKey   = `,"result":`
	errorKey    = `,"error":`

	// nullID answers a request whose id was absent, null or unparseable.
	nullID = "null"
)

// Transcribed from jrpc2's unexported error vocabulary. An upgrade that moves
// one of the originals leaves these frozen; the byte-exact tests notice.
var (
	errEmptyMethod         = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty method name"}
	errEmptyBatch          = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty request batch"}
	errInvalidRequestValue = &jrpc2.Error{Code: jrpc2.ParseError, Message: "invalid request value"}
)

// Handler serves JSON-RPC 2.0 for one method table, mounted last in the chain
// jsonrpc.NewHandler builds. Do not bypass or wrap the duration limiter above
// it: it abandons slow clients, safe only while nothing beneath it has written
// to the socket, and rpcv2's deriveLifecycleGrace reads its deadline.
type Handler struct {
	methods map[string]jrpc2.Handler

	// sem bounds handler and marshal work, one for the whole handler, released
	// before any byte reaches the ResponseWriter. Weight is GOMAXPROCS, which
	// follows a container's CPU quota. It counts dispatches, not handler
	// goroutines: a request past its budget returns its permit and keeps
	// running, bounded instead by QueueLimit.
	sem *semaphore.Weighted

	// weight is sem's size; Weighted does not expose it, and Shutdown drains by
	// acquiring all of it.
	weight int64

	// root is the server-scoped context handlers run on. No request cancels it;
	// Shutdown does, which is what stops a straggler reading a closed store.
	//
	//nolint:containedctx // a server's own lifetime, as http.Server holds one
	root     context.Context
	stopRoot context.CancelFunc

	// liveHandlers counts every handler execution under a budgeted method, not
	// only the abandoned ones. An abandoned one has returned its permit, so sem
	// cannot see it and Shutdown joins this group separately.
	liveHandlers *network.LiveHandlers

	drainOnce sync.Once
	drainErr  error
}

// NewHandler returns the handler over methods, read-only from here on.
// liveHandlers must be the group the method table's duration limiters count
// into; jsonrpc.NewHandler is the only caller that holds both ends.
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

// Shutdown cancels the root, acquires the whole bound (every invocation holds
// a permit, so that is "no dispatch in flight"), then joins liveHandlers for
// the handlers a duration limiter abandoned. Permits are not returned; an
// error means a straggler is still running. Idempotent. The caller must close
// its connections first, or live requests keep starting elements; convergence
// relies on x/sync queueing a full-weight waiter ahead of Acquire(1)s.
func (h *Handler) Shutdown(ctx context.Context) error {
	h.stopRoot()
	h.drainOnce.Do(func() {
		if err := h.sem.Acquire(ctx, h.weight); err != nil {
			h.drainErr = fmt.Errorf("wire: draining handlers: %w", err)
			return
		}
		// Safe only with every permit held: no limiter is running, so none can
		// add to the group behind the Wait.
		if err := joinLiveHandlers(ctx, h.liveHandlers); err != nil {
			h.drainErr = fmt.Errorf("wire: joining timed-out handlers: %w", err)
		}
	})
	return h.drainErr
}

// joinLiveHandlers waits for wg or for ctx, whichever first. On ctx the waiter
// outlives this call and ends when the stragglers do.
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

// ServeHTTP implements http.Handler, byte for byte as jhttp.Bridge did:
// Accept-Post on every response, non-POST 405, wrong media type or charset 415.
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
		// No request to answer and no id to echo. A body over the size cap
		// also arrives here.
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err.Error())
		return
	}

	// The request context decides only whether dispatch keeps starting
	// elements; handlers run on h.root.
	h.serve(req.Context(), w, body)
}

// serve parses, dispatches and writes. ctx is the request's, used only to
// decide whether dispatch continues.
func (h *Handler) serve(ctx context.Context, w http.ResponseWriter, body []byte) {
	reqs, err := jrpc2.ParseRequests(body)
	if err != nil {
		// Delta (a).
		writeFrames(w, false, [][]byte{errorFrame("", parseError(err))})
		return
	}
	if len(reqs) == 0 {
		// Delta (b). A non-batch body parses to exactly one element, so an
		// empty result means the body was `[]`.
		writeFrames(w, false, [][]byte{errorFrame("", errEmptyBatch)})
		return
	}

	// Batch-ness is a property of the body, recorded on every element.
	isBatch := reqs[0].Batch

	frames := h.dispatchAll(ctx, reqs)
	if ctx.Err() != nil {
		// Delta (g). Nobody to answer; a partial array and a 204 are both
		// wrong.
		return
	}
	if len(frames) == 0 {
		// Delta (c). Each has finished, unless its budget expired first:
		// dispatchAll joins dispatches, not what a limiter abandoned.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeFrames(w, isBatch, frames)
}

// dispatchAll answers every element in input order (delta (e)) and returns
// only once every handler it started has. A batch's elements run concurrently;
// serially they would sum their durations under one HTTP deadline. The request
// context decides whether an element may start (delta (g)), the root whether a
// running handler continues. An element that never started yields no frame.
func (h *Handler) dispatchAll(ctx context.Context, reqs []*jrpc2.ParsedRequest) [][]byte {
	// One slot per element, by index, so response order does not depend on
	// completion order. A nil slot gets no answer.
	frames := make([][]byte, len(reqs))

	if len(reqs) == 1 {
		// Not an optimization: staying here lets a panic reach the limiter's
		// recover with its own stack rather than a relayedPanic.
		frames[0] = h.dispatchOne(ctx, reqs[0])
	} else {
		h.dispatchBatch(ctx, reqs, frames)
	}
	return slices.DeleteFunc(frames, func(f []byte) bool { return f == nil })
}

// dispatchBatch runs a multi-element body into its own slots of frames. See
// dispatchAll for the permit ordering and the dead-request rule.
func (h *Handler) dispatchBatch(ctx context.Context, reqs []*jrpc2.ParsedRequest, frames [][]byte) {
	var wg sync.WaitGroup
	var raised atomic.Pointer[relayedPanic]

	// Read once and probe per element: ctx.Err() takes a lock every call.
	dead := ctx.Done()

	// Errors answerable without a handler, recorded here and framed below
	// under one permit: a body holds ~260k of them, ~23MB of frames.
	var static []*jrpc2.Error

	for i, pr := range reqs {
		if isClosed(dead) {
			// Delta (g) for elements that never reach acquirePermit.
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
			continue // no permit and no goroutine spent
		}
		if !h.acquirePermit(ctx) {
			break // the request died waiting for the bound
		}
		wg.Go(func() {
			// Deferred: returned once the frame is built, before any write,
			// and on a panicking handler too.
			defer h.sem.Release(1)
			defer catchPanic(&raised)
			frames[i] = h.invoke(pr, method)
		})
	}

	// One permit for all of them, not one per element: ~260k acquire cycles
	// would tax every ordinary request. Safe here — this goroutine holds
	// nothing, so it cannot deadlock against its workers. The parse costs more
	// again and is bounded by the backlog limit and the body cap, not by this.
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
		// Nothing above a worker recovers, so an escaping panic ends the
		// process; re-raised here it fails only this request.
		panic(p)
	}
}

// dispatchOne answers one request on the calling goroutine, releasing its
// permit before returning so the caller writes without one, panics included.
func (h *Handler) dispatchOne(ctx context.Context, pr *jrpc2.ParsedRequest) []byte {
	method, rerr := h.route(pr)
	if method == nil && rerr == nil {
		return nil
	}
	if !h.acquirePermit(ctx) {
		return nil // delta (g): never started, so nothing to answer
	}
	defer h.sem.Release(1)
	if method == nil {
		// Framed under the same bound as every other frame construction: an
		// id near the body cap escapes to megabytes. A dead request builds
		// nothing.
		return errorFrame(pr.ID, rerr)
	}
	return h.invoke(pr, method)
}

// acquirePermit takes one permit, blocking until it gets one or ctx ends.
// x/sync returns a permit won in the race, so false means "did not run".
func (h *Handler) acquirePermit(ctx context.Context) bool {
	return h.sem.Acquire(ctx, 1) == nil
}

// isClosed reports whether c has been closed, without blocking on it.
func isClosed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// route decides how one request is answered without running anything: the
// handler, or a nil handler and the error (itself nil for no response). It
// returns the error, not the frame, so the caller chooses when to spend the
// bytes. Order is load-bearing: parse and shape errors answer even for a
// notification, then an empty method (delta (d)), and only then does
// notification-ness suppress dispatch results.
func (h *Handler) route(pr *jrpc2.ParsedRequest) (jrpc2.Handler, *jrpc2.Error) {
	if pr.Error != nil {
		return nil, pr.Error
	}
	if pr.Method == "" {
		return nil, errEmptyMethod
	}
	// Checked for nil as well as present: this signals "run this" by returning
	// a non-nil handler, so a nil entry would drop the element silently.
	if method, ok := h.methods[pr.Method]; ok && method != nil {
		return method, nil
	}
	if pr.ID == "" {
		return nil, nil // an unknown method on a notification is silent
	}
	// Answered without invoking anything, so no metric is labeled: invented
	// method names cannot grow the summary's cardinality.
	return nil, (&jrpc2.Error{
		Code:    jrpc2.MethodNotFound,
		Message: jrpc2.MethodNotFound.String(),
	}).WithData(pr.Method)
}

// invoke runs one handler and builds its frame; the caller holds the permit.
func (h *Handler) invoke(pr *jrpc2.ParsedRequest, method jrpc2.Handler) []byte {
	// The server's lifetime: a client's disconnect does not cancel a handler,
	// the method budget does. sendTransaction depends on that.
	ctx := h.root

	// Delta (f). rpcv2's getEventsV2 reads ParamString, so the raw bytes are
	// contract.
	result, err := method(ctx, pr.ToRequest())
	if pr.ID == "" {
		// "The Server MUST NOT reply to a Notification, including those that
		// are within a batch request." An over-budget one is not waited for:
		// that would tie the 204 and its backlog slot to straggler duration.
		return nil
	}
	if err != nil {
		return errorFrame(pr.ID, handlerError(err))
	}

	// The one marshal, and it stays json.Marshal: Encode returns its pooled
	// buffer only after writing, so stalled clients pin one each.
	bits, err := json.Marshal(result)
	if err != nil {
		// Through handlerError: jrpc2 maps a marshal failure down the handler
		// ladder, which makes it -32098, not -32603.
		return errorFrame(pr.ID, handlerError(err))
	}
	return frame(pr.ID, resultKey, bits)
}

// relayedPanic carries a worker's panic to the serving goroutine with the
// stack it panicked on; the recover that logs it runs elsewhere.
type relayedPanic struct {
	value any
	stack []byte
}

// String is what the limiter prints for the recovered value.
func (p *relayedPanic) String() string {
	return fmt.Sprintf("%v (panicked on a batch worker goroutine)\n%s", p.value, p.stack)
}

// catchPanic must be deferred directly, so its recover is the worker's.
func catchPanic(raised *atomic.Pointer[relayedPanic]) {
	if v := recover(); v != nil {
		raised.CompareAndSwap(nil, &relayedPanic{value: v, stack: debug.Stack()})
	}
}

// handlerError reproduces jrpc2's tasks.responses, bare type assertion
// included. network's limiter sentinels are jrpc2.Error values, not pointers,
// so the assertion misses them and their message keeps its own "[-32001] "
// prefix. Pinned, not a bug.
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
// text, or "" for one with no usable id, answered as null. The capacity must
// be the frame's exact length — append grows silently and the bytes come out
// identical — so TestFrameIsExactlyOneAllocation pins it on cap.
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

// appendID writes the id back as the client sent it, applying the stdlib's
// HTML escaping (<, >, & and U+2028/U+2029) at the splice. A plain append
// reflects up to 512KB of client bytes into a response with no nosniff header
// from a mount with open CORS. Numbers pass through untouched.
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
// JSON array for a batch, a one-element batch included. The array is
// hand-appended; json.Marshal of a []json.RawMessage re-validates and re-copies
// every frame.
func writeFrames(w http.ResponseWriter, isBatch bool, frames [][]byte) {
	var body []byte
	if !isBatch && len(frames) == 1 {
		// The fat case: the frame the dispatcher already built, uncopied.
		body = frames[0]
	} else {
		// Brackets are 2 and n frames carry n-1 commas: 1 + sum(len+1).
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
