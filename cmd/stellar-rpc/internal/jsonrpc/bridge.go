package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"

	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/util"
)

const contentTypeJSON = "application/json"

// A response gets responseWriteTime to be written, plus responseWriteTimePerMiB
// for each MiB of it, so a client that stops reading is dropped and no
// response is held for long.
const (
	responseWriteTimePerMiB = time.Second
	mib                     = 1 << 20
)

// A response up to smallResponseBytes is held outside the pool, so getHealth
// and the like still answer while the pool is full.
const smallResponseBytes = 4 << 10

var (
	errBridgeClosed     = errors.New("the bridge is closed")
	errBusy             = errors.New("too many requests are running")
	errResponsePoolFull = errors.New("the node holds its most response bytes already; retry later")
	errResponseTooLarge = errors.New("the response is larger than the node may hold; request fewer items")
	errEmptyMethod      = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty method name"}
	errNoSuchMethod     = &jrpc2.Error{Code: jrpc2.MethodNotFound, Message: jrpc2.MethodNotFound.String()}
)

// Bridge serves JSON-RPC 2.0 over HTTP POST by calling an Assigner's handlers
// directly, at most GOMAXPROCS at once apart from the methods in unslotted.
// The zero value is not usable; build it with NewBridge.
type Bridge struct {
	// Bridge keeps the wire behavior of jhttp.Bridge with parseRequests as its
	// ParseRequest hook (TestBridge_ParityWithJHTTP), but encodes each result
	// once. It enforces the duration limit itself, so a response is never
	// buffered to be swapped for a 504.
	methods     jrpc2.Assigner
	sem         *semaphore.Weighted // bounds the handlers running at once, as jrpc2.Server does
	pool        *semaphore.Weighted // bounds the bytes of the responses held before they are written; nil is no bound
	poolBytes   int64               // the pool's size
	rejections  interface{ Inc() }  // counts the responses the pool had no room for
	maxHandlers int
	// unslotted are the methods that bound their own concurrency and run
	// without one of sem's slots.
	unslotted    map[string]bool
	maxBatchSize uint
	limits       durationLimits
	// requests, when set, bounds the requests whose calls run at once.
	requests *requestLimit

	mu     sync.RWMutex // read-held while a request's handlers run
	closed bool
	ctx    context.Context //nolint:containedctx // ends at Close, canceling the requests in flight
	cancel context.CancelFunc
}

// durationLimits are the global request-duration thresholds, with the counters
// and logger for requests that pass them, as network's HTTP limiter had them.
type durationLimits struct {
	warning, limit     time.Duration
	warnings, timeouts interface{ Inc() }
	logger             *log.Entry
}

// NewBridge returns a Bridge over methods with no request duration limit and
// no batch size limit. logger receives panic stacks. The caller must Close it.
func NewBridge(methods jrpc2.Assigner, logger *log.Entry) *Bridge {
	return newBridge(methods, math.MaxUint, durationLimits{limit: network.RequestDurationLimiterNoLimit, logger: logger})
}

// newBridge returns a Bridge that answers a batch of more than maxBatchSize
// calls with a single error.
func newBridge(methods jrpc2.Assigner, maxBatchSize uint, limits durationLimits) *Bridge {
	ctx, cancel := context.WithCancel(context.Background())
	maxHandlers := runtime.GOMAXPROCS(0)
	return &Bridge{
		methods:      methods,
		sem:          semaphore.NewWeighted(int64(maxHandlers)),
		maxHandlers:  maxHandlers,
		maxBatchSize: maxBatchSize,
		limits:       limits,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// MaxHandlers returns how many handlers b runs at once.
func (b *Bridge) MaxHandlers() int { return b.maxHandlers }

// Close cancels the requests in flight, waits for their handlers to return,
// and fails later requests.
func (b *Bridge) Close() {
	b.cancel()
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
}

// ServeHTTP implements http.Handler.
func (b *Bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer b.recoverPanic(w)
	w.Header().Set("Accept-Post", contentTypeJSON)
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	mt, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mt != contentTypeJSON {
		http.Error(w, "content-type must be application/json", http.StatusUnsupportedMediaType)
		return
	}
	if cs, ok := params["charset"]; ok && !strings.EqualFold(cs, "utf-8") && !strings.EqualFold(cs, "utf8") {
		http.Error(w, "invalid content-type charset", http.StatusUnsupportedMediaType)
		return
	}
	start := time.Now()
	ctx, cancel := b.limits.bound(r.Context())
	defer cancel()
	batch, rsps, err := b.serve(ctx, r)
	defer b.release(rsps) // written, or dropped for a 504
	// The handlers can outlast the server's WriteTimeout, so the response gets
	// its own write time, which write sets from its size.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(responseWriteTime))
	if errors.Is(err, errBusy) {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if b.limits.timedOut(r, time.Since(start)) {
		if r.Context().Err() == nil {
			w.WriteHeader(http.StatusGatewayTimeout)
		}
		return
	}
	if err == nil && len(rsps) == 0 {
		w.WriteHeader(http.StatusNoContent) // only notifications, or an empty batch
		return
	}
	if err == nil {
		err = write(w, rc, batch, rsps)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, err.Error()) //nolint:gosec // a read or parse error, as text; jhttp.Bridge did the same
	}
}

// serve runs the requests in r's body on ctx and returns their responses.
func (b *Bridge) serve(ctx context.Context, r *http.Request) (bool, []*response, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return false, nil, err
	}
	if !b.requests.acquire() {
		return false, nil, errBusy
	}
	defer b.requests.release()
	reqs, err := parseRequests(body, b.maxBatchSize)
	if err != nil {
		return false, nil, err
	}
	batch := len(reqs) > 0 && reqs[0].Batch
	// Invalid requests are answered first, as jhttp.Bridge did.
	slices.SortStableFunc(reqs, func(x, y *jrpc2.ParsedRequest) int { return rank(x) - rank(y) })
	rsps, err := b.run(ctx, reqs)
	return batch, rsps, err
}

// bound returns ctx ending at the limit, as the handlers see it.
func (l durationLimits) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	if l.limit == network.RequestDurationLimiterNoLimit {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, l.limit)
}

// timedOut reports whether a request that took elapsed passed the limit, and
// counts and logs it if it passed the limit or, failing that, the warning.
func (l durationLimits) timedOut(r *http.Request, elapsed time.Duration) bool {
	switch {
	case l.limit == network.RequestDurationLimiterNoLimit:
	case l.limit != 0 && elapsed >= l.limit:
		l.timeouts.Inc()
		l.logger.Infof("Request processing for %s exceed limiting threshold of %v", r.URL.Path, l.limit)
		return true
	case l.warning != 0 && l.warning < l.limit && elapsed >= l.warning:
		l.warnings.Inc()
		l.logger.Infof("Request processing for %s exceed warning threshold of %v", r.URL.Path, l.warning)
	}
	return false
}

// requestLimit bounds the requests whose calls run at once, as network's HTTP
// backlog limiter did, but counts a request only from when its body has been
// read, so a client that sends its body slowly holds no slot.
type requestLimit struct {
	limit   uint64
	running atomic.Uint64
	logged  atomic.Bool // the limit was logged since a request last finished
	gauge   prometheus.Gauge
	logger  *log.Entry
}

// acquire counts a request, unless limit requests are running. A nil l has no
// limit.
func (l *requestLimit) acquire() bool {
	if l == nil {
		return true
	}
	if l.running.Add(1) > l.limit {
		l.running.Add(^uint64(0))
		if l.logged.CompareAndSwap(false, true) {
			l.logger.Infof("Backlog queue limiter reached the queue limit of %d executing concurrent http requests.",
				l.limit)
		}
		return false
	}
	l.gauge.Inc()
	return true
}

// release stops counting a request that acquire counted.
func (l *requestLimit) release() {
	if l == nil {
		return
	}
	l.running.Add(^uint64(0))
	l.gauge.Dec()
	l.logged.Store(false)
}

// recoverPanic answers 500 to a request whose serving panicked outside a
// handler call and logs the stack, as network's HTTP limiter did.
func (b *Bridge) recoverPanic(w http.ResponseWriter) {
	p := recover()
	if p == nil {
		return
	}
	w.WriteHeader(http.StatusInternalServerError)
	for _, line := range util.CallStack(p, "", "(*Bridge).ServeHTTP", 8) {
		b.limits.logger.Warn(line)
	}
}

// rank orders invalid requests before valid ones.
func rank(pr *jrpc2.ParsedRequest) int {
	if pr.Error != nil {
		return 0
	}
	return 1
}

// run calls the handlers for reqs, on a context that also ends at Close, and
// returns the responses in request order. Notifications get none.
func (b *Bridge) run(ctx context.Context, reqs []*jrpc2.ParsedRequest) ([]*response, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errBridgeClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(b.ctx, cancel)() //nolint:contextcheck // b.ctx ends at Close, not with the request

	rsps := make([]*response, len(reqs))
	var wg sync.WaitGroup
	for i, pr := range reqs {
		method, rsp := b.route(ctx, pr)
		if method == nil {
			rsps[i] = rsp
			continue
		}
		slotted := !b.unslotted[pr.Method]
		if slotted {
			if err := b.sem.Acquire(ctx, 1); err != nil {
				rsps[i] = respond(pr, nil, err)
				continue
			}
		}
		call := func() {
			if slotted {
				defer b.sem.Release(1)
			}
			share := &poolShare{bridge: b}
			defer func() {
				if p := recover(); p != nil { // answer just this call, as the duration limiter does
					for _, line := range util.CallStack(p, pr.Method, "(*Bridge).run.func", 8) {
						b.limits.logger.Warn(line)
					}
					rsps[i] = b.reserve(pr, respond(pr, nil, network.ErrFailToProcessDueToInternalIssue), share)
				}
			}()
			v, err := method(methods.WithResponsePool(ctx, share), pr.ToRequest())
			rsps[i] = b.reserve(pr, respond(pr, v, err), share)
		}
		if i == len(reqs)-1 {
			call() // the last request runs here, as in jrpc2.Server
		} else {
			wg.Go(call)
		}
	}
	wg.Wait()
	return slices.DeleteFunc(rsps, func(r *response) bool { return r == nil }), nil
}

// poolShare is one call's share of the pool: what its handler took before
// rendering, which reserve settles against the response.
type poolShare struct {
	bridge *Bridge
	taken  atomic.Int64
}

// Take implements methods.ResponsePool. An item up to smallResponseBytes is
// not taken: rendering it holds almost nothing, and reserve still charges a
// response over that size in full.
func (s *poolShare) Take(n int64) error {
	if s.bridge.pool == nil || n <= smallResponseBytes {
		return nil
	}
	if !s.bridge.pool.TryAcquire(n) {
		return s.bridge.noRoom(s.taken.Load() + n)
	}
	s.taken.Add(n)
	return nil
}

// setPool bounds the response bytes b holds at once at n.
func (b *Bridge) setPool(n int64) {
	b.pool, b.poolBytes = semaphore.NewWeighted(n), n
}

// noRoom counts a rejected call and returns its error. need is all the bytes the call asked for.
func (b *Bridge) noRoom(need int64) error {
	b.rejections.Inc()
	if need > b.poolBytes {
		return errResponseTooLarge
	}
	return errResponsePoolFull
}

// reserve settles rsp's result bytes with the pool, while the call still
// holds its CPU slot: it takes what the handler did not, returns what the
// handler took beyond them, or answers pr with noRoom's error.
func (b *Bridge) reserve(pr *jrpc2.ParsedRequest, rsp *response, share *poolShare) *response {
	if b.pool == nil {
		return rsp
	}
	taken := share.taken.Load()
	var n int64
	if rsp != nil && rsp.err == nil && len(rsp.result) > smallResponseBytes {
		n = int64(len(rsp.result))
	}
	switch {
	case n > taken:
		if !b.pool.TryAcquire(n - taken) {
			if taken > 0 {
				b.pool.Release(taken)
			}
			return respond(pr, nil, b.noRoom(n))
		}
	case n < taken:
		b.pool.Release(taken - n)
	}
	if n > 0 {
		rsp.pooled = n
	}
	return rsp
}

// release returns to the pool the bytes rsps took from it.
func (b *Bridge) release(rsps []*response) {
	for _, r := range rsps {
		if r.pooled > 0 {
			b.pool.Release(r.pooled)
		}
	}
}

// route returns pr's handler, or else the response pr gets without one:
// invalid requests are always answered, notifications to unknown methods never.
func (b *Bridge) route(ctx context.Context, pr *jrpc2.ParsedRequest) (jrpc2.Handler, *response) {
	if pr.Error != nil {
		return nil, &response{id: pr.ID, err: pr.Error}
	}
	if pr.Method != "" {
		if method := b.methods.Assign(ctx, pr.Method); method != nil {
			return method, nil
		}
	}
	switch {
	case pr.ID == "":
		return nil, nil
	case pr.Method == "":
		return nil, &response{id: pr.ID, err: errEmptyMethod}
	}
	return nil, &response{id: pr.ID, err: errNoSuchMethod.WithData(pr.Method)}
}

// respond builds the response to pr from a handler's result, passing a
// non-empty json.RawMessage through as is. A notification gets none.
func respond(pr *jrpc2.ParsedRequest, v any, err error) *response {
	if pr.ID == "" {
		return nil
	}
	rsp := &response{id: pr.ID}
	if err == nil {
		raw, ok := v.(json.RawMessage)
		if !ok || len(raw) == 0 {
			raw, err = json.Marshal(v)
		}
		if err == nil {
			rsp.result = raw
			return rsp
		}
	}
	if e, ok := err.(*jrpc2.Error); ok { //nolint:errorlint // as jrpc2.Server does: no unwrapping
		rsp.err = e
	} else if c := jrpc2.ErrorCode(err); c != jrpc2.NoError {
		rsp.err = &jrpc2.Error{Code: c, Message: err.Error()}
	} else {
		rsp.err = &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}
	return rsp
}

// A response is one JSON-RPC response, with a result or an error.
type response struct {
	id     string // the request ID as sent, "" for null
	result json.RawMessage
	err    *jrpc2.Error
	pooled int64 // bytes of result taken from the pool
}

// write writes rsps as the body of a 200 response, as an array if batch is
// set. Results are written without copying.
func write(w http.ResponseWriter, rc *http.ResponseController, batch bool, rsps []*response) error {
	parts := make([][]byte, 0, 3*len(rsps)+2)
	if batch {
		parts = append(parts, []byte("["))
	}
	for i, r := range rsps {
		head := `{"jsonrpc":"2.0","id":` + escapeID(r.id)
		if i > 0 {
			head = "," + head
		}
		body := r.result
		if r.err != nil {
			e, err := json.Marshal(r.err)
			if err != nil {
				return err
			}
			head, body = head+`,"error":`, e
		} else {
			head += `,"result":`
		}
		parts = append(parts, []byte(head), body, []byte("}"))
	}
	if batch {
		parts = append(parts, []byte("]"))
	}
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	_ = rc.SetWriteDeadline(time.Now().Add(writeTime(n)))
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("Content-Length", strconv.Itoa(n))
	w.WriteHeader(http.StatusOK)
	writeParts(w, parts)
	return nil
}

// writeParts writes parts to w, stopping at the first write error, which means
// the client is gone.
func writeParts(w io.Writer, parts [][]byte) {
	for _, p := range parts {
		if _, err := w.Write(p); err != nil {
			return
		}
	}
}

// writeTime is how long a response of n bytes may take to write.
func writeTime(n int) time.Duration {
	return responseWriteTime + time.Duration(n/mib)*responseWriteTimePerMiB
}

// escapeID HTML-escapes a request ID before it is echoed, as re-encoding the
// response through jhttp.Bridge did. An absent ID is null.
func escapeID(id string) string {
	if id == "" {
		return "null"
	}
	if !strings.ContainsAny(id, "<>&\u2028\u2029") {
		return id
	}
	var buf bytes.Buffer
	json.HTMLEscape(&buf, []byte(id))
	return buf.String()
}
