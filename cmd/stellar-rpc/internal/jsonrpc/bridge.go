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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/util"
)

const contentTypeJSON = "application/json"

// A response gets responseChunkTimeout for each responseChunkSize bytes, so a
// client that stops reading is dropped without capping how long a large
// response may take.
const (
	responseChunkSize    = 1 << 20
	responseChunkTimeout = 30 * time.Second
)

var (
	errBridgeClosed = errors.New("the bridge is closed")
	errBusy         = errors.New("too many requests are running")
	errEmptyMethod  = &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: "empty method name"}
	errNoSuchMethod = &jrpc2.Error{Code: jrpc2.MethodNotFound, Message: jrpc2.MethodNotFound.String()}
)

// Bridge serves JSON-RPC 2.0 over HTTP POST by calling an Assigner's handlers
// directly, at most one per CPU. The zero value is not usable; build it with
// NewBridge.
type Bridge struct {
	// Bridge keeps the wire behavior of jhttp.Bridge with parseRequests as its
	// ParseRequest hook (TestBridge_ParityWithJHTTP), but encodes each result
	// once. It enforces the duration limit itself, so a response is never
	// buffered to be swapped for a 504.
	methods      jrpc2.Assigner
	sem          *semaphore.Weighted // bounds the handlers running at once, as jrpc2.Server does
	maxHandlers  int
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
	maxHandlers := runtime.NumCPU()
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
	// The handlers can outlast the server's WriteTimeout, so the response gets
	// its own write time, which write extends as it goes.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(responseChunkTimeout))
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
		if err := b.sem.Acquire(ctx, 1); err != nil {
			rsps[i] = respond(pr, nil, err)
			continue
		}
		call := func() {
			defer b.sem.Release(1)
			defer func() {
				if p := recover(); p != nil { // answer just this call, as the duration limiter does
					for _, line := range util.CallStack(p, pr.Method, "(*Bridge).run.func", 8) {
						b.limits.logger.Warn(line)
					}
					rsps[i] = respond(pr, nil, network.ErrFailToProcessDueToInternalIssue)
				}
			}()
			v, err := method(ctx, pr.ToRequest())
			rsps[i] = respond(pr, v, err)
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
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("Content-Length", strconv.Itoa(n))
	w.WriteHeader(http.StatusOK)
	writeChunked(w, rc, parts)
	return nil
}

// writeChunked writes parts to w. The deadline the caller just set on rc covers
// the first responseChunkSize bytes, and writeChunked moves it before each
// responseChunkSize bytes after that. It stops at the first write error, which
// means the client is gone.
func writeChunked(w io.Writer, rc *http.ResponseController, parts [][]byte) {
	left := responseChunkSize // bytes the current deadline still covers
	for _, p := range parts {
		for len(p) > 0 {
			if left == 0 {
				_ = rc.SetWriteDeadline(time.Now().Add(responseChunkTimeout))
				left = responseChunkSize
			}
			n := min(len(p), left)
			if _, err := w.Write(p[:n]); err != nil {
				return
			}
			p, left = p[n:], left-n
		}
	}
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
