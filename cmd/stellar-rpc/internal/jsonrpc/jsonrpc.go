// Package jsonrpc assembles the JSON-RPC method table shared by both RPC
// backends. Each backend builds a []HandlerSpec from its own config and
// passes it to NewHandler, which wraps every method with the backlog-queue
// and request-duration limiters from the network package.
//
//nolint:funcorder // constructor is kept near handler setup for readability
package jsonrpc

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/go-chi/chi/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/cors"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc/wire"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
)

const (
	// LedgerKeyDecodeMaxMemory and TransactionDecodeMaxMemory bound the decoded
	// output size when XDR-unmarshaling user-supplied input, shared by both
	// daemons' method tables so the two cannot drift on a security-relevant
	// bound.
	LedgerKeyDecodeMaxMemory   = 16 * 1024   // 16 KB
	TransactionDecodeMaxMemory = 1024 * 1024 // 1 MB

	// metric label/subsystem names shared across the assembly below
	subsystemNetwork = "network"
	labelStatus      = "status"

	// maxHTTPRequestSize defines the largest request size that the http handler
	// would be willing to accept before dropping the request. The implementation
	// uses the default MaxBytesHandler to limit the request size.
	maxHTTPRequestSize          = 512 * 1024 // half a megabyte
	warningThresholdDenominator = 3
)

// Handler is the HTTP handler which serves the Soroban JSON RPC responses.
type Handler struct {
	http.Handler

	framing *wire.Handler
}

// Shutdown cancels the context every method handler runs on and waits for the
// ones still running, or for ctx to end. Handler contexts are server-scoped,
// so nothing else ends a request that outlived the HTTP deadline: call this
// after the server stops accepting and BEFORE closing the stores it reads.
func (h Handler) Shutdown(ctx context.Context) error { return h.framing.Shutdown(ctx) }

// HandlerSpec describes one JSON-RPC method: its handler plus the per-method
// request limits applied around it.
type HandlerSpec struct {
	MethodName           string
	Handler              jrpc2.Handler
	QueueLimit           uint
	RequestDurationLimit time.Duration
}

// Params carries everything NewHandler needs besides the method specs: the
// daemon (for metric namespacing and registry), the logger, and the global
// request limits applied across all methods.
type Params struct {
	Daemon                host.Daemon
	Logger                *log.Entry
	Specs                 []HandlerSpec
	GlobalQueueLimit      uint
	GlobalDurationWarning time.Duration
	GlobalDurationLimit   time.Duration
}

// decorateHandlers wraps every method with request logging and the duration
// summary. Each daemon builds its handler once, so creating and registering
// the collector here happens once per registry.
// registerOrReuse registers c and returns it, or returns the collector already
// registered under the same name. Each daemon builds its handler once today,
// but a second build on the same registry (a reload, a re-bind) must keep
// counting on the existing series rather than panic on a duplicate.
//
// EVERY collector this package builds must go through here. Six families were
// constructed and never registered between #804 and this commit, so the
// per-method and global limiter signals did not exist on /metrics at all.
func registerOrReuse[T prometheus.Collector](registry *prometheus.Registry, c T) T {
	rerr := registry.Register(c)
	if rerr == nil {
		return c
	}
	are := prometheus.AlreadyRegisteredError{}
	if !errors.As(rerr, &are) {
		panic(rerr)
	}
	existing, ok := are.ExistingCollector.(T)
	if !ok {
		panic(rerr)
	}
	return existing
}

func decorateHandlers(daemon host.Daemon, logger *log.Entry, m handler.Map) handler.Map {
	requestMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  daemon.MetricsNamespace(),
		Subsystem:  "json_rpc",
		Name:       "request_duration_seconds",
		Help:       "JSON RPC request duration",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"endpoint", labelStatus})
	requestMetric = registerOrReuse(daemon.MetricsRegistry(), requestMetric)
	decorated := handler.Map{}
	for endpoint, h := range m {
		decorated[endpoint] = handler.New(func(ctx context.Context, r *jrpc2.Request) (any, error) {
			reqID := strconv.FormatUint(middleware.NextRequestID(), 10)
			logRequest(logger, reqID, r)
			startTime := time.Now()
			result, err := h(ctx, r)
			duration := time.Since(startTime)
			label := prometheus.Labels{"endpoint": r.Method(), "status": "ok"}
			simulateTransactionResponse, ok := result.(protocol.SimulateTransactionResponse)
			if ok && simulateTransactionResponse.Error != "" {
				label[labelStatus] = "error"
			} else if err != nil {
				var jsonRPCErr *jrpc2.Error
				if errors.As(err, &jsonRPCErr) {
					prometheusLabelReplacer := strings.NewReplacer(" ", "_", "-", "_", "(", "", ")", "")
					status := prometheusLabelReplacer.Replace(jsonRPCErr.Code.String())
					label[labelStatus] = status
				}
			}
			requestMetric.With(label).Observe(duration.Seconds())
			logResponse(logger, reqID, duration, label[labelStatus])
			return result, err
		})
	}
	return decorated
}

// maxLoggedRequestID bounds the client-controlled text that reaches the log:
// jrpc2.Request.ID() is the raw id token the client sent, so this field is
// anything up to the 512KB body cap, once per request at INFO. A quoted UUID
// is 38 bytes. It bounds what is WRITTEN, not what is copied — ID() has
// already copied the token, and jrpc2 exposes no length accessor.
const maxLoggedRequestID = 64

func loggedRequestID(req *jrpc2.Request) string {
	id := req.ID()
	if len(id) <= maxLoggedRequestID {
		return id
	}
	return id[:maxLoggedRequestID] + "…(truncated)"
}

func logRequest(logger *log.Entry, reqID string, req *jrpc2.Request) {
	logger = logger.WithFields(log.F{
		"subsys":   "jsonrpc",
		"req":      reqID,
		"json_req": loggedRequestID(req),
		"method":   req.Method(),
	})
	logger.Info("starting JSONRPC request")

	// Params are useful but can be really verbose, let's only print them in debug level
	logger = logger.WithField("params", req.ParamString())
	logger.Debug("starting JSONRPC request params")
}

func logResponse(logger *log.Entry, reqID string, duration time.Duration, status string) {
	logger = logger.WithFields(log.F{
		"subsys":   "jsonrpc",
		"req":      reqID,
		"duration": duration.String(),
		"json_req": reqID,
		"status":   status,
	})
	logger.Info("finished JSONRPC request")
}

func toSnakeCase(s string) string {
	var result strings.Builder
	result.Grow(len(s) * 2)
	for _, v := range s {
		if unicode.IsUpper(v) {
			result.WriteByte('_')
		}
		result.WriteRune(v)
	}
	return strings.ToLower(result.String())
}

// wrapWithLimiters applies the per-method backlog-queue and request-duration
// limiters (and their metrics) around a single method handler.
//
// LOAD-BEARING nesting: the backlog limiter wraps spec.Handler and the
// duration limiter wraps queueLimiter.Handle, so the backlog slot is held
// ACROSS the real call — including after the duration limiter has answered its
// timeout and walked away from it. That is the only thing bounding live
// handler bodies (QueueLimit per method); the wire semaphore counts
// dispatches and cannot see them. Swap the two and the slot is released when
// the wrapper returns, bounding nothing.
func wrapWithLimiters(
	spec HandlerSpec, daemon host.Daemon, logger *log.Entry, liveHandlers *network.LiveHandlers,
) jrpc2.Handler {
	longName := toSnakeCase(spec.MethodName)
	queueLimiterGaugeName := longName + "_inflight_requests"
	queueLimiterGaugeHelp := "Number of concurrenty in-flight " + spec.MethodName + " requests"

	queueLimiterGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: daemon.MetricsNamespace(), Subsystem: subsystemNetwork,
		Name: queueLimiterGaugeName,
		Help: queueLimiterGaugeHelp,
	})
	queueLimiter := network.MakeJrpcBacklogQueueLimiter(
		spec.Handler,
		registerOrReuse(daemon.MetricsRegistry(), queueLimiterGauge),
		uint64(spec.QueueLimit),
		logger)

	durationWarnCounterName := longName + "_execution_threshold_warning"
	durationLimitCounterName := longName + "_execution_threshold_limit"
	durationWarnCounterHelp := "The metric measures the count of " + spec.MethodName +
		" requests that surpassed the warning threshold for execution time"
	durationLimitCounterHelp := "The metric measures the count of " + spec.MethodName +
		" requests that surpassed the limit threshold for execution time"

	requestDurationWarnCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: daemon.MetricsNamespace(), Subsystem: subsystemNetwork,
		Name: durationWarnCounterName,
		Help: durationWarnCounterHelp,
	})
	requestDurationLimitCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: daemon.MetricsNamespace(), Subsystem: subsystemNetwork,
		Name: durationLimitCounterName,
		Help: durationLimitCounterHelp,
	})
	// set the warning threshold to be one third of the limit.
	requestDurationWarn := spec.RequestDurationLimit / warningThresholdDenominator
	durationLimiter := network.MakeJrpcRequestDurationLimiter(
		queueLimiter.Handle,
		requestDurationWarn,
		spec.RequestDurationLimit,
		registerOrReuse(daemon.MetricsRegistry(), requestDurationWarnCounter),
		registerOrReuse(daemon.MetricsRegistry(), requestDurationLimitCounter),
		logger,
		liveHandlers)
	return durationLimiter.Handle
}

// NewHandler constructs a Handler instance from the given method specs
func NewHandler(params Params) Handler {
	// The duration limiter answers at its timeout and leaves its handler
	// running, so the framing's bound loses sight of it. Both ends of that
	// share this group: the limiters count into it, Handler.Shutdown joins it.
	liveHandlers := new(network.LiveHandlers)
	handlersMap := handler.Map{}
	for _, spec := range params.Specs {
		handlersMap[spec.MethodName] = wrapWithLimiters(spec, params.Daemon, params.Logger, liveHandlers)
	}
	// The framing at the bottom of the chain. Both mounts use the same one:
	// internal/jsonrpc/wire, which parses with jrpc2.ParseRequests and then
	// writes its own envelope. Everything assembled around it below — cors, the
	// 512KB body cap, the request-duration limiter and its buffered response
	// writer, the global backlog limiter — is unchanged and stays in this
	// order. The duration limiter is the slow-client decoupler and must remain
	// OUTSIDE the framing, never beside or under it.
	framing := wire.NewHandler(decorateHandlers(
		params.Daemon,
		params.Logger,
		handlersMap), liveHandlers)

	// globalQueueRequestBacklogLimiter is a metric for measuring the total concurrent inflight requests
	globalQueueRequestBacklogLimiter := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: params.Daemon.MetricsNamespace(), Subsystem: subsystemNetwork, Name: "global_inflight_requests",
		Help: "Number of concurrenty in-flight http requests",
	})

	queueLimitedFraming := network.MakeHTTPBacklogQueueLimiter(
		framing,
		registerOrReuse(params.Daemon.MetricsRegistry(), globalQueueRequestBacklogLimiter),
		uint64(params.GlobalQueueLimit),
		params.Logger)

	globalQueueRequestExecutionDurationWarningCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: params.Daemon.MetricsNamespace(),
		Subsystem: subsystemNetwork,
		Name:      "global_request_execution_duration_threshold_warning",
		Help:      "The metric measures the count of requests that surpassed the warning threshold for execution time",
	})
	globalQueueRequestExecutionDurationLimitCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: params.Daemon.MetricsNamespace(),
		Subsystem: subsystemNetwork,
		Name:      "global_request_execution_duration_threshold_limit",
		Help:      "The metric measures the count of requests that surpassed the limit threshold for execution time",
	})
	handler := network.MakeHTTPRequestDurationLimiter(
		queueLimitedFraming,
		params.GlobalDurationWarning,
		params.GlobalDurationLimit,
		registerOrReuse(params.Daemon.MetricsRegistry(), globalQueueRequestExecutionDurationWarningCounter),
		registerOrReuse(params.Daemon.MetricsRegistry(), globalQueueRequestExecutionDurationLimitCounter),
		params.Logger)

	handler = http.MaxBytesHandler(handler, maxHTTPRequestSize)

	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins:         []string{},
		AllowOriginRequestFunc: func(*http.Request, string) bool { return true },
		AllowedHeaders:         []string{"*"},
		AllowedMethods:         []string{"GET", "PUT", "POST", "PATCH", "DELETE", "HEAD", "OPTIONS"},
	})

	return Handler{Handler: corsMiddleware.Handler(handler), framing: framing}
}
