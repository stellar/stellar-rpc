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
	"sync"
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

// Handler is the HTTP handler which serves the Soroban JSON RPC responses
type Handler struct {
	http.Handler

	bridge   *bridge
	inflight *sync.WaitGroup
}

// Close stops accepting JSON-RPC requests, cancels those in flight and waits
// for their handlers to return, so none outlives the stores closed after it.
func (h Handler) Close() {
	h.bridge.Close()
	h.inflight.Wait() // handlers a duration limiter stopped waiting for
}

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
func decorateHandlers(daemon host.Daemon, logger *log.Entry, m handler.Map) handler.Map {
	requestMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  daemon.MetricsNamespace(),
		Subsystem:  "json_rpc",
		Name:       "request_duration_seconds",
		Help:       "JSON RPC request duration",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"endpoint", labelStatus})
	// Register-or-reuse: each daemon builds its handler once today, but a
	// second build on the same registry (a future reload or re-bind) must keep
	// counting on the existing series, not panic on duplicate registration.
	if rerr := daemon.MetricsRegistry().Register(requestMetric); rerr != nil {
		are := prometheus.AlreadyRegisteredError{}
		if !errors.As(rerr, &are) {
			panic(rerr)
		}
		existing, ok := are.ExistingCollector.(*prometheus.SummaryVec)
		if !ok {
			panic(rerr)
		}
		requestMetric = existing
	}
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
			simulateFailed := ok && simulateTransactionResponse.Error != ""
			if simulateFailed {
				label[labelStatus] = "error"
			} else if err != nil {
				var jsonRPCErr *jrpc2.Error
				if errors.As(err, &jsonRPCErr) {
					prometheusLabelReplacer := strings.NewReplacer(" ", "_", "-", "_", "(", "", ")", "")
					status := prometheusLabelReplacer.Replace(jsonRPCErr.Code.String())
					label[labelStatus] = status
				}
			}
			if ctx.Err() != nil && (err != nil || simulateFailed) {
				// Failed after its context ended: the client left or timed out, not a server fault.
				label[labelStatus] = "canceled"
			}
			requestMetric.With(label).Observe(duration.Seconds())
			logResponse(logger, reqID, r.ID(), duration, label[labelStatus])
			return result, err
		})
	}
	return decorated
}

func logRequest(logger *log.Entry, reqID string, req *jrpc2.Request) {
	logger = logger.WithFields(log.F{
		"subsys":   "jsonrpc",
		"req":      reqID,
		"json_req": req.ID(),
		"method":   req.Method(),
	})
	logger.Info("starting JSONRPC request")

	// Params are useful but can be really verbose, let's only print them in debug level
	logger = logger.WithField("params", req.ParamString())
	logger.Debug("starting JSONRPC request params")
}

func logResponse(logger *log.Entry, reqID, jsonReq string, duration time.Duration, status string) {
	logger = logger.WithFields(log.F{
		"subsys":   "jsonrpc",
		"req":      reqID,
		"duration": duration.String(),
		"json_req": jsonReq,
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
func wrapWithLimiters(spec HandlerSpec, daemon host.Daemon, logger *log.Entry, inflight *sync.WaitGroup) jrpc2.Handler {
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
		queueLimiterGauge,
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
		inflight,
		requestDurationWarn,
		spec.RequestDurationLimit,
		requestDurationWarnCounter,
		requestDurationLimitCounter,
		logger)
	if spec.MethodName == protocol.SendTransactionMethodName {
		// The bridge cancels a handler's context when its client disconnects.
		// Submission has side effects, so let it finish; the duration limit
		// still applies.
		return func(ctx context.Context, req *jrpc2.Request) (any, error) {
			return durationLimiter.Handle(context.WithoutCancel(ctx), req)
		}
	}
	return durationLimiter.Handle
}

// NewHandler constructs a Handler instance from the given method specs
func NewHandler(params Params) Handler {
	handlersMap := handler.Map{}
	inflight := new(sync.WaitGroup)
	for _, spec := range params.Specs {
		handlersMap[spec.MethodName] = wrapWithLimiters(spec, params.Daemon, params.Logger, inflight)
	}

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
	rpc := newBridge(decorateHandlers(params.Daemon, params.Logger, handlersMap), durationLimits{
		warning:  params.GlobalDurationWarning,
		limit:    params.GlobalDurationLimit,
		warnings: globalQueueRequestExecutionDurationWarningCounter,
		timeouts: globalQueueRequestExecutionDurationLimitCounter,
		logger:   params.Logger,
	})

	// globalQueueRequestBacklogLimiter is a metric for measuring the total concurrent inflight requests
	globalQueueRequestBacklogLimiter := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: params.Daemon.MetricsNamespace(), Subsystem: subsystemNetwork, Name: "global_inflight_requests",
		Help: "Number of concurrenty in-flight http requests",
	})

	queueLimitedBridge := network.MakeHTTPBacklogQueueLimiter(
		rpc,
		globalQueueRequestBacklogLimiter,
		uint64(params.GlobalQueueLimit),
		params.Logger)

	handler := http.MaxBytesHandler(queueLimitedBridge, maxHTTPRequestSize)

	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins:         []string{},
		AllowOriginRequestFunc: func(*http.Request, string) bool { return true },
		AllowedHeaders:         []string{"*"},
		AllowedMethods:         []string{"GET", "PUT", "POST", "PATCH", "DELETE", "HEAD", "OPTIONS"},
	})

	return Handler{
		bridge:   rpc,
		inflight: inflight,
		Handler:  corsMiddleware.Handler(handler),
	}
}
