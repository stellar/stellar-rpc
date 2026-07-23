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
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/go-chi/chi/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/cors"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
)

const (
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

	bridge jhttp.Bridge
	logger *log.Entry
}

// Close closes all the resources held by the Handler instances.
// After Close is called the Handler instance will stop accepting JSON RPC requests.
func (h Handler) Close() {
	if err := h.bridge.Close(); err != nil {
		h.logger.WithError(err).Warn("could not close bridge")
	}
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
// daemon (for metrics), the logger, and the global request limits applied
// across all methods.
type Params struct {
	Daemon                host.Daemon
	Logger                *log.Entry
	Specs                 []HandlerSpec
	GlobalQueueLimit      uint
	GlobalDurationWarning time.Duration
	GlobalDurationLimit   time.Duration
}

func decorateHandlers(daemon host.Daemon, logger *log.Entry, m handler.Map) handler.Map {
	requestMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  daemon.MetricsNamespace(),
		Subsystem:  "json_rpc",
		Name:       "request_duration_seconds",
		Help:       "JSON RPC request duration",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"endpoint", labelStatus})
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
	daemon.MetricsRegistry().MustRegister(requestMetric)
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
func wrapWithLimiters(spec HandlerSpec, daemon host.Daemon, logger *log.Entry) jrpc2.Handler {
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
		requestDurationWarn,
		spec.RequestDurationLimit,
		requestDurationWarnCounter,
		requestDurationLimitCounter,
		logger)
	return durationLimiter.Handle
}

// NewHandler constructs a Handler instance from the given method specs
func NewHandler(params Params) Handler {
	bridgeOptions := jhttp.BridgeOptions{
		Server: &jrpc2.ServerOptions{
			Logger: func(text string) { params.Logger.Debug(text) },
			// Disable built-in rpc.* methods (e.g. rpc.serverInfo) that
			// bypass the handler allowlist and request limiters.
			DisableBuiltin: true,
		},
	}

	handlersMap := handler.Map{}
	for _, spec := range params.Specs {
		handlersMap[spec.MethodName] = wrapWithLimiters(spec, params.Daemon, params.Logger)
	}
	bridge := jhttp.NewBridge(decorateHandlers(
		params.Daemon,
		params.Logger,
		handlersMap),
		&bridgeOptions)

	// globalQueueRequestBacklogLimiter is a metric for measuring the total concurrent inflight requests
	globalQueueRequestBacklogLimiter := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: params.Daemon.MetricsNamespace(), Subsystem: subsystemNetwork, Name: "global_inflight_requests",
		Help: "Number of concurrenty in-flight http requests",
	})

	queueLimitedBridge := network.MakeHTTPBacklogQueueLimiter(
		bridge,
		globalQueueRequestBacklogLimiter,
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
		queueLimitedBridge,
		params.GlobalDurationWarning,
		params.GlobalDurationLimit,
		globalQueueRequestExecutionDurationWarningCounter,
		globalQueueRequestExecutionDurationLimitCounter,
		params.Logger)

	handler = http.MaxBytesHandler(handler, maxHTTPRequestSize)

	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins:         []string{},
		AllowOriginRequestFunc: func(*http.Request, string) bool { return true },
		AllowedHeaders:         []string{"*"},
		AllowedMethods:         []string{"GET", "PUT", "POST", "PATCH", "DELETE", "HEAD", "OPTIONS"},
	})

	return Handler{
		bridge:  bridge,
		logger:  params.Logger,
		Handler: corsMiddleware.Handler(handler),
	}
}
