package serve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/latencytrack"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/network"
)

// The HTTP-layer limits, copied from the v1 RPC daemon (internal/jsonrpc.go).
const (
	// maxHTTPRequestSize is the largest request body accepted before the
	// request is dropped (http.MaxBytesHandler).
	maxHTTPRequestSize          = 512 * 1024 // half a megabyte
	warningThresholdDenominator = 3

	// Decoded output size limits for XDR unmarshaling of user-supplied input.
	ledgerKeyDecodeMaxMemory = 16 * 1024 // 16 KB
)

// methodEntry is one row of the v2 method table: the JSON-RPC method name plus
// its backlog queue limit and execution duration limit. The limits are the v1
// daemon's defaults, fixed in code — [serving] deliberately exposes only the
// global knobs; per-method keys can be added later if operators ever need them.
type methodEntry struct {
	name                 string
	queueLimit           uint
	requestDurationLimit time.Duration

	// gateExempt marks the methods that answer while backfill is running:
	// getHealth (reports the backfill), metrics (measures it), and the
	// stubbed/unsupported methods (their answer never depends on serving
	// state).
	gateExempt bool
}

// methodTable lists every method this service registers. Kept as a package
// var (not built inline) so GraceFor can derive the reaper grace period from
// the same duration limits the limiters enforce.
//
//nolint:gochecknoglobals // read-only table shared by assembly and GraceFor
var methodTable = []methodEntry{
	{name: protocol.GetHealthMethodName, queueLimit: 1000, requestDurationLimit: 5 * time.Second, gateExempt: true},
	{name: metricsMethodName, queueLimit: 1000, requestDurationLimit: 5 * time.Second, gateExempt: true},
	{name: protocol.GetEventsMethodName, queueLimit: 1000, requestDurationLimit: 10 * time.Second},
	{name: protocol.GetNetworkMethodName, queueLimit: 1000, requestDurationLimit: 5 * time.Second},
	{name: protocol.GetVersionInfoMethodName, queueLimit: 1000, requestDurationLimit: 5 * time.Second},
	{name: protocol.GetLatestLedgerMethodName, queueLimit: 1000, requestDurationLimit: 5 * time.Second},
	{name: protocol.GetLedgersMethodName, queueLimit: 1000, requestDurationLimit: 10 * time.Second},
	{name: protocol.GetLedgerEntriesMethodName, queueLimit: 1000, requestDurationLimit: 5 * time.Second},
	{name: protocol.GetTransactionMethodName, queueLimit: 1000, requestDurationLimit: 5 * time.Second},
	{name: protocol.GetTransactionsMethodName, queueLimit: 1000, requestDurationLimit: 5 * time.Second},
	{name: protocol.SendTransactionMethodName, queueLimit: 500, requestDurationLimit: 15 * time.Second, gateExempt: true},
	{name: protocol.SimulateTransactionMethodName, queueLimit: 100, requestDurationLimit: 15 * time.Second, gateExempt: true},
	{name: protocol.GetFeeStatsMethodName, queueLimit: 100, requestDurationLimit: 5 * time.Second, gateExempt: true},
}

// unsupported returns the handler for a method this service registers but
// does not serve: a clean JSON-RPC error, never a fake success.
func unsupported(method string) jrpc2.Handler {
	err := &jrpc2.Error{
		Code:    jrpc2.MethodNotFound,
		Message: fmt.Sprintf("method %q is not supported by the full-history service", method),
	}
	return func(context.Context, *jrpc2.Request) (any, error) { return nil, err }
}

// rpcHandler is the assembled HTTP stack; http.Handler plus the bridge close.
type rpcHandler struct {
	http.Handler
	bridge *jhttp.Bridge
	logger *supportlog.Entry
}

func (h *rpcHandler) close() {
	if err := h.bridge.Close(); err != nil {
		h.logger.WithError(err).Warn("could not close JSON-RPC bridge")
	}
}

// newRPCHandler builds the method map and the v1 daemon's middleware stack
// around it. Per method, innermost to outermost:
//
//	underlying → backlog queue limiter → duration limiter → latency tracker
//	→ backfill gate → logging + request_duration_seconds summary
//
// The gate sits OUTSIDE the latency tracker on purpose: the exact-quantile
// series never decay, so instant gate rejections during a long backfill would
// permanently poison the per-endpoint quantiles. Gated rejections still show
// up in the logs and the Prometheus summary (outermost layer), labeled by
// error status.
func newRPCHandler(cfg Config, g *gate) *rpcHandler {
	logger := cfg.Logger
	serving := cfg.Serving
	rpcDaemon := &rpcDaemon{promRegistry: cfg.PromRegistry, fastClient: cfg.FastCoreClient}

	// The db.* faces every v1 handler reads through. They resolve the current
	// run's readers per call, so the once-built method map follows the gate.
	ledgers := gatedLedgerReader{g: g}
	transactions := gatedTransactionReader{g: g}
	events := gatedEventReader{g: g}

	underlying := map[string]jrpc2.Handler{
		protocol.GetHealthMethodName: newHealthHandler(
			g, cfg.Health, derefOr(serving.MaxHealthyLedgerLatency, 0), cfg.RetentionChunks, ledgers),
		metricsMethodName: newMetricsHandler(cfg.Latency),
		protocol.GetEventsMethodName: newGetEventsHandler(
			logger, events, derefOr(serving.MaxEventsLimit, 0), derefOr(serving.DefaultEventsLimit, 0), ledgers),
		protocol.GetNetworkMethodName: methods.NewGetNetworkHandler(
			cfg.NetworkPassphrase, "" /* no friendbot */, ledgers),
		protocol.GetVersionInfoMethodName:  methods.NewGetVersionInfoHandler(logger, ledgers, rpcDaemon),
		protocol.GetLatestLedgerMethodName: methods.NewGetLatestLedgerHandler(ledgers),
		protocol.GetLedgersMethodName: methods.NewGetLedgersHandler(ledgers,
			derefOr(serving.MaxLedgersLimit, 0), derefOr(serving.DefaultLedgersLimit, 0),
			nil /* no datastore fallback: strict retention floor */, logger),
		protocol.GetTransactionMethodName: methods.NewGetTransactionHandler(logger, transactions, ledgers),
		protocol.GetTransactionsMethodName: methods.NewGetTransactionsHandler(logger, ledgers,
			derefOr(serving.MaxTransactionsLimit, 0), derefOr(serving.DefaultTransactionsLimit, 0),
			cfg.NetworkPassphrase),
		protocol.GetLedgerEntriesMethodName: methods.NewGetLedgerEntriesHandler(logger,
			rpcDaemon.FastCoreClient(), ledgers,
			xdr.DecodeOptions{MaxMemoryBytes: ledgerKeyDecodeMaxMemory}),
		protocol.SendTransactionMethodName:     unsupported(protocol.SendTransactionMethodName),
		protocol.SimulateTransactionMethodName: unsupported(protocol.SimulateTransactionMethodName),
		protocol.GetFeeStatsMethodName:         unsupported(protocol.GetFeeStatsMethodName),
	}
	// With the query server off, getLedgerEntries is permanently disabled —
	// a config condition, not a backfill phase — so it answers in every phase
	// (gate-exempt) with its own clean error, like the stubbed methods.
	ledgerEntriesDisabled := cfg.FastCoreClient == nil
	if ledgerEntriesDisabled {
		underlying[protocol.GetLedgerEntriesMethodName] = func(context.Context, *jrpc2.Request) (any, error) {
			return nil, &jrpc2.Error{
				Code: jrpc2.MethodNotFound,
				Message: "getLedgerEntries is disabled: the captive-core query server is off " +
					"([ingestion].captive_core_http_query_port = 0)",
			}
		}
	}

	handlersMap := handler.Map{}
	for _, m := range methodTable {
		h := wrapWithLimiters(cfg, m, underlying[m.name], logger)
		// Resolved once here, not per request: Set.Tracker takes a lock.
		h = wrapWithLatencyTracker(cfg.Latency.Tracker(rpcSeriesPrefix+m.name), h)
		gateExempt := m.gateExempt ||
			(m.name == protocol.GetLedgerEntriesMethodName && ledgerEntriesDisabled)
		if !gateExempt {
			h = wrapWithGate(g, h)
		}
		handlersMap[m.name] = h
	}

	bridge := jhttp.NewBridge(decorateHandlers(rpcDaemon, logger, handlersMap), &jhttp.BridgeOptions{
		Server: &jrpc2.ServerOptions{
			Logger: func(text string) { logger.Debug(text) },
		},
	})

	return &rpcHandler{
		Handler: wrapWithHTTPLimits(cfg, rpcDaemon, bridge, logger),
		bridge:  &bridge,
		logger:  logger,
	}
}

func wrapWithGate(g *gate, h jrpc2.Handler) jrpc2.Handler {
	return func(ctx context.Context, req *jrpc2.Request) (any, error) {
		if g.load() == nil {
			return nil, errBackfillInProgress
		}
		return h(ctx, req)
	}
}

func wrapWithLatencyTracker(tracker *latencytrack.Tracker, h jrpc2.Handler) jrpc2.Handler {
	return func(ctx context.Context, req *jrpc2.Request) (any, error) {
		start := time.Now()
		res, err := h(ctx, req)
		tracker.Record(time.Since(start))
		return res, err
	}
}

// wrapWithLimiters is the v1 daemon's per-method limiter pair: a backlog
// queue limiter inside a request duration limiter, each feeding the
// v1-compatible network gauges/counters.
func wrapWithLimiters(
	cfg Config, m methodEntry, h jrpc2.Handler, logger *supportlog.Entry,
) jrpc2.Handler {
	longName := toSnakeCase(m.name)
	queueLimiterGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: interfaces.PrometheusNamespace, Subsystem: "network",
		Name: longName + "_inflight_requests",
		Help: "Number of concurrenty in-flight " + m.name + " requests",
	})
	queueLimiter := network.MakeJrpcBacklogQueueLimiter(
		h, queueLimiterGauge, uint64(m.queueLimit), logger)

	requestDurationWarnCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: interfaces.PrometheusNamespace, Subsystem: "network",
		Name: longName + "_execution_threshold_warning",
		Help: "The metric measures the count of " + m.name +
			" requests that surpassed the warning threshold for execution time",
	})
	requestDurationLimitCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: interfaces.PrometheusNamespace, Subsystem: "network",
		Name: longName + "_execution_threshold_limit",
		Help: "The metric measures the count of " + m.name +
			" requests that surpassed the limit threshold for execution time",
	})
	cfg.PromRegistry.MustRegister(queueLimiterGauge, requestDurationWarnCounter, requestDurationLimitCounter)

	durationLimiter := network.MakeJrpcRequestDurationLimiter(
		queueLimiter.Handle,
		m.requestDurationLimit/warningThresholdDenominator,
		m.requestDurationLimit,
		requestDurationWarnCounter,
		requestDurationLimitCounter,
		logger)
	return durationLimiter.Handle
}

// wrapWithHTTPLimits is the v1 daemon's HTTP-layer stack around the bridge:
// global backlog queue limit, global request duration limit, request size
// cap, CORS.
func wrapWithHTTPLimits(
	cfg Config, d *rpcDaemon, bridge http.Handler, logger *supportlog.Entry,
) http.Handler {
	globalQueueGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: d.MetricsNamespace(), Subsystem: "network", Name: "global_inflight_requests",
		Help: "Number of concurrenty in-flight http requests",
	})
	queueLimited := network.MakeHTTPBacklogQueueLimiter(
		bridge, globalQueueGauge,
		uint64(derefOr(cfg.Serving.RequestBacklogGlobalQueueLimit, 0)), logger)

	globalWarnCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: d.MetricsNamespace(), Subsystem: "network",
		Name: "global_request_execution_duration_threshold_warning",
		Help: "The metric measures the count of requests that surpassed the warning threshold for execution time",
	})
	globalLimitCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: d.MetricsNamespace(), Subsystem: "network",
		Name: "global_request_execution_duration_threshold_limit",
		Help: "The metric measures the count of requests that surpassed the limit threshold for execution time",
	})
	cfg.PromRegistry.MustRegister(globalQueueGauge, globalWarnCounter, globalLimitCounter)

	maxRequestDuration := derefOr(cfg.Serving.MaxRequestExecutionDuration, 0)
	h := network.MakeHTTPRequestDurationLimiter(
		queueLimited,
		maxRequestDuration/warningThresholdDenominator,
		maxRequestDuration,
		globalWarnCounter,
		globalLimitCounter,
		logger)

	limited := http.MaxBytesHandler(h, maxHTTPRequestSize)

	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins:         []string{},
		AllowOriginRequestFunc: func(*http.Request, string) bool { return true },
		AllowedHeaders:         []string{"*"},
		AllowedMethods:         []string{"GET", "PUT", "POST", "PATCH", "DELETE", "HEAD", "OPTIONS"},
	})
	return corsMiddleware.Handler(limited)
}

// decorateHandlers mirrors the v1 daemon's outermost decoration: per-request
// logging plus the v1-compatible request_duration_seconds summary, wrapped
// around every method (gated rejections included).
func decorateHandlers(d *rpcDaemon, logger *supportlog.Entry, m handler.Map) handler.Map {
	requestMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  d.MetricsNamespace(),
		Subsystem:  "json_rpc",
		Name:       "request_duration_seconds",
		Help:       "JSON RPC request duration",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"endpoint", "status"})
	decorated := handler.Map{}
	for endpoint, h := range m {
		decorated[endpoint] = handler.New(func(ctx context.Context, r *jrpc2.Request) (any, error) {
			reqID := strconv.FormatUint(middleware.NextRequestID(), 10)
			logRequest(logger, reqID, r)
			startTime := time.Now()
			result, err := h(ctx, r)
			duration := time.Since(startTime)
			label := prometheus.Labels{"endpoint": r.Method(), "status": "ok"}
			if err != nil {
				var jsonRPCErr *jrpc2.Error
				if errors.As(err, &jsonRPCErr) {
					prometheusLabelReplacer := strings.NewReplacer(" ", "_", "-", "_", "(", "", ")", "")
					label["status"] = prometheusLabelReplacer.Replace(jsonRPCErr.Code.String())
				}
			}
			requestMetric.With(label).Observe(duration.Seconds())
			logResponse(logger, reqID, duration, label["status"], result)
			return result, err
		})
	}
	d.MetricsRegistry().MustRegister(requestMetric)
	return decorated
}

func logRequest(logger *supportlog.Entry, reqID string, req *jrpc2.Request) {
	logger = logger.WithFields(supportlog.F{
		"subsys":   "jsonrpc",
		"req":      reqID,
		"json_req": req.ID(),
		"method":   req.Method(),
	})
	logger.Info("starting JSONRPC request")

	// Params are useful but can be really verbose; debug level only.
	logger.WithField("params", req.ParamString()).Debug("starting JSONRPC request params")
}

func logResponse(logger *supportlog.Entry, reqID string, duration time.Duration, status string, response any) {
	logger = logger.WithFields(supportlog.F{
		"subsys":   "jsonrpc",
		"req":      reqID,
		"duration": duration.String(),
		"json_req": reqID,
		"status":   status,
	})
	logger.Info("finished JSONRPC request")

	if status == "ok" {
		responseBytes, err := json.Marshal(response)
		if err == nil {
			// The result is useful but can be really verbose; debug level only.
			logger.WithField("result", string(responseBytes)).Debug("finished JSONRPC request result")
		}
	}
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
