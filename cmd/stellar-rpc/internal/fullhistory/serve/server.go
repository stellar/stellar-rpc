package serve

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/jhttp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
)

// HealthLike is the tiny read side of the daemon's health signal the server's
// /health + /ready probes consume. It is the shape of fullhistory.HealthSignal,
// declared locally because serve must NOT import the fullhistory root (the root
// imports serve — the reverse edge would be an import cycle).
type HealthLike interface {
	Ready() bool
	LastCommitClose() (time.Time, bool)
}

// ServerParams is everything StartServer needs to mount the read-side JSON-RPC
// server over one serve Registry.
type ServerParams struct {
	Registry   *Registry
	Layout     geometry.Layout
	Passphrase string
	Health     HealthLike
	Metrics    *prometheus.Registry
	Logger     *supportlog.Entry
	Cfg        config.ServeConfig
}

// requestDurationBuckets spans 100µs to 10s — the benchmark's latency dynamic
// range (a cold multi-chunk scan can reach seconds; a hot single-ledger hit is
// sub-millisecond). This is THE instrument the fhbench harness reads.
var requestDurationBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
	0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// StartServer binds the JSON-RPC server on p.Cfg.Endpoint and starts serving in
// a background goroutine. It returns the bound address (so an endpoint of
// host:0 yields the real OS-assigned port), a shutdown func, and any bind error.
// The server also shuts down when ctx is cancelled.
//
// POC: no CORS, no backlog/duration limiters, no request-size cap — the
// benchmark client controls load and the server is not internet-facing. v1's
// jsonrpc.go stacks all of those; add them at productionization.
func StartServer(ctx context.Context, p ServerParams) (net.Addr, func(), error) {
	durations := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fullhistory_rpc_request_duration_seconds",
		Help:    "Per-endpoint JSON-RPC request latency for the full-history read server.",
		Buckets: requestDurationBuckets,
	}, []string{"endpoint", "status"})
	p.Metrics.MustRegister(durations)

	lr := NewLedgerReader(p.Registry, p.Layout)
	txr := NewTransactionReader(p.Registry, p.Layout, p.Passphrase)

	mux := handler.Map{
		protocol.GetLedgersMethodName: instrument(protocol.GetLedgersMethodName, durations,
			methods.NewGetLedgersHandler(lr,
				derefUint(p.Cfg.MaxLedgersLimit, config.DefaultServeMaxLedgersLimit),
				derefUint(p.Cfg.DefaultLedgersLimit, config.DefaultServeDefaultLedgersLimit),
				nil /* POC: no datastore fallback */, p.Logger)),
		protocol.GetTransactionsMethodName: instrument(protocol.GetTransactionsMethodName, durations,
			methods.NewGetTransactionsHandler(p.Logger, lr,
				derefUint(p.Cfg.MaxTransactionsLimit, config.DefaultServeMaxTransactionsLimit),
				derefUint(p.Cfg.DefaultTransactionsLimit, config.DefaultServeDefaultTransactionsLimit),
				p.Passphrase)),
		protocol.GetTransactionMethodName: instrument(protocol.GetTransactionMethodName, durations,
			methods.NewGetTransactionHandler(p.Logger, txr, lr)),
		protocol.GetEventsMethodName: instrument(protocol.GetEventsMethodName, durations,
			NewGetEventsHandler(p.Logger, p.Registry, p.Layout,
				derefUint(p.Cfg.MaxEventsLimit, config.DefaultServeMaxEventsLimit),
				derefUint(p.Cfg.DefaultEventsLimit, config.DefaultServeDefaultEventsLimit))),
	}

	bridge := jhttp.NewBridge(mux, &jhttp.BridgeOptions{
		Server: &jrpc2.ServerOptions{Logger: func(text string) { p.Logger.Debug(text) }},
	})

	httpMux := http.NewServeMux()
	httpMux.Handle("/", bridge)
	httpMux.Handle("/metrics", promhttp.HandlerFor(p.Metrics, promhttp.HandlerOpts{}))
	httpMux.HandleFunc("/health", healthProbe(p.Health))
	httpMux.HandleFunc("/ready", readyProbe(p.Health))

	// Listen first so a host:0 endpoint resolves to a real port before we return.
	ln, err := net.Listen("tcp", p.Cfg.Endpoint)
	if err != nil {
		_ = bridge.Close()
		return nil, nil, err
	}

	srv := &http.Server{
		Handler: httpMux,
		// ReadHeaderTimeout guards against a slow-loris even in the POC (cheap,
		// keeps gosec quiet); every other limiter is deliberately omitted.
		ReadHeaderTimeout: 30 * time.Second,
	}

	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
			p.Logger.WithError(serveErr).Error("fullhistory serve: http server stopped")
		}
	}()

	var once sync.Once
	shutdown := func() {
		once.Do(func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
			_ = bridge.Close()
		})
	}
	// Shut down on ctx cancel too, so the daemon's lifecycle owns the server's
	// lifetime without the caller having to plumb the shutdown func everywhere.
	go func() {
		<-ctx.Done()
		shutdown()
	}()

	return ln.Addr(), shutdown, nil
}

// instrument wraps a jrpc2.Handler to record its wall-clock latency into the
// per-endpoint histogram, labelled by method and ok/error status.
func instrument(endpoint string, vec *prometheus.HistogramVec, h jrpc2.Handler) jrpc2.Handler {
	return func(ctx context.Context, req *jrpc2.Request) (any, error) {
		start := time.Now()
		res, err := h(ctx, req)
		status := "ok"
		if err != nil {
			status = "error"
		}
		vec.WithLabelValues(endpoint, status).Observe(time.Since(start).Seconds())
		return res, err
	}
}

// healthProbe reports liveness: 200 once at least one ledger has been committed
// (carrying its close time), 503 while the daemon is still starting up. The
// staleness judgment (close-time age vs a latency threshold) is left to the
// scraper — the POC probe keeps the raw signal trivial.
func healthProbe(h HealthLike) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		closeTime, committed := lastCommit(h)
		if !committed {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "starting"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":                "healthy",
			"latestLedgerCloseTime": closeTime.Unix(),
		})
	}
}

// readyProbe reflects the latched readiness signal: 200 when the ingestion loop
// has proven itself (committed a ledger), 503 otherwise — the deploy-gate probe.
func readyProbe(h HealthLike) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if h != nil && h.Ready() {
			writeJSON(w, http.StatusOK, map[string]any{"ready": true})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false})
	}
}

func lastCommit(h HealthLike) (time.Time, bool) {
	if h == nil {
		return time.Time{}, false
	}
	return h.LastCommitClose()
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// derefUint returns *p, or def when the key was absent (nil). StartServer accepts
// a bare config.ServeConfig (unpinned limits) and still mounts at the v1 caps.
func derefUint(p *uint, def uint) uint {
	if p == nil {
		return def
	}
	return *p
}
