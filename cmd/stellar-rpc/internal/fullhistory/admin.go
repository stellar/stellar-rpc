package fullhistory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/latencytrack"
)

// httpShutdownGrace bounds how long a listener's stop func waits for in-flight
// requests (a /metrics scrape, a JSON-RPC response) before the daemon exits
// anyway.
const httpShutdownGrace = 2 * time.Second

// startAdminServer starts the admin HTTP listener ([serving].admin_endpoint):
//
//	GET /metrics      — Prometheus exposition of the daemon's registry
//	GET /latency.json — latency.SnapshotAll() as JSON, durations in seconds
//	/debug/pprof/...  — the standard Go profiling endpoints
//
// It binds synchronously (a bad endpoint fails daemon startup) and serves from
// a goroutine. The returned address is the bound one — with an ":0" endpoint
// the kernel picks the port, so the address is how tests learn it. The returned
// stop func shuts the server down; runDaemonWith defers it around the
// supervised run loop, so the listener spans restarts and dies only with the
// daemon itself.
func startAdminServer(
	endpoint string,
	registry *prometheus.Registry,
	latency *latencytrack.Set,
	logger *supportlog.Entry,
) (string, func(), error) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /latency.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if encErr := json.NewEncoder(w).Encode(latency.SnapshotAll()); encErr != nil {
			logger.WithError(encErr).Warn("admin: write /latency.json response")
		}
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	addr, stop, err := startHTTPServer("admin", endpoint, server, logger)
	if err != nil {
		return "", nil, err
	}
	logger.WithField("addr", addr).Info("admin server listening (/metrics, /latency.json, /debug/pprof)")
	return addr, stop, nil
}

// startHTTPServer binds endpoint synchronously (a bad endpoint fails daemon
// startup) and serves from a goroutine. It returns the bound address (with an
// ":0" endpoint the kernel picks the port — the address is how tests learn it)
// and a stop func that shuts the server down with a short drain.
func startHTTPServer(
	name, endpoint string, server *http.Server, logger *supportlog.Entry,
) (string, func(), error) {
	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		return "", nil, fmt.Errorf("%s listener on %q: %w", name, endpoint, err)
	}
	go func() {
		if serveErr := server.Serve(listener); !errors.Is(serveErr, http.ErrServerClosed) {
			logger.WithError(serveErr).Warnf("%s server exited", name)
		}
	}()
	stop := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownGrace)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}
	return listener.Addr().String(), stop, nil
}
