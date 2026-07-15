package fullhistory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/latencytrack"
)

// adminShutdownGrace bounds how long stop() waits for in-flight admin requests
// (a /metrics scrape) before the daemon exits anyway.
const adminShutdownGrace = 2 * time.Second

// startAdminServer starts the admin HTTP listener ([serving].admin_endpoint):
//
//	GET /metrics      — Prometheus exposition of the daemon's registry
//	GET /latency.json — latency.SnapshotAll() as JSON, durations in seconds
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

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		return "", nil, fmt.Errorf("admin listener on %q: %w", endpoint, err)
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if serveErr := server.Serve(listener); !errors.Is(serveErr, http.ErrServerClosed) {
			logger.WithError(serveErr).Warn("admin server exited")
		}
	}()

	addr := listener.Addr().String()
	logger.WithField("addr", addr).Info("admin server listening (/metrics, /latency.json)")
	stop := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), adminShutdownGrace)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}
	return addr, stop, nil
}
