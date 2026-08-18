package rpcv2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

const (
	// httpReadTimeout mirrors v1's read-server ReadTimeout.
	httpReadTimeout = 5 * time.Second
	// readShutdownTimeout bounds the graceful drain when a supervised attempt
	// ends; whatever is still in flight after it is cut off. The next attempt
	// cannot bind the port until this returns, so it stays short.
	readShutdownTimeout = 5 * time.Second
)

// The JSON-RPC server is rebuilt every supervised attempt (its handlers hold
// that attempt's query.Registry — "no query survives a restart"), and the
// shared jsonrpc builder MustRegisters its collectors on whatever registry the
// daemon hands it — registering twice panics. So each attempt gets its own
// prometheus registry, and the process-wide /metrics endpoint reads the
// current attempt's through this indirection.
type attemptGatherer struct {
	reg atomic.Pointer[prometheus.Registry]
}

func (g *attemptGatherer) Gather() ([]*dto.MetricFamily, error) {
	if r := g.reg.Load(); r != nil {
		return r.Gather()
	}
	return nil, nil
}

// attemptDaemon is the host.Daemon one supervised attempt's method table sees:
// the process-wide core-backed daemon, except that collectors register on the
// attempt's own registry (see attemptGatherer).
type attemptDaemon struct {
	host.Daemon

	registry *prometheus.Registry
}

func (d attemptDaemon) MetricsRegistry() *prometheus.Registry { return d.registry }

// readServerDeps is everything the production ServeReads needs besides the
// per-attempt query.Registry. All of it is built once per process in
// runDaemonWith; only the registry changes across supervised attempts.
type readServerDeps struct {
	cfg               config.Config
	logger            *supportlog.Entry
	daemon            host.Daemon
	metrics           observability.Metrics
	preflightGetter   methods.PreflightGetter
	feeWindows        *feewindow.FeeWindows
	networkPassphrase string
	retentionWindow   uint32
	attempts          *attemptGatherer
}

// newServeReads returns the production ServeReads: per supervised attempt it
// builds the method table over that attempt's registry, binds
// [service].endpoint, and serves until the returned stop function runs. run()
// calls stop before its registry closes, so the port is released before the
// next attempt binds and no handler outlives its stores.
func newServeReads(deps readServerDeps) func(context.Context, *query.Registry) (func(), error) {
	return func(ctx context.Context, reg *query.Registry) (func(), error) {
		attemptReg := prometheus.NewRegistry()
		handler := newJSONRPCHandler(deps.cfg, handlerParams{
			daemon:            attemptDaemon{Daemon: deps.daemon, registry: attemptReg},
			logger:            deps.logger,
			metrics:           deps.metrics,
			ledgerReader:      adapters.NewLedgerReader(reg),
			transactionReader: adapters.NewTransactionReader(reg, deps.networkPassphrase),
			feeWindows:        deps.feeWindows,
			preflightGetter:   deps.preflightGetter,
			networkPassphrase: deps.networkPassphrase,
			retentionWindow:   deps.retentionWindow,
		})

		var lc net.ListenConfig
		listener, err := lc.Listen(ctx, "tcp", deps.cfg.Service.Endpoint)
		if err != nil {
			handler.Close()
			return nil, fmt.Errorf("read server listen on %q: %w", deps.cfg.Service.Endpoint, err)
		}
		deps.attempts.reg.Store(attemptReg)

		server := &http.Server{Handler: handler, ReadTimeout: httpReadTimeout}
		go func() {
			if serr := server.Serve(listener); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
				deps.logger.WithError(serr).Warn("read server exited")
			}
		}()
		deps.logger.WithField("endpoint", deps.cfg.Service.Endpoint).Info("read server listening")

		// stop runs during teardown, when the attempt's ctx is typically already
		// canceled — a fresh Background timeout is what bounds the drain.
		stop := func() { //nolint:contextcheck
			shutdownCtx, cancel := context.WithTimeout(context.Background(), readShutdownTimeout)
			defer cancel()
			if serr := server.Shutdown(shutdownCtx); serr != nil {
				_ = server.Close()
			}
			handler.Close()
		}
		return stop, nil
	}
}

// startAdminServer binds [service].admin_endpoint and serves pprof plus
// /metrics over the process registry and the current attempt's collectors. One
// per process — nothing here depends on a supervised attempt. The caller owns
// the returned stop.
func startAdminServer(
	ctx context.Context, endpoint string, logger *supportlog.Entry,
	processRegistry *prometheus.Registry, attempts *attemptGatherer,
) (func(), error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/metrics", promhttp.HandlerFor(
		prometheus.Gatherers{processRegistry, attempts}, promhttp.HandlerOpts{}))

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", endpoint)
	if err != nil {
		return nil, fmt.Errorf("admin server listen on %q: %w", endpoint, err)
	}
	server := &http.Server{Handler: mux, ReadTimeout: httpReadTimeout}
	go func() {
		if serr := server.Serve(listener); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			logger.WithError(serr).Warn("admin server exited")
		}
	}()
	logger.WithField("endpoint", endpoint).Info("admin server listening (pprof, /metrics)")

	return func() { _ = server.Close() }, nil
}
