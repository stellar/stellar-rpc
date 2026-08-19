package rpcv2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// readShutdownTimeout bounds the graceful drain when a supervised attempt
// ends; whatever is still in flight after it is cut off. The next attempt
// cannot bind the port until this returns, so it stays short.
const readShutdownTimeout = 5 * time.Second

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
	// params carries the process-wide handler inputs; newServeReads overrides
	// the per-attempt pieces (the daemon's metrics registry, the two readers)
	// on its copy each attempt.
	params   handlerParams
	cfg      config.Config
	attempts *attemptGatherer
}

// newServeReads returns the production ServeReads: per supervised attempt it
// builds the method table over that attempt's registry, binds
// [service].endpoint, and serves until the returned stop function runs. run()
// calls stop before its registry closes, so the port is released before the
// next attempt binds and no handler outlives its stores.
func newServeReads(deps readServerDeps) func(context.Context, *query.Registry) (func(), error) {
	return func(ctx context.Context, reg *query.Registry) (func(), error) {
		attemptReg := prometheus.NewRegistry()
		p := deps.params
		p.daemon = attemptDaemon{Daemon: deps.params.daemon, registry: attemptReg}
		p.ledgerReader = adapters.NewLedgerReader(reg)
		p.transactionReader = adapters.NewTransactionReader(reg, p.networkPassphrase)
		handler := newJSONRPCHandler(deps.cfg, p)

		var lc net.ListenConfig
		listener, err := lc.Listen(ctx, "tcp", deps.cfg.Service.Endpoint)
		if err != nil {
			handler.Close()
			return nil, fmt.Errorf("read server listen on %q: %w", deps.cfg.Service.Endpoint, err)
		}
		deps.attempts.reg.Store(attemptReg)

		server := &http.Server{Handler: handler, ReadTimeout: jsonrpc.DefaultHTTPReadTimeout}
		go func() {
			if serr := server.Serve(listener); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
				deps.params.logger.WithError(serr).Warn("read server exited")
			}
		}()
		deps.params.logger.WithField("endpoint", deps.cfg.Service.Endpoint).Info("read server listening")

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
	mux := jsonrpc.NewAdminMux(logger, prometheus.Gatherers{processRegistry, attempts})

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", endpoint)
	if err != nil {
		return nil, fmt.Errorf("admin server listen on %q: %w", endpoint, err)
	}
	server := &http.Server{Handler: mux, ReadTimeout: jsonrpc.DefaultHTTPReadTimeout}
	go func() {
		if serr := server.Serve(listener); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			logger.WithError(serr).Warn("admin server exited")
		}
	}()
	logger.WithField("endpoint", endpoint).Info("admin server listening (pprof, /metrics)")

	return func() { _ = server.Close() }, nil
}
