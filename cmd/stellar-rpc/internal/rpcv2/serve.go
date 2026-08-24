package rpcv2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// readShutdownTimeout bounds the graceful drain when a supervised attempt
// ends; whatever is still in flight after it is cut off. The next attempt
// cannot bind the port until this returns, so it stays short.
const readShutdownTimeout = 5 * time.Second

// readServerDeps is everything the production ServeReads needs besides the
// per-attempt query.Registry. All of it is built once per process in
// runDaemonWith; only the registry changes across supervised attempts.
type readServerDeps struct {
	// params carries the process-wide handler inputs; newServeReads overrides
	// the per-attempt pieces (the two readers) on its copy each attempt.
	params handlerParams
	cfg    config.Config
}

// newServeReads returns the production ServeReads (the contract lives on
// StartConfig.ServeReads): per supervised attempt it builds the method table
// over that attempt's registry, binds [service].endpoint, and serves until the
// returned stop runs — releasing the port before the next attempt binds. A
// Serve failure that is NOT the graceful shutdown (v1 treats these as fatal)
// lands on the returned channel.
func newServeReads(deps readServerDeps) func(context.Context, *query.Registry) (func(), <-chan error, error) {
	return func(ctx context.Context, reg *query.Registry) (func(), <-chan error, error) {
		p := deps.params
		p.ledgerReader = adapters.NewLedgerReader(reg)
		p.transactionReader = adapters.NewTransactionReader(reg, p.networkPassphrase)
		handler := newJSONRPCHandler(deps.cfg, p)

		var lc net.ListenConfig
		listener, err := lc.Listen(ctx, "tcp", deps.cfg.Service.Endpoint)
		if err != nil {
			handler.Close()
			return nil, nil, fmt.Errorf("read server listen on %q: %w", deps.cfg.Service.Endpoint, err)
		}

		server := &http.Server{Handler: handler, ReadTimeout: jsonrpc.DefaultHTTPReadTimeout}
		// Buffered so the send never blocks: after a graceful stop the watcher
		// in run() is already gone. ErrServerClosed (the graceful path) is
		// filtered here, so nothing is ever sent for a clean shutdown.
		died := make(chan error, 1)
		go func() {
			if serr := server.Serve(listener); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
				died <- serr
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
		return stop, died, nil
	}
}

// startAdminServer binds [service].admin_endpoint and serves pprof plus
// /metrics over the process registry, where the serving collectors also live.
// One per process — nothing here depends on a supervised attempt. The caller
// owns the returned stop.
func startAdminServer(
	ctx context.Context, endpoint string, logger *supportlog.Entry,
	processRegistry *prometheus.Registry,
) (func(), error) {
	mux := jsonrpc.NewAdminMux(logger, processRegistry)

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", endpoint)
	if err != nil {
		return nil, fmt.Errorf("admin server listen on %q: %w", endpoint, err)
	}
	server := &http.Server{Handler: mux, ReadTimeout: jsonrpc.DefaultHTTPReadTimeout}
	go func() {
		// Log-only on purpose, matching v1: a dead admin server (pprof,
		// /metrics) must not take down a node that is still serving reads.
		if serr := server.Serve(listener); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			logger.WithError(serr).Error("admin server exited")
		}
	}()
	logger.WithField("endpoint", endpoint).Info("admin server listening (pprof, /metrics)")

	return func() { _ = server.Close() }, nil
}
