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

// readShutdownTimeout bounds the graceful drain at shutdown; whatever is
// still in flight after it is cut off.
const readShutdownTimeout = 5 * time.Second

// newServeReads returns the production ServeReads — method table over the
// registry, bound to [service].endpoint; the contract lives on
// StartConfig.ServeReads. params carries the handler inputs; the registry and
// the two readers are filled here.
func newServeReads(cfg config.Config, params handlerParams) serveReadsFn {
	return func(ctx context.Context, reg *query.Registry) (readRunner, error) {
		p := params
		p.registry = reg
		p.ledgerReader = adapters.NewLedgerReader()
		p.transactionReader = adapters.NewTransactionReader(p.networkPassphrase, p.metrics)
		handler := newJSONRPCHandler(cfg, p)

		var lc net.ListenConfig
		listener, err := lc.Listen(ctx, "tcp", cfg.Service.Endpoint)
		if err != nil {
			handler.Close()
			return nil, fmt.Errorf("read server listen on %q: %w", cfg.Service.Endpoint, err)
		}
		server := &http.Server{Handler: handler, ReadTimeout: jsonrpc.DefaultHTTPReadTimeout}
		params.logger.WithField("endpoint", cfg.Service.Endpoint).Info("read server listening")

		run := func(runCtx context.Context) error {
			// Both exits close the server: on death this reaps established
			// keep-alive conns Serve abandoned; after the graceful path's
			// Shutdown it is a no-op.
			defer func() { _ = server.Close() }()
			defer handler.Close()
			died := make(chan error, 1)
			go func() { died <- server.Serve(listener) }()
			select {
			case serr := <-died:
				// Before any shutdown, Serve can only exit on a real failure
				// (nothing else closes the server), so this fails the errgroup
				// and the process.
				return fmt.Errorf("read server died: %w", serr)
			case <-runCtx.Done():
				// Graceful drain, bounded: runCtx is already canceled, so a
				// fresh Background timeout bounds the drain.
				shutdownCtx, cancel := context.WithTimeout(context.Background(), readShutdownTimeout)
				defer cancel()
				if serr := server.Shutdown(shutdownCtx); serr != nil { //nolint:contextcheck // ctx is canceled
					_ = server.Close()
				}
				<-died // ErrServerClosed, by construction
				return runCtx.Err()
			}
		}
		return run, nil
	}
}

// startAdminServer binds [service].admin_endpoint and serves pprof plus
// /metrics over the process registry, where the serving collectors also live.
// One per process — nothing here depends on run()'s query registry. The caller
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
