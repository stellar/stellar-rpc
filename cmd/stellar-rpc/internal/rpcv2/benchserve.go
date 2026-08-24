package rpcv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// The read-only serving seam for `bench-serve`. The daemon's own entry
// point (RunDaemon) cannot serve a prepared dataset: it requires live captive
// core plus a bulk source with a reachable tip, and it backfills to that tip
// before the port binds. A read benchmark over a fixed dataset has neither, so
// it needs a way in that skips ingestion and backfill while keeping the read
// path itself untouched.
//
// This is that way in, and it is deliberately thin: BenchServeReads composes
// the SAME newServeReads the daemon calls, so the method table, the per-request
// shared read view, the routing error classification, and the page limits are
// production's, not a copy. What the bench supplies instead of the daemon is
// only the query.Registry — built from an adopted catalog rather than from
// backfill (see the bench package's serve command).
//
// Nothing here is reachable from the daemon path; it exists for the bench
// subcommand alone.

// BenchServeConfig configures BenchServeReads.
type BenchServeConfig struct {
	// Endpoint is the host:port the read server binds, replacing
	// [service].endpoint.
	Endpoint string

	// NetworkPassphrase is the passphrase the transaction reader hashes
	// envelopes with, so a served transaction hash matches the dataset's.
	NetworkPassphrase string

	// Registry is the serving state reads route through: the adopted catalog's
	// frozen chunks, the published hot handles, and the latest ledger.
	Registry *query.Registry

	// Logger receives the read server's lifecycle lines.
	Logger *supportlog.Entry

	// RetentionWindow is the window width in ledgers getHealth reports. Zero
	// (full history) is what a Retention of size 0 yields, matching the daemon.
	RetentionWindow uint32
}

func (c BenchServeConfig) validate() error {
	switch {
	case c.Endpoint == "":
		return errors.New("bench serve: Endpoint is required")
	case c.NetworkPassphrase == "":
		return errors.New("bench serve: NetworkPassphrase is required")
	case c.Registry == nil:
		return errors.New("bench serve: Registry is required")
	case c.Logger == nil:
		return errors.New("bench serve: Logger is required")
	}
	return nil
}

// BenchServeReads binds cfg.Endpoint and serves the v2 read methods over
// cfg.Registry until ctx is done, returning nil on that clean stop and the
// server's error if it dies first. The port is released before the return.
//
// Serving limits come from config.ParseConfig(nil) — the compiled defaults, so
// the measured page caps are the ones a real deployment gets (getLedgers 20/5,
// getTransactions 200/50) rather than bench-chosen values. There is no config
// file: a benchmark that could quietly widen a cap would not be measuring the
// daemon.
//
// The write-side dependencies are stubs, since nothing here can write: the
// no-op daemon satisfies the handler's host interface, and no preflight getter
// is supplied. sendTransaction and simulateTransaction therefore fail per
// request instead of at startup, which is the honest outcome for a read-only
// process.
func BenchServeReads(ctx context.Context, cfg BenchServeConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	base, err := config.ParseConfig(nil)
	if err != nil {
		return fmt.Errorf("resolve serving defaults: %w", err)
	}
	base.Service.Endpoint = cfg.Endpoint

	serve := newServeReads(readServerDeps{
		cfg: base,
		params: handlerParams{
			daemon:         host.MakeNoOpDaemon(),
			logger:         cfg.Logger,
			handlerMetrics: jsonrpc.NewHandlerMetrics("bench_serve", prometheus.NewRegistry()),
			metrics:        observability.NopMetrics{},
			// Empty windows: no ingestion feeds them in a static run, so
			// getFeeStats answers from nothing rather than from stale numbers.
			feeWindows: feewindow.NewFeeWindows(
				deref(base.Service.FeeStats.ClassicFeeWindowLedgers),
				deref(base.Service.FeeStats.SorobanInclusionFeeWindowLedgers),
			),
			networkPassphrase: cfg.NetworkPassphrase,
			retentionWindow:   cfg.RetentionWindow,
		},
	})

	stop, died, err := serve(ctx, cfg.Registry)
	if err != nil {
		return err
	}
	defer stop()

	select {
	case <-ctx.Done():
		return nil
	case serr := <-died:
		return fmt.Errorf("bench serve read server: %w", serr)
	}
}
