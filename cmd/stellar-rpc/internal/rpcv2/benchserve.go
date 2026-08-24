package rpcv2

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
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

// BenchOpenReplayChunk opens the hot DB the replay writes into and publishes
// its handle, so serving can start with the hot tier already in place.
//
// Ordering matters and mirrors the daemon's: startup opens the resume chunk's
// hot DB BEFORE it serves reads, because the ready key that open writes is what
// read-view acquisition derives its frontier from. Opening after the port was
// already bound would leave a window where every read fails.
//
// The open goes through the production bracket, so a chunk with no ready key is
// WIPED and created fresh — which is what a replay wants, and why the caller
// must not point it at a dataset it means to keep.
func BenchOpenReplayChunk(
	cat *catalog.Catalog, resume uint32, reg *query.Registry, logger *supportlog.Entry,
) (*hotchunk.DB, error) {
	c := chunk.IDFromLedger(resume)
	db, err := openHotDBForChunk(cat, c, logger)
	if err != nil {
		return nil, fmt.Errorf("open hot DB for replay chunk %s: %w", c, err)
	}
	reg.PublishHandle(c, db)
	return db, nil
}

// BenchReplayConfig configures BenchReplayIntoRegistry.
type BenchReplayConfig struct {
	// Stream is the bounded ledger stream to replay; its end stops the loop.
	Stream ledgerbackend.LedgerStream

	// Resume is the first ledger to ingest.
	Resume uint32

	// HotDB is the open write target from BenchOpenReplayChunk.
	HotDB *hotchunk.DB

	// Catalog is the adopted catalog; the loop reopens hot DBs through it at
	// every chunk boundary.
	Catalog *catalog.Catalog

	// Registry is the SERVING registry. This is the whole point of the call:
	// every handle the loop opens and every committed ledger's stamp lands
	// here, so reads in flight see the tip advance.
	Registry *query.Registry

	Logger  *supportlog.Entry
	Metrics observability.Metrics
	Sink    ingest.MetricSink
}

func (c BenchReplayConfig) validate() error {
	switch {
	case c.Stream == nil:
		return errors.New("bench replay: Stream is required")
	case c.HotDB == nil:
		return errors.New("bench replay: HotDB is required")
	case c.Catalog == nil:
		return errors.New("bench replay: Catalog is required")
	case c.Registry == nil:
		return errors.New("bench replay: Registry is required")
	case c.Logger == nil:
		return errors.New("bench replay: Logger is required")
	}
	return nil
}

// BenchReplayIntoRegistry runs the daemon's ingestion loop over a bounded
// stream, publishing into the SERVING registry so reads observe the ingest as
// it happens. A bounded stream ending is the expected termination and returns
// nil.
//
// This differs from `bench-ingest hot` in exactly one way, and it is the way
// that matters: RunBoundedIngestionLoop hands the loop a closingSink, which
// discards every latest-ledger stamp and closes each completed chunk's DB
// because nothing is reading it. Here the loop's handoffs go to the real
// registry instead. The ingestion work itself is identical — the same
// runIngestionLoop, the same per-ledger atomic synced batch.
//
// Fee windows are deliberately nil, as in the bounded bench loop: getFeeStats
// is not under measurement and folding fees in would add per-ledger work the
// daemon's own numbers do not attribute to reads.
//
// Chunk boundaries are published to a logging boundary, NOT to a lifecycle
// runner, so no freeze, discard, or prune ever runs. That is a deliberate
// limit: those operations delete files, and pointing them at an operator's
// prepared dataset to satisfy a read benchmark is not a trade worth making.
// What the benchmark measures — read latency while ingestion competes for the
// box — does not depend on the freeze.
func BenchReplayIntoRegistry(ctx context.Context, cfg BenchReplayConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	err := runIngestionLoop(ctx, ingestionLoopConfig{
		Stream:   cfg.Stream,
		Resume:   cfg.Resume,
		HotDB:    cfg.HotDB,
		Catalog:  cfg.Catalog,
		Boundary: loggingBoundary{logger: cfg.Logger},
		Logger:   cfg.Logger,
		Metrics:  cfg.Metrics,
		Sink:     cfg.Sink,
		Registry: cfg.Registry,
		// nil: see the fee-window note above.
		FeeWindows: nil,
	})
	// A bounded stream running out is the expected end, not a failure — the
	// same remap RunBoundedIngestionLoop applies. Without it the caller sees a
	// spurious error, and worse, the loop's deferred close has already shut the
	// hot DB, so every read that follows fails as temporarily-unavailable.
	if errors.Is(err, errStreamEnded) {
		return nil
	}
	return err
}

// loggingBoundary records chunk completions without acting on them — the
// replay's stand-in for the lifecycle a serving daemon would wake. The loop has
// already opened and published the next chunk's handle by the time this runs,
// so reads continue across the boundary; only the cold-side freeze is absent.
type loggingBoundary struct{ logger *supportlog.Entry }

func (b loggingBoundary) Publish() {
	b.logger.Info("bench replay: chunk boundary reached (no freeze: bench-serve runs no lifecycle)")
}
