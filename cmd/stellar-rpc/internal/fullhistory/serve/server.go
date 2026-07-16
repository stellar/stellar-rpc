package serve

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	proto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/registry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/latencytrack"
)

// Config is everything the JSON-RPC assembly needs, resolved once at daemon
// startup. The registry is deliberately absent: it is per-run and arrives
// later, through Server.ServeReads.
type Config struct {
	Logger *supportlog.Entry

	// PromRegistry is the daemon's Prometheus registry; the assembly registers
	// the v1-compatible request metrics (request_duration_seconds, per-method
	// inflight gauges, duration-threshold counters) on it.
	PromRegistry *prometheus.Registry

	// Latency is the daemon's exact-quantile latency set. The assembly feeds
	// the per-endpoint "rpc.<method>" series into it and the `metrics` method
	// snapshots ALL of it (ingestion series included).
	Latency *latencytrack.Set

	// Health is the ingestion loop's readiness/health feed (the fullhistory
	// daemon's health state satisfies it).
	Health HealthSignal

	// NetworkPassphrase comes from the captive-core config file — the one
	// place the network is configured. Required.
	NetworkPassphrase string

	// Serving carries the [serving] limits and durations, defaults already
	// applied (config.WithDefaults).
	Serving config.ServingConfig

	// RetentionChunks is [retention].retention_chunks (0 = full history);
	// getHealth advertises the retention window from it.
	RetentionChunks uint32

	// FastCoreClient serves getLedgerEntries from the live captive core's
	// HTTP query server. nil means the query server is disabled
	// ([ingestion].captive_core_http_query_port = 0) — getLedgerEntries then
	// answers with a clean "disabled" error.
	FastCoreClient interfaces.FastCoreClient
}

func (cfg Config) validate() error {
	if cfg.Logger == nil {
		return errors.New("serve.Config.Logger is required")
	}
	if cfg.PromRegistry == nil {
		return errors.New("serve.Config.PromRegistry is required")
	}
	if cfg.Latency == nil {
		return errors.New("serve.Config.Latency is required")
	}
	if cfg.Health == nil {
		return errors.New("serve.Config.Health is required")
	}
	if cfg.NetworkPassphrase == "" {
		// Fail at assembly, not per run: an empty passphrase would otherwise
		// error every ServeReads and turn the daemon into a restart loop.
		return errors.New("serve.Config.NetworkPassphrase is required")
	}
	return nil
}

// Server is the always-up JSON-RPC serving face of the full-history daemon.
// Its HTTP handler exists (and answers) from daemon startup; whether requests
// reach real data is decided per request by the backfill gate, which
// ServeReads opens once a run's registry exists.
type Server struct {
	cfg     Config
	gate    *gate
	handler *rpcHandler
}

// NewServer assembles the JSON-RPC handler stack with a closed gate. The
// caller owns serving the returned Server's Handler and must Close the Server
// when the daemon exits.
func NewServer(cfg Config) (*Server, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, gate: &gate{}}
	s.handler = newRPCHandler(cfg, s.gate)
	return s, nil
}

// Handler is the complete HTTP stack (CORS, request-size cap, global queue and
// duration limits, JSON-RPC bridge) for the [serving].endpoint listener.
func (s *Server) Handler() http.Handler { return s.handler }

// Close shuts the JSON-RPC bridge down; in-flight requests are abandoned.
func (s *Server) Close() { s.handler.close() }

// ServeReads opens the gate over one daemon run's registry: it builds the
// run's db.* readers and publishes them as the serving state. It matches the
// daemon's ServeReads seam (launch and return, never block).
//
// ctx is the run's context: when it ends — daemon shutdown or a failure
// tearing the run down for a supervised restart — the state this call
// published is withdrawn and every gated method reports "backfill in
// progress" again until the next run publishes. The registry is closed by the
// run that owns it, never here; a request already past the gate when the run
// dies reads from closing stores and surfaces their errors, which is the
// bounded cost of not draining requests on teardown.
func (s *Server) ServeReads(ctx context.Context, reg *registry.Registry) error {
	transactions, err := NewTransactionReader(reg, s.cfg.NetworkPassphrase)
	if err != nil {
		return fmt.Errorf("serve reads: %w", err)
	}
	undo := s.gate.publish(&state{
		ledgers:      NewLedgerReader(reg),
		transactions: transactions,
		events:       NewEventReader(reg),
	})
	go func() {
		<-ctx.Done()
		undo()
	}()
	return nil
}

// GraceFor is the reaper grace period the daemon hands the registry: longer
// than any request the serving config admits (the per-method execution limits
// in the method table, and the global HTTP ceiling), plus margin — so a
// retired store outlives every request that could still be reading it.
func GraceFor(s config.ServingConfig) time.Duration {
	maxDuration := derefOr(s.MaxRequestExecutionDuration, config.DefaultMaxRequestExecutionDuration)
	for _, m := range methodTable {
		maxDuration = max(maxDuration, m.requestDurationLimit)
	}
	return maxDuration + 5*time.Second
}

func derefOr[T any](p *T, def T) T {
	if p == nil {
		return def
	}
	return *p
}

// ---------------------------------------------------------------------------
// The v1 interfaces.Daemon face the reused v1 method handlers consume.
// ---------------------------------------------------------------------------

// rpcDaemon implements interfaces.Daemon for the reused v1 handlers. Only the
// metrics registry/namespace and the fast core client are real; the rest of
// the interface exists for v1 methods this service stubs out.
type rpcDaemon struct {
	promRegistry *prometheus.Registry
	fastClient   interfaces.FastCoreClient
}

var _ interfaces.Daemon = (*rpcDaemon)(nil)

func (d *rpcDaemon) MetricsRegistry() *prometheus.Registry { return d.promRegistry }

// MetricsNamespace keeps the v1 metric names, so dashboards work unchanged.
func (d *rpcDaemon) MetricsNamespace() string { return interfaces.PrometheusNamespace }

func (d *rpcDaemon) FastCoreClient() interfaces.FastCoreClient { return d.fastClient }

// CoreClient exists on the interface for sendTransaction, which this service
// stubs out; any call is a wiring bug, so it errors loudly.
func (d *rpcDaemon) CoreClient() interfaces.CoreClient { return unsupportedCoreClient{} }

// GetCore returns nil: the full-history daemon drives captive core through
// the SDK's stream (which owns the process), so there is no
// CaptiveStellarCore handle to expose. getVersionInfo nil-guards this.
func (d *rpcDaemon) GetCore() *ledgerbackend.CaptiveStellarCore { return nil }

type unsupportedCoreClient struct{}

var errCoreClientUnsupported = errors.New("core client is not supported by the full-history service")

func (unsupportedCoreClient) Info(context.Context) (*proto.InfoResponse, error) {
	return nil, errCoreClientUnsupported
}

func (unsupportedCoreClient) SubmitTransaction(context.Context, string) (*proto.TXResponse, error) {
	return nil, errCoreClientUnsupported
}
