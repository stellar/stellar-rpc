// Package corestate reads current ledger state from captive core, which is
// where it lives — the bucket list, not this daemon's stores.
//
// Three methods need it, all over HTTP: sendTransaction submits to core's admin
// server, getLedgerEntries reads from core's query server, and
// simulateTransaction runs the Rust preflight library in-process, which fetches
// the entries it touches from that same query server.
//
// Daemon is the host.Daemon those handlers take. It holds two HTTP clients, a
// metrics registry, and the core binary's version string — no reference to the
// running core process, because nothing here needs one.
//
// Both clients assume core is up. Before ingestion starts it isn't, so calls
// fail with connection refused.
package corestate

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go-stellar-sdk/clients/stellarcore"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// Config is what New needs to build a Daemon. Every field comes from the
// daemon's [ingestion] config section except Registry and Logger.
type Config struct {
	// CoreURL is the base URL of core's admin HTTP server, where transactions
	// are submitted ([ingestion].core_url). Required.
	CoreURL string

	// QueryPort is the port of core's query server ([ingestion].core_http_query_port).
	// Required. A bare port, not a URL: that server is always the local
	// captive-core process this daemon launched.
	QueryPort uint

	// RequestTimeout bounds every request to either server
	// ([ingestion].core_request_timeout). Required: zero would mean no timeout.
	RequestTimeout time.Duration

	// StellarCoreBinaryPath is the resolved stellar-core binary — an explicit
	// [ingestion].stellar_core_binary_path, or the one found on PATH. New runs
	// `stellar-core version` against it once. Empty ⇒ CoreVersion reports "".
	StellarCoreBinaryPath string

	// Registry is where handlers register their collectors. Required, and it must
	// be the registry the daemon's metrics endpoint serves.
	Registry *prometheus.Registry

	// Namespace prefixes those collectors' names; empty ⇒ host.PrometheusNamespace.
	Namespace string

	// Logger is optional; it only records whether the version lookup worked.
	Logger *supportlog.Entry
}

// Daemon implements host.Daemon for the full-history daemon. Build it with New.
type Daemon struct {
	registry       *prometheus.Registry
	namespace      string
	coreClient     *stellarcore.Client
	fastCoreClient *stellarcore.Client
	coreVersion    string
}

// New builds the Daemon: one HTTP client per core server, plus one read of the
// binary's version.
//
// A failed version lookup is not an error — the version is only a field in
// getVersionInfo's response, so the daemon starts anyway and reports "".
func New(cfg Config) (*Daemon, error) {
	if cfg.CoreURL == "" {
		return nil, errors.New("corestate: CoreURL is required (default http://localhost:{core_http_port})")
	}
	if cfg.QueryPort == 0 {
		return nil, errors.New("corestate: QueryPort is required")
	}
	if cfg.RequestTimeout <= 0 {
		return nil, fmt.Errorf("corestate: RequestTimeout must be positive, got %v", cfg.RequestTimeout)
	}
	if cfg.Registry == nil {
		return nil, errors.New("corestate: Registry is required")
	}

	namespace := cfg.Namespace
	if namespace == "" {
		namespace = host.PrometheusNamespace
	}

	// Separate http.Clients so a slow submission cannot tie up a connection the
	// query path needs.
	//
	// TODO(#889): the submission client is unwrapped, so v2 does not yet publish
	// v1's txsub metrics. v1's wrapper for them lives in internal/rpcv1/daemon and
	// must move to a shared package before sendTransaction is served.
	return &Daemon{
		registry:  cfg.Registry,
		namespace: namespace,
		coreClient: &stellarcore.Client{
			URL:  cfg.CoreURL,
			HTTP: &http.Client{Timeout: cfg.RequestTimeout},
		},
		fastCoreClient: &stellarcore.Client{
			URL:  fmt.Sprintf("http://localhost:%d", cfg.QueryPort),
			HTTP: &http.Client{Timeout: cfg.RequestTimeout},
		},
		coreVersion: readCoreVersion(cfg.StellarCoreBinaryPath, cfg.Logger),
	}, nil
}

// MetricsRegistry returns the daemon's own registry, so collectors a handler
// registers show up on the metrics endpoint.
func (d *Daemon) MetricsRegistry() *prometheus.Registry {
	return d.registry
}

func (d *Daemon) MetricsNamespace() string {
	return d.namespace
}

// CoreClient talks to core's admin server: sendTransaction's submission path.
func (d *Daemon) CoreClient() host.CoreClient {
	return d.coreClient
}

// FastCoreClient talks to core's query server: getLedgerEntries and preflight's
// entry lookups.
func (d *Daemon) FastCoreClient() host.FastCoreClient {
	return d.fastCoreClient
}

// CoreVersion returns the version read from the core binary at startup, or "".
func (d *Daemon) CoreVersion() string {
	return d.coreVersion
}

// LedgerEntryGetter builds the entry reader getLedgerEntries and
// simulateTransaction share. It reads from core's query server at the ledger
// this daemon last committed, rather than letting core answer from its own
// latest, so a read agrees with what the daemon's other responses report even
// while core runs ahead.
//
//	getter := coreDaemon.LedgerEntryGetter(ledgerReader)
//	entries, atLedger, err := getter.GetLedgerEntries(ctx, keys)
func (d *Daemon) LedgerEntryGetter(latestLedgerReader store.LedgerReader) ledgerentries.LedgerEntryGetter {
	return ledgerentries.NewLedgerEntryGetter(d.FastCoreClient(), latestLedgerReader)
}

// readCoreVersion runs `stellar-core version` once; an empty path skips it.
func readCoreVersion(binaryPath string, logger *supportlog.Entry) string {
	if binaryPath == "" {
		return ""
	}
	version, err := ledgerbackend.CoreBuildVersion(binaryPath)
	if err != nil {
		if logger != nil {
			logger.WithError(err).WithField("binary_path", binaryPath).
				Warn("could not read the stellar-core version; getVersionInfo will report it as empty")
		}
		return ""
	}
	if logger != nil {
		logger.WithField("stellar_core_version", version).Info("read stellar-core version")
	}
	return version
}
