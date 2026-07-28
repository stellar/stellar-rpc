// Package corestate is the full-history daemon's window onto the CURRENT
// ledger state, which lives in captive core's bucket list rather than in any of
// this daemon's stores.
//
// Three JSON-RPC methods need that state, and all three reach it over HTTP:
//
//   - sendTransaction submits to core's admin server (the only path that floods
//     a transaction to the network);
//   - getLedgerEntries reads entries from core's high-performance query server;
//   - simulateTransaction runs the Rust preflight library in-process, and the
//     library fetches every entry it touches back through that same query
//     server.
//
// Daemon is the concrete host.Daemon those handlers are written against. It is
// deliberately thin: two HTTP clients pointed at the two ports, the metrics
// registry the handlers register on, and the stellar-core binary's version
// string. It holds no reference to the running core process — nothing the
// handlers need requires one, and the ingestion stream owns that process's
// lifecycle (a fresh core per supervised run).
//
// Both clients assume core is up. Before ingestion starts nothing has launched
// core yet, so calls fail at the HTTP layer (connection refused) and surface as
// handler errors.
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

	// QueryPort is the port of core's high-performance query server on localhost
	// ([ingestion].core_http_query_port). Required. Unlike CoreURL this is a bare
	// port: the query server is only ever the local captive-core process, because
	// this daemon is what configured and launched it.
	QueryPort uint

	// RequestTimeout bounds every HTTP request to either server
	// ([ingestion].core_request_timeout). Required — a zero value would mean "no
	// timeout at all", so New rejects it.
	RequestTimeout time.Duration

	// StellarCoreBinaryPath is the RESOLVED path to the stellar-core binary (an
	// explicit [ingestion].stellar_core_binary_path, or whatever was found on
	// PATH). New runs `stellar-core version` against it once. Optional: when it is
	// empty CoreVersion reports "".
	StellarCoreBinaryPath string

	// Registry is where handlers register their collectors. Required, and it must
	// be the daemon's own registry — the one its metrics endpoint serves.
	Registry *prometheus.Registry

	// Namespace prefixes those collectors' names; empty means
	// host.PrometheusNamespace.
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

// New builds the Daemon: one HTTP client per core server, and one up-front read
// of the binary's version.
//
// A failed version lookup is NOT an error. The version is a cosmetic field of
// getVersionInfo's response, so a daemon that cannot run `stellar-core version`
// (no binary on this host yet, wrong permissions) still starts, logs the
// failure, and reports an empty version.
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

	// One client per server, each with its own http.Client so a slow submission
	// cannot consume a connection the query path needs.
	//
	// TODO(#889): the submission client is unwrapped, so v2 does not yet publish
	// v1's two txsub metrics (soroban_rpc_txsub_submission_duration_seconds and
	// _operation_count). v1's wrapper for them lives in internal/rpcv1/daemon and
	// has to move to a shared package before sendTransaction is served.
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

// MetricsRegistry returns the daemon's registry — the real one, so a collector
// registered by a handler shows up on the metrics endpoint.
func (d *Daemon) MetricsRegistry() *prometheus.Registry {
	return d.registry
}

func (d *Daemon) MetricsNamespace() string {
	return d.namespace
}

// CoreClient is the client for core's admin server: sendTransaction's submission
// path.
func (d *Daemon) CoreClient() host.CoreClient {
	return d.coreClient
}

// FastCoreClient is the client for core's query server: getLedgerEntries and
// every entry lookup a preflight makes.
func (d *Daemon) FastCoreClient() host.FastCoreClient {
	return d.fastCoreClient
}

// CoreVersion returns the version string read from the stellar-core binary at
// startup, or "" if it could not be read.
func (d *Daemon) CoreVersion() string {
	return d.coreVersion
}

// LedgerEntryGetter builds the entry reader getLedgerEntries and
// simulateTransaction share: it fetches from core's query server, at the ledger
// this daemon has most recently committed.
//
// The ledger matters. Passing core the daemon's latest sequence — rather than
// letting core answer from its own latest — keeps a read consistent with what
// the rest of this daemon's responses report, even while core runs ahead.
//
// Typical use, from the handler wiring:
//
//	getter := coreDaemon.LedgerEntryGetter(ledgerReader)
//	entries, atLedger, err := getter.GetLedgerEntries(ctx, keys)
func (d *Daemon) LedgerEntryGetter(latestLedgerReader store.LedgerReader) ledgerentries.LedgerEntryGetter {
	return ledgerentries.NewLedgerEntryGetter(d.FastCoreClient(), latestLedgerReader)
}

// readCoreVersion runs `stellar-core version` against the binary once. An empty
// path (no binary configured or found) skips the call entirely.
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
