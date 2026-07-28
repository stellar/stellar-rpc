package host

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	proto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// PrometheusNamespace is the metrics namespace used by the RPC service.
// TODO: deprecate and rename to stellar_rpc
const PrometheusNamespace = "soroban_rpc"

// Daemon defines the interface that the Daemon would be implementing.
// this would be useful for decoupling purposes, allowing to test components without
// the actual daemon.
type Daemon interface {
	MetricsRegistry() *prometheus.Registry
	MetricsNamespace() string
	CoreClient() CoreClient
	FastCoreClient() FastCoreClient

	// CoreVersion is the version string of the stellar-core BINARY this daemon
	// runs — the first line of `stellar-core version`. It describes the binary,
	// not a running process, so an implementation can answer it by looking at the
	// configured binary path alone; getVersionInfo is its only consumer. An
	// implementation that cannot determine the version returns "".
	CoreVersion() string
}

type CoreClient interface {
	Info(ctx context.Context) (*proto.InfoResponse, error)
	SubmitTransaction(ctx context.Context, txBase64 string) (*proto.TXResponse, error)
}

type FastCoreClient interface {
	GetLedgerEntries(ctx context.Context, ledgerSeq uint32, keys ...xdr.LedgerKey) (proto.GetLedgerEntryResponse, error)
}
