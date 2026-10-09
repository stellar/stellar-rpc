package daemon

import (
	"github.com/prometheus/client_golang/prometheus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
)

// Field names of the startup log line. They match the build_info metric
// labels that host.RegisterProcessMetrics registers.
const (
	versionLabel = "version"
	commitLabel  = "commit"
)

func (d *Daemon) registerMetrics() {
	host.RegisterProcessMetrics(d.metricsRegistry, d.logger)
}

func (d *Daemon) MetricsRegistry() *prometheus.Registry {
	return d.metricsRegistry
}

func (d *Daemon) MetricsNamespace() string {
	return host.PrometheusNamespace
}

func (d *Daemon) CoreClient() host.CoreClient {
	return d.coreClient
}

func (d *Daemon) FastCoreClient() host.FastCoreClient {
	return d.coreQueryingClient
}

func (d *Daemon) CoreVersion() string {
	return d.core.GetCoreVersion()
}

func (d *Daemon) Logger() *supportlog.Entry {
	return d.logger
}
