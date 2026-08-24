package daemon

import (
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/support/logmetrics"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

// Shared by the build-info metric labels and the startup log fields.
const (
	versionLabel = "version"
	commitLabel  = "commit"
)

func (d *Daemon) registerMetrics() {
	// LogMetricsHook is a metric which counts log lines emitted by stellar rpc
	logMetricsHook := logmetrics.New(host.PrometheusNamespace)
	d.logger.AddHook(logMetricsHook)
	for _, counter := range logMetricsHook {
		d.metricsRegistry.MustRegister(counter)
	}

	buildInfoGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: host.PrometheusNamespace, Subsystem: "build", Name: "info"},
		[]string{versionLabel, "goversion", commitLabel, "branch", "build_timestamp"},
	)
	buildInfoGauge.With(prometheus.Labels{
		versionLabel:      version.Version,
		commitLabel:       version.CommitHash,
		"branch":          version.Branch,
		"build_timestamp": version.BuildTimestamp,
		"goversion":       runtime.Version(),
	}).Inc()

	d.metricsRegistry.MustRegister(collectors.NewGoCollector())
	d.metricsRegistry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	d.metricsRegistry.MustRegister(buildInfoGauge)
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
