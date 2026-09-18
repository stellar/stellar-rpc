package host

import (
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/support/logmetrics"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

// RegisterProcessMetrics registers the process-level metric families both
// daemons expose on /metrics: per-level log-line counters, the build_info
// gauge, and the Go runtime and process collectors. One dashboard reads both
// daemons, so the label set lives here and cannot drift between them.
func RegisterProcessMetrics(registry *prometheus.Registry, logger *supportlog.Entry) {
	logCounters := logmetrics.New(PrometheusNamespace)
	logger.AddHook(logCounters)
	for _, counter := range logCounters {
		registry.MustRegister(counter)
	}

	buildInfo := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: PrometheusNamespace, Subsystem: "build", Name: "info"},
		[]string{"version", "goversion", "commit", "branch", "build_timestamp"},
	)
	buildInfo.With(prometheus.Labels{
		"version":         version.Version,
		"commit":          version.CommitHash,
		"branch":          version.Branch,
		"build_timestamp": version.BuildTimestamp,
		"goversion":       runtime.Version(),
	}).Inc()
	registry.MustRegister(buildInfo)

	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}
