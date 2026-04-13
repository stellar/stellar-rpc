package daemon

import (
	"context"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/stellar/go-stellar-sdk/clients/stellarcore"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	proto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/support/logmetrics"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
)

func (d *Daemon) registerMetrics() {
	// LogMetricsHook is a metric which counts log lines emitted by stellar rpc
	logMetricsHook := logmetrics.New(interfaces.PrometheusNamespace)
	d.logger.AddHook(logMetricsHook)
	for _, counter := range logMetricsHook {
		d.metricsRegistry.MustRegister(counter)
	}

	buildInfoGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: interfaces.PrometheusNamespace, Subsystem: "build", Name: "info"},
		[]string{"version", "goversion", "commit", "branch", "build_timestamp"},
	)
	buildInfoGauge.With(prometheus.Labels{
		"version":         config.Version,
		"commit":          config.CommitHash,
		"branch":          config.Branch,
		"build_timestamp": config.BuildTimestamp,
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
	return interfaces.PrometheusNamespace
}

type CoreClientWithMetrics struct {
	stellarcore.Client
	submitMetric  *prometheus.SummaryVec
	opCountMetric *prometheus.SummaryVec
}

func newCoreClientWithMetrics(client stellarcore.Client, registry *prometheus.Registry) *CoreClientWithMetrics {
	submitMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: interfaces.PrometheusNamespace, Subsystem: "txsub", Name: "submission_duration_seconds",
		Help:       "submission durations to Stellar-Core, sliding window = 10m",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, //nolint:mnd
	}, []string{"status"})
	opCountMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: interfaces.PrometheusNamespace, Subsystem: "txsub", Name: "operation_count",
		Help:       "number of operations included in a transaction, sliding window = 10m",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, //nolint:mnd
	}, []string{"status"})
	registry.MustRegister(submitMetric, opCountMetric)

	return &CoreClientWithMetrics{
		Client:        client,
		submitMetric:  submitMetric,
		opCountMetric: opCountMetric,
	}
}

func (c *CoreClientWithMetrics) SubmitTransaction(ctx context.Context,
	envelopeBase64 string,
) (*proto.TXResponse, error) {
	var envelope xdr.TransactionEnvelope
	err := xdr.SafeUnmarshalBase64(envelopeBase64, &envelope)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	response, err := c.Client.SubmitTransaction(ctx, envelopeBase64)
	duration := time.Since(startTime).Seconds()

	var status string
	switch {
	case err != nil:
		status = "request_error"
	case response.IsException():
		status = "exception"
	default:
		status = response.Status
	}

	label := prometheus.Labels{"status": status}
	c.submitMetric.With(label).Observe(duration)
	c.opCountMetric.With(label).Observe(float64(len(envelope.Operations())))
	return response, err
}

func (d *Daemon) CoreClient() interfaces.CoreClient {
	return d.coreClient
}

func (d *Daemon) FastCoreClient() interfaces.FastCoreClient {
	return d.coreQueryingClient
}

func (d *Daemon) GetCore() *ledgerbackend.CaptiveStellarCore {
	return d.core
}

func (d *Daemon) Logger() *supportlog.Entry {
	return d.logger
}
