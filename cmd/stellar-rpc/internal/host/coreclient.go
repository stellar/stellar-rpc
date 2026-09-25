package host

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go-stellar-sdk/clients/stellarcore"
	proto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// CoreClientWithMetrics is a stellarcore.Client whose SubmitTransaction also
// records the txsub summaries (submission duration, operation count) that both
// daemons publish under identical series names. Every other method passes
// through to the embedded client unchanged.
type CoreClientWithMetrics struct {
	stellarcore.Client

	submitMetric  *prometheus.SummaryVec
	opCountMetric *prometheus.SummaryVec
}

const submissionStatusLabel = "status"

func NewCoreClientWithMetrics(client stellarcore.Client, registry *prometheus.Registry,
	namespace string,
) *CoreClientWithMetrics {
	submitMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: namespace, Subsystem: "txsub", Name: "submission_duration_seconds",
		Help:       "submission durations to Stellar-Core, sliding window = 10m",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, //nolint:mnd
	}, []string{submissionStatusLabel})
	opCountMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: namespace, Subsystem: "txsub", Name: "operation_count",
		Help:       "number of operations included in a transaction, sliding window = 10m",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, //nolint:mnd
	}, []string{submissionStatusLabel})
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

	label := prometheus.Labels{submissionStatusLabel: status}
	c.submitMetric.With(label).Observe(duration)
	c.opCountMetric.With(label).Observe(float64(len(envelope.Operations())))
	return response, err
}
