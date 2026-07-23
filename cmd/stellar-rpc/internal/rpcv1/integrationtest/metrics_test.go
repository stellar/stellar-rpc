package integrationtest

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"testing"

	"github.com/pkg/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

func TestMetrics(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	metricsURL, err := url.JoinPath(test.GetAdminURL(), "/metrics")
	require.NoError(t, err)
	metrics := getMetrics(t, metricsURL)
	buildMetric := fmt.Sprintf(
		"soroban_rpc_build_info{branch=\"%s\",build_timestamp=\"%s\",commit=\"%s\",goversion=\"%s\",version=\"%s\"} 1",
		version.Branch,
		version.BuildTimestamp,
		version.CommitHash,
		runtime.Version(),
		version.Version,
	)
	require.Contains(t, metrics, buildMetric)

	daemon := test.GetDaemon()
	logger := daemon.Logger()
	err = errors.Errorf("test-error")
	logger.WithError(err).Error("test error 1")
	logger.WithError(err).Error("test error 2")

	metricFamilies, err := daemon.MetricsRegistry().Gather()
	require.NoError(t, err)
	var metric *io_prometheus_client.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "soroban_rpc_log_error_total" {
			metric = mf
			break
		}
	}
	assert.NotNil(t, metric)
	val := metric.GetMetric()[0].GetCounter().GetValue()
	assert.GreaterOrEqual(t, val, 2.0)
}

func getMetrics(t *testing.T, url string) string {
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	responseBytes, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return string(responseBytes)
}
