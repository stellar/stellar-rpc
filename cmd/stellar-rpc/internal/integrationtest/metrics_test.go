package integrationtest

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

func TestMetrics(t *testing.T) {
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{ApplyLimits: infrastructure.SkipLimitsUpgrade()})
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

	logger := test.Logger()
	err = errors.Errorf("test-error")
	logger.WithError(err).Error("test error 1")
	logger.WithError(err).Error("test error 2")

	val := metricValue(t, getMetrics(t, metricsURL), "soroban_rpc_log_error_total")
	assert.GreaterOrEqual(t, val, 2.0)
}

func metricValue(t *testing.T, metrics, name string) float64 {
	for line := range strings.SplitSeq(metrics, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			val, err := strconv.ParseFloat(fields[1], 64)
			require.NoError(t, err)
			return val
		}
	}
	require.Failf(t, "metric not found", "%s is not on /metrics", name)
	return 0
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
