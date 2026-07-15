package fullhistory

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/latencytrack"
)

// httpGetBody GETs url and returns the status code and body.
func httpGetBody(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

// latencySnapshot is the /latency.json wire form a test needs: series name to
// count + max/avg seconds.
type latencySnapshot map[string]struct {
	Count uint64  `json:"count"`
	Avg   float64 `json:"avg"`
	Max   float64 `json:"max"`
}

func getLatencyJSON(t *testing.T, addr string) latencySnapshot {
	t.Helper()
	status, body := httpGetBody(t, "http://"+addr+"/latency.json")
	require.Equal(t, http.StatusOK, status)
	var snap latencySnapshot
	require.NoError(t, json.Unmarshal([]byte(body), &snap))
	return snap
}

func TestStartAdminServer_ServesMetricsAndLatencyJSON(t *testing.T) {
	registry := prometheus.NewRegistry()
	probe := prometheus.NewCounter(prometheus.CounterOpts{Name: "admin_probe_total", Help: "probe"})
	registry.MustRegister(probe)
	probe.Inc()

	set := new(latencytrack.Set)
	set.Tracker(latSeriesIngestE2E).Record(1500 * time.Millisecond)

	addr, stop, err := startAdminServer("127.0.0.1:0", registry, set, silentLogger())
	require.NoError(t, err)
	defer stop()

	snap := getLatencyJSON(t, addr)
	require.Contains(t, snap, latSeriesIngestE2E)
	assert.Equal(t, uint64(1), snap[latSeriesIngestE2E].Count)
	assert.InDelta(t, 1.5, snap[latSeriesIngestE2E].Max, 1e-9, "durations serialize as seconds")

	status, body := httpGetBody(t, "http://"+addr+"/metrics")
	require.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "admin_probe_total 1")
}

func TestStartAdminServer_BadEndpointFailsStartup(t *testing.T) {
	_, _, err := startAdminServer("not-a-listen-address", prometheus.NewRegistry(), nil, silentLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "admin listener")
}

func TestChunkLatencySink_MirrorsColdChunkTotal(t *testing.T) {
	set := new(latencytrack.Set)
	sink := chunkLatencySink{MetricSink: ingest.NopSink{}, chunk: set.Tracker(latSeriesBackfillChunk)}

	sink.ColdChunkTotal(3 * time.Second)
	sink.ColdChunkTotal(5 * time.Second)

	stats := set.SnapshotAll()[latSeriesBackfillChunk]
	assert.Equal(t, uint64(2), stats.Count)
	assert.Equal(t, 5*time.Second, stats.Max)
}
