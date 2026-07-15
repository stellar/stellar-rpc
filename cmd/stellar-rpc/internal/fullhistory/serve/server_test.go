package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/fhtest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"

	"github.com/prometheus/client_golang/prometheus"
)

// fakeHealth is a mutable HealthLike for the server probes: Ready() and the last
// commit close time are both flipped by the test to prove /ready and /health
// reflect the live signal.
type fakeHealth struct {
	ready      atomic.Bool
	closeUnix  atomic.Int64
	haveCommit atomic.Bool
}

func (f *fakeHealth) Ready() bool { return f.ready.Load() }

func (f *fakeHealth) LastCommitClose() (time.Time, bool) {
	if !f.haveCommit.Load() {
		return time.Time{}, false
	}
	return time.Unix(f.closeUnix.Load(), 0), true
}

// buildServerRegistry wires a tiny live-hot registry (one chunk, two zero-tx
// ledgers) so StartServer has a real getLedgers path to serve.
func buildServerRegistry(t *testing.T) (*Registry, geometry.Layout, uint32, uint32) {
	t.Helper()
	cat, layout := newTestCatalog(t)
	first := chunk.ID(0).FirstLedger()
	last := first + 1
	require.NoError(t, cat.PinEarliestLedger(first))

	db0 := openHotChunk(t, cat, layout, chunk.ID(0), first, last)
	t.Cleanup(func() { _ = db0.Close() })

	r := NewRegistry(cat, fhtest.RetentionFor(t, cat, 0), silentLogger())
	require.NoError(t, r.BuildInitial(last))
	r.HotOpened(chunk.ID(0), db0)
	return r, layout, first, last
}

// startTestServer boots a server over the tiny hot fixture and returns its base
// URL, the metrics registry, and the fixture's first/last served ledger.
func startTestServer(t *testing.T, health HealthLike) (string, *prometheus.Registry, uint32, uint32) {
	t.Helper()
	reg, layout, first, last := buildServerRegistry(t)
	metrics := prometheus.NewRegistry()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	addr, shutdown, err := StartServer(ctx, ServerParams{
		Registry:   reg,
		Layout:     layout,
		Passphrase: "Test SDF Network ; September 2015",
		Health:     health,
		Metrics:    metrics,
		Logger:     silentLogger(),
		Cfg:        config.ServeConfig{Endpoint: "127.0.0.1:0"},
	})
	require.NoError(t, err)
	t.Cleanup(shutdown)
	return "http://" + addr.String(), metrics, first, last
}

func postJSON(t *testing.T, url, body string) map[string]any {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out), "response: %s", raw)
	return out
}

func TestStartServer_GetLedgersOverHTTP(t *testing.T) {
	h := &fakeHealth{}
	h.ready.Store(true)
	base, _, first, last := startTestServer(t, h)

	req := `{"jsonrpc":"2.0","id":1,"method":"getLedgers","params":{"startLedger":` +
		itoa(int(first)) + `,"pagination":{"limit":10}}}`
	got := postJSON(t, base+"/", req)

	require.Nil(t, got["error"], "getLedgers must not error: %v", got["error"])
	result, ok := got["result"].(map[string]any)
	require.True(t, ok, "result present: %v", got)
	assert.EqualValues(t, last, result["latestLedger"])
	ledgers, ok := result["ledgers"].([]any)
	require.True(t, ok)
	assert.Len(t, ledgers, int(last-first+1))
}

func TestStartServer_MetricsExposeLatencyHistogram(t *testing.T) {
	h := &fakeHealth{}
	h.ready.Store(true)
	base, _, first, _ := startTestServer(t, h)

	req := `{"jsonrpc":"2.0","id":1,"method":"getLedgers","params":{"startLedger":` +
		itoa(int(first)) + `,"pagination":{"limit":10}}}`
	postJSON(t, base+"/", req)

	resp, err := http.Get(base + "/metrics")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "fullhistory_rpc_request_duration_seconds",
		"the benchmark latency histogram must be exposed after a request")
	assert.Contains(t, string(body), `endpoint="getLedgers"`,
		"the histogram must be labelled per endpoint")
}

func TestStartServer_ReadyReflectsHealth(t *testing.T) {
	h := &fakeHealth{} // not ready yet
	base, _, _, _ := startTestServer(t, h)

	resp, err := http.Get(base + "/ready")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "not ready -> 503")

	h.ready.Store(true)
	resp, err = http.Get(base + "/ready")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "ready -> 200")
}

func TestStartServer_HealthReportsCommit(t *testing.T) {
	h := &fakeHealth{}
	base, _, _, _ := startTestServer(t, h)

	resp, err := http.Get(base + "/health")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "no commit -> 503")

	h.haveCommit.Store(true)
	h.closeUnix.Store(1234567890)
	resp, err = http.Get(base + "/health")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "committed -> 200")
}
