package rpcv2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
)

func TestRunDaemon_ExposesProcessMetrics(t *testing.T) {
	logger, _ := capturingLogger()
	adminEndpoint := startDaemonWithAdmin(t, &fakeCore{}, logger)

	logger.WithError(errors.New("test-error")).Error("test error 1")
	logger.WithError(errors.New("test-error")).Error("test error 2")

	body := scrapeMetrics(t, "http://"+adminEndpoint+"/metrics")
	assert.Contains(t, body, "soroban_rpc_build_info{")
	assert.Contains(t, body, "soroban_rpc_log_error_total 2")
	assert.Contains(t, body, "go_goroutines ")
	assert.Contains(t, body, "process_start_time_seconds ")
}

// The daemon exports the in-memory events index of the chunk that ingest
// writes to. A broken wiring exports zero, so the test needs a real term.
func TestRunDaemon_ExposesEventsIndexMetrics(t *testing.T) {
	// Core gives one ledger with one event, then blocks.
	first := uint32(chunk.FirstLedgerSeq)
	core := &fakeCore{stream: &fakeCoreStream{
		frames:     map[uint32][]byte{first: rpcv2test.EventLCMBytes(t, first)},
		blockOnCtx: true,
	}}
	adminEndpoint := startDaemonWithAdmin(t, core, silentLogger())

	const ingesting = `stellar_rpc_events_inmemory_index_terms{state="ingesting"} `
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		body := scrapeMetrics(c, "http://"+adminEndpoint+"/metrics")
		// Not assert.Contains: on a failure it prints the full scrape.
		hasTerms := strings.Contains(body, ingesting) && !strings.Contains(body, ingesting+"0\n")
		assert.True(c, hasTerms, "the ingesting chunk must show the terms of the ingested event")
	}, 3*time.Second, 20*time.Millisecond)

	body := scrapeMetrics(t, "http://"+adminEndpoint+"/metrics")
	assert.Contains(t, body, `stellar_rpc_events_inmemory_index_terms{state="completed"} 0`)
	assert.Contains(t, body, `stellar_rpc_events_inmemory_index_dense_terms{state="ingesting"} 0`)
}

// startDaemonWithAdmin starts the daemon with an admin endpoint and returns
// the endpoint after the daemon serves reads. The test cleanup stops the
// daemon and fails the test if the daemon returns an error.
func startDaemonWithAdmin(t *testing.T, core *fakeCore, logger *supportlog.Entry) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	adminEndpoint := l.Addr().String()
	require.NoError(t, l.Close())

	configPath, _ := writeTempConfig(t, fmt.Sprintf("[service]\nadmin_endpoint = %q\n", adminEndpoint))

	var served atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runDaemonWith(ctx, configPath, daemonOptions{
			Backend:    &fakeBackend{tip: chunk.FirstLedgerSeq + 10},
			Core:       core,
			ServeReads: countingServeReads(&served),
			Logger:     logger,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("runDaemonWith did not return after ctx cancel")
		}
	})
	require.Eventually(t, func() bool { return served.Load() == 1 }, 3*time.Second, 5*time.Millisecond)
	return adminEndpoint
}

// scrapeMetrics takes a require.TestingT, so a retry loop can call it with
// an *assert.CollectT.
func scrapeMetrics(t require.TestingT, url string) string {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(raw)
}
