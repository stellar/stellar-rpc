package rpcv2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

func TestRunDaemon_ExposesProcessMetrics(t *testing.T) {
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	adminEndpoint := l.Addr().String()
	require.NoError(t, l.Close())

	configPath, _ := writeTempConfig(t, fmt.Sprintf("[service]\nadmin_endpoint = %q\n", adminEndpoint))

	logger, _ := capturingLogger()
	var served atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- runDaemonWith(ctx, configPath, daemonOptions{
			Backend:    &fakeBackend{tip: chunk.FirstLedgerSeq + 10},
			Core:       &fakeCore{},
			ServeReads: countingServeReads(&served),
			Logger:     logger,
		})
	}()
	require.Eventually(t, func() bool { return served.Load() == 1 }, 3*time.Second, 5*time.Millisecond)

	logger.WithError(errors.New("test-error")).Error("test error 1")
	logger.WithError(errors.New("test-error")).Error("test error 2")

	body := scrapeMetrics(t, "http://"+adminEndpoint+"/metrics")
	assert.Contains(t, body, "soroban_rpc_build_info{")
	assert.Contains(t, body, "soroban_rpc_log_error_total 2")
	assert.Contains(t, body, "go_goroutines ")
	assert.Contains(t, body, "process_start_time_seconds ")

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("runDaemonWith did not return after ctx cancel")
	}
}

func scrapeMetrics(t *testing.T, url string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(raw)
}
