package rpcv2

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/jsonrpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
)

func freeEndpoint(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func TestServeReads_ServesAndRebindsAcrossAttempts(t *testing.T) {
	r, _ := servingRegistry(t)
	cfg := defaultsConfig(t)
	cfg.Service.Endpoint = freeEndpoint(t)

	serve := newServeReads(readServerDeps{
		cfg: cfg,
		params: handlerParams{
			daemon:            host.MakeNoOpDaemon(),
			logger:            silentLogger(),
			handlerMetrics:    jsonrpc.NewHandlerMetrics("test", prometheus.NewRegistry()),
			metrics:           observability.NopMetrics{},
			feeWindows:        feewindow.NewFeeWindows(10, 10),
			networkPassphrase: "test passphrase",
			retentionWindow:   1,
		},
	})

	url := "http://" + cfg.Service.Endpoint

	// First attempt serves; stop releases the port.
	stop, died, err := serve(context.Background(), r)
	require.NoError(t, err)
	out := postRPC(t, url, "getVersionInfo", `{}`)
	assert.Nil(t, out.Error)
	stop()

	// A graceful stop must not read as the server dying — a false death here
	// would make every teardown restart the attempt it is tearing down.
	select {
	case serr := <-died:
		t.Fatalf("graceful stop reported a server death: %v", serr)
	case <-time.After(100 * time.Millisecond):
	}

	// Second attempt on the same port: rebinding must succeed, and rebuilding
	// the method table over the shared per-process metrics must not panic on a
	// duplicate collector registration.
	stop, _, err = serve(context.Background(), r)
	require.NoError(t, err)
	defer stop()
	out = postRPC(t, url, "getVersionInfo", `{}`)
	assert.Nil(t, out.Error)
}
