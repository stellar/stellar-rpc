package rpcv2

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
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

func TestServeReads_ServesAndDrainsOnCancel(t *testing.T) {
	r := seedServingRegistry(t)
	cfg := defaultsConfig(t)
	cfg.Service.Endpoint = freeEndpoint(t)

	serve := newServeReads(cfg, handlerParams{
		daemon:            host.MakeNoOpDaemon(),
		logger:            silentLogger(),
		metrics:           observability.NopMetrics{},
		feeWindows:        feewindow.NewFeeWindows(10, 10),
		networkPassphrase: "test passphrase",
		retentionWindow:   1,
	})

	url := "http://" + cfg.Service.Endpoint

	run, err := serve(context.Background(), r)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(runCtx) }()

	out := rpcv2test.PostRPC(t, url, "getVersionInfo", `{}`)
	assert.Nil(t, out.Error)

	// Cancel is a graceful drain: the runner returns its ctx error, never a
	// death — a false death here would crash a process that is shutting down.
	cancel()
	select {
	case rerr := <-done:
		require.ErrorIs(t, rerr, context.Canceled, "a graceful shutdown is not a server death")
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not return after cancel")
	}
}
