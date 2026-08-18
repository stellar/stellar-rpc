package rpcv2

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
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
			metrics:           observability.NopMetrics{},
			feeWindows:        feewindow.NewFeeWindows(10, 10),
			networkPassphrase: "test passphrase",
			retentionWindow:   1,
		},
		attempts: &attemptGatherer{},
	})

	url := "http://" + cfg.Service.Endpoint

	// First attempt serves; stop releases the port.
	stop, err := serve(context.Background(), r)
	require.NoError(t, err)
	out := postRPC(t, url, "getVersionInfo", `{}`)
	assert.Nil(t, out.Error)
	stop()

	// Second attempt on the same port: rebinding must succeed and the fresh
	// per-attempt metrics registry must not collide with the first attempt's.
	stop, err = serve(context.Background(), r)
	require.NoError(t, err)
	defer stop()
	out = postRPC(t, url, "getVersionInfo", `{}`)
	assert.Nil(t, out.Error)
}
