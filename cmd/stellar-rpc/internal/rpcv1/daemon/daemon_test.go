package daemon

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
)

func TestNewClosesWhatItOpenedWhenStartupFails(t *testing.T) {
	// The database pool and the preflight workers each run goroutines. None
	// may outlive a failed New.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	cfg := startupConfig(t)
	// The admin endpoint is the last thing New binds. Taking its port first
	// makes New fail after every other component is open.
	taken := loopbackListener(t)
	defer taken.Close()
	cfg.AdminEndpoint = taken.Addr().String()

	d, err := New(t.Context(), cfg, supportlog.New())
	require.ErrorContains(t, err, "cannot listen on admin endpoint")
	require.Nil(t, d)

	var dialer net.Dialer
	_, err = dialer.DialContext(t.Context(), "tcp", cfg.Endpoint)
	require.Error(t, err, "the JSON-RPC listener is still bound")
}

func TestNewStopsWhenContextIsCanceled(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	d, err := New(ctx, startupConfig(t), supportlog.New())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, d)
}

// startupConfig is a config New can take all the way to binding its
// listeners without a stellar-core binary or a reachable history archive.
func startupConfig(t *testing.T) *config.Config {
	dir := t.TempDir()
	var cfg config.Config
	require.NoError(t, cfg.SetValues(func(string) (string, bool) { return "", false }))

	// Captive core only runs `stellar-core version` before ingestion starts.
	fakeCore := filepath.Join(dir, "stellar-core")
	require.NoError(t, os.WriteFile(fakeCore,
		[]byte("#!/bin/sh\necho 'stellar-core 29.0.0 (test fake)'\necho 'ledger protocol version: 29'\n"), 0o755))
	captiveCoreConfig := filepath.Join(dir, "captive-core.cfg")
	require.NoError(t, os.WriteFile(captiveCoreConfig, nil, 0o644))

	// A port chosen up front, so the test can dial it after New fails.
	endpoint := loopbackListener(t)
	require.NoError(t, endpoint.Close())

	cfg.StellarCoreBinaryPath = fakeCore
	cfg.CaptiveCoreConfigPath = captiveCoreConfig
	cfg.CaptiveCoreStoragePath = dir
	cfg.NetworkPassphrase = network.TestNetworkPassphrase
	cfg.HistoryArchiveURLs = []string{"http://127.0.0.1:0"} // unreachable, never dialed
	cfg.SQLiteDBPath = filepath.Join(dir, "rpc.sqlite")
	cfg.Endpoint = endpoint.Addr().String()
	return &cfg
}

func loopbackListener(t *testing.T) net.Listener {
	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return l
}

// runnable is a daemon New built against the unreachable archive of
// startupConfig. Its ingestion worker fails for good once its retries run
// out, about five seconds in, unless Run returns first.
func runnable(t *testing.T) *Daemon {
	cfg := startupConfig(t)
	cfg.AdminEndpoint = "127.0.0.1:0"
	d, err := New(t.Context(), cfg, supportlog.New())
	require.NoError(t, err)
	return d
}

func TestRunReturnsNilWhenContextIsCanceled(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	d := runnable(t)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	cancel()
	require.NoError(t, <-done)
}

func TestRunReturnsNilWhenContextDeadlineExpires(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	d := runnable(t)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, d.Run(ctx))
}

func TestRunReturnsTheServerFailure(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	d := runnable(t)

	// Serve on a closed listener fails at once.
	require.NoError(t, d.listener.Close())
	err := d.Run(t.Context())
	require.ErrorContains(t, err, "soroban JSON RPC server encountered fatal error")
}

func TestRunReturnsAFailurePendingNextToACancel(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	d := runnable(t)

	// Let ingestion fail for good before Run starts, so the failure and the
	// cancel are both ready at Run's first select. The failure must win.
	require.Eventually(t, func() bool { return len(d.ingestService.Failed()) == 1 }, 30*time.Second, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := d.Run(ctx)
	require.ErrorContains(t, err, "could not run ingestion")
}
