package infrastructure

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

const (
	rpcv2ConfigFilename = "rpcv2-integration-tests.toml"

	// The rpcv2 lifecycle loop returns as soon as its context is canceled and
	// the read server drains for at most 5 seconds, so a stop that takes longer
	// than this is a hang worth failing on.
	rpcv2StopTimeout = 30 * time.Second
)

type rpcv2Daemon struct {
	test *Test
	log  *supportlog.Entry

	// The rpcv2 daemon exposes its own captive core's admin HTTP port; rpcv1
	// runs its captive core without one. This is the extra port rpcv2 needs.
	captiveCoreHTTPPort uint16

	cancel context.CancelFunc
	// done carries the daemon's exit error once, for waitForRPC. stopped is
	// closed at the same moment and is what close waits on, so a stop after
	// waitForRPC already consumed the error does not wait out the timeout.
	done    chan error
	stopped chan struct{}
}

func (d *rpcv2Daemon) start() {
	i := d.test
	ports := getFreeTCPPorts(i.t, 5)
	i.testPorts.captiveCorePeerPort = ports[0]
	i.testPorts.captiveCoreHTTPQueryPort = ports[1]
	d.captiveCoreHTTPPort = ports[2]
	i.testPorts.RPCPort = ports[3]
	i.testPorts.RPCAdminPort = ports[4]

	i.generateCaptiveCoreCfgForDaemon()
	d.prependNetworkPassphrase()

	if d.log == nil {
		// [logging].level only shapes a logger the daemon builds itself, so the
		// injected one is set to debug here to match the test TOML.
		d.log = supportlog.New()
		d.log.SetLevel(logrus.DebugLevel)
		d.log.SetOutput(newTestLogWriter(i.t, `rpc="daemon" `))
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	// The goroutine gets its own copies of the channels: a restart after a
	// port collision replaces the fields while the old daemon may still be
	// finishing, and the old goroutine must report on the channels it was
	// started with.
	done := make(chan error, 1)
	stopped := make(chan struct{})
	d.done, d.stopped = done, stopped
	configPath := filepath.Join(GetCurrentDirectory(), "docker", rpcv2ConfigFilename)
	flags := d.flags()
	log := d.log
	go func() {
		done <- rpcv2.RunDaemonWithOptions(ctx, configPath, rpcv2.Options{Logger: log, Flags: flags})
		close(stopped)
	}()
}

// The rpcv1 daemon takes the passphrase as a setting of its own; rpcv2 reads
// it from the captive-core file and refuses to start without it. The shared
// template stays as it is and the line goes at the top of the generated copy:
// the template ends inside a [[VALIDATORS]] table, so a line appended at the
// end would belong to that table.
func (d *rpcv2Daemon) prependNetworkPassphrase() {
	i := d.test
	fileName := filepath.Join(i.rpcConfigFilesDir, captiveCoreConfigFilename)
	body, err := os.ReadFile(fileName)
	require.NoError(i.t, err)
	line := fmt.Sprintf("NETWORK_PASSPHRASE=%q\n", i.networkPassphrase)
	require.NoError(i.t, os.WriteFile(fileName, append([]byte(line), body...), 0o666))
}

func (d *rpcv2Daemon) flags() *pflag.FlagSet {
	i := d.test
	fs := pflag.NewFlagSet("rpcv2-integration-tests", pflag.ContinueOnError)
	config.BindFlags(fs)
	values := map[string]string{
		"storage.default_data_dir":                 filepath.Join(i.t.TempDir(), "rpcv2"),
		"service.endpoint":                         fmt.Sprintf("127.0.0.1:%d", i.testPorts.RPCPort),
		"service.admin_endpoint":                   fmt.Sprintf("127.0.0.1:%d", i.testPorts.RPCAdminPort),
		"service.methods.getNetwork.friendbot_url": FriendbotURL,
		"ingestion.captive_core_config":            filepath.Join(i.rpcConfigFilesDir, captiveCoreConfigFilename),
		"ingestion.history_archive_urls":           "http://" + i.testPorts.CoreArchiveHostPort,
		"ingestion.stellar_core_binary_path":       findCoreBinary(i.t),
		"ingestion.captive_core_storage_path":      i.captiveCoreStoragePath,
		"ingestion.core_http_port":                 strconv.Itoa(int(d.captiveCoreHTTPPort)),
		"ingestion.core_url":                       fmt.Sprintf("http://127.0.0.1:%d", d.captiveCoreHTTPPort),
		"ingestion.core_http_query_port":           strconv.Itoa(int(i.testPorts.captiveCoreHTTPQueryPort)),
	}
	for name, value := range values {
		require.NoError(i.t, fs.Set(name, value), "flag %s", name)
	}
	return fs
}

func (d *rpcv2Daemon) close() {
	if d.cancel == nil {
		return
	}
	d.cancel()
	select {
	case <-d.stopped:
		select {
		case err := <-d.done:
			if err != nil {
				d.test.t.Logf("rpcv2 daemon stopped with: %v", err)
			}
		default: // waitForRPC already reported the exit error
		}
	case <-time.After(rpcv2StopTimeout):
		d.test.t.Errorf("rpcv2 daemon did not stop within %s", rpcv2StopTimeout)
	}
	d.cancel = nil
}

func (d *rpcv2Daemon) exited() <-chan error {
	return d.done
}

func (d *rpcv2Daemon) logger() *supportlog.Entry {
	return d.log
}

// isBindError reports whether a daemon exit was a port collision. Ports are
// chosen before the daemon binds them, so another process on the host can
// take one in between; the harness then restarts with fresh ports. The
// daemon's own listeners say so in the error. Captive core says it only in
// its log ("bind: Address already in use") and the daemon reports just that
// core exited, so a core exit while the daemon is starting counts too.
func isBindError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "address already in use") ||
		strings.Contains(msg, "stellar core exited unexpectedly")
}
