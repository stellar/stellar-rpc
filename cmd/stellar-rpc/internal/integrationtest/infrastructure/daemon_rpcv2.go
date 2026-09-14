package infrastructure

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
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
	// Only captive core needs its ports chosen up front, because stellar-core
	// reads fixed port numbers from its config file. The daemon's own two
	// listeners bind port 0 and report what the kernel chose.
	ports := getFreeTCPPorts(i.t, 3)
	i.testPorts.captiveCorePeerPort = ports[0]
	i.testPorts.captiveCoreHTTPQueryPort = ports[1]
	d.captiveCoreHTTPPort = ports[2]

	i.generateCaptiveCoreCfgForDaemon()
	d.prependNetworkPassphrase()

	if d.log == nil {
		// [logging].level only shapes a logger the daemon builds itself, so the
		// injected one is set to debug here to match the test TOML.
		d.log = supportlog.New()
		d.log.SetLevel(logrus.DebugLevel)
		d.log.SetOutput(newTestLogWriter(i.t, `rpc="daemon" `))
	}

	configPath := filepath.Join(GetCurrentDirectory(), "docker", rpcv2ConfigFilename)
	listening := make(chan struct{})
	opts := rpcv2.Options{
		Logger: d.log,
		Flags:  d.flags(),
		OnListen: func(rpc, admin net.Addr) {
			i.testPorts.RPCPort = tcpPort(rpc)
			i.testPorts.RPCAdminPort = tcpPort(admin)
			close(listening)
		},
	}

	// Nothing above can fail once these are set: close waits on stopped, and
	// only the goroutine below closes it.
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.done = make(chan error, 1)
	d.stopped = make(chan struct{})
	go func() {
		d.done <- rpcv2.RunDaemonWithOptions(ctx, configPath, opts)
		close(d.stopped)
	}()

	// The daemon binds its read listener only after it has caught up with the
	// history archive, so this wait can be as long as the health wait. A daemon
	// that exits first leaves its error on done for waitForRPC to report.
	select {
	case <-listening:
	case <-d.stopped:
	case <-time.After(rpcHealthyTimeout):
		i.t.Fatalf("rpcv2 daemon did not bind its listeners within %s", rpcHealthyTimeout)
	}
}

func tcpPort(addr net.Addr) uint16 {
	return uint16(addr.(*net.TCPAddr).Port) //nolint:forcetypeassert // the daemon listens on tcp
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
		"service.endpoint":                         "127.0.0.1:0",
		"service.admin_endpoint":                   "127.0.0.1:0",
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
