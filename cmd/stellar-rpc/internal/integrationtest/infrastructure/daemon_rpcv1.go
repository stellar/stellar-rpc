package infrastructure

import (
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/daemon"
)

type rpcv1Daemon struct {
	test   *Test
	daemon *daemon.Daemon
	log    *supportlog.Entry
	done   chan error
	// building is true while MustNew runs on the test goroutine.
	building atomic.Bool
}

func (d *rpcv1Daemon) start() {
	i := d.test
	// We need to dynamically allocate port numbers since tests run in parallel.
	// Unfortunately this isn't completely clash-free, but there is no way to
	// tell core to allocate the port dynamically.
	// Allocate both ports together so the OS doesn't hand out the same port twice.
	ports := getFreeTCPPorts(i.t, 3)
	i.testPorts.captiveCorePeerPort = ports[0]
	i.testPorts.captiveCoreHTTPQueryPort = ports[1]
	i.generateCaptiveCoreCfgForDaemon()
	cfg := i.getRPConfigForDaemon()
	if !i.ingestLoadTest.Enabled() {
		// Submit through the daemon's own captive core, as rpcv1 does by
		// default in production and as rpcv2 always does.
		cfg.captiveCoreHTTPPort = ports[2]
		cfg.stellarCoreURL = fmt.Sprintf("http://127.0.0.1:%d", ports[2])
	}
	d.daemon = d.create(cfg)
	d.fillPorts()
	go d.daemon.Run()
}

func (d *rpcv1Daemon) create(c rpcConfig) *daemon.Daemon {
	i := d.test
	var cfg config.Config
	m := c.toMap()
	lookup := func(s string) (string, bool) {
		ret, ok := m[s]
		return ret, ok
	}
	require.NoError(i.t, cfg.SetValues(lookup))
	require.NoError(i.t, cfg.Validate())

	if i.datastoreConfigFunc != nil {
		i.datastoreConfigFunc(&cfg)
	}

	if i.ingestLoadTest.Enabled() {
		cfg.IngestLoadTest = i.ingestLoadTest
	}

	d.log = supportlog.New()
	d.log.SetOutput(newTestLogWriter(i.t, `rpc="daemon" `))
	// The daemon's Fatal calls land here. During MustNew they come from the
	// test goroutine, where FailNow is the one correct way to end the test: a
	// bare Goexit there makes the test runner panic the whole binary. Later
	// they come from a daemon goroutine, where FailNow is not allowed, so Error
	// marks the test failed, the channel lets waitForRPC stop at once, and
	// Goexit ends that goroutine the way Fatal would.
	d.done = make(chan error, 1)
	d.log.SetExitFunc(func(code int) {
		err := fmt.Errorf("rpcv1 daemon exited with code %d", code)
		select {
		case d.done <- err:
		default:
		}
		if d.building.Load() {
			i.t.Fatal(err)
		}
		i.t.Error(err)
		runtime.Goexit()
	})
	d.building.Store(true)
	defer d.building.Store(false)
	return daemon.MustNew(&cfg, d.log)
}

func (d *rpcv1Daemon) fillPorts() {
	endpointAddr, adminEndpointAddr := d.daemon.GetEndpointAddrs()
	d.test.testPorts.RPCPort = uint16(endpointAddr.Port)
	if adminEndpointAddr != nil {
		d.test.testPorts.RPCAdminPort = uint16(adminEndpointAddr.Port)
	}
}

func (d *rpcv1Daemon) close() {
	// start may have failed before the daemon was built; there is nothing to stop then.
	if d.daemon != nil {
		d.daemon.Close()
	}
}

func (d *rpcv1Daemon) exited() <-chan error {
	return d.done
}

func (d *rpcv1Daemon) logger() *supportlog.Entry {
	return d.log
}
