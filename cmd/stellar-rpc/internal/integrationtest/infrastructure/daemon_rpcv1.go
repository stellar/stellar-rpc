package infrastructure

import (
	"fmt"

	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/daemon"
)

type rpcv1Daemon struct {
	test   *Test
	daemon *daemon.Daemon
	log    *supportlog.Entry
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
	d.log.SetExitFunc(func(code int) {
		i.t.Fatalf("Exited with code %d", code)
	})
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

// The rpcv1 daemon reports a fatal exit through the logger's exit hook, which
// fails the test directly, so there is nothing to read here.
func (d *rpcv1Daemon) exited() <-chan error {
	return nil
}

func (d *rpcv1Daemon) logger() *supportlog.Entry {
	return d.log
}
