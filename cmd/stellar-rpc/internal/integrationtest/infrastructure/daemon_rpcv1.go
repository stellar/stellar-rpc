package infrastructure

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/daemon"
)

// Close drains the HTTP servers for up to 10 seconds and then waits for
// captive core to exit, so a stop that takes longer than this is a hang worth
// failing on.
const rpcv1StopTimeout = 60 * time.Second

type rpcv1Daemon struct {
	*runningDaemon

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
	rpcCfg := d.config(cfg)

	// The daemon is built on the run goroutine so that its startup, like its
	// serving, runs on the context close cancels. built is closed once the
	// daemon exists; a build that fails leaves its error on done for
	// waitForRPC or close to report.
	built := make(chan struct{})
	d.runningDaemon = startDaemon(i.t, daemonRPCv1, rpcv1StopTimeout, func(ctx context.Context) error {
		rpcDaemon, err := daemon.New(ctx, rpcCfg, d.log)
		if err != nil {
			// A close during startup is a shutdown request, not a failure,
			// the same way the rpcv1 main treats it.
			if ctx.Err() != nil {
				d.log.WithError(err).Info("shutdown requested during startup")
				return nil
			}
			return err
		}
		d.daemon = rpcDaemon
		close(built)
		return rpcDaemon.Run(ctx)
	})
	select {
	case <-built:
		d.fillPorts()
	case <-d.stopped:
	case <-time.After(rpcHealthyTimeout):
		i.t.Fatalf("rpcv1 daemon did not finish starting within %s", rpcHealthyTimeout)
	}
}

func (d *rpcv1Daemon) config(c rpcConfig) *config.Config {
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
	return &cfg
}

func (d *rpcv1Daemon) fillPorts() {
	endpointAddr, adminEndpointAddr := d.daemon.GetEndpointAddrs()
	d.test.testPorts.RPCPort = uint16(endpointAddr.Port)
	if adminEndpointAddr != nil {
		d.test.testPorts.RPCAdminPort = uint16(adminEndpointAddr.Port)
	}
}

func (d *rpcv1Daemon) logger() *supportlog.Entry {
	return d.log
}
