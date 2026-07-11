package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"
)

// rpcPort is where the daemon serves; it crosses the box boundary (bind
// endpoint, local health probe, and the URL advertised to the chained leg).
const rpcPort = "8000"

const localURL = "http://localhost:" + rpcPort

// servePhase is the handoff-mode tail of the backfill leg: the daemon is left
// running so this box becomes the endpoint load test's target RPC. It waits
// out captive-core catchup, advertises the box through a serve-ready object,
// then holds until the chained leg's result appears (or the serve deadline
// passes) before stopping the daemon and powering the box off. Failures here
// never touch the already-published backfill verdict: they surface through
// the serve-ready object and the chained leg's own deadlines.
func servePhase(ctx context.Context, leg *harness.Leg, daemon *daemonHandle) {
	defer schedulePoweroff(ctx) // runs last (after daemon.Stop): the box owns its shutdown
	defer daemon.Stop()

	// One deadline bounds the whole serve phase (catchup + holding for the
	// chained leg's result): a slow catchup eats serving time rather than
	// failing the handoff. This is the box's lifecycle bound -- no GHA job
	// watches it anymore -- and the rescheduled shutdown ceiling backstops it.
	serveDeadline := harness.DurationEnv("SERVE_DEADLINE", 195*time.Minute)
	readyKey := path.Join(path.Dir(leg.ResultKey), harness.ServeReadyName)
	// the chained peer's result object is this box's stop signal
	stopKey := ""
	if peer := os.Getenv("CHAIN_PEER"); peer != "" {
		stopKey = harness.SiblingKey(leg.ResultKey, peer, path.Base(leg.ResultKey))
	}

	extendShutdownCeiling(ctx, int(serveDeadline.Minutes())+30)

	wctx, wcancel := context.WithTimeout(ctx, serveDeadline)
	defer wcancel()

	publishReady := func(r harness.ServeReady) {
		r.SchemaVersion = 1
		r.RunID = leg.RunID
		r.TargetSHA = leg.TargetSHA
		if err := harness.PutJSON(ctx, leg.Fetch.Client, leg.Bucket, readyKey, r); err != nil {
			logger.Errorf("publishing serve-ready object: %v", err)
		}
	}

	logger.Infof("serve phase: awaiting catchup (serve window %s)", serveDeadline)
	start := time.Now()
	health, err := harness.AwaitHealthy(wctx, localURL, 15*time.Second)
	if err != nil {
		logger.Errorf("serve phase: %v", err)
		publishReady(harness.ServeReady{Status: harness.ServeStatusFailed, Error: err.Error()})
		return
	}
	catchupSecs := int(time.Since(start).Seconds())

	ip, err := privateIPv4()
	if err != nil {
		logger.Errorf("serve phase: %v", err)
		publishReady(harness.ServeReady{Status: harness.ServeStatusFailed, Error: err.Error()})
		return
	}
	url := fmt.Sprintf("http://%s:%s", ip, rpcPort)
	publishReady(harness.ServeReady{
		Status:         harness.ServeStatusReady,
		URL:            url,
		OldestLedger:   health.OldestLedger,
		LatestLedger:   health.LatestLedger,
		CatchupSeconds: catchupSecs,
	})
	logger.Infof("serving %s (ledgers [%d, %d], catchup %ds); holding until peer result or %s",
		url, health.OldestLedger, health.LatestLedger, catchupSecs, serveDeadline)

	if stopKey == "" {
		logger.Warnf("CHAIN_PEER unset; serving until the deadline")
		<-wctx.Done()
		return
	}
	for {
		var res harness.Result
		err := harness.GetJSON(wctx, leg.Fetch.Client, leg.Bucket, stopKey, &res)
		switch {
		// A leftover result from an earlier attempt is not a stop signal: this
		// attempt's peer leg still needs the box.
		case err == nil && harness.SameWorkflowRun(res.RunID, leg.RunID) &&
			harness.RunAttempt(res.RunID) >= harness.RunAttempt(leg.RunID):
			logger.Infof("peer leg reported %q; serve phase over", res.Verdict)
			return
		case err == nil:
			logger.Infof("ignoring stale peer result from run %s", res.RunID)
		case !errors.Is(err, harness.ErrResultNotReady):
			logger.Warnf("peer result fetch failed; retrying: %v", err)
		}
		select {
		case <-wctx.Done():
			logger.Warnf("serve deadline passed without a peer result; shutting down")
			return
		case <-time.After(30 * time.Second):
		}
	}
}

// extendShutdownCeiling replaces the pending `shutdown -P` with one minutes
// from now. Best-effort: the old ceiling would cut the serve phase short, but
// a failure here only means the box may terminate early, never that it leaks.
func extendShutdownCeiling(ctx context.Context, minutes int) {
	if out, err := exec.CommandContext(ctx, "shutdown", "-c").CombinedOutput(); err != nil {
		logger.Warnf("canceling pending shutdown: %v (%s)", err, out)
	}
	arg := fmt.Sprintf("+%d", minutes)
	cmd := exec.CommandContext(ctx, "shutdown", "-P", arg, "serve-phase self-terminate ceiling")
	if out, err := cmd.CombinedOutput(); err != nil {
		logger.Warnf("rescheduling self-terminate ceiling: %v (%s)", err, out)
	} else {
		logger.Infof("self-terminate ceiling pushed to %s from now", arg)
	}
}

// schedulePoweroff powers the box off in a minute (shutdown behavior is
// terminate), leaving the leg script's EXIT trap time to upload the box log.
func schedulePoweroff(ctx context.Context) {
	logger.Infof("serve phase over; powering off")
	_ = exec.CommandContext(ctx, "shutdown", "-c").Run()
	if out, err := exec.CommandContext(ctx, "shutdown", "-P", "+1", "serve phase complete").CombinedOutput(); err != nil {
		logger.Warnf("scheduling poweroff: %v (%s)", err, out)
	}
}

// privateIPv4 returns the box's primary non-loopback IPv4 address, which is
// what the chained leg's box dials within the VPC.
func privateIPv4() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", fmt.Errorf("listing interfaces: %w", err)
	}
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() {
			if v4 := ipn.IP.To4(); v4 != nil {
				return v4.String(), nil
			}
		}
	}
	return "", errors.New("no non-loopback IPv4 address found")
}
