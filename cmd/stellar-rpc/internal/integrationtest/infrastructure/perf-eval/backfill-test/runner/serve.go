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

// servePhase is the serving side of the leg chaining protocol: await catchup,
// advertise, hold for the peer's result, power off.
func servePhase(
	ctx context.Context, fetch *harness.S3Fetcher, resultKey, runID, targetSHA string, daemon *daemonHandle,
) {
	// runs last (after daemon.Stop): the box owns its shutdown, leaving the leg
	// script's EXIT trap a minute to upload the box log
	defer rescheduleShutdown(ctx, 1, "serve phase complete")
	defer daemon.Stop()

	// one deadline bounds catchup + holding, so a slow catchup eats serving
	// time rather than failing the handoff
	serveDeadline := harness.DurationEnv("SERVE_DEADLINE", 195*time.Minute)
	readyKey := path.Join(path.Dir(resultKey), harness.ServeReadyName)
	// the chained peer's result object is this box's stop signal
	stopKey := ""
	if peer := os.Getenv("CHAIN_PEER"); peer != "" {
		stopKey = harness.SiblingKey(resultKey, peer, path.Base(resultKey))
	}

	// the bootstrap's pending self-terminate would cut the serve window short
	rescheduleShutdown(ctx, int(serveDeadline.Minutes())+30, "serve-phase self-terminate ceiling")

	wctx, wcancel := context.WithTimeout(ctx, serveDeadline)
	defer wcancel()

	publishReady := func(r harness.ServeReady) {
		r.SchemaVersion = 1
		r.RunID = runID
		r.TargetSHA = targetSHA
		if err := harness.PutJSON(ctx, fetch.Client, fetch.Bucket, readyKey, r); err != nil {
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
		err := harness.GetJSON(wctx, fetch.Client, fetch.Bucket, stopKey, &res)
		switch {
		// A leftover result from an earlier attempt is not a stop signal: this
		// attempt's peer leg still needs the box.
		case err == nil && harness.SameWorkflowRun(res.RunID, runID) &&
			harness.RunAttempt(res.RunID) >= harness.RunAttempt(runID):
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
		case <-time.After(harness.RendezvousPollInterval):
		}
	}
}

// rescheduleShutdown replaces the box's pending `shutdown -P` with one minutes
// from now (shutdown behavior is terminate). Best-effort: a failure only means
// the box may power off off-schedule, never that it leaks.
func rescheduleShutdown(ctx context.Context, minutes int, reason string) {
	if out, err := exec.CommandContext(ctx, "shutdown", "-c").CombinedOutput(); err != nil {
		logger.Warnf("canceling pending shutdown: %v (%s)", err, out)
	}
	arg := fmt.Sprintf("+%d", minutes)
	if out, err := exec.CommandContext(ctx, "shutdown", "-P", arg, reason).CombinedOutput(); err != nil {
		logger.Warnf("rescheduling shutdown: %v (%s)", err, out)
	} else {
		logger.Infof("shutdown rescheduled to %s from now (%s)", arg, reason)
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
