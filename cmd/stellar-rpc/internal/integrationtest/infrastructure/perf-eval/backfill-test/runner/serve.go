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

const (
	localURL       = "http://localhost:8000"
	serveReadyName = "serve-ready.json" // sibling of this leg's result object
)

// servePhase is the handoff-mode tail of the backfill leg: the daemon is left
// running so this box becomes the endpoint load test's target RPC. It waits
// out captive-core catchup, advertises the box through a serve-ready object,
// then holds until the blaster leg's result appears (or the serve deadline
// passes) before stopping the daemon. Failures here never touch the
// already-published backfill verdict: they surface through the serve-ready
// object and the blaster leg's own deadlines.
func servePhase(
	ctx context.Context, fetch *harness.S3Fetcher, resultKey, runID, targetSHA string, daemon *daemonHandle,
) {
	defer daemon.Stop()

	catchupDeadline := harness.DurationEnv("CATCHUP_DEADLINE", 90*time.Minute)
	serveDeadline := harness.DurationEnv("SERVE_DEADLINE", 195*time.Minute)
	stopKey := os.Getenv("BLASTER_RESULT_KEY")
	readyKey := path.Join(path.Dir(resultKey), serveReadyName)

	// the bootstrap self-terminate ceiling was sized for the backfill phase;
	// push it out to cover the serve window (the box owns its shutdown from here)
	extendShutdownCeiling(ctx, int(serveDeadline.Minutes())+30)

	publishReady := func(r harness.ServeReady) {
		r.SchemaVersion = 1
		r.RunID = runID
		r.TargetSHA = targetSHA
		if err := harness.PutJSON(ctx, fetch.Client, fetch.Bucket, readyKey, r); err != nil {
			logger.Errorf("publishing serve-ready object: %v", err)
		}
	}

	logger.Infof("serve phase: awaiting catchup (deadline %s)", catchupDeadline)
	hctx, hcancel := context.WithTimeout(ctx, catchupDeadline)
	start := time.Now()
	health, err := harness.AwaitHealthy(hctx, localURL, 15*time.Second)
	hcancel()
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
	publishReady(harness.ServeReady{
		Status:         harness.ServeStatusReady,
		URL:            fmt.Sprintf("http://%s:8000", ip),
		OldestLedger:   health.OldestLedger,
		LatestLedger:   health.LatestLedger,
		CatchupSeconds: catchupSecs,
	})
	logger.Infof("serving http://%s:8000 (ledgers [%d, %d], catchup %ds); holding until blaster result or %s",
		ip, health.OldestLedger, health.LatestLedger, catchupSecs, serveDeadline)

	wctx, wcancel := context.WithTimeout(ctx, serveDeadline)
	defer wcancel()
	if stopKey == "" {
		logger.Warnf("BLASTER_RESULT_KEY unset; serving until the deadline")
		<-wctx.Done()
		return
	}
	for {
		var res harness.Result
		err := harness.GetJSON(wctx, fetch.Client, fetch.Bucket, stopKey, &res)
		switch {
		// A leftover result from an earlier attempt is not a stop signal: this
		// attempt's blaster leg still needs the box.
		case err == nil && harness.SameWorkflowRun(res.RunID, runID) &&
			harness.RunAttempt(res.RunID) >= harness.RunAttempt(runID):
			logger.Infof("blaster leg reported %q; serve phase over", res.Verdict)
			return
		case err == nil:
			logger.Infof("ignoring stale blaster result from run %s", res.RunID)
		case !errors.Is(err, harness.ErrResultNotReady):
			logger.Warnf("blaster result fetch failed; retrying: %v", err)
		}
		select {
		case <-wctx.Done():
			logger.Warnf("serve deadline passed without a blaster result; shutting down")
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

// privateIPv4 returns the box's primary non-loopback IPv4 address, which is
// what the blaster box dials within the VPC.
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
