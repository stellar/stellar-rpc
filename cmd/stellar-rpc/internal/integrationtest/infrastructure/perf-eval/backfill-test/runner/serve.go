package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"
)

// rpcPort is where the daemon serves; it crosses the box boundary (bind
// endpoint here, TARGET_RPC in the coordinator's chained handoff).
const rpcPort = "8000"

// servePhase parks the box serving for the chained blaster leg, which gets
// this box's address through GHA (the coordinator's TARGET_RPC handoff) and
// probes getHealth directly. Termination is external (blaster job's adopt
// cleanup) so the hold only backstops orphaned runs.
func servePhase(ctx context.Context, daemon *daemonHandle) {
	// on the backstop path the leg script's EXIT trap gets a minute to upload
	// the box log before poweroff
	defer rescheduleShutdown(ctx, 1, "serve phase over")
	defer daemon.Stop()

	// the boot-time self-terminate (budget+15m) can predate the blast's end
	ceiling := harness.DurationEnv("SERVE_CEILING", 6*time.Hour)
	rescheduleShutdown(ctx, int(ceiling.Minutes()), "serve ceiling")

	// the happy-path hard kill skips the EXIT-trap upload, persist the log up to now
	snapshotBoxLog(ctx)

	logger.Infof("serving :%s until external termination (ceiling %s)", rpcPort, ceiling)
	select {
	case <-ctx.Done():
	case <-time.After(ceiling):
		logger.Warnf("serve ceiling passed without external termination")
	}
}

// snapshotBoxLog copies the box log so far next to the result object.
func snapshotBoxLog(ctx context.Context) {
	bucket, key := os.Getenv("BUCKET"), os.Getenv("RESULT_KEY")
	if bucket == "" || key == "" {
		return
	}
	dst := fmt.Sprintf("s3://%s/%s/user-data-serving.log", bucket, path.Dir(key))
	cmd := exec.CommandContext(ctx, "aws", "s3", "cp", "/var/log/user-data.log", dst)
	if out, err := cmd.CombinedOutput(); err != nil {
		logger.Warnf("snapshotting box log to %s: %v (%s)", dst, err, out)
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
