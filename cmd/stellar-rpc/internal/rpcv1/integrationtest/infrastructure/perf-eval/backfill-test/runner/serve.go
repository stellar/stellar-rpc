package main

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

const (
	rpcPort               = "8000"
	serveSnapshotInterval = 2 * time.Minute
	// snapshotTimeout bounds one box-log upload. Under the tick, so uploads
	// never overlap and a stalled S3 path cannot keep the serve loop from
	// noticing a daemon exit or the ceiling.
	snapshotTimeout = time.Minute
	// ceilingBackstopSlackMinutes keeps the OS poweroff behind the in-process
	// ceiling, so the teardown defers (the final snapshot included) run first.
	ceilingBackstopSlackMinutes = 2
)

type serveEnv struct {
	Ceiling time.Duration `env:"SERVE_CEILING" envDefault:"6h"`
}

// servePhase keeps the daemon available to the blaster leg, snapshotting the
// box log next to the result object until the box is terminated.
func servePhase(ctx context.Context, daemon *daemonHandle, bucket, resultKey string) {
	cfg, err := env.ParseAs[serveEnv]()
	if err != nil {
		logger.Warnf("parsing serve env: %v", err)
		cfg.Ceiling = 6 * time.Hour
	}

	// teardown outlives ctx: the final snapshot and the poweroff must run
	// however the loop below exits
	teardown := context.WithoutCancel(ctx)
	defer rescheduleShutdown(teardown, 1, "serve phase over")
	defer snapshotBoxLog(teardown, bucket, resultKey)
	defer daemon.Stop()

	// the OS poweroff is only the safety net; the in-process ceiling fires first
	backstop := int(math.Ceil(cfg.Ceiling.Minutes())) + ceilingBackstopSlackMinutes
	rescheduleShutdown(ctx, backstop, "serve ceiling backstop")
	snapshotBoxLog(ctx, bucket, resultKey)

	logger.Infof("serving :%s until external termination (ceiling %s)", rpcPort, cfg.Ceiling)
	ceiling := time.After(cfg.Ceiling)
	ticker := time.NewTicker(serveSnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ceiling:
			logger.Warnf("serve ceiling passed without external termination")
			return
		case <-daemon.done:
			logger.Warnf("serving daemon exited: %v", daemon.err)
			dumpDmesgTail(teardown)
			return
		case <-ticker.C:
			snapshotBoxLog(ctx, bucket, resultKey)
		}
	}
}

func dumpDmesgTail(ctx context.Context) {
	out, err := exec.CommandContext(ctx, "dmesg").CombinedOutput()
	if err != nil {
		logger.Warnf("dumping dmesg tail: %v (%s)", err, out)
		return
	}
	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(lines) > 100 {
		lines = lines[len(lines)-100:]
	}
	logger.Warnf("dmesg tail:\n%s", strings.Join(lines, "\n"))
}

// snapshotBoxLog copies the box log so far next to the result object.
func snapshotBoxLog(ctx context.Context, bucket, key string) {
	if bucket == "" || key == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, snapshotTimeout)
	defer cancel()
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
