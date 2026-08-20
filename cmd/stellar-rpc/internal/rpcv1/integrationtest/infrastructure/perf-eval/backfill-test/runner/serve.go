package main

import (
	"context"
	"fmt"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

const (
	rpcPort               = "8000"
	serveSnapshotInterval = 2 * time.Minute
)

type serveEnv struct {
	Ceiling   time.Duration `env:"SERVE_CEILING" envDefault:"6h"`
	Bucket    string        `env:"BUCKET"        envDefault:"stellar-rpc-ci-load-test"`
	ResultKey string        `env:"RESULT_KEY"`
}

// servePhase keeps the daemon available to the blaster leg.
func servePhase(ctx context.Context, daemon *daemonHandle) {
	cfg, err := env.ParseAs[serveEnv]()
	if err != nil {
		logger.Warnf("parsing serve env: %v", err)
		cfg.Ceiling = 6 * time.Hour
	}

	defer rescheduleShutdown(ctx, 1, "serve phase over")
	defer snapshotBoxLog(context.WithoutCancel(ctx), cfg.Bucket, cfg.ResultKey)
	defer daemon.Stop()

	rescheduleShutdown(ctx, int(cfg.Ceiling.Minutes()), "serve ceiling")
	snapshotBoxLog(ctx, cfg.Bucket, cfg.ResultKey)

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
			logger.Warnf("serving daemon exited")
			dumpDmesgTail(ctx)
			return
		case <-ticker.C:
			snapshotBoxLog(ctx, cfg.Bucket, cfg.ResultKey)
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
