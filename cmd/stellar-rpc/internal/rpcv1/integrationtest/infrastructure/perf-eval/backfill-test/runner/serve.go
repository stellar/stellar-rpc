package main

import (
	"context"
	"fmt"
	"os/exec"
	"path"
	"time"

	"github.com/caarlos0/env/v11"
)

const rpcPort = "8000" // rpcPort is where the daemon serves, crosses VPC

// serveEnv is the serve phase's env-derived config.
type serveEnv struct {
	Ceiling   time.Duration `env:"SERVE_CEILING" envDefault:"6h"`
	Bucket    string        `env:"BUCKET"`
	ResultKey string        `env:"RESULT_KEY"`
}

// servePhase parks the box serving for the blaster leg, which gets this box's
// IP through the coordinator. Termination is through other job's adopt cleanup.
func servePhase(ctx context.Context, daemon *daemonHandle) {
	// on the backstop path the leg script's EXIT trap gets a minute to upload
	// the box log before poweroff
	defer rescheduleShutdown(ctx, 1, "serve phase over")
	defer daemon.Stop()

	cfg, err := env.ParseAs[serveEnv]()
	if err != nil {
		logger.Warnf("parsing serve env: %v", err)
		cfg.Ceiling = 6 * time.Hour
	}

	// the boot-time self-terminate (budget+15m) can predate the blast's end
	rescheduleShutdown(ctx, int(cfg.Ceiling.Minutes()), "serve ceiling")

	// the happy-path hard kill skips the EXIT-trap upload, persist the log up to now
	snapshotBoxLog(ctx, cfg.Bucket, cfg.ResultKey)

	logger.Infof("serving :%s until external termination (ceiling %s)", rpcPort, cfg.Ceiling)
	select {
	case <-ctx.Done():
	case <-time.After(cfg.Ceiling):
		logger.Warnf("serve ceiling passed without external termination") // backstops orphaned runs
	}
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
