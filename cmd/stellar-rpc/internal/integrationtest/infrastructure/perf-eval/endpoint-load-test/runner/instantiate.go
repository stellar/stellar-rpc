package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"
)

// legDir is this leg's path under the repo root, where its blaster endpoint
// roster is checked in (the runner runs with cwd = repo root).
const legDir = "cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/endpoint-load-test"

// blasterEnv is the leg's env-derived config.
type blasterEnv struct {
	RampUp    string `env:"BLASTER_RAMP_UP"  envDefault:"2m"`
	Duration  string `env:"BLASTER_DURATION" envDefault:"3m"`
	SeedCount string `env:"SEED_COUNT"       envDefault:"1000"`
	// left buffer outruns retention trimming during the blast; right buffer
	// keeps clear of the (still advancing) tip
	BufferLow  int64 `env:"SEED_BUFFER_LOW"  envDefault:"1000"`
	BufferHigh int64 `env:"SEED_BUFFER_HIGH" envDefault:"128"`
	// serving box's address, passed by the coordinator once the backfill leg passes
	TargetRPC      string        `env:"TARGET_RPC"`
	CatchupTimeout time.Duration `env:"CATCHUP_TIMEOUT" envDefault:"60m"`
	BudgetMinutes  int           `env:"BUDGET_MINUTES"`
	BlasterRepo    string        `env:"BLASTER_REPO"    envDefault:"stellar/stellar-rpc-blaster"`
	BlasterRef     string        `env:"BLASTER_REF"     envDefault:"f6085c38900f1b1c031dfb78658ea81917f58a30"`
}

// instantiate is the instance's blast task: it receives the chained peer's serving
// RPC, generates seed data, runs the endpoint blast, and publishes the stats.
func instantiate(ctx context.Context) error {
	leg, err := harness.LegSetup(ctx, "Endpoint load test")
	if err != nil {
		return err
	}
	cfg, err := env.ParseAs[blasterEnv]()
	if err != nil {
		return leg.Bail("parsing env: %v", err)
	}

	if deadline, ok := harness.BootDeadline(cfg.BudgetMinutes, 25*time.Minute); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
		logger.Infof("leg deadline in %s (budget-derived)", time.Until(deadline).Round(time.Minute))
	}

	if cfg.TargetRPC == "" {
		return leg.Bail("TARGET_RPC unset; nothing to blast")
	}
	target := cfg.TargetRPC

	// fetch + build overlap the target box's catchup
	blasterBin, blasterSHA, err := fetchBlaster(ctx,
		filepath.Join(leg.WorkDir, "stellar-rpc-blaster"), cfg.BlasterRepo, cfg.BlasterRef)
	if err != nil {
		return leg.Bail("%v", err)
	}

	wctx, wcancel := context.WithTimeout(ctx, cfg.CatchupTimeout)
	waitStart := time.Now()
	health, err := harness.AwaitHealthy(wctx, target, 15*time.Second) // await catchup
	wcancel()
	if err != nil {
		return leg.Bail("target RPC %s: %v", target, err)
	}
	handoffSecs := int(time.Since(waitStart).Seconds())
	logger.Infof("target RPC %s serving ledgers [%d, %d] (handoff wait %ds)",
		target, health.OldestLedger, health.LatestLedger, handoffSecs)

	lo, hi := int64(health.OldestLedger)+cfg.BufferLow, int64(health.LatestLedger)-cfg.BufferHigh
	if hi <= lo {
		return leg.Bail("ledger window [%d, %d] leaves no room after buffers +%d/-%d",
			health.OldestLedger, health.LatestLedger, cfg.BufferLow, cfg.BufferHigh)
	}

	// launch blast
	call := blastCall{
		bin: blasterBin, url: target,
		configPath:  filepath.Join(leg.RepoRoot, legDir, "testdata", "endpoints.toml"),
		seedPath:    filepath.Join(leg.WorkDir, "blaster-seed.json"),
		resultsPath: filepath.Join(leg.WorkDir, "blaster-results.json"),
		rampUp:      cfg.RampUp, duration: cfg.Duration,
	}
	if err := generateSeed(ctx, call, lo, hi, cfg.SeedCount); err != nil {
		return leg.Bail("%v", err)
	}
	if err := blast(ctx, call); err != nil {
		return leg.Bail("%v", err)
	}
	data, err := os.ReadFile(call.resultsPath)
	if err != nil {
		return leg.Bail("reading blaster results: %v", err)
	}
	rows, err := summarize(data)
	if err != nil {
		return leg.Bail("summarizing blaster results: %v", err)
	}

	md := renderMarkdown(leg.TargetSHA, blasterSHA, cfg.RampUp, cfg.Duration,
		health.OldestLedger, health.LatestLedger, handoffSecs, rows)
	if err := os.WriteFile(leg.ResultsFile, []byte(md), 0o644); err != nil {
		return leg.Bail("writing results: %v", err)
	}
	if err := leg.Publish(ctx, call.resultsPath); err != nil {
		return leg.Bail("publishing result: %v", err)
	}
	return nil
}

// fetchBlaster checks-out and builds stellar-rpc-blaster
func fetchBlaster(ctx context.Context, dir, repo, ref string) (string, string, error) {
	logger.Infof("fetching stellar-rpc-blaster (%s@%s)", repo, ref)
	if err := os.RemoveAll(dir); err != nil {
		return "", "", err
	}
	for _, args := range [][]string{
		{"init", "-q", dir},
		{"-C", dir, "remote", "add", "origin", "https://github.com/" + repo + ".git"},
		{"-C", dir, "fetch", "--depth", "1", "origin", ref},
		{"-C", dir, "checkout", "-q", "--detach", "FETCH_HEAD"},
	} {
		if err := harness.RunStreaming(ctx, "", nil, 20, "git", args...); err != nil {
			return "", "", fmt.Errorf("git %s failed: %w", args[0], err)
		}
	}
	out, err := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", "", fmt.Errorf("resolving blaster commit: %w", err)
	}
	sha := strings.TrimSpace(string(out))

	logger.Infof("building stellar-rpc-blaster at %s", sha)
	if err := harness.RunStreaming(ctx, dir, nil, 40, "make", "build"); err != nil {
		return "", "", fmt.Errorf("blaster build failed: %w", err)
	}
	return filepath.Join(dir, "stellar-rpc-blaster"), sha, nil
}

// blastCall parameterizes one serial blaster sweep.
type blastCall struct {
	bin, url              string
	configPath            string
	seedPath, resultsPath string
	rampUp, duration      string
}

// generateSeed samples the request corpus from the target RPC's ledger window.
func generateSeed(ctx context.Context, c blastCall, lo, hi int64, count string) error {
	logger.Infof("generating seed data: %s ledgers sampled from [%d, %d]", count, lo, hi)
	if err := harness.RunStreaming(ctx, filepath.Dir(c.bin), nil, 40, c.bin, "generate",
		"--rpc-url", c.url,
		"--output", c.seedPath,
		"--ledger-window", fmt.Sprintf("%d,%d", lo, hi),
		"--count", count); err != nil {
		return fmt.Errorf("blaster generate failed: %w", err)
	}
	return nil
}

// blast runs the serial endpoint sweep, writing results to c.resultsPath.
func blast(ctx context.Context, c blastCall) error {
	logger.Infof("blasting endpoints in serial (ramp-up %s, duration %s per endpoint)", c.rampUp, c.duration)
	if err := harness.RunStreaming(ctx, filepath.Dir(c.bin), nil, 80, c.bin, "run",
		"--rpc-url", c.url,
		"--config-path", c.configPath,
		"--input-data-path", c.seedPath,
		"--serial",
		"--ramp-up", c.rampUp,
		"--duration", c.duration,
		"--test-output-path", c.resultsPath); err != nil {
		return fmt.Errorf("blaster run failed: %w", err)
	}
	return nil
}
