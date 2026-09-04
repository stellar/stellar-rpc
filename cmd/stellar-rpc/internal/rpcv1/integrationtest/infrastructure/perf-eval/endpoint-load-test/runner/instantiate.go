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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure/perf-eval/harness"
)

// legDir holds the leg's traffic-profile config; runners start w/ cwd = repo root.
const legDir = "cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure/perf-eval/endpoint-load-test"

// blasterEnv is the leg's env-derived config.
type blasterEnv struct {
	RampUp   string `env:"BLASTER_RAMP_UP"  envDefault:"2m"`
	Duration string `env:"BLASTER_DURATION" envDefault:"3m"`
	// max % of acceptable response failure rate before terminating the blast
	ErrorThreshold string `env:"BLASTER_ERROR_THRESHOLD" envDefault:"75"`
	// recovery gap between serial endpoints, so one endpoint's failures
	// don't cascade into the next
	Cooloff   string `env:"BLASTER_COOLOFF" envDefault:"30s"`
	SeedCount string `env:"SEED_COUNT"      envDefault:"1000"`
	// left buffer outruns retention trimming during the blast; right buffer
	// keeps clear of the (still advancing) tip
	BufferLow  int64 `env:"SEED_BUFFER_LOW"  envDefault:"1000"`
	BufferHigh int64 `env:"SEED_BUFFER_HIGH" envDefault:"128"`
	// serving box's address, passed by the coordinator once the backfill leg passes
	TargetRPC      string        `env:"TARGET_RPC"`
	CatchupTimeout time.Duration `env:"CATCHUP_TIMEOUT" envDefault:"60m"`
	BudgetMinutes  int           `env:"BUDGET_MINUTES"`
	BlasterRepo    string        `env:"BLASTER_REPO"    envDefault:"stellar/stellar-rpc-blaster"`
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

	// fetch + build overlap the target box's catchup
	blasterDir := filepath.Join(leg.WorkDir, "stellar-rpc-blaster")
	blasterBin, blasterSHA, err := fetchBlaster(ctx, blasterDir, cfg.BlasterRepo)
	if err != nil {
		return leg.Bail("%v", err)
	}

	wctx, wcancel := context.WithTimeout(ctx, cfg.CatchupTimeout)
	waitStart := time.Now()
	health, err := harness.AwaitHealthy(wctx, cfg.TargetRPC, 15*time.Second) // await catchup
	wcancel()
	if err != nil {
		return leg.Bail("target RPC %s: %v", cfg.TargetRPC, err)
	}
	handoffSecs := int(time.Since(waitStart).Seconds())
	logger.Infof("target RPC %s serving ledgers [%d, %d] (handoff wait %ds)",
		cfg.TargetRPC, health.OldestLedger, health.LatestLedger, handoffSecs)

	lo, hi := int64(health.OldestLedger)+cfg.BufferLow, int64(health.LatestLedger)-cfg.BufferHigh
	if hi <= lo {
		return leg.Bail("ledger window [%d, %d] leaves no room after buffers +%d/-%d",
			health.OldestLedger, health.LatestLedger, cfg.BufferLow, cfg.BufferHigh)
	}

	// launch blast
	call := blastCall{
		bin: blasterBin, url: cfg.TargetRPC,
		configPath: filepath.Join(leg.RepoRoot, legDir, "testdata", "blaster-test-profile.toml"),
		// the profile config pins input_data_path to ./output/seed.json, resolved
		// against the blaster cwd; passing it on the CLI too is a config error
		seedPath:    filepath.Join(blasterDir, "output", "seed.json"),
		resultsPath: filepath.Join(leg.WorkDir, "blaster-results.json"),
		rampUp:      cfg.RampUp, duration: cfg.Duration, cooloff: cfg.Cooloff,
		errorThreshold: cfg.ErrorThreshold,
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
	rows, archRows, aborted, err := summarize(data)
	if err != nil {
		return leg.Bail("summarizing blaster results: %v", err)
	}
	if aborted {
		logger.Warnf("blaster aborted the run: an endpoint crossed the %s%% error kill switch", cfg.ErrorThreshold)
	}

	md := renderMarkdown(leg.TargetSHA, blasterSHA, cfg.RampUp, cfg.Duration, cfg.ErrorThreshold,
		health.OldestLedger, health.LatestLedger, handoffSecs, aborted, rows, archRows)
	if err := os.WriteFile(leg.ResultsFile, []byte(md), 0o644); err != nil {
		return leg.Bail("writing results: %v", err)
	}
	if err := leg.Publish(ctx, call.resultsPath); err != nil {
		return leg.Bail("publishing result: %v", err)
	}
	return nil
}

// fetchBlaster clones and builds stellar-rpc-blaster at dev HEAD.
func fetchBlaster(ctx context.Context, dir, repo string) (string, string, error) {
	// TEMP: restore to @dev before merge
	logger.Infof("fetching stellar-rpc-blaster (%s@dev)", repo)
	if err := os.RemoveAll(dir); err != nil {
		return "", "", err
	}
	if err := harness.RunStreaming(ctx, "", nil, 20, "git", "clone", "-q", "--depth", "1",
		"--branch", "dev", "https://github.com/"+repo+".git", dir); err != nil {
		return "", "", fmt.Errorf("git clone failed: %w", err)
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
	bin, url                string
	configPath              string
	seedPath, resultsPath   string
	rampUp, duration        string
	errorThreshold, cooloff string
}

// generateSeed samples the request corpus from the target RPC's ledger window.
func generateSeed(ctx context.Context, c blastCall, lo, hi int64, count string) error {
	logger.Infof("generating seed data: %s ledgers sampled from [%d, %d]", count, lo, hi)
	if err := os.MkdirAll(filepath.Dir(c.seedPath), 0o755); err != nil {
		return err
	}
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
	logger.Infof("running blaster (--serial enabled, ramp-up %s, duration %s, cooloff %s per endpoint, "+
		"error killswitch %s%%)", c.rampUp, c.duration, c.cooloff, c.errorThreshold)
	if err := harness.RunStreaming(ctx, filepath.Dir(c.bin), nil, 80, c.bin, "run",
		"--rpc-url", c.url,
		"--config-path", c.configPath,
		"--serial",
		"--ramp-up", c.rampUp, "--duration", c.duration,
		"--error-percent", c.errorThreshold,
		"--cooloff", c.cooloff,
		"--test-output-path", c.resultsPath); err != nil {
		return fmt.Errorf("blaster run failed: %w", err)
	}
	return nil
}
