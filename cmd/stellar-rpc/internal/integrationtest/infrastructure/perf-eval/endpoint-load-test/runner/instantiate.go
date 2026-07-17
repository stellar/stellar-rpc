package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"
)

// legDir is this leg's path under the repo root, where its blaster endpoint
// roster is checked in (the runner runs with cwd = repo root).
const legDir = "cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/endpoint-load-test"

// instantiate is the instance's blast task: it rendezvouses with the chained
// peer's serving RPC, generates seed data across its ledger window, runs the
// serial endpoint blast, and publishes the stats.
func instantiate(ctx context.Context) error {
	leg, err := harness.LegSetup(ctx, "Endpoint load test")
	if err != nil {
		return err
	}
	var (
		rampUp    = harness.Env("BLASTER_RAMP_UP", "2m")
		duration  = harness.Env("BLASTER_DURATION", "3m")
		seedCount = harness.Env("SEED_COUNT", "1000")
		// left buffer outruns retention trimming during the blast; right buffer
		// keeps clear of the (still advancing) tip
		bufLow  = harness.Int64Env("SEED_BUFFER_LOW", 1000)
		bufHigh = harness.Int64Env("SEED_BUFFER_HIGH", 128)
	)

	if deadline, ok := harness.LegDeadline(25 * time.Minute); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
		logger.Infof("leg deadline in %s (budget-derived)", time.Until(deadline).Round(time.Minute))
	}

	peer := os.Getenv("CHAIN_PEER")
	if peer == "" {
		return leg.Bail("CHAIN_PEER unset; nothing to rendezvous with")
	}
	readyKey := harness.SiblingKey(leg.ResultKey, peer, harness.ServeReadyName)

	// fetch + build overlap the peer box's captive-core catchup
	blasterBin, blasterSHA, err := fetchBlaster(ctx, filepath.Join(leg.WorkDir, "stellar-rpc-blaster"))
	if err != nil {
		return leg.Bail("%v", err)
	}

	ready, health, err := awaitServeReady(ctx, leg.Fetch.Client, leg.Bucket, readyKey, leg.RunID)
	if err != nil {
		return leg.Bail("%v", err)
	}
	logger.Infof("target RPC %s serving ledgers [%d, %d] (catchup %ds)",
		ready.URL, health.OldestLedger, health.LatestLedger, ready.CatchupSeconds)

	lo := int64(health.OldestLedger) + bufLow
	hi := int64(health.LatestLedger) - bufHigh
	if hi <= lo {
		return leg.Bail("ledger window [%d, %d] leaves no room after buffers +%d/-%d",
			health.OldestLedger, health.LatestLedger, bufLow, bufHigh)
	}

	call := blastCall{
		bin: blasterBin, url: ready.URL,
		configPath:  filepath.Join(leg.RepoRoot, legDir, "testdata", "endpoints.toml"),
		seedPath:    filepath.Join(leg.WorkDir, "blaster-seed.json"),
		resultsPath: filepath.Join(leg.WorkDir, "blaster-results.json"),
		rampUp:      rampUp, duration: duration,
	}
	if err := generateSeed(ctx, call, lo, hi, seedCount); err != nil {
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

	md := renderMarkdown(leg.TargetSHA, blasterSHA, rampUp, duration,
		health.OldestLedger, health.LatestLedger, ready.CatchupSeconds, rows)
	if err := os.WriteFile(leg.ResultsFile, []byte(md), 0o644); err != nil {
		return leg.Bail("writing results: %v", err)
	}
	if err := leg.Publish(ctx, call.resultsPath); err != nil {
		return leg.Bail("publishing result: %v", err)
	}
	return nil
}

// fetchBlaster shallow-checks-out and builds stellar-rpc-blaster (BLASTER_REPO
// at BLASTER_REF -- branch, tag, or SHA), returning the binary path and the
// resolved commit.
func fetchBlaster(ctx context.Context, dir string) (string, string, error) {
	repo := harness.Env("BLASTER_REPO", "stellar/stellar-rpc-blaster")
	ref := harness.Env("BLASTER_REF", "f6085c38900f1b1c031dfb78658ea81917f58a30")
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

// awaitServeReady polls for the serve-ready object the peer box publishes,
// then probes the advertised RPC directly; the probe's getHealth response
// carries the live ledger bounds the seed window derives from. A leftover
// object from a prior attempt is honored only if its box still answers.
func awaitServeReady(
	ctx context.Context, client *s3.Client, bucket, key, runID string,
) (*harness.ServeReady, *protocol.GetHealthResponse, error) {
	probe := func(url string, window time.Duration) (*protocol.GetHealthResponse, error) {
		pctx, cancel := context.WithTimeout(ctx, window)
		defer cancel()
		res, err := harness.AwaitHealthy(pctx, url, 10*time.Second)
		if err != nil {
			return nil, err
		}
		return &res, nil
	}

	staleProbed := "" // prior-attempt RunID already probed dead; don't re-dial it every poll
	for {
		var ready harness.ServeReady
		err := harness.GetJSON(ctx, client, bucket, key, &ready)
		switch {
		case errors.Is(err, harness.ErrResultNotReady):
			logger.Infof("waiting for serve-ready object s3://%s/%s", bucket, key)
		case err != nil:
			logger.Warnf("serve-ready fetch failed; retrying: %v", err)
		case !harness.SameWorkflowRun(ready.RunID, runID):
			logger.Infof("ignoring serve-ready from foreign run %s", ready.RunID)
		case harness.RunAttempt(ready.RunID) < harness.RunAttempt(runID):
			// prior-attempt leftover: its box may be long gone, so one probe
			// decides between reusing it and waiting for this attempt's object
			if ready.Status == harness.ServeStatusReady && ready.RunID != staleProbed {
				if health, perr := probe(ready.URL, time.Minute); perr == nil {
					logger.Infof("reusing still-serving box from prior attempt (%s)", ready.RunID)
					return &ready, health, nil
				}
				staleProbed = ready.RunID
			}
			logger.Infof("stale serve-ready from attempt %s; waiting for a fresh one", ready.RunID)
		case ready.Status != harness.ServeStatusReady:
			return nil, nil, fmt.Errorf("peer box reported serve failure: %s", ready.Error)
		default:
			// the box just reported healthy, so a short window suffices
			health, perr := probe(ready.URL, 3*time.Minute)
			if perr != nil {
				return nil, nil, fmt.Errorf("serve-ready published but %s unreachable: %w", ready.URL, perr)
			}
			return &ready, health, nil
		}
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("no usable serve-ready object at s3://%s/%s before the leg deadline: %w",
				bucket, key, ctx.Err())
		case <-time.After(harness.RendezvousPollInterval):
		}
	}
}
