package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"
)

// legDir is this leg's path under the repo root, where its blaster endpoint
// roster is checked in (the runner runs with cwd = repo root).
const legDir = "cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/endpoint-load-test"

// instantiate is the instance's blast task: it rendezvouses with the backfill
// box's serving RPC, generates seed data across its ledger window, runs the
// serial endpoint blast, and publishes the stats.
func instantiate(ctx context.Context) error {
	var (
		bucket      = harness.Env("BUCKET", "stellar-rpc-ci-load-test")
		region      = harness.Env("REGION", "us-east-1")
		workDir     = harness.Env("WORK_DIR", "/data")
		resultsFile = harness.Env("RESULTS_FILE", "/tmp/results.md")
		resultKey   = os.Getenv("RESULT_KEY")
		targetSHA   = os.Getenv("TARGET_SHA")
		runID       = harness.Env("RUN_ID", "manual")

		blasterDir = harness.Env("BLASTER_DIR", filepath.Join(workDir, "stellar-rpc-blaster"))
		rampUp     = harness.Env("BLASTER_RAMP_UP", "2m")
		duration   = harness.Env("BLASTER_DURATION", "3m")
		seedCount  = harness.Env("SEED_COUNT", "1000")
		// left buffer outruns retention trimming during the blast; right buffer
		// keeps clear of the (still advancing) tip
		bufLow  = harness.Env("SEED_BUFFER_LOW", "1000")
		bufHigh = harness.Env("SEED_BUFFER_HIGH", "128")

		readyDeadline = harness.DurationEnv("READY_DEADLINE", 100*time.Minute)
		seedDeadline  = harness.DurationEnv("SEED_DEADLINE", 20*time.Minute)
		blastDeadline = harness.DurationEnv("BLAST_DEADLINE", 60*time.Minute)

		blasterBin = filepath.Join(blasterDir, "stellar-rpc-blaster")
	)
	repoRoot, err := os.Getwd()
	if err != nil {
		return err
	}
	bail := func(format string, args ...any) error {
		return harness.BailInstance(resultsFile, "Endpoint load test", runID, targetSHA, fmt.Sprintf(format, args...))
	}

	readyKey := os.Getenv("READY_KEY")
	if readyKey == "" {
		return bail("READY_KEY unset; nothing to rendezvous with")
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return bail("loading AWS config: %v", err)
	}
	s3Client := s3.NewFromConfig(awsCfg)

	ready, health, err := awaitServeReady(ctx, s3Client, bucket, readyKey, runID, readyDeadline)
	if err != nil {
		return bail("%v", err)
	}
	logger.Infof("target RPC %s serving ledgers [%d, %d] (catchup %ds)",
		ready.URL, health.OldestLedger, health.LatestLedger, ready.CatchupSeconds)

	lo := int64(health.OldestLedger) + parseInt64(bufLow)
	hi := int64(health.LatestLedger) - parseInt64(bufHigh)
	if hi <= lo {
		return bail("ledger window [%d, %d] leaves no room after buffers +%s/-%s",
			health.OldestLedger, health.LatestLedger, bufLow, bufHigh)
	}

	seedPath := filepath.Join(workDir, "blaster-seed.json")
	logger.Infof("generating seed data: %s ledgers sampled from [%d, %d]", seedCount, lo, hi)
	sctx, scancel := context.WithTimeout(ctx, seedDeadline)
	err = harness.RunStreaming(sctx, blasterDir, nil, 40, blasterBin, "generate",
		"--rpc-url", ready.URL,
		"--output", seedPath,
		"--ledger-window", fmt.Sprintf("%d,%d", lo, hi),
		"--count", seedCount)
	scancel()
	if err != nil {
		return bail("blaster generate failed: %v", err)
	}

	resultsJSON, err := blast(ctx, blastCall{
		bin: blasterBin, dir: blasterDir, url: ready.URL,
		configPath: filepath.Join(repoRoot, legDir, "testdata", "endpoints.toml"),
		seedPath:   seedPath,
		outDir:     filepath.Join(workDir, "blaster-out"),
		rampUp:     rampUp, duration: duration, deadline: blastDeadline,
	})
	if err != nil {
		return bail("%v", err)
	}
	data, err := os.ReadFile(resultsJSON)
	if err != nil {
		return bail("reading blaster results: %v", err)
	}
	rows, err := summarize(data)
	if err != nil {
		return bail("summarizing blaster results: %v", err)
	}

	md := renderMarkdown(targetSHA, rampUp, duration, health.OldestLedger, health.LatestLedger, ready.CatchupSeconds, rows)
	if err := os.WriteFile(resultsFile, []byte(md), 0o644); err != nil {
		return bail("writing results: %v", err)
	}
	if err := harness.PublishResult(
		ctx, s3Client, bucket, resultKey, "ok", runID, targetSHA, resultsFile, resultsJSON); err != nil {
		return bail("publishing result: %v", err)
	}
	return nil
}

// blastCall parameterizes one serial blaster sweep.
type blastCall struct {
	bin, dir, url    string
	configPath       string
	seedPath, outDir string
	rampUp, duration string
	deadline         time.Duration
}

// blast runs the serial endpoint sweep and returns the results JSON path.
func blast(ctx context.Context, c blastCall) (string, error) {
	logger.Infof("blasting endpoints in serial (ramp-up %s, duration %s per endpoint)", c.rampUp, c.duration)
	bctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	if err := harness.RunStreaming(bctx, c.dir, nil, 80, c.bin, "run",
		"--rpc-url", c.url,
		"--config-path", c.configPath,
		"--input-data-path", c.seedPath,
		"--serial",
		"--ramp-up", c.rampUp,
		"--duration", c.duration,
		"--test-output-path", c.outDir); err != nil {
		return "", fmt.Errorf("blaster run failed: %w", err)
	}
	return newestResults(c.outDir)
}

// awaitServeReady polls for the serve-ready object the backfill box publishes,
// then probes the advertised RPC directly; the probe's getHealth response
// carries the live ledger bounds the seed window derives from. A leftover
// object from a prior attempt is honored only if its box still answers.
func awaitServeReady(
	ctx context.Context, client *s3.Client, bucket, key, runID string, deadline time.Duration,
) (*harness.ServeReady, *healthResponse, error) {
	probe := func(url string, window time.Duration) (*healthResponse, error) {
		pctx, cancel := context.WithTimeout(ctx, window)
		defer cancel()
		res, err := harness.AwaitHealthy(pctx, url, 10*time.Second)
		if err != nil {
			return nil, err
		}
		return &healthResponse{OldestLedger: res.OldestLedger, LatestLedger: res.LatestLedger}, nil
	}

	dl := time.Now().Add(deadline)
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
			// prior-attempt leftover: its box may be long gone, so a quick probe
			// decides between reusing it and waiting for this attempt's object
			if ready.Status == harness.ServeStatusReady {
				if health, perr := probe(ready.URL, time.Minute); perr == nil {
					logger.Infof("reusing still-serving box from prior attempt (%s)", ready.RunID)
					return &ready, health, nil
				}
			}
			logger.Infof("stale serve-ready from attempt %s; waiting for a fresh one", ready.RunID)
		case ready.Status != harness.ServeStatusReady:
			return nil, nil, fmt.Errorf("backfill box reported serve failure: %s", ready.Error)
		default:
			// the box just reported healthy, so a short window suffices
			health, perr := probe(ready.URL, 3*time.Minute)
			if perr != nil {
				return nil, nil, fmt.Errorf("serve-ready published but %s unreachable: %w", ready.URL, perr)
			}
			return &ready, health, nil
		}
		if time.Now().After(dl) {
			return nil, nil, fmt.Errorf("no usable serve-ready object at s3://%s/%s within %s", bucket, key, deadline)
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(30 * time.Second):
		}
	}
}

// healthResponse is the slice of getHealth the seed window derives from.
type healthResponse struct {
	OldestLedger uint32
	LatestLedger uint32
}

func parseInt64(s string) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		logger.Warnf("invalid ledger buffer %q; using 0", s)
		return 0
	}
	return v
}

// newestResults returns the latest test-results-*.json blaster wrote under dir
// (the filenames embed a sortable timestamp).
func newestResults(dir string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "test-results-*.json"))
	if err != nil || len(matches) == 0 {
		return "", fmt.Errorf("no test-results-*.json under %s", dir)
	}
	sort.Strings(matches)
	return matches[len(matches)-1], nil
}
