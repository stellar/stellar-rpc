package harness

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

// The workflow reads exactly one of these states per poll window.
const (
	relayStateOK      = "ok"      // verdict seen, verdict == "ok"
	relayStateFail    = "fail"    // verdict seen and not "ok", or the budget deadline passed with none
	relayStateRunning = "running" // window closed with budget left: the next poll job takes over
)

// Relay is the poll half of a campaign that outlives one GHA job: it polls one
// bounded window and reports ok, fail, or running, where running hands off to
// the next poll job. All three states exit 0; the workflow gates on the state
// output.
func Relay(ctx context.Context) error {
	cfg, err := loadRelayConfig()
	if err != nil {
		return err
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.region))
	if err != nil {
		return err
	}
	s3Client := s3.NewFromConfig(awsCfg)
	runner := &ssmRunner{client: ssm.NewFromConfig(awsCfg), instanceID: cfg.instanceID}

	start := time.Now()
	// Clamp the poll window to the campaign deadline.
	windowEnd := start.Add(cfg.window)
	if cfg.deadline.Before(windowEnd) {
		windowEnd = cfg.deadline
	}
	logger.Infof("polling s3://%s/%s until %s (deadline %s)",
		cfg.bucket, cfg.resultKey, windowEnd.UTC().Format(time.RFC3339), cfg.deadline.UTC().Format(time.RFC3339))

	poller := &resultPoller{
		s3Client: s3Client, runner: runner,
		bucket: cfg.bucket, key: cfg.resultKey, runID: cfg.runID,
		interval: cfg.pollInterval, keySeeded: true,
		debugLogLines: cfg.debugLogLines, debugEveryPolls: cfg.debugEveryPolls,
	}
	res, err := poller.poll(ctx, windowEnd)
	switch {
	case err != nil:
		return cfg.reportFault(ctx, runner, err.Error())
	case res != nil:
		return cfg.reportVerdict(res)
	}

	if relayState(time.Now(), cfg.deadline) == relayStateRunning {
		logger.Infof("window closed with %s of budget left; handing off to the next poll job",
			time.Until(cfg.deadline).Round(time.Second))
		return appendOutputs(cfg.githubOutput, "state="+relayStateRunning)
	}

	// Budget exhausted: a failure, not a handoff. One last fetch first, because
	// the window can close before the loop's first poll (a job that starts past
	// the deadline) or during its final sleep.
	if last, lerr := poller.checkOnce(ctx); lerr == nil && last != nil {
		logger.Infof("verdict published after the last poll; reporting it instead of a timeout")
		return cfg.reportVerdict(last)
	}
	// The duration is this window's wait only; no single job sees the whole chain.
	logger.Warnf("budget deadline passed with no verdict after %s", time.Since(start).Round(time.Second))
	return cfg.reportFault(ctx, runner, fmt.Sprintf(
		"❌ Campaign budget deadline passed with no verdict (this window waited %s).",
		time.Since(start).Round(time.Second)))
}

// reportFault writes context for the workflow summary and relays a fail state.
func (c *relayConfig) reportFault(ctx context.Context, runner *ssmRunner, headline string) error {
	logger.Warnf("%s", headline)
	if err := writeNoVerdictComment(ctx, runner, c.instanceID, headline, c.debugLogLines); err != nil {
		return err
	}
	return appendOutputs(c.githubOutput, "state="+relayStateFail)
}

func relayState(now, deadline time.Time) string {
	if now.Before(deadline) {
		return relayStateRunning
	}
	return relayStateFail
}

type relayConfig struct {
	instanceID      string
	region          string
	githubOutput    string
	bucket          string
	resultKey       string
	runID           string
	pollInterval    time.Duration
	debugLogLines   int
	debugEveryPolls int
	window          time.Duration
	deadline        time.Time
}

var relayIntKeys = []string{
	"POLL_INTERVAL", "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS", "WINDOW_SECONDS", "DEADLINE_EPOCH",
}

func loadRelayConfig() (*relayConfig, error) {
	strs, err := RequireEnv(
		"INSTANCE_ID", "AWS_REGION", "GITHUB_OUTPUT", "BUCKET", "RESULT_KEY", "RUN_ID",
	)
	if err != nil {
		return nil, err
	}
	ints, err := RequireEnvInts(relayIntKeys...)
	if err != nil {
		return nil, err
	}
	if err := requirePositive(ints, "POLL_INTERVAL", "WINDOW_SECONDS", "DEBUG_LOG_EVERY_POLLS"); err != nil {
		return nil, err
	}
	return &relayConfig{
		instanceID:      strs[0],
		region:          strs[1],
		githubOutput:    strs[2],
		bucket:          strs[3],
		resultKey:       strs[4],
		runID:           strs[5],
		pollInterval:    time.Duration(ints["POLL_INTERVAL"]) * time.Second,
		debugLogLines:   ints["DEBUG_LOG_LINES"],
		debugEveryPolls: ints["DEBUG_LOG_EVERY_POLLS"],
		window:          time.Duration(ints["WINDOW_SECONDS"]) * time.Second,
		deadline:        time.Unix(int64(ints["DEADLINE_EPOCH"]), 0),
	}, nil
}

// reportVerdict writes the box's markdown to the results file, where the
// workflow's summary step reads it, and relays the verdict as the state.
func (c *relayConfig) reportVerdict(res *Result) error {
	logger.Infof("result published by instance (verdict: %s)", res.Verdict)
	if err := os.WriteFile(defaultResultsFile, []byte(res.Markdown), 0o644); err != nil {
		return err
	}
	state := relayStateFail
	if res.Verdict == VerdictOK {
		state = relayStateOK
	}
	return appendOutputs(c.githubOutput, "state="+state)
}
