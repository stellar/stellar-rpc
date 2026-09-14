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

// Relay's state output describes the campaign outcome at the end of a window.
const (
	relayStateOK      = "ok"      // successful final result
	relayStateFail    = "fail"    // failed verdict, polling fault, or deadline without a verdict
	relayStateRunning = "running" // window closed with budget left: the next poll job takes over
)

// Relay lets long campaigns span multiple workflow jobs by reporting one
// polling window's outcome through the state output: ok, fail, or running.
// The caller uses that output to fail the job or schedule another window.
// Configuration, cancellation, and local output failures return an error.
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
	poller := &resultPoller{
		s3Client: s3Client, runner: runner,
		bucket: cfg.bucket, key: cfg.resultKey, runID: cfg.runID,
		interval:      cfg.pollInterval,
		debugLogLines: cfg.debugLogLines, debugEveryPolls: cfg.debugEveryPolls,
	}
	r := &relay{cfg: cfg, poller: poller}
	return r.poll(ctx)
}

type relay struct {
	cfg    *relayConfig
	poller *resultPoller
}

func (r *relay) poll(ctx context.Context) error {
	start := time.Now()
	windowEnd := start.Add(r.cfg.window)
	if r.cfg.deadline.Before(windowEnd) {
		windowEnd = r.cfg.deadline
	}
	logger.Infof("polling s3://%s/%s until %s (deadline %s)",
		r.cfg.bucket, r.cfg.resultKey, windowEnd.UTC().Format(time.RFC3339), r.cfg.deadline.UTC().Format(time.RFC3339))

	res, err := r.poller.poll(ctx, windowEnd)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return r.reportFault(ctx, fmt.Sprintf("❌ Result polling failed: %v", err))
	}
	if res != nil {
		return r.reportVerdict(res)
	}

	if time.Now().Before(r.cfg.deadline) {
		logger.Infof("window closed with %s of budget left; handing off to the next poll job",
			time.Until(r.cfg.deadline).Round(time.Second))
		return appendOutputs(r.cfg.githubOutput, "state="+relayStateRunning)
	}

	// Check for a final result even if this job started after the deadline
	// or the producer published during the last polling sleep.
	last, lerr := r.poller.checkOnce(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if lerr != nil {
		return r.reportFault(ctx,
			fmt.Sprintf("❌ Campaign deadline passed; final result fetch failed: %v", lerr))
	}
	if last != nil {
		return r.reportVerdict(last)
	}
	return r.reportFault(ctx, fmt.Sprintf(
		"❌ Campaign budget deadline passed with no verdict (this window waited %s).",
		time.Since(start).Round(time.Second)))
}

// reportFault writes context for the workflow summary and relays a fail state.
func (r *relay) reportFault(ctx context.Context, headline string) error {
	logger.Warnf("%s", headline)
	if err := writeNoVerdictComment(ctx, r.poller.runner, r.cfg.instanceID, headline, r.cfg.debugLogLines); err != nil {
		return err
	}
	return appendOutputs(r.cfg.githubOutput, "state="+relayStateFail)
}

// reportVerdict writes the report before exposing its state to the workflow.
func (r *relay) reportVerdict(res *Result) error {
	logger.Infof("result published by instance (verdict: %s)", res.Verdict)
	if err := os.WriteFile(Env("RESULTS_FILE", defaultResultsFile), []byte(res.Markdown), 0o644); err != nil {
		return err
	}
	state := relayStateFail
	if res.Verdict == VerdictOK {
		state = relayStateOK
	}
	return appendOutputs(r.cfg.githubOutput, "state="+state)
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

func loadRelayConfig() (*relayConfig, error) {
	strs, err := RequireEnv(
		"INSTANCE_ID", "AWS_REGION", "GITHUB_OUTPUT", "BUCKET", "RESULT_KEY", "RUN_ID",
	)
	if err != nil {
		return nil, err
	}
	ints, err := RequireEnvInts(
		"POLL_INTERVAL", "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS", "WINDOW_SECONDS", "DEADLINE_EPOCH",
	)
	if err != nil {
		return nil, err
	}
	if err := requirePositive(ints, "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS", "DEADLINE_EPOCH"); err != nil {
		return nil, err
	}
	if err := requireSeconds(ints, "POLL_INTERVAL", "WINDOW_SECONDS"); err != nil {
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
