package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

// Relay step-output states. Exactly one is written per poll window.
const (
	relayStateOK      = "ok"      // verdict seen, verdict == "ok"
	relayStateFail    = "fail"    // verdict seen and not "ok", or the budget deadline passed with none
	relayStateRunning = "running" // window closed with budget left: the next poll job takes over
)

// relayVerdictPending is the marker verdict the workflow's launch job seeds at
// RESULT_KEY, so the key exists for the campaign's whole life. It reads as
// "still running", and it makes a persistent fetch error a genuine fault
// rather than a maybe-not-published-yet.
const relayVerdictPending = "pending"

// maxConsecutiveFetchErrors is how many failed polls in a row (~5 min at the
// 30 s interval) the relay tolerates before calling the run faulted. The
// launch job seeds RESULT_KEY, so persistent errors mean a config or
// permissions fault, not a slow campaign; transient S3 hiccups never chain
// this long.
const maxConsecutiveFetchErrors = 10

// Relay is the poll half of a campaign that outlives one GHA job. Where Gather
// treats its whole timeout as a failure, Relay polls for a bounded window and
// reports back which of three things happened: a verdict arrived, the campaign's
// budget deadline passed without one, or the window merely closed — a handoff to
// the next poll job, not a failure. The process exits 0 in all three cases; the
// workflow gates on the state output.
//
// The workflow's launch job seeds RESULT_KEY with a "pending" marker, so the
// object always exists: pending reads as still-running, and persistent fetch
// errors read as a fault worth failing fast on instead of a slow campaign.
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
	// The window never runs past the deadline: budget exhaustion is a verdict of
	// its own and there is no point polling beyond it.
	windowEnd := start.Add(cfg.window)
	if cfg.deadline.Before(windowEnd) {
		windowEnd = cfg.deadline
	}
	logger.Infof("polling s3://%s/%s until %s (deadline %s)",
		cfg.bucket, cfg.resultKey, windowEnd.UTC().Format(time.RFC3339), cfg.deadline.UTC().Format(time.RFC3339))

	fetchErrs := 0
	for pollCount := 1; time.Now().Before(windowEnd); pollCount++ {
		res, derr := FetchResult(ctx, s3Client, cfg.bucket, cfg.resultKey)
		if derr == nil || errors.Is(derr, ErrResultNotReady) {
			fetchErrs = 0
		}
		switch {
		case errors.Is(derr, ErrResultNotReady):
			logger.Infof("still waiting for s3://%s/%s", cfg.bucket, cfg.resultKey)
		case derr != nil:
			fetchErrs++
			logger.Warnf("result fetch failed (%d/%d); retrying: %v", fetchErrs, maxConsecutiveFetchErrors, derr)
			if fetchErrs >= maxConsecutiveFetchErrors {
				return cfg.reportFault(ctx, runner, fmt.Sprintf(
					"❌ Relay gave up: %d consecutive result fetches failed (last: %v). "+
						"The launch job seeds this key, so this is a permissions or config fault, not a pending campaign.",
					fetchErrs, derr))
			}
		// A leftover object from a prior attempt (re-runs share RESULT_KEY) is
		// "not published yet" so this attempt's box overwrites it.
		case res.RunID != cfg.runID:
			logger.Infof("ignoring stale result from run %s (want %s)", res.RunID, cfg.runID)
		case res.Verdict == relayVerdictPending:
			logger.Infof("campaign still running (pending marker at s3://%s/%s)", cfg.bucket, cfg.resultKey)
		default:
			return cfg.reportVerdict(res)
		}

		if pollCount%cfg.debugEveryPolls == 0 {
			logger.Infof("debug tail:\n%s", runner.debugTail(ctx, cfg.debugLogLines))
		}
		time.Sleep(cfg.pollInterval)
	}

	if relayState(time.Now(), cfg.deadline) == relayStateRunning {
		logger.Infof("window closed with %s of budget left; handing off to the next poll job",
			time.Until(cfg.deadline).Round(time.Second))
		return appendOutputs(cfg.githubOutput, "state="+relayStateRunning)
	}

	// Budget exhausted: the campaign overran the wall-clock estimate the workflow
	// validated it against, so this is a failure and not a handoff. The reported
	// duration is this window's wait — the poll chain spans jobs, so no single one
	// of them knows the total.
	logger.Warnf("budget deadline passed with no verdict after %s", time.Since(start).Round(time.Second))
	return cfg.reportFault(ctx, runner, fmt.Sprintf(
		"❌ Campaign budget deadline passed with no verdict (this window waited %s).",
		time.Since(start).Round(time.Second)))
}

// reportFault is the no-verdict fail path: it logs the headline, writes it
// (plus the box tail) where the workflow's summary step looks, and relays a
// fail state.
func (c *relayConfig) reportFault(ctx context.Context, runner *ssmRunner, headline string) error {
	logger.Warnf("%s", headline)
	if err := writeNoVerdictComment(
		ctx, runner, c.githubOutput, c.instanceID, headline, c.debugLogLines,
	); err != nil {
		return err
	}
	return appendOutputs(c.githubOutput, "state="+relayStateFail)
}

// relayState reports the state of a poll window that ended without a verdict:
// the campaign is still running while its budget deadline is in the future, and
// has failed once the deadline has passed.
func relayState(now, deadline time.Time) string {
	if now.Before(deadline) {
		return relayStateRunning
	}
	return relayStateFail
}

// relayConfig is Relay's validated environment.
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

// relayIntKeys are the numeric slots of the poller env contract.
var relayIntKeys = []string{
	"POLL_INTERVAL", "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS", "WINDOW_SECONDS", "DEADLINE_EPOCH",
}

// loadRelayConfig reads and validates the whole poller env before any AWS client
// is built, so a mis-plumbed workflow fails in the first second.
func loadRelayConfig() (*relayConfig, error) {
	strs, err := RequireEnv(
		"INSTANCE_ID", "AWS_REGION", "GITHUB_OUTPUT", "BUCKET", "RESULT_KEY", "RUN_ID",
	)
	if err != nil {
		return nil, err
	}
	if _, err := RequireEnv(relayIntKeys...); err != nil {
		return nil, err
	}
	ints := map[string]int{}
	for _, k := range relayIntKeys {
		n, cerr := strconv.Atoi(os.Getenv(k))
		if cerr != nil {
			return nil, fmt.Errorf("%s: %w", k, cerr)
		}
		ints[k] = n
	}
	if ints["DEBUG_LOG_EVERY_POLLS"] < 1 {
		return nil, fmt.Errorf("DEBUG_LOG_EVERY_POLLS must be positive, got %d", ints["DEBUG_LOG_EVERY_POLLS"])
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

// reportVerdict writes the box's markdown where the workflow expects it and
// relays the verdict as the window's state.
func (c *relayConfig) reportVerdict(res *Result) error {
	logger.Infof("result published by instance (verdict: %s)", res.Verdict)
	if err := os.WriteFile("/tmp/results.md", []byte(res.Markdown), 0o644); err != nil {
		return err
	}
	state := relayStateFail
	if res.Verdict == relayStateOK {
		state = relayStateOK
	}
	return appendOutputs(c.githubOutput, "state="+state)
}
