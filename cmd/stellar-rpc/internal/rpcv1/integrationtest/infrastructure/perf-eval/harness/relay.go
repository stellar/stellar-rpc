package harness

import (
	"context"
	"fmt"
	"os"
	"time"
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
	var cfg relayConfig
	if err := loadEnv(&cfg); err != nil {
		return err
	}
	poller, err := newResultPoller(ctx, cfg.PollerConfig)
	if err != nil {
		return err
	}
	r := &relay{
		poller:       poller,
		githubOutput: cfg.GitHubOutput,
		window:       cfg.Window.duration(),
		deadline:     time.Time(cfg.Deadline),
	}
	return r.poll(ctx)
}

// relay holds one polling window: where to poll, how long for, and where the
// campaign budget ends.
type relay struct {
	poller       *resultPoller
	githubOutput string
	window       time.Duration
	deadline     time.Time
}

func (r *relay) poll(ctx context.Context) error {
	start := time.Now()
	windowEnd := start.Add(r.window)
	if r.deadline.Before(windowEnd) {
		windowEnd = r.deadline
	}
	logger.Infof("polling s3://%s/%s until %s (deadline %s)",
		r.poller.bucket, r.poller.key, windowEnd.UTC().Format(time.RFC3339), r.deadline.UTC().Format(time.RFC3339))

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

	if time.Now().Before(r.deadline) {
		logger.Infof("window closed with %s of budget left; handing off to the next poll job",
			time.Until(r.deadline).Round(time.Second))
		return appendOutputs(r.githubOutput, "state="+relayStateRunning)
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
	if err := r.poller.writeNoVerdictComment(ctx, headline); err != nil {
		return err
	}
	return appendOutputs(r.githubOutput, "state="+relayStateFail)
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
	return appendOutputs(r.githubOutput, "state="+state)
}

// relayConfig is the environment Relay reads.
type relayConfig struct {
	PollerConfig

	Window   seconds  `env:"WINDOW_SECONDS,required,notEmpty"`
	Deadline unixTime `env:"DEADLINE_EPOCH,required,notEmpty"`
}
