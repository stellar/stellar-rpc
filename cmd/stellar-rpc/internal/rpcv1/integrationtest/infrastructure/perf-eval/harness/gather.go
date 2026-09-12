package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

// commandWaitTimeout bounds a diagnostic, including dispatch, retries, and reads.
const commandWaitTimeout = 60 * time.Second

// gatherConfig is the environment Gather reads.
type gatherConfig struct {
	Poller             pollerConfig
	ResultsTimeoutSecs int `env:"RESULTS_TIMEOUT,required,notEmpty"`
}

func (c *gatherConfig) validate() error {
	if err := c.Poller.validate(); err != nil {
		return err
	}
	return requirePositiveInts(envInt{"RESULTS_TIMEOUT", c.ResultsTimeoutSecs})
}

// Gather is the GHA-runner half: it polls S3 until the box reports a verdict
// and relays the result as step outputs. On timeout it writes a debug comment
// instead. Used by every leg's runner.
func Gather(ctx context.Context) error {
	var cfg gatherConfig
	if err := loadEnv(&cfg); err != nil {
		return err
	}
	poller, err := newResultPoller(ctx, cfg.Poller)
	if err != nil {
		return err
	}
	timeout := time.Duration(cfg.ResultsTimeoutSecs) * time.Second
	res, err := poller.poll(ctx, time.Now().Add(timeout))
	return reportGather(ctx, poller, cfg.Poller.GitHubOutput, timeout, res, err)
}

func reportGather(
	ctx context.Context, p *resultPoller, githubOutput string,
	resultsTimeout time.Duration, res *Result, pollErr error,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if res != nil {
		logger.Infof("result published by instance (verdict: %s)", res.Verdict)
		if werr := os.WriteFile(Env("RESULTS_FILE", defaultResultsFile), []byte(res.Markdown), 0o644); werr != nil {
			return werr
		}
		return appendOutputs(githubOutput,
			"found=true",
			fmt.Sprintf("passed=%t", res.Verdict == VerdictOK))
	}

	headline := fmt.Sprintf("❌ Load test did not produce results within %.0fs.", resultsTimeout.Seconds())
	if pollErr != nil {
		headline = fmt.Sprintf("❌ Result polling failed: %v", pollErr)
	}
	if werr := p.writeNoVerdictComment(ctx, headline); werr != nil {
		return werr
	}
	return appendOutputs(githubOutput, "found=false")
}

// ssmRunner runs shell commands on one instance over SSM RunShellScript.
type ssmRunner struct {
	client     *ssm.Client
	instanceID string
}

// capture dispatches command, waits for it, and returns its stdout.
func (r *ssmRunner) capture(ctx context.Context, command string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, commandWaitTimeout)
	defer cancel()
	var id string
	var sendErr error
	for attempt := 1; attempt <= 3; attempt++ {
		out, err := r.client.SendCommand(ctx, &ssm.SendCommandInput{
			InstanceIds:  []string{r.instanceID},
			DocumentName: aws.String("AWS-RunShellScript"),
			Parameters:   map[string][]string{"commands": {command}},
		})
		if err == nil {
			id = aws.ToString(out.Command.CommandId)
			break
		}
		sendErr = err
		logger.Warnf("ssm send-command attempt %d failed", attempt)
		if err := sleepContext(ctx, 5*time.Second); err != nil {
			return "", err
		}
	}
	if id == "" {
		return "", fmt.Errorf("ssm send-command failed: %w", sendErr)
	}

	in := &ssm.GetCommandInvocationInput{CommandId: &id, InstanceId: &r.instanceID}
	_ = ssm.NewCommandExecutedWaiter(r.client).Wait(ctx, in, commandWaitTimeout)
	inv, err := r.client.GetCommandInvocation(ctx, in)
	if err != nil {
		// Unreadable result is "not ready", not a dispatch failure.
		return "", nil
	}
	return aws.ToString(inv.StandardOutputContent), nil
}

// debugTail returns the last n lines of the box's user-data log, or a sentinel.
func (r *ssmRunner) debugTail(ctx context.Context, n int) string {
	cmd := fmt.Sprintf("if [ -f /var/log/user-data.log ]; then tail -n %d /var/log/user-data.log; "+
		"else echo __NO_DEBUG_LOG__; fi", n)
	out, err := r.capture(ctx, cmd)
	if err != nil || out == "" {
		return "__DEBUG_TAIL_UNAVAILABLE__"
	}
	return out
}

// writeNoVerdictComment writes diagnostics to timeout-comment.md beside
// RESULTS_FILE. The callers reach it only when polling ended without a
// verdict, where the poller has a runner.
func (p *resultPoller) writeNoVerdictComment(ctx context.Context, headline string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n\n", headline)
	fmt.Fprintf(&b, "Instance: `%s`\n", p.runner.instanceID)
	srv, repo, run := os.Getenv("GITHUB_SERVER_URL"), os.Getenv("GITHUB_REPOSITORY"), os.Getenv("GITHUB_RUN_ID")
	if srv != "" && repo != "" && run != "" {
		fmt.Fprintf(&b, "Workflow run: %s/%s/actions/runs/%s\n", srv, repo, run)
	}
	if tail := p.runner.debugTail(ctx, p.debugLogLines); tail != "" {
		fmt.Fprintf(&b, "\nLast %d lines of /var/log/user-data.log:\n\n```\n%s\n```\n", p.debugLogLines, tail)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	commentPath := filepath.Join(filepath.Dir(Env("RESULTS_FILE", defaultResultsFile)), "timeout-comment.md")
	return os.WriteFile(commentPath, []byte(b.String()), 0o644)
}
