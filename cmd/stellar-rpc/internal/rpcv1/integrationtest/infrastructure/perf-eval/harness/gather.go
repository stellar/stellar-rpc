package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

// commandWaitTimeout backstops a stuck SSM command (the debug-tail reads).
const commandWaitTimeout = 60 * time.Second

// Gather is the GHA-runner half: it polls S3 until the box reports a verdict
// and relays the result as step outputs. On timeout it writes a debug comment
// instead. Used by every leg's runner.
func Gather(ctx context.Context) error {
	envStr := map[string]string{}
	var missing []string
	for _, k := range []string{
		"INSTANCE_ID", "AWS_REGION", "RESULTS_TIMEOUT", "POLL_INTERVAL", "GITHUB_OUTPUT",
		"DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS", "BUCKET", "RESULT_KEY", "RUN_ID",
	} {
		if envStr[k] = os.Getenv(k); envStr[k] == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required env: %s", strings.Join(missing, ", "))
	}
	instanceID, region, githubOutput := envStr["INSTANCE_ID"], envStr["AWS_REGION"], envStr["GITHUB_OUTPUT"]
	bucket, resultKey, runID := envStr["BUCKET"], envStr["RESULT_KEY"], envStr["RUN_ID"]

	envInt := map[string]int{}
	for _, k := range []string{"RESULTS_TIMEOUT", "POLL_INTERVAL", "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS"} {
		n, err := strconv.Atoi(envStr[k])
		if err != nil {
			return fmt.Errorf("%s: %w", k, err)
		}
		envInt[k] = n
	}
	debugLogLines, debugEveryPolls := envInt["DEBUG_LOG_LINES"], envInt["DEBUG_LOG_EVERY_POLLS"]
	resultsTimeout := time.Duration(envInt["RESULTS_TIMEOUT"]) * time.Second
	pollInterval := time.Duration(envInt["POLL_INTERVAL"]) * time.Second

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return err
	}
	s3Client := s3.NewFromConfig(awsCfg)
	runner := &ssmRunner{client: ssm.NewFromConfig(awsCfg), instanceID: instanceID}

	deadline := time.Now().Add(resultsTimeout)
	for pollCount := 1; time.Now().Before(deadline); pollCount++ {
		res, derr := FetchResult(ctx, s3Client, bucket, resultKey)
		switch {
		case errors.Is(derr, ErrResultNotReady):
			logger.Infof("still waiting for s3://%s/%s", bucket, resultKey)
		case derr != nil:
			logger.Warnf("result fetch failed; retrying: %v", derr)
		// A leftover object from a prior attempt (re-runs share RESULT_KEY) is
		// "not published yet" so this attempt's box overwrites it.
		case res.RunID != runID:
			logger.Infof("ignoring stale result from run %s (want %s)", res.RunID, runID)
		default:
			logger.Infof("result published by instance (verdict: %s)", res.Verdict)
			if werr := os.WriteFile("/tmp/results.md", []byte(res.Markdown), 0o644); werr != nil {
				return werr
			}
			return appendOutputs(githubOutput,
				"found=true",
				fmt.Sprintf("passed=%t", res.Verdict == "ok"))
		}

		if pollCount%debugEveryPolls == 0 {
			logger.Infof("debug tail:\n%s", runner.debugTail(ctx, debugLogLines))
		}
		time.Sleep(pollInterval)
	}

	return writeTimeoutComment(ctx, runner, githubOutput, instanceID, resultsTimeout, debugLogLines)
}

// ssmRunner runs shell commands on one instance over SSM RunShellScript.
type ssmRunner struct {
	client     *ssm.Client
	instanceID string
}

// capture dispatches command, waits for it, and returns its stdout.
func (r *ssmRunner) capture(ctx context.Context, command string) (string, error) {
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
		time.Sleep(5 * time.Second)
	}
	if id == "" {
		return "", fmt.Errorf("ssm send-command failed: %w", sendErr)
	}

	in := &ssm.GetCommandInvocationInput{CommandId: &id, InstanceId: &r.instanceID}
	_ = ssm.NewCommandExecutedWaiter(r.client).Wait(ctx, in, commandWaitTimeout)
	inv, err := r.client.GetCommandInvocation(ctx, in)
	if err != nil {
		// Unreadable result is "not ready", not a dispatch failure.
		return "", nil //nolint:nilerr
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

// writeTimeoutComment is the no-verdict path: it writes a comment to
// /tmp/timeout-comment.md and records found=false.
func writeTimeoutComment(
	ctx context.Context,
	runner *ssmRunner,
	githubOutput, instanceID string,
	resultsTimeout time.Duration,
	debugLogLines int,
) error {
	var b strings.Builder
	fmt.Fprintf(&b, "❌ Load test did not produce results within %.0fs.\n\n", resultsTimeout.Seconds())
	fmt.Fprintf(&b, "Instance: `%s`\n", instanceID)
	srv, repo, run := os.Getenv("GITHUB_SERVER_URL"), os.Getenv("GITHUB_REPOSITORY"), os.Getenv("GITHUB_RUN_ID")
	if srv != "" && repo != "" && run != "" {
		fmt.Fprintf(&b, "Workflow run: %s/%s/actions/runs/%s\n", srv, repo, run)
	}
	if tail := runner.debugTail(ctx, debugLogLines); tail != "" {
		fmt.Fprintf(&b, "\nLast %d lines of /var/log/user-data.log:\n\n```\n%s\n```\n", debugLogLines, tail)
	}
	if err := os.WriteFile("/tmp/timeout-comment.md", []byte(b.String()), 0o644); err != nil {
		return err
	}
	return appendOutputs(githubOutput, "found=false")
}
