package harness

import (
	"context"
	"fmt"
	"os"
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
	strs, err := RequireEnv("INSTANCE_ID", "AWS_REGION", "GITHUB_OUTPUT", "BUCKET", "RESULT_KEY", "RUN_ID")
	if err != nil {
		return err
	}
	instanceID, region, githubOutput := strs[0], strs[1], strs[2]
	bucket, resultKey, runID := strs[3], strs[4], strs[5]

	ints, err := RequireEnvInts("RESULTS_TIMEOUT", "POLL_INTERVAL", "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS")
	if err != nil {
		return err
	}
	if err := requirePositive(ints, "POLL_INTERVAL", "DEBUG_LOG_EVERY_POLLS", "RESULTS_TIMEOUT"); err != nil {
		return err
	}
	debugLogLines := ints["DEBUG_LOG_LINES"]
	resultsTimeout := time.Duration(ints["RESULTS_TIMEOUT"]) * time.Second

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return err
	}
	s3Client := s3.NewFromConfig(awsCfg)
	runner := &ssmRunner{client: ssm.NewFromConfig(awsCfg), instanceID: instanceID}
	poller := &resultPoller{
		s3Client: s3Client, runner: runner,
		bucket: bucket, key: resultKey, runID: runID,
		interval:      time.Duration(ints["POLL_INTERVAL"]) * time.Second,
		debugLogLines: debugLogLines, debugEveryPolls: ints["DEBUG_LOG_EVERY_POLLS"],
	}
	res, err := poller.poll(ctx, time.Now().Add(resultsTimeout))
	if res != nil {
		logger.Infof("result published by instance (verdict: %s)", res.Verdict)
		if werr := os.WriteFile(defaultResultsFile, []byte(res.Markdown), 0o644); werr != nil {
			return werr
		}
		return appendOutputs(githubOutput,
			"found=true",
			fmt.Sprintf("passed=%t", res.Verdict == VerdictOK))
	}

	headline := fmt.Sprintf("❌ Load test did not produce results within %.0fs.", resultsTimeout.Seconds())
	if err != nil {
		headline = err.Error()
	}
	if werr := writeNoVerdictComment(ctx, runner, instanceID, headline, debugLogLines); werr != nil {
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

// writeNoVerdictComment is the no-verdict path: it writes the caller's
// headline plus the box context to /tmp/timeout-comment.md.
func writeNoVerdictComment(
	ctx context.Context,
	runner *ssmRunner,
	instanceID, headline string,
	debugLogLines int,
) error {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n\n", headline)
	fmt.Fprintf(&b, "Instance: `%s`\n", instanceID)
	srv, repo, run := os.Getenv("GITHUB_SERVER_URL"), os.Getenv("GITHUB_REPOSITORY"), os.Getenv("GITHUB_RUN_ID")
	if srv != "" && repo != "" && run != "" {
		fmt.Fprintf(&b, "Workflow run: %s/%s/actions/runs/%s\n", srv, repo, run)
	}
	if tail := runner.debugTail(ctx, debugLogLines); tail != "" {
		fmt.Fprintf(&b, "\nLast %d lines of /var/log/user-data.log:\n\n```\n%s\n```\n", debugLogLines, tail)
	}
	return os.WriteFile("/tmp/timeout-comment.md", []byte(b.String()), 0o644)
}
