// Command runner drives the ephemeral RPC ingestion load test. It has two
// subcommands, one per environment the test spans:
//
//	runner instantiate   on the EC2 box, after a shell preamble has installed
//	                     the toolchain and checked out the repo: streams the
//	                     golden DB, stellar-core, and ledger bundles from S3
//	                     (sha-verified), runs the ingest benchmark, and writes
//	                     an ok/fail verdict.
//	runner orchestrate   on the GHA runner: polls S3 for the result object the
//	                     instance publishes and relays the verdict + results as
//	                     step outputs. SSM carries only best-effort debug tails.
//
// The two halves coordinate through a single S3 result object (see type result).
// SSM is used only for live-progress and timeout diagnostics.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/smithy-go"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

var logger = supportlog.New()

func main() {
	logger.SetLevel(supportlog.InfoLevel)

	cmd := "instantiate"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	ctx := context.Background()
	var err error
	switch cmd {
	case "instantiate":
		err = instantiate(ctx)
	case "orchestrate":
		err = orchestrate(ctx)
	default:
		fmt.Fprintf(os.Stderr, "usage: %s [instantiate|orchestrate]\n", os.Args[0])
		os.Exit(64)
	}
	if err != nil {
		logger.Errorf("fatal: %v", err)
		os.Exit(1)
	}
}

// commandWaitTimeout backstops a stuck SSM command (the debug-tail reads).
const commandWaitTimeout = 60 * time.Second

// requireEnv returns the values of keys in order, erroring with every unset one.
func requireEnv(keys ...string) ([]string, error) {
	vals := make([]string, len(keys))
	var missing []string
	for i, k := range keys {
		if vals[i] = os.Getenv(k); vals[i] == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required env: %s", strings.Join(missing, ", "))
	}
	return vals, nil
}

// orchestrate polls the box until it reports a verdict and relays the result as step outputs
// On timeout it writes a debug comment instead.
func orchestrate(ctx context.Context) error {
	vals, err := requireEnv("INSTANCE_ID", "AWS_REGION",
		"RESULTS_TIMEOUT", "POLL_INTERVAL", "GITHUB_OUTPUT", "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS",
		"BUCKET", "RESULT_KEY")
	if err != nil {
		return err
	}
	instanceID, region, githubOutput := vals[0], vals[1], vals[4]
	bucket, resultKey := vals[7], vals[8]

	resultsTimeoutSec, err := strconv.Atoi(vals[2])
	if err != nil {
		return fmt.Errorf("RESULTS_TIMEOUT: %w", err)
	}
	pollIntervalSec, err := strconv.Atoi(vals[3])
	if err != nil {
		return fmt.Errorf("POLL_INTERVAL: %w", err)
	}
	debugLogLines, err := strconv.Atoi(vals[5])
	if err != nil {
		return fmt.Errorf("DEBUG_LOG_LINES: %w", err)
	}
	debugEveryPolls, err := strconv.Atoi(vals[6])
	if err != nil {
		return fmt.Errorf("DEBUG_LOG_EVERY_POLLS: %w", err)
	}
	resultsTimeout := time.Duration(resultsTimeoutSec) * time.Second
	pollInterval := time.Duration(pollIntervalSec) * time.Second

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return err
	}
	s3Client := s3.NewFromConfig(awsCfg)
	runner := &ssmRunner{client: ssm.NewFromConfig(awsCfg), instanceID: instanceID}

	deadline := time.Now().Add(resultsTimeout)
	for pollCount := 1; time.Now().Before(deadline); pollCount++ {
		res, derr := fetchResult(ctx, s3Client, bucket, resultKey)
		switch {
		case errors.Is(derr, errResultNotReady):
			logger.Infof("still waiting for s3://%s/%s", bucket, resultKey)
		case derr != nil:
			logger.Warnf("result fetch failed; retrying: %v", derr)
		default:
			logger.Infof("result published by instance (verdict: %s)", res.Verdict)
			if werr := os.WriteFile("/tmp/results.md", []byte(res.Markdown), 0o644); werr != nil {
				return werr
			}
			return appendOutputs(githubOutput,
				"found=true",
				fmt.Sprintf("passed=%t", res.Verdict == "ok"),
				"verdict="+res.Verdict,
				"result_key="+resultKey,
				"bucket="+bucket)
		}

		if pollCount%debugEveryPolls == 0 {
			logger.Infof("debug tail:\n%s", runner.debugTail(ctx, debugLogLines))
		}
		time.Sleep(pollInterval)
	}

	return writeTimeoutComment(ctx, runner, githubOutput, bucket, resultKey, instanceID, resultsTimeout, debugLogLines)
}

// ssmRunner runs shell commands on one instance over SSM RunShellScript.
type ssmRunner struct {
	client     *ssm.Client
	instanceID string
}

// capture dispatches command, waits for it, and returns its stdout. A non-nil
// error means dispatch failed; an unreadable result is "".
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

// errResultNotReady means the result object hasn't been published yet.
var errResultNotReady = errors.New("result not published yet")

// fetchResult gets and decodes the result object, returning errResultNotReady
// when it is absent.
func fetchResult(ctx context.Context, client *s3.Client, bucket, key string) (*result, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			return nil, errResultNotReady
		}
		return nil, err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	var res result
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, fmt.Errorf("decoding result object: %w", err)
	}
	return &res, nil
}

// isNotFound reports whether a GetObject error means the key is absent.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}

// appendOutputs appends lines to the GitHub Actions step-output file.
func appendOutputs(path string, lines ...string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, strings.Join(lines, "\n"))
	return err
}

// writeTimeoutComment is the no-verdict path: it writes a comment to
// /tmp/timeout-comment.md and records found=false.
func writeTimeoutComment(
	ctx context.Context,
	runner *ssmRunner,
	githubOutput, bucket, resultKey, instanceID string,
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
	return appendOutputs(githubOutput,
		"found=false",
		"result_key="+resultKey,
		"bucket="+bucket)
}
