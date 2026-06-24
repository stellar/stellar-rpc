// Command runner drives the ephemeral RPC ingestion load test. It has two
// subcommands, one per environment the test spans:
//
//	runner instantiate   on the EC2 box, after a shell preamble has installed
//	                     the toolchain and checked out the repo: streams the
//	                     golden DB, stellar-core, and ledger bundles from S3
//	                     (sha-verified), runs the ingest benchmark, and writes
//	                     an ok/fail verdict.
//	runner orchestrate   on the GHA runner: polls the box over SSM and relays
//	                     the verdict + results as step outputs.
//
// The two halves coordinate only through a /tmp marker protocol.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

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

// pollCommand reports the box's state each poll; /tmp/done holds the verdict on
// its first line and the results body on the rest.
const pollCommand = `if [ -f /tmp/done ]; then cat /tmp/done /tmp/results.md; ` +
	`elif [ -f /tmp/download-complete ]; then echo __DOWNLOAD_COMPLETE__; else echo __NOT_READY__; fi`

// commandWaitTimeout backstops a stuck SSM command; the polled ones are instant.
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
		"RESULTS_TIMEOUT", "POLL_INTERVAL", "GITHUB_OUTPUT", "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS")
	if err != nil {
		return err
	}
	instanceID, region, githubOutput := vals[0], vals[1], vals[4]

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
	runner := &ssmRunner{client: ssm.NewFromConfig(awsCfg), instanceID: instanceID}

	deadline := time.Now().Add(resultsTimeout)
	for pollCount := 1; time.Now().Before(deadline); pollCount++ {
		out, derr := runner.capture(ctx, pollCommand)
		if derr != nil {
			logger.Warn("ssm poll dispatch failed; retrying")
			time.Sleep(pollInterval)
			continue
		}

		switch state, verdict, body := classifyPollOutput(out); state {
		case pollDone:
			logger.Infof("result payload from instance (verdict: %s)", verdict)
			_ = os.WriteFile("/tmp/results.md", []byte(body), 0o644)
			runner.captureBenchResults(ctx)
			return appendOutputs(githubOutput,
				"found=true",
				fmt.Sprintf("passed=%t", verdict == "ok"))
		case pollDownloadComplete:
			logger.Infof("download stage complete; benchmark running, waiting for /tmp/done")
		case pollNotReady:
			logger.Infof("still waiting for /tmp/done")
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

// benchResultsCommand cats the structured perf JSON the benchmark wrote, or
// nothing when the file is absent (so an absent file reads as empty, not error).
const benchResultsCommand = `[ -f /tmp/bench-results.json ] && cat /tmp/bench-results.json || true`

// captureBenchResults pulls the structured per-profile perf summary back over
// the same SSM channel and lands it at /tmp/bench-results.json for the release
// gate to consume. Best-effort and validated: a missing, truncated, or invalid
// payload is skipped, leaving consumers to fall back to the ok/fail verdict.
func (r *ssmRunner) captureBenchResults(ctx context.Context) {
	out, err := r.capture(ctx, benchResultsCommand)
	if err != nil || !json.Valid([]byte(out)) {
		logger.Info("bench-results.json unavailable or not valid JSON; skipping")
		return
	}
	if err := os.WriteFile("/tmp/bench-results.json", []byte(out), 0o644); err != nil {
		logger.Warnf("failed to write bench-results.json: %v", err)
		return
	}
	logger.Infof("captured bench-results.json (%d bytes)", len(out))
}

type pollState int

const (
	pollNotReady pollState = iota
	pollDownloadComplete
	pollDone
)

// classifyPollOutput decodes pollCommand's stdout into (state, verdict, body).
// For pollDone, verdict is the first line ("ok"/"fail") and body is the rest.
func classifyPollOutput(out string) (pollState, string, string) {
	if out == "" {
		return pollNotReady, "", ""
	}
	first, rest, _ := strings.Cut(out, "\n")
	switch first {
	case "__NOT_READY__":
		return pollNotReady, "", ""
	case "__DOWNLOAD_COMPLETE__":
		return pollDownloadComplete, "", ""
	default:
		return pollDone, first, rest
	}
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
