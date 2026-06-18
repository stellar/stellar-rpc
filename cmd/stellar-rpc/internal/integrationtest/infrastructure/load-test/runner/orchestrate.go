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
// The benchmark's I/O throttle is applied locally on the box (see instantiate.go),
// so the two halves coordinate only through a /tmp marker protocol.
package main

import (
	"context"
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

// logger is the shared logger for both halves, via the repo's house log package.
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

// pollCommand reports the box's state each poll: once /tmp/done exists its first
// line is the verdict and the rest is the results body.
const pollCommand = `if [ -f /tmp/done ]; then cat /tmp/done /tmp/results.md; ` +
	`elif [ -f /tmp/download-complete ]; then echo __DOWNLOAD_COMPLETE__; else echo __NOT_READY__; fi`

// commandWaitTimeout backstops a stuck SSM command; the polled ones are instant.
const commandWaitTimeout = 60 * time.Second

// requireEnv collects the named env vars, returning their values in order and
// an error naming every one that is unset/empty (mirrors the shell ${VAR:?}).
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

// envInt reads key as an int, falling back to def when unset or unparseable.
func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

// orchestrate is the runner half: it polls the box over SSM until it reports a
// verdict and relays the result as step outputs. On timeout it writes a debug
// comment instead.
func orchestrate(ctx context.Context) error {
	vals, err := requireEnv("INSTANCE_ID", "AWS_REGION",
		"RESULTS_TIMEOUT", "POLL_INTERVAL", "GITHUB_OUTPUT", "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS")
	if err != nil {
		return err
	}
	instanceID, region := vals[0], vals[1]
	var (
		resultsTimeout  = time.Duration(envInt("RESULTS_TIMEOUT", 0)) * time.Second
		pollInterval    = time.Duration(envInt("POLL_INTERVAL", 30)) * time.Second
		githubOutput    = vals[4]
		debugLogLines   = envInt("DEBUG_LOG_LINES", 40)
		debugEveryPolls = envInt("DEBUG_LOG_EVERY_POLLS", 5)
	)

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

// --- SSM ---------------------------------------------------------------

// ssmRunner runs shell commands on one instance over SSM RunShellScript.
type ssmRunner struct {
	client     *ssm.Client
	instanceID string
}

// capture dispatches command (retrying dispatch), waits for it, and returns its
// stdout. A non-nil error means dispatch failed; an unreadable result is "".
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
		// Unreadable result is treated as "not ready" (empty), not a dispatch failure.
		return "", nil //nolint:nilerr
	}
	return aws.ToString(inv.StandardOutputContent), nil
}

// debugTail returns the last n lines of the box's user-data log, or a sentinel
// when it can't be read.
func (r *ssmRunner) debugTail(ctx context.Context, n int) string {
	cmd := fmt.Sprintf("if [ -f /var/log/user-data.log ]; then tail -n %d /var/log/user-data.log; "+
		"else echo __NO_DEBUG_LOG__; fi", n)
	out, err := r.capture(ctx, cmd)
	if err != nil || out == "" {
		return "__DEBUG_TAIL_UNAVAILABLE__"
	}
	return out
}

// --- poll output ------------------------------------------------------

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

// --- results / outputs ------------------------------------------------

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

// writeTimeoutComment is the no-verdict path: it captures a debug tail, writes
// a PR/summary comment to /tmp/timeout-comment.md, and records found=false.
func writeTimeoutComment(ctx context.Context, runner *ssmRunner, githubOutput, instanceID string,
	resultsTimeout time.Duration, debugLogLines int,
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
