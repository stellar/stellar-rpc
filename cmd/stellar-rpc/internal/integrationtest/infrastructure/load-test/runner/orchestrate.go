// Command runner drives the ephemeral RPC ingestion load test. It has two
// subcommands, one per environment the test spans:
//
//	runner instantiate   on the EC2 box, after a shell preamble has installed
//	                     the toolchain and checked out the repo: streams the
//	                     golden DB, stellar-core, and ledger bundles from S3
//	                     (sha-verified), runs the ingest benchmark, and writes
//	                     an ok/fail verdict.
//	runner orchestrate   on the GHA runner: polls the box over SSM, drives the
//	                     gp3 throughput downshift handshake, and relays the
//	                     verdict + results as step outputs.
//
// The two halves coordinate through a /tmp marker protocol (see markers in
// instantiate.go).
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
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
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

// throttleTimeout bounds the gp3 downshift handshake: both halves give the
// modification this long to reach `completed` before declaring it failed.
const throttleTimeout = 45 * time.Minute

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
// verdict, drives the gp3 throttle handshake once downloads finish, and relays
// the result as step outputs. On timeout it writes a debug comment instead.
func orchestrate(ctx context.Context) error {
	vals, err := requireEnv("INSTANCE_ID", "AWS_REGION", "BENCH_VOLUME_THROUGHPUT",
		"RESULTS_TIMEOUT", "POLL_INTERVAL", "GITHUB_OUTPUT", "DEBUG_LOG_LINES", "DEBUG_LOG_EVERY_POLLS")
	if err != nil {
		return err
	}
	instanceID, region := vals[0], vals[1]
	var (
		benchThroughput = int32(envInt("BENCH_VOLUME_THROUGHPUT", 125))
		resultsTimeout  = time.Duration(envInt("RESULTS_TIMEOUT", 0)) * time.Second
		pollInterval    = time.Duration(envInt("POLL_INTERVAL", 30)) * time.Second
		githubOutput    = vals[5]
		debugLogLines   = envInt("DEBUG_LOG_LINES", 40)
		debugEveryPolls = envInt("DEBUG_LOG_EVERY_POLLS", 5)
		rootVolumeID    = os.Getenv("ROOT_VOLUME_ID")
	)

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return err
	}
	runner := &ssmRunner{client: ssm.NewFromConfig(awsCfg), instanceID: instanceID}
	reconciler := &throttleReconciler{
		volumeID: rootVolumeID,
		target:   benchThroughput,
		timeout:  throttleTimeout,
		now:      time.Now,
		throttle: &ec2Throttler{client: ec2.NewFromConfig(awsCfg), volumeID: rootVolumeID},
		// Advance only on a confirmed marker, never on a dropped touch.
		signal: func(outcome string) bool {
			return runner.touch(ctx, "/tmp/volume-throttle-"+outcome)
		},
	}

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
			fmt.Println(body)
			_ = os.WriteFile("/tmp/results.md", []byte(body), 0o644)
			return appendOutputs(githubOutput,
				"found=true",
				fmt.Sprintf("passed=%t", verdict == "ok"))
		case pollDownloadComplete:
			reconciler.reconcile(ctx)
			logger.Infof("download stage complete; waiting for /tmp/done")
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

// run dispatches command, waits for it to finish, and returns its status and
// stdout. A non-nil error means dispatch failed (after retries); a command that
// runs but fails yields a non-Success status, not an error.
func (r *ssmRunner) run(ctx context.Context, command string, sendAttempts int) (ssmtypes.CommandInvocationStatus, string, error) {
	var id string
	var sendErr error
	for attempt := 1; attempt <= sendAttempts; attempt++ {
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
		return "", "", fmt.Errorf("ssm send-command failed: %w", sendErr)
	}

	in := &ssm.GetCommandInvocationInput{CommandId: &id, InstanceId: &r.instanceID}
	// Tolerate waiter failure: GetCommandInvocation below still reports status.
	_ = ssm.NewCommandExecutedWaiter(r.client).Wait(ctx, in, commandWaitTimeout)
	inv, err := r.client.GetCommandInvocation(ctx, in)
	if err != nil {
		return "", "", nil // command ran but result unreadable; treat as empty
	}
	return inv.Status, aws.ToString(inv.StandardOutputContent), nil
}

// capture returns the command's stdout, retrying dispatch. The returned error
// is non-nil only on dispatch failure (caller retries the whole poll).
func (r *ssmRunner) capture(ctx context.Context, command string) (string, error) {
	_, stdout, err := r.run(ctx, command, 3)
	return stdout, err
}

// touch creates marker on the box, returning true only on confirmed success.
func (r *ssmRunner) touch(ctx context.Context, marker string) bool {
	status, _, err := r.run(ctx, "touch "+marker, 2)
	return err == nil && status == ssmtypes.CommandInvocationStatusSuccess
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

// classifyPollOutput decodes pollCommand's stdout. For pollDone, verdict is the
// first line ("ok"/"fail") and body is everything after it.
func classifyPollOutput(out string) (state pollState, verdict, body string) {
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

// --- throttle reconciliation ------------------------------------------

// modState is a volume modification's convergence outcome. We key on reaching
// `completed`, not on the reported throughput: that flips to the target at once
// while the volume is still `optimizing` and un-throttled (per the EBS docs).
type modState int

const (
	modInProgress modState = iota // modifying or optimizing — not yet at target
	modCompleted                  // fully applied; volume now delivers the target
	modFailed
)

func (s modState) String() string {
	switch s {
	case modCompleted:
		return "completed"
	case modFailed:
		return "failed"
	default:
		return "in-progress"
	}
}

// volumeThrottler is the EC2 surface the reconciler drives: request a new
// throughput, then poll the modification's state to detect when it is fully
// applied.
type volumeThrottler interface {
	modify(ctx context.Context, throughput int32) error
	// state reports modification progress (0-100); a non-nil error is transient.
	state(ctx context.Context) (st modState, progress int32, err error)
}

// throttleReconciler drives the asynchronous gp3 downshift across poll passes:
// modify-volume only *requests* the change, so the first call fires it and each
// later call re-checks convergence, signalling the outcome exactly once.
type throttleReconciler struct {
	volumeID string
	target   int32
	timeout  time.Duration
	now      func() time.Time
	throttle volumeThrottler
	signal   func(outcome string) bool // returns true once the outcome is confirmed

	attempted bool
	signalled bool
	startedAt time.Time
}

func (r *throttleReconciler) reconcile(ctx context.Context) {
	if r.signalled {
		return
	}
	if r.volumeID == "" {
		r.doSignal("failed")
		return
	}
	if !r.attempted {
		if err := r.throttle.modify(ctx, r.target); err != nil {
			r.doSignal("failed")
			return
		}
		logger.Infof("requested gp3 downshift to %d MiB/s on %s", r.target, r.volumeID)
		r.attempted = true
		r.startedAt = r.now()
		return
	}

	st, progress, err := r.throttle.state(ctx)
	if err != nil {
		logger.Warnf("throttle state poll failed (will retry): %v", err)
	}
	elapsed := r.now().Sub(r.startedAt).Round(time.Second)
	logger.Infof("throttle modification state=%s progress=%d%% target=%d MiB/s elapsed=%s", st, progress, r.target, elapsed)
	switch {
	case st == modCompleted:
		logger.Infof("throttle fully applied after %s", elapsed)
		r.doSignal("requested")
	case st == modFailed:
		logger.Warnf("volume modification reported failed after %s", elapsed)
		r.doSignal("failed")
	case r.now().Sub(r.startedAt) >= r.timeout:
		logger.Warnf("throttle convergence timed out after %s", r.timeout)
		r.doSignal("failed")
	}
}

// doSignal records the outcome only when the signal is confirmed, so a dropped
// marker is retried on the next pass.
func (r *throttleReconciler) doSignal(outcome string) {
	if r.signal(outcome) {
		r.signalled = true
	}
}

// ec2Throttler is the production volumeThrottler backed by EC2.
type ec2Throttler struct {
	client   *ec2.Client
	volumeID string
}

func (e *ec2Throttler) modify(ctx context.Context, throughput int32) error {
	_, err := e.client.ModifyVolume(ctx, &ec2.ModifyVolumeInput{
		VolumeId:   &e.volumeID,
		Throughput: aws.Int32(throughput),
	})
	return err
}

// state polls the modification record (not the volume's throughput attribute,
// which flips to the target immediately) for its true ModificationState.
func (e *ec2Throttler) state(ctx context.Context) (modState, int32, error) {
	out, err := e.client.DescribeVolumesModifications(ctx, &ec2.DescribeVolumesModificationsInput{
		VolumeIds: []string{e.volumeID},
	})
	if err != nil {
		return modInProgress, 0, err
	}
	if len(out.VolumesModifications) == 0 {
		// No record yet (eventual consistency right after the request).
		return modInProgress, 0, nil
	}
	m := out.VolumesModifications[0]
	var progress int32
	if m.Progress != nil {
		progress = int32(*m.Progress)
	}
	switch m.ModificationState {
	case ec2types.VolumeModificationStateCompleted:
		return modCompleted, progress, nil
	case ec2types.VolumeModificationStateFailed:
		return modFailed, progress, nil
	default: // modifying, optimizing
		return modInProgress, progress, nil
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
	if srv, repo, run := os.Getenv("GITHUB_SERVER_URL"), os.Getenv("GITHUB_REPOSITORY"), os.Getenv("GITHUB_RUN_ID"); srv != "" && repo != "" && run != "" {
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
