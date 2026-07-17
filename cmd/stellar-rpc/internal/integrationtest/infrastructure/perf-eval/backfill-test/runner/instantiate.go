package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"
)

const (
	// runner runs w/ cwd = repo root, so paths are relative to there
	legDir   = "cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/backfill-test"
	corePath = "/usr/local/bin/stellar-core" // fetched from S3
)

const ledgerThreshold = 384 // mirrors ingest.ledgerThreshold in backfill.go

// backfillDoneRe matches the terminal line emitted on backfill's completion
var backfillDoneRe = regexp.MustCompile(`Backfill process complete, ledgers \[(\d+) -> (\d+)\]`)

// instantiate is the instance's backfill task: it fetches + builds test fixtures,
// runs a timed backfill, then publishes the verdict. With SERVE_AFTER_BACKFILL
// it then holds the daemon serving for the chained blaster leg.
func instantiate(ctx context.Context) error {
	var (
		bucket      = harness.Env("BUCKET", "stellar-rpc-ci-load-test")
		region      = harness.Env("REGION", "us-east-1")
		workDir     = harness.Env("WORK_DIR", "/data")
		resultsFile = harness.Env("RESULTS_FILE", "/tmp/results.md")
		resultKey   = os.Getenv("RESULT_KEY")
		targetSHA   = os.Getenv("TARGET_SHA")
		runID       = harness.Env("RUN_ID", "manual")
		// ~1 day by default for cheap test runs; the full week is 120960.
		retention = harness.Env("HISTORY_RETENTION_WINDOW", "17280")
		deadline  = harness.Env("BACKFILL_DEADLINE", "4h")

		binaryPath = filepath.Join(workDir, "stellar-rpc-bin") // built here (the repo checkout is in WORK_DIR)
	)
	repoRoot, err := os.Getwd()
	if err != nil {
		return err
	}
	bail := func(format string, args ...any) error {
		return harness.BailInstance(resultsFile, "Backfill ingestion", runID, targetSHA, fmt.Sprintf(format, args...))
	}

	want, err := strconv.Atoi(retention) // used to compare against ingested ledgers below
	if err != nil {
		return bail("parsing retention window %q: %v", retention, err)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return bail("loading AWS config: %v", err)
	}
	fetch := &harness.S3Fetcher{Client: s3.NewFromConfig(awsCfg), Bucket: bucket}

	coreCfg, err := prepareFixtures(ctx, fetch, repoRoot, workDir, binaryPath)
	if err != nil {
		return bail("%v", err)
	}

	// with SERVE_AFTER_BACKFILL the chained blaster leg drives this box over the
	// VPC, so the daemon binds a non-loopback address and stays up to serve
	serveAfter := os.Getenv("SERVE_AFTER_BACKFILL") == "true"
	endpoint := "localhost:" + rpcPort
	if serveAfter {
		endpoint = "0.0.0.0:" + rpcPort
	}

	cfgPath, err := renderConfig(repoRoot, workDir, coreCfg, retention, endpoint)
	if err != nil {
		return bail("rendering config: %v", err)
	}

	dl, err := time.ParseDuration(deadline)
	if err != nil {
		return bail("invalid BACKFILL_DEADLINE %q: %v", deadline, err)
	}
	logger.Infof("starting backfill (retention=%s, deadline=%s, serve-after=%t)", retention, deadline, serveAfter)
	elapsed, lo, hi, daemon, err := runBackfill(ctx, dl, binaryPath, cfgPath, serveAfter)
	if err != nil {
		return bail("%v", err)
	}
	if daemon != nil {
		defer daemon.Stop() // covers the bail paths below; Stop is idempotent
	}
	ingested := hi - lo + 1
	if ingested+ledgerThreshold < want {
		return bail("backfill reported complete but ingested %d of %s ledgers", ingested, retention)
	}
	logger.Infof("backfill complete: %d ledgers [%d -> %d] in %s", ingested, lo, hi, elapsed.Round(time.Second))

	md := renderMarkdown(targetSHA, retention, lo, hi, ingested, elapsed)
	if err := os.WriteFile(resultsFile, []byte(md), 0o644); err != nil {
		return bail("writing results: %v", err)
	}
	if err := harness.PublishResult(
		ctx, fetch.Client, bucket, resultKey, "ok", runID, targetSHA, resultsFile, ""); err != nil {
		return bail("publishing result: %v", err)
	}

	if daemon != nil {
		// hand the serving box off to the chained blaster leg
		servePhase(ctx, fetch, resultKey, runID, targetSHA, daemon)
	}
	return nil
}

// prepareFixtures fetches stellar-core, builds stellar-rpc into binaryPath,
// and writes the SDK's captive-core pubnet config, returning its path.
func prepareFixtures(
	ctx context.Context, fetch *harness.S3Fetcher, repoRoot, workDir, binaryPath string,
) (string, error) {
	if err := fetch.FetchVerified(ctx, "core/stellar-core.zst", corePath, true, "stellar-core"); err != nil {
		return "", err
	}
	if err := os.Chmod(corePath, 0o755); err != nil {
		return "", fmt.Errorf("chmod stellar-core: %w", err)
	}

	logger.Infof("building stellar-rpc")
	if err := harness.RunStreaming(ctx, repoRoot, nil, 40, "make", "build-libs"); err != nil {
		return "", fmt.Errorf("make build-libs failed: %w", err)
	}
	if err := harness.RunStreaming(ctx, repoRoot, nil, 40,
		"go", "build", "-o", binaryPath, "./cmd/stellar-rpc"); err != nil {
		return "", fmt.Errorf("go build failed: %w", err)
	}

	// fetch + write core config from SDK
	coreCfg := filepath.Join(workDir, "captive-core-pubnet.cfg")
	if err := os.WriteFile(coreCfg, ledgerbackend.PubnetDefaultConfig, 0o644); err != nil {
		return "", fmt.Errorf("writing captive-core config: %w", err)
	}
	return coreCfg, nil
}

// renderConfig fills the config template's ${...} placeholders (box paths, the
// retention window, and the bind endpoint) via os.Expand
func renderConfig(repoRoot, workDir, coreCfg, retention, endpoint string) (string, error) {
	tmpl, err := os.ReadFile(filepath.Join(repoRoot, legDir, "testdata", "backfill-pubnet.toml.tmpl"))
	if err != nil {
		return "", err
	}
	mapping := func(in string) string {
		switch in {
		case "CAPTIVE_CORE_CONFIG_PATH":
			return coreCfg
		case "CAPTIVE_CORE_STORAGE_PATH":
			return filepath.Join(workDir, "core-storage")
		case "DB_PATH":
			return filepath.Join(workDir, "backfill.sqlite")
		case "STELLAR_CORE_BINARY_PATH":
			return corePath
		case "HISTORY_RETENTION_WINDOW":
			return retention
		case "ENDPOINT":
			return endpoint
		default:
			return "${" + in + "}" // leave unknown placeholders intact
		}
	}
	body := os.Expand(string(tmpl), mapping)
	cfgPath := filepath.Join(workDir, "backfill-rpc.toml")
	if err := os.WriteFile(cfgPath, []byte(body), 0o644); err != nil {
		return "", err
	}
	return cfgPath, nil
}

// daemonHandle controls a daemon left running past its backfill phase.
type daemonHandle struct {
	cancel context.CancelFunc
	done   chan struct{} // closed once the daemon is reaped
}

// Stop kills the daemon and waits (bounded) for it to be reaped.
func (d *daemonHandle) Stop() {
	d.cancel()
	select {
	case <-d.done:
	case <-time.After(30 * time.Second):
		logger.Warnf("daemon not reaped within 30s of stop")
	}
}

// runBackfill launches the daemon and streams its output (teeing to the box log)
// until the backfill-complete line fires, recording the wall-clock. Without
// keepAlive the daemon is stopped there, before it starts live ingestion; with
// keepAlive it is left running (catchup + live ingestion, output still teed)
// and the returned handle owns stopping it.
func runBackfill(
	ctx context.Context, deadline time.Duration, binary, cfgPath string, keepAlive bool,
) (time.Duration, int, int, *daemonHandle, error) {
	runCtx, cancel := context.WithCancel(ctx)
	// deadline covers only the backfill phase, which the daemon may outlive
	watchdog := time.AfterFunc(deadline, cancel)

	cmd := exec.CommandContext(runCtx, binary, "--config-path", cfgPath)
	// hide this box's IMDS creds as the public datalake 403s signed requests
	cmd.Env = append(os.Environ(), "AWS_EC2_METADATA_DISABLED=true")
	pr, pw, err := os.Pipe()
	if err != nil {
		cancel()
		return 0, 0, 0, nil, err
	}
	cmd.Stdout, cmd.Stderr = pw, pw

	start := time.Now()
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		cancel()
		return 0, 0, 0, nil, fmt.Errorf("starting daemon: %w", err)
	}
	pw.Close() // the child holds the write end and we read until it dies

	var elapsed time.Duration
	var lo, hi int
	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Fprintln(os.Stderr, line) // tee to the box user-data log (SSM debug tail)
		if m := backfillDoneRe.FindStringSubmatch(line); m != nil {
			elapsed = time.Since(start)
			lo, _ = strconv.Atoi(m[1])
			hi, _ = strconv.Atoi(m[2])
			watchdog.Stop()
			break
		}
	}

	if elapsed == 0 { // daemon died, read failure, or the watchdog killed it
		cancel()
		pr.Close()
		_ = cmd.Wait()
		if scanErr := scanner.Err(); scanErr != nil {
			return 0, 0, 0, nil, fmt.Errorf("reading daemon output: %w", scanErr)
		}
		return 0, 0, 0, nil, fmt.Errorf("daemon exited or hit the %s deadline before backfill completed", deadline)
	}

	// keep draining the pipe (the daemon blocks on it once full) and teeing
	// its catchup/ingestion output to the box log until it dies
	done := make(chan struct{})
	go func() {
		defer close(done)
		for scanner.Scan() {
			fmt.Fprintln(os.Stderr, scanner.Text())
		}
		pr.Close()
		_ = cmd.Wait() // reap; a kill from cancel surfaces here and is expected
	}()
	daemon := &daemonHandle{cancel: cancel, done: done}
	if !keepAlive {
		daemon.Stop() // stop the daemon before it starts live ingestion
		return elapsed, lo, hi, nil, nil
	}
	return elapsed, lo, hi, daemon, nil
}

func renderMarkdown(sha, retention string, lo, hi, ingested int, elapsed time.Duration) string {
	shortSHA := sha
	if len(shortSHA) > 12 {
		shortSHA = shortSHA[:12]
	}
	lps := 0.0
	if s := elapsed.Seconds(); s > 0 {
		lps = float64(ingested) / s
	}
	return fmt.Sprintf("### ⏳ Backfill ingestion — `%s`\n\n"+
		"| Metric | Value |\n|---|---|\n"+
		"| Ledgers ingested | %d (`[%d -> %d]`) |\n"+
		"| Retention window | %s |\n"+
		"| Wall-clock | %s |\n"+
		"| Ledgers/sec | %.1f |\n",
		shortSHA, ingested, lo, hi, retention, elapsed.Round(time.Second), lps)
}
