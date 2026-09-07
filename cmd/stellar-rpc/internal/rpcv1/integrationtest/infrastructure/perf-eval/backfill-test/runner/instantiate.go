package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/caarlos0/env/v11"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure/perf-eval/harness"
)

const (
	// runner runs w/ cwd = repo root, so paths are relative to there
	legDir   = "cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure/perf-eval/backfill-test"
	corePath = "/usr/local/bin/stellar-core" // fetched from S3
)

const ledgerThreshold = 384 // mirrors ingest.ledgerThreshold in backfill.go

// backfillDoneRe matches the line ending the ingest phase of backfill
var backfillDoneRe = regexp.MustCompile(`Backfill process complete, ledgers \[(\d+) -> (\d+)\]`)

// finalizeDoneRe matches the line ending the finalize phase
var finalizeDoneRe = regexp.MustCompile(`Bulk-load finalize complete`)

// backfillEnv is the leg's env-derived config.
type backfillEnv struct {
	Bucket      string        `env:"BUCKET"                   envDefault:"stellar-rpc-ci-load-test"`
	Region      string        `env:"REGION"                   envDefault:"us-east-1"`
	WorkDir     string        `env:"WORK_DIR"                 envDefault:"/data"`
	ResultsFile string        `env:"RESULTS_FILE"             envDefault:"/tmp/results.md"`
	ResultKey   string        `env:"RESULT_KEY"`
	TargetSHA   string        `env:"TARGET_SHA"`
	RunID       string        `env:"RUN_ID"                   envDefault:"manual"`
	Retention   int           `env:"HISTORY_RETENTION_WINDOW" envDefault:"120960"`
	Deadline    time.Duration `env:"BACKFILL_DEADLINE"        envDefault:"4h"`
	// serve on a non-loopback bind after the backfill completes
	ServeAfter bool `env:"SERVE_AFTER_BACKFILL"`
}

// instantiate fetches + builds test fixtures + runs a timed backfill, then
// publishes the verdict. With SERVE_AFTER_BACKFILL it keeps the daemon serving.
func instantiate(ctx context.Context) error {
	cfg, err := env.ParseAs[backfillEnv]()
	if err != nil {
		return fmt.Errorf("parsing env: %w", err) // run_leg publishes the generic fail
	}
	binaryPath := filepath.Join(cfg.WorkDir, "stellar-rpc-bin") // built here (the repo checkout is in WORK_DIR)
	retention := strconv.Itoa(cfg.Retention)                    // config template + report take it as a string

	repoRoot, err := os.Getwd()
	if err != nil {
		return err
	}
	bail := func(format string, args ...any) error {
		return harness.BailInstance(cfg.ResultsFile, "Backfill ingestion", cfg.RunID, cfg.TargetSHA,
			fmt.Sprintf(format, args...))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return bail("loading AWS config: %v", err)
	}
	fetch := &harness.S3Fetcher{Client: s3.NewFromConfig(awsCfg), Bucket: cfg.Bucket}

	coreCfg, err := prepareFixtures(ctx, fetch, repoRoot, cfg.WorkDir, binaryPath)
	if err != nil {
		return bail("%v", err)
	}

	endpoint := "localhost:" + rpcPort
	if cfg.ServeAfter {
		endpoint = "0.0.0.0:" + rpcPort
	}

	cfgPath, err := renderConfig(repoRoot, cfg.WorkDir, coreCfg, retention, endpoint)
	if err != nil {
		return bail("rendering config: %v", err)
	}

	logger.Infof("starting backfill (retention=%s, deadline=%s, serve-after=%t)", retention, cfg.Deadline, cfg.ServeAfter)
	timings, lo, hi, daemon, err := runBackfill(ctx, cfg.Deadline, binaryPath, cfgPath, cfg.ServeAfter)
	if err != nil {
		return bail("%v", err)
	}
	if daemon != nil {
		defer daemon.Stop() // covers the bail paths below; Stop is idempotent
	}
	ingested := hi - lo + 1
	if ingested+ledgerThreshold < cfg.Retention {
		return bail("backfill reported complete but ingested %d of %s ledgers", ingested, retention)
	}
	logger.Infof("backfill complete: %d ledgers [%d -> %d] in %s (ingest %s + finalize %s)",
		ingested, lo, hi, timings.total().Round(time.Second),
		timings.ingest.Round(time.Second), timings.finalize.Round(time.Second))

	md := renderMarkdown(cfg.TargetSHA, retention, lo, hi, ingested, timings)
	if err := os.WriteFile(cfg.ResultsFile, []byte(md), 0o644); err != nil {
		return bail("writing results: %v", err)
	}
	if err := harness.PublishResult(
		ctx, fetch.Client, cfg.Bucket, cfg.ResultKey, "ok", cfg.RunID, cfg.TargetSHA, cfg.ResultsFile, ""); err != nil {
		return bail("publishing result: %v", err)
	}

	if daemon != nil {
		// hand the serving box off to the chained blaster leg
		servePhase(ctx, daemon, cfg.Bucket, cfg.ResultKey)
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
		"go", "build", "-o", binaryPath, "./cmd/stellar-rpc/rpcv1"); err != nil {
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
	cancel   context.CancelFunc
	done     chan struct{} // closed once the daemon is reaped
	err      error         // cmd.Wait's result; valid once done is closed
	stopping atomic.Bool   // set by Stop, so the reaper can tell a requested kill from a crash
}

// Stop kills the daemon and waits (bounded) for it to be reaped.
func (d *daemonHandle) Stop() {
	d.stopping.Store(true)
	d.cancel()
	select {
	case <-d.done:
	case <-time.After(30 * time.Second):
		logger.Warnf("daemon not reaped within 30s of stop")
	}
}

// phaseTimings holds the measured wall-clock of the daemon's backfill phases.
type phaseTimings struct {
	ingest   time.Duration
	finalize time.Duration
}

func (t phaseTimings) total() time.Duration { return t.ingest + t.finalize }

// scanUntil tees lines to the box user-data log (SSM debug tail) until re
// matches, returning the submatches, or nil if the output ended first.
func scanUntil(scanner *bufio.Scanner, re *regexp.Regexp) []string {
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Fprintln(os.Stderr, line)
		if m := re.FindStringSubmatch(line); m != nil {
			return m
		}
	}
	return nil
}

// runBackfill launches the daemon and streams its output (teeing to the box log)
// through the bulk-load finalize line, recording per-phase wall-clock.
func runBackfill(
	ctx context.Context, deadline time.Duration, binary, cfgPath string, keepAlive bool,
) (phaseTimings, int, int, *daemonHandle, error) {
	var timings phaseTimings
	runCtx, cancel := context.WithCancel(ctx)
	// deadline covers the backfill + finalize phases, which the daemon may outlive
	watchdog := time.AfterFunc(deadline, cancel)

	cmd := exec.CommandContext(runCtx, binary, "--config-path", cfgPath)
	// hide this box's IMDS creds as the public datalake 403s signed requests
	cmd.Env = append(os.Environ(), "AWS_EC2_METADATA_DISABLED=true")
	pr, pw, err := os.Pipe()
	if err != nil {
		cancel()
		return timings, 0, 0, nil, err
	}
	cmd.Stdout, cmd.Stderr = pw, pw

	start := time.Now()
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		cancel()
		return timings, 0, 0, nil, fmt.Errorf("starting daemon: %w", err)
	}
	pw.Close() // the child holds the write end and we read until it dies

	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	// daemon died, read failure, or the watchdog killed it
	bail := func(phase string) error {
		cancel()
		pr.Close()
		_ = cmd.Wait()
		if scanErr := scanner.Err(); scanErr != nil {
			return fmt.Errorf("reading daemon output: %w", scanErr)
		}
		return fmt.Errorf("daemon exited or hit the %s deadline before %s completed", deadline, phase)
	}

	var lo, hi int
	m := scanUntil(scanner, backfillDoneRe)
	if m == nil {
		return timings, 0, 0, nil, bail("backfill")
	}
	timings.ingest = time.Since(start)
	lo, _ = strconv.Atoi(m[1])
	hi, _ = strconv.Atoi(m[2])

	finalizeStart := time.Now()
	if scanUntil(scanner, finalizeDoneRe) == nil {
		return timings, 0, 0, nil, bail("bulk-load finalize")
	}
	timings.finalize = time.Since(finalizeStart)
	watchdog.Stop()

	daemon := &daemonHandle{cancel: cancel, done: make(chan struct{})}
	go daemon.reap(scanner, pr, cmd)
	if !keepAlive {
		daemon.Stop() // stop the daemon before the frontfill top-up and live ingestion
		return timings, lo, hi, nil, nil
	}
	return timings, lo, hi, daemon, nil
}

// reap keeps draining the pipe (the daemon blocks on it once full), teeing its
// catchup/ingestion output to the box log until it dies, then records how.
func (d *daemonHandle) reap(scanner *bufio.Scanner, pr *os.File, cmd *exec.Cmd) {
	defer close(d.done)
	for scanner.Scan() {
		fmt.Fprintln(os.Stderr, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		// a line over the scanner's cap must not stop the drain: closing the
		// read end would SIGPIPE-kill a healthy daemon on its next write
		logger.Warnf("reading daemon output: %v; draining the rest unbuffered", err)
		_, _ = io.Copy(os.Stderr, pr)
	}
	pr.Close()
	d.err = cmd.Wait()
	if d.stopping.Load() {
		logger.Infof("daemon stopped on request: %v", d.err)
	} else {
		logger.Warnf("daemon exited on its own: %v", d.err)
	}
}

func renderMarkdown(sha, retention string, lo, hi, ingested int, timings phaseTimings) string {
	shortSHA := sha
	if len(shortSHA) > 12 {
		shortSHA = shortSHA[:12]
	}
	lps := 0.0
	if s := timings.ingest.Seconds(); s > 0 {
		lps = float64(ingested) / s
	}
	return fmt.Sprintf("### ⏳ Backfill ingestion — `%s`\n\n"+
		"| Metric | Value |\n|---|---|\n"+
		"| Ledgers ingested | %d (`[%d -> %d]`) |\n"+
		"| Retention window | %s |\n"+
		"| Wall-clock (total) | %s |\n"+
		"| Ingest phase | %s |\n"+
		"| Bulk-load finalize phase | %s |\n"+
		"| Ledgers/sec (ingest) | %.1f |\n",
		shortSHA, ingested, lo, hi, retention, timings.total().Round(time.Second),
		timings.ingest.Round(time.Second), timings.finalize.Round(time.Second), lps)
}
