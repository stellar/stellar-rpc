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

// legDir is this leg's path under the repo root, where its config template is
// checked in (the runner runs with cwd = repo root).
const legDir = "cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/backfill-test"

const corePath = "/usr/local/bin/stellar-core" // fetched from S3

// backfillDoneRe matches the line emitted when the ledger fill completes;
// finalizeDoneRe the terminal one emitted after the deferred index rebuild.
var (
	backfillDoneRe = regexp.MustCompile(`Backfill process complete, ledgers \[(\d+) -> (\d+)\]`)
	finalizeDoneRe = regexp.MustCompile(`Bulk-load finalize complete`)
)

// instantiate is the instance's backfill task: it fetches + builds test fixtures,
// runs a timed backfill, then publishes the verdict.
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

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return bail("loading AWS config: %v", err)
	}
	fetch := &harness.S3Fetcher{Client: s3.NewFromConfig(awsCfg), Bucket: bucket}

	if err := fetch.FetchVerified(ctx, "core/stellar-core.zst", corePath, true, "stellar-core"); err != nil {
		return bail("%v", err)
	}
	if err := os.Chmod(corePath, 0o755); err != nil {
		return bail("chmod stellar-core: %v", err)
	}

	logger.Infof("building stellar-rpc")
	if err := harness.RunStreaming(ctx, repoRoot, nil, 40, "make", "build-libs"); err != nil {
		return bail("make build-libs failed: %v", err)
	}
	if err := harness.RunStreaming(ctx, repoRoot, nil, 40,
		"go", "build", "-o", binaryPath, "./cmd/stellar-rpc"); err != nil {
		return bail("go build failed: %v", err)
	}

	// fetch + write core config from SDK
	coreCfg := filepath.Join(workDir, "captive-core-pubnet.cfg")
	if err := os.WriteFile(coreCfg, ledgerbackend.PubnetDefaultConfig, 0o644); err != nil {
		return bail("writing captive-core config: %v", err)
	}

	cfgPath, err := renderConfig(repoRoot, workDir, coreCfg, retention)
	if err != nil {
		return bail("rendering config: %v", err)
	}

	dl, err := time.ParseDuration(deadline)
	if err != nil {
		return bail("invalid BACKFILL_DEADLINE %q: %v", deadline, err)
	}
	logger.Infof("starting backfill (retention=%s, deadline=%s)", retention, deadline)
	elapsed, lo, hi, err := runBackfill(ctx, dl, binaryPath, cfgPath)
	if err != nil {
		return bail("%v", err)
	}
	ingested := hi - lo + 1
	logger.Infof("backfill complete: %d ledgers [%d -> %d] in %s", ingested, lo, hi, elapsed.Round(time.Second))

	md := renderMarkdown(targetSHA, retention, lo, hi, ingested, elapsed)
	if err := os.WriteFile(resultsFile, []byte(md), 0o644); err != nil {
		return bail("writing results: %v", err)
	}
	if err := harness.PublishResult(
		ctx, fetch.Client, bucket, resultKey, "ok", runID, targetSHA, resultsFile, ""); err != nil {
		return bail("publishing result: %v", err)
	}
	return nil
}

// renderConfig fills the config template's ${...} placeholders (box paths + the
// retention window) via os.Expand
func renderConfig(repoRoot, workDir, coreCfg, retention string) (string, error) {
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

// runBackfill launches the daemon and streams its output (teeing to the box log)
// until the finalize-complete line fires, recording the wall-clock
func runBackfill(ctx context.Context, deadline time.Duration, binary, cfgPath string) (time.Duration, int, int, error) {
	runCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	cmd := exec.CommandContext(runCtx, binary, "--config-path", cfgPath)
	// hide this box's IMDS creds as the public datalake 403s signed requests
	cmd.Env = append(os.Environ(), "AWS_EC2_METADATA_DISABLED=true")
	pr, pw, err := os.Pipe()
	if err != nil {
		return 0, 0, 0, err
	}
	cmd.Stdout, cmd.Stderr = pw, pw

	start := time.Now()
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return 0, 0, 0, fmt.Errorf("starting daemon: %w", err)
	}
	pw.Close() // the child holds the write end and we read until it dies
	defer pr.Close()

	var elapsed time.Duration
	var lo, hi int
	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Fprintln(os.Stderr, line) // tee to the box user-data log (SSM debug tail)
		if m := backfillDoneRe.FindStringSubmatch(line); m != nil {
			lo, _ = strconv.Atoi(m[1])
			hi, _ = strconv.Atoi(m[2])
		}
		if hi != 0 && finalizeDoneRe.MatchString(line) {
			elapsed = time.Since(start)
			cancel() // stop the daemon before it starts live ingestion
			break
		}
	}
	_ = cmd.Wait() // reap; a kill from cancel surfaces here and is expected

	if elapsed == 0 {
		return 0, 0, 0, fmt.Errorf("daemon exited or hit the %s deadline before backfill finalized", deadline)
	}
	return elapsed, lo, hi, nil
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
