package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

const (
	corePath = "/usr/local/bin/stellar-core" // fetched from S3

	// The template's ENDPOINT; getHealth here reports healthy once the latest
	// ledger is within max-healthy-ledger-latency (30s) of now, i.e. at tip.
	healthURL          = "http://localhost:8000"
	healthPollInterval = 10 * time.Second
)

// backfillDoneRe matches the line emitted when the ledger fill completes;
// finalizeDoneRe the one emitted after the bulk-load schema restore.
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
		retention       = harness.Env("HISTORY_RETENTION_WINDOW", "17280")
		deadline        = harness.Env("BACKFILL_DEADLINE", "4h")
		catchupDeadline = harness.Env("CATCHUP_DEADLINE", "2h")

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
	cdl, err := time.ParseDuration(catchupDeadline)
	if err != nil {
		return bail("invalid CATCHUP_DEADLINE %q: %v", catchupDeadline, err)
	}
	logger.Infof("starting backfill (retention=%s, deadline=%s, catch-up deadline=%s)",
		retention, deadline, catchupDeadline)
	stats, err := runBackfill(ctx, dl, cdl, binaryPath, cfgPath)
	if err != nil {
		return bail("%v", err)
	}
	ingested := stats.hi - stats.lo + 1
	logger.Infof("backfill complete: %d ledgers [%d -> %d] in %s (fill %s + finalize %s), catch-up %s",
		ingested, stats.lo, stats.hi, stats.total.Round(time.Second), stats.fill.Round(time.Second),
		(stats.total - stats.fill).Round(time.Second), stats.describeCatchup())

	md := renderMarkdown(targetSHA, retention, ingested, stats)
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

// runStats are the timed run's measurements. fill and total cover daemon start
// to the fill and finalize sentinels; catchup covers finalize to getHealth
// reporting healthy (captive core caught up to the live tip), with tip the
// ledger it reported. A missed catch-up leaves catchup zero and note set.
type runStats struct {
	fill, total, catchup time.Duration
	lo, hi, tip          int
	catchupNote          string
}

// describeCatchup renders the catch-up outcome for the log and report table.
func (s runStats) describeCatchup() string {
	if s.catchupNote != "" {
		return "❌ " + s.catchupNote
	}
	return fmt.Sprintf("%s (to ledger %d, %d past the backfill tip)",
		s.catchup.Round(time.Second), s.tip, s.tip-s.hi)
}

// runBackfill launches the daemon and streams its output (teeing to the box
// log) until the fill and finalize lines fire, then polls getHealth until
// captive core catches up to the live tip, recording all three wall-clocks. A
// missed catch-up is reported in the stats, not as an error.
func runBackfill(ctx context.Context, deadline, catchupDeadline time.Duration, binary, cfgPath string,
) (runStats, error) {
	var stats runStats
	runCtx, cancel := context.WithTimeout(ctx, deadline+catchupDeadline)
	defer cancel()

	cmd := exec.CommandContext(runCtx, binary, "--config-path", cfgPath)
	// hide this box's IMDS creds as the public datalake 403s signed requests
	cmd.Env = append(os.Environ(), "AWS_EC2_METADATA_DISABLED=true")
	pr, pw, err := os.Pipe()
	if err != nil {
		return stats, err
	}
	cmd.Stdout, cmd.Stderr = pw, pw

	start := time.Now()
	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return stats, fmt.Errorf("starting daemon: %w", err)
	}
	pw.Close() // the child holds the write end and we read until it dies
	defer pr.Close()

	// The scan keeps draining past finalize so the daemon never blocks on a
	// full pipe; stats writes all happen before close(finalized).
	finalized := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		scanner := bufio.NewScanner(pr)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		done := false
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Fprintln(os.Stderr, line) // tee to the box user-data log (SSM debug tail)
			if done {
				continue
			}
			if m := backfillDoneRe.FindStringSubmatch(line); m != nil {
				stats.fill = time.Since(start)
				stats.lo, _ = strconv.Atoi(m[1])
				stats.hi, _ = strconv.Atoi(m[2])
			}
			if stats.fill != 0 && finalizeDoneRe.MatchString(line) {
				stats.total = time.Since(start)
				done = true
				close(finalized)
			}
		}
	}()

	select {
	case <-finalized:
	case <-exited:
		_ = cmd.Wait()
		phase := "backfill"
		if stats.fill != 0 {
			phase = "finalize"
		}
		return stats, fmt.Errorf("daemon exited or hit the %s+%s deadline before %s completed",
			deadline, catchupDeadline, phase)
	}

	logger.Infof("fill+finalize complete; polling getHealth for captive-core catch-up (deadline %s)",
		catchupDeadline)
	catchupStart := time.Now()
	tip, err := awaitHealthy(runCtx, catchupDeadline)
	if err != nil {
		stats.catchupNote = err.Error()
		logger.Errorf("catch-up not observed: %v", err)
	} else {
		stats.catchup = time.Since(catchupStart)
		stats.tip = tip
	}
	cancel() // stop the daemon
	<-exited // let the tee drain
	_ = cmd.Wait()
	return stats, nil
}

// awaitHealthy polls getHealth until the daemon reports healthy (latest ledger
// within max-healthy-ledger-latency of now), returning the ledger it reported.
// Tolerates connection errors: the JSON-RPC server only comes up after finalize.
func awaitHealthy(ctx context.Context, timeout time.Duration) (int, error) {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(healthPollInterval)
	defer ticker.Stop()
	reqBody := []byte(`{"jsonrpc":"2.0","id":1,"method":"getHealth"}`)
	lastErr := errors.New("no getHealth response yet")
	for {
		select {
		case <-pollCtx.Done():
			return 0, fmt.Errorf("catch-up unfinished after %s (last: %w)", timeout, lastErr)
		case <-ticker.C:
		}
		req, err := http.NewRequestWithContext(pollCtx, http.MethodPost, healthURL, bytes.NewReader(reqBody))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		var parsed struct {
			Result *struct {
				Status       string `json:"status"`
				LatestLedger int    `json:"latestLedger"`
			} `json:"result"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		err = json.NewDecoder(resp.Body).Decode(&parsed)
		resp.Body.Close()
		switch {
		case err != nil:
			lastErr = err
		case parsed.Result != nil && parsed.Result.Status == "healthy":
			return parsed.Result.LatestLedger, nil
		case parsed.Error != nil:
			lastErr = errors.New(parsed.Error.Message) // e.g. "latency ... is too high"
		}
	}
}

func renderMarkdown(sha, retention string, ingested int, stats runStats) string {
	shortSHA := sha[:min(len(sha), 12)]
	lps := 0.0
	if s := stats.total.Seconds(); s > 0 {
		lps = float64(ingested) / s
	}
	return fmt.Sprintf("### ⏳ Backfill ingestion — `%s`\n\n"+
		"| Metric | Value |\n|---|---|\n"+
		"| Ledgers ingested | %d (`[%d -> %d]`) |\n"+
		"| Retention window | %s |\n"+
		"| Fill wall-clock | %s |\n"+
		"| Finalize wall-clock | %s |\n"+
		"| Total wall-clock | %s |\n"+
		"| End-to-end ledgers/sec | %.1f |\n"+
		"| Captive-core catch-up | %s |\n",
		shortSHA, ingested, stats.lo, stats.hi, retention,
		stats.fill.Round(time.Second), (stats.total - stats.fill).Round(time.Second),
		stats.total.Round(time.Second), lps, stats.describeCatchup())
}
