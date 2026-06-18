package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/zstd"
)

// Marker files of the cross-half protocol (see package doc), both written here
// and polled by the runner half.
const (
	markerDownloadComplete = "/tmp/download-complete"
	markerDone             = "/tmp/done"
)

// env returns the value of key, or def if unset/empty.
func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ledgerScenarios are the apply-load profiles ingested as one concatenated
// stream: bundle i is paired with config i (apply-load-v27-<scenario>.cfg).
var ledgerScenarios = []string{"oz", "sac", "soroswap"}

// instantiate is the instance half (the bootstrap has already installed the
// toolchain and checked out the repo): it streams the corpus from S3, then runs
// the benchmark under a cgroup I/O throttle and writes the ok/fail verdict.
func instantiate(ctx context.Context) error {
	var (
		bucket      = env("BUCKET", "stellar-rpc-ci-load-test")
		region      = env("REGION", "us-east-1")
		workDir     = env("WORK_DIR", "/data")
		goldenDB    = env("GOLDEN_DB", filepath.Join(workDir, "golden.sqlite"))
		resultsFile = env("RESULTS_FILE", "/tmp/results.md")
		targetSHA   = os.Getenv("TARGET_SHA")
		runID       = env("RUN_ID", "manual")
	)

	// repoRoot is the checkout the bootstrap cd'd into before `go run`.
	repoRoot, err := os.Getwd()
	if err != nil {
		return err
	}
	bail := func(format string, args ...any) error {
		return bailInstance(resultsFile, runID, targetSHA, fmt.Sprintf(format, args...))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return bail("loading AWS config: %v", err)
	}
	fetch := &s3Fetcher{client: s3.NewFromConfig(awsCfg), bucket: bucket}

	logger.Infof("clearing stale run state")
	for _, m := range []string{markerDone, markerDownloadComplete} {
		_ = os.Remove(m)
	}

	configDir := filepath.Join(repoRoot, "cmd/stellar-rpc/internal/integrationtest/infrastructure/load-test/testdata")
	bundlePaths, configPaths, goldenFetchSecs, err := fetchCorpus(ctx, fetch, goldenDB, configDir)
	if err != nil {
		return bail("%v", err)
	}

	logger.Infof("download complete")
	if err := os.WriteFile(markerDownloadComplete, nil, 0o644); err != nil {
		return bail("writing %s: %v", markerDownloadComplete, err)
	}

	logger.Infof("building rpc libs")
	if err := runAt(ctx, repoRoot, "make", "build-libs"); err != nil {
		return bail("make build-libs failed: %v", err)
	}

	// EXPERIMENT: no throttle. The benchmark runs at the volume's provisioned
	// throughput, which load-test.yml pins to 125 MiB/s for the whole run.
	logger.Infof("running ingest perf benchmark (un-throttled; volume-rate)")
	benchEnv := []string{
		"LOADTEST_INGEST_LEDGER_PATH=" + strings.Join(bundlePaths, ","),
		"LOADTEST_CONFIG_PATH=" + strings.Join(configPaths, ","),
		"LOADTEST_INGEST_DEADLINE=" + env("LOADTEST_INGEST_DEADLINE", "150m"),
		"LOADTEST_SQLITE_PATH=" + goldenDB,
		"PERF_RESULTS_PATH=/tmp/bench-results.json",
		"PERF_RESULTS_MD_PATH=" + resultsFile,
		"PERF_TARGET_SHA=" + targetSHA,
		"PERF_RUN_ID=" + runID,
		"PERF_REPO=" + env("REPO", "stellar/stellar-rpc"),
		fmt.Sprintf("PERF_GOLDEN_FETCH_SECONDS=%d", goldenFetchSecs),
		"STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true",
	}
	if tail, err := runBenchmark(ctx, repoRoot, benchEnv); err != nil {
		return bail("benchmark failed:\n%s", tail)
	}

	if fi, err := os.Stat(resultsFile); err != nil || fi.Size() == 0 {
		return bail("benchmark succeeded but did not emit %s", resultsFile)
	}
	logger.Infof("results ready; signaling %s", markerDone)
	return os.WriteFile(markerDone, []byte("ok\n"), 0o644)
}

// bailInstance writes the failure body to the results file, flips the verdict
// marker to "fail", and exits non-zero — the instance half's hard-stop, so the
// runner always sees a verdict instead of hanging until the results timeout.
func bailInstance(resultsFile, runID, targetSHA, msg string) error {
	logger.Error(msg)
	body := fmt.Sprintf("❌ **Ingest load test failed** (run %s on `%s`)\n\n```\n%s\n```\n", runID, targetSHA, msg)
	_ = os.WriteFile(resultsFile, []byte(body), 0o644)
	_ = os.WriteFile(markerDone, []byte("fail\n"), 0o644)
	os.Exit(1)
	return nil // unreachable
}

// runAt runs name in dir, streaming combined output to our log; on failure the
// returned error carries the last lines so the verdict explains what broke.
func runAt(ctx context.Context, dir, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	var buf strings.Builder
	w := io.MultiWriter(os.Stderr, &buf)
	cmd.Stdout, cmd.Stderr = w, w
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w\n%s", err, lastLines(buf.String(), 40))
	}
	return nil
}

// runBenchmark runs the ingest test, capturing combined output to
// /tmp/benchmark.log; on failure it returns the last lines for the verdict.
func runBenchmark(ctx context.Context, dir string, extraEnv []string) (string, error) {
	const benchLogPath = "/tmp/benchmark.log"
	benchLog, err := os.Create(benchLogPath)
	if err != nil {
		return "", err
	}
	defer benchLog.Close()

	cmd := exec.CommandContext(ctx, "go", "test", "-run", "TestIngestSyntheticLedgers", "-timeout", "170m", "-v",
		"./cmd/stellar-rpc/internal/integrationtest/")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), extraEnv...)
	w := io.MultiWriter(benchLog, os.Stderr)
	cmd.Stdout, cmd.Stderr = w, w
	if err := cmd.Run(); err != nil {
		return tailFile(benchLogPath, 80), err
	}
	return "", nil
}

// tailFile returns the last n lines of path (best-effort).
func tailFile(path string, n int) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return lastLines(string(data), n)
}

// lastLines returns the last n lines of s.
func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// fetchCorpus streams the golden DB, stellar-core, and ledger bundles from S3,
// returning the bundle paths, their matching checked-out config paths (config i
// describes bundle i), and the golden DB fetch duration.
func fetchCorpus(ctx context.Context, fetch *s3Fetcher, goldenDB, configDir string) ([]string, []string, int, error) {
	// Golden DB: newest available snapshot wins. current/prev1/prev2 lets a run
	// fall back to an older snapshot while a fresh one is being published.
	var goldenFetchSecs int
	goldenKey := ""
	for _, pfx := range []string{"current", "prev1", "prev2"} {
		key := pfx + "/golden.sqlite.zst"
		logger.Infof("streaming s3://%s/%s", fetch.bucket, key)
		start := time.Now()
		if err := fetch.fetchVerified(ctx, key, goldenDB, true, "golden DB"); err != nil {
			logger.Infof("%v", err)
			_ = os.Remove(goldenDB)
			continue
		}
		goldenKey = key
		goldenFetchSecs = int(time.Since(start).Seconds())
		logger.Infof("golden DB ready in %ds", goldenFetchSecs)
		break
	}
	if goldenKey == "" {
		return nil, nil, 0, errors.New("no golden.sqlite.zst in current/, prev1/, or prev2/")
	}

	// Stock SDF apt-package stellar-core lacks apply-load (BUILD_TESTS-gated),
	// so we ship a pre-built binary under a separate core/ prefix.
	const corePath = "/usr/local/bin/stellar-core"
	if err := fetch.fetchVerified(ctx, "core/stellar-core.zst", corePath, true, "stellar-core"); err != nil {
		return nil, nil, 0, err
	}
	if err := os.Chmod(corePath, 0o755); err != nil {
		return nil, nil, 0, fmt.Errorf("chmod stellar-core: %w", err)
	}

	var bundlePaths, configPaths []string
	for _, sc := range ledgerScenarios {
		bundlePath := fmt.Sprintf("/tmp/load-test-ledgers-v27-%s.xdr.zstd", sc)
		key := fmt.Sprintf("ledgers/load-test-ledgers-v27-%s.xdr.zstd", sc)
		if err := fetch.fetchVerified(ctx, key, bundlePath, false, "ledger bundle ("+sc+")"); err != nil {
			return nil, nil, 0, err
		}
		bundlePaths = append(bundlePaths, bundlePath)

		cfg := filepath.Join(configDir, fmt.Sprintf("apply-load-v27-%s.cfg", sc))
		if _, err := os.Stat(cfg); err != nil {
			return nil, nil, 0, fmt.Errorf("missing apply-load config %s in checkout", cfg)
		}
		configPaths = append(configPaths, cfg)
	}
	return bundlePaths, configPaths, goldenFetchSecs, nil
}

// s3Fetcher streams objects from one bucket, verifying the sha256-raw
// user-metadata when present.
type s3Fetcher struct {
	client *s3.Client
	bucket string
}

// fetchVerified downloads key to dst (zstd-decoding when zstdMode), then checks
// its sha256 against the object's sha256-raw metadata (unverified if absent).
func (f *s3Fetcher) fetchVerified(ctx context.Context, key, dst string, zstdMode bool, label string) error {
	expected := f.expectedSHA(ctx, key, label)
	logger.Infof("fetching %s", label)
	got, err := f.streamObject(ctx, key, dst, zstdMode)
	if err != nil {
		return fmt.Errorf("failed to download %s: %w", label, err)
	}
	if expected != "" && expected != got {
		return fmt.Errorf("%s hash mismatch: expected %s, got %s", label, expected, got)
	}
	if expected == "" {
		logger.Infof("%s hash computed (unverified) (%s)", label, got)
	} else {
		logger.Infof("%s hash OK (%s)", label, got)
	}
	return nil
}

// expectedSHA returns the object's sha256-raw user-metadata, or "" when the
// object or the metadata key is absent (caller then fetches unverified).
func (f *s3Fetcher) expectedSHA(ctx context.Context, key, label string) string {
	head, err := f.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &f.bucket, Key: &key})
	if err != nil {
		logger.Warnf("head-object failed for s3://%s/%s; fetching %s without checksum", f.bucket, key, label)
		return ""
	}
	// S3 lowercases user-metadata keys; the SDK strips the x-amz-meta- prefix.
	if sha := head.Metadata["sha256-raw"]; sha != "" {
		return sha
	}
	logger.Warnf("no sha256-raw on s3://%s/%s; skipping %s checksum", f.bucket, key, label)
	return ""
}

// streamObject downloads key to dst (zstd-decoding when zstdMode) and returns
// the sha256 of the bytes written.
func (f *s3Fetcher) streamObject(ctx context.Context, key, dst string, zstdMode bool) (string, error) {
	out, err := f.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &f.bucket, Key: &key})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()

	var src io.Reader = out.Body
	if zstdMode {
		zr, err := zstd.NewReader(out.Body)
		if err != nil {
			return "", err
		}
		defer zr.Close()
		src = zr
	}

	file, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(file, hasher), src); err != nil {
		return "", err
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
