package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"
)

// ledgerScenarios are the apply-load profiles ingested as one concatenated stream,
// one bundle per scenario (load-test-ledgers-<version>-<scenario>.xdr.zstd).
var (
	ledgerScenarios = []string{"oz", "sac", "soroswap"}
	curVersion      = "v27" // version of the above bundles
)

// instantiate is the instance half after the bootstrap, which streams the corpus
// from S3, runs the benchmark, and writes the ok/fail verdict.
func instantiate(ctx context.Context) error {
	var (
		bucket       = harness.Env("BUCKET", "stellar-rpc-ci-load-test")
		region       = harness.Env("REGION", "us-east-1")
		workDir      = harness.Env("WORK_DIR", "/data")
		goldenDB     = harness.Env("GOLDEN_DB", filepath.Join(workDir, "golden.sqlite"))
		resultsFile  = harness.Env("RESULTS_FILE", "/tmp/results.md")
		benchResults = harness.Env("BENCH_RESULTS", "/tmp/bench-results.json")
		resultKey    = os.Getenv("RESULT_KEY")
		targetSHA    = os.Getenv("TARGET_SHA")
		runID        = harness.Env("RUN_ID", "manual")
	)

	repoRoot, err := os.Getwd()
	if err != nil {
		return err
	}
	bail := func(format string, args ...any) error {
		return harness.BailInstance(resultsFile, "Ingest load test", runID, targetSHA, fmt.Sprintf(format, args...))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return bail("loading AWS config: %v", err)
	}
	fetch := &harness.S3Fetcher{Client: s3.NewFromConfig(awsCfg), Bucket: bucket}

	bundlePaths, goldenFetchSecs, err := fetchCorpus(ctx, fetch, goldenDB)
	if err != nil {
		return bail("%v", err)
	}

	logger.Infof("download complete")

	logger.Infof("building rpc libs")
	if err := harness.RunStreaming(ctx, repoRoot, nil, 40, "make", "build-libs"); err != nil {
		return bail("make build-libs failed: %v", err)
	}

	logger.Infof("running ingest perf benchmark")
	benchEnv := []string{
		"LOADTEST_INGEST_LEDGER_PATH=" + strings.Join(bundlePaths, ","),
		"LOADTEST_INGEST_DEADLINE=" + harness.Env("LOADTEST_INGEST_DEADLINE", "150m"),
		"LOADTEST_SQLITE_PATH=" + goldenDB,
		"PERF_RESULTS_PATH=" + benchResults,
		"PERF_RESULTS_MD_PATH=" + resultsFile,
		"PERF_TARGET_SHA=" + targetSHA,
		"PERF_RUN_ID=" + runID,
		"PERF_REPO=" + harness.Env("REPO", "stellar/stellar-rpc"),
		fmt.Sprintf("PERF_GOLDEN_FETCH_SECONDS=%d", goldenFetchSecs),
		"STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true",
	}
	if err := harness.RunStreaming(ctx, repoRoot, benchEnv, 80,
		"go", "test", "-run", "TestIngestSyntheticLedgers", "-timeout", "170m", "-v",
		"./cmd/stellar-rpc/internal/integrationtest/"); err != nil {
		return bail("benchmark failed:\n%v", err)
	}

	if fi, err := os.Stat(resultsFile); err != nil || fi.Size() == 0 {
		return bail("benchmark succeeded but did not emit %s", resultsFile)
	}
	logger.Infof("results ready; publishing verdict")
	if err := harness.PublishResult(
		ctx, fetch.Client, bucket, resultKey, "ok", runID, targetSHA, resultsFile, benchResults,
	); err != nil {
		return bail("publishing result: %v", err)
	}
	return nil
}

// fetchCorpus streams the golden DB, stellar-core, and ledger bundles from S3,
// returning the bundle paths and the golden DB fetch duration.
func fetchCorpus(ctx context.Context, fetch *harness.S3Fetcher, goldenDB string) ([]string, int, error) {
	// current/prev1/prev2 lets a run fall back to an older golden DB snapshot
	// while a fresh one is being published.
	goldenFetchSecs := -1
	for _, pfx := range []string{"current", "prev1", "prev2"} {
		key := pfx + "/golden.sqlite.zst"
		logger.Infof("streaming s3://%s/%s", fetch.Bucket, key)
		start := time.Now()
		if err := fetch.FetchVerified(ctx, key, goldenDB, true, "golden DB"); err != nil {
			logger.Infof("%v", err)
			_ = os.Remove(goldenDB)
			continue
		}
		goldenFetchSecs = int(time.Since(start).Seconds())
		logger.Infof("golden DB ready in %ds", goldenFetchSecs)
		break
	}
	if goldenFetchSecs < 0 {
		return nil, 0, errors.New("no golden.sqlite.zst in current/, prev1/, or prev2/")
	}

	const corePath = "/usr/local/bin/stellar-core" // fetch pre-built core cached in S3
	if err := fetch.FetchVerified(ctx, "core/stellar-core.zst", corePath, true, "stellar-core"); err != nil {
		return nil, 0, err
	}
	if err := os.Chmod(corePath, 0o755); err != nil {
		return nil, 0, fmt.Errorf("chmod stellar-core: %w", err)
	}

	var bundlePaths []string
	for _, sc := range ledgerScenarios {
		bundlePath := fmt.Sprintf("/tmp/load-test-ledgers-%s-%s.xdr.zstd", curVersion, sc)
		key := fmt.Sprintf("ledgers/load-test-ledgers-%s-%s.xdr.zstd", curVersion, sc)
		if err := fetch.FetchVerified(ctx, key, bundlePath, false, "ledger bundle ("+sc+")"); err != nil {
			return nil, 0, err
		}
		bundlePaths = append(bundlePaths, bundlePath)
	}
	return bundlePaths, goldenFetchSecs, nil
}
