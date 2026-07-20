package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"
)

const legTitle = "Go endpoint benchmarks"

// benchDenylist is the set of benchmarks to exclude from the suite.
var benchDenylist = []string{
	"BenchmarkGetLedgerEntries", // is an integration test
	"BenchmarkTransactionFetch", // is currently broken on main
}

// instantiate is the instance half after the bootstrap, which runs the benches
// and writes the results file to be published to S3.
func instantiate(ctx context.Context) error {
	var (
		env         = harness.GetEnv()
		baselineRef = os.Getenv("BASELINE_REF")
		countStr    = harness.Env("BENCH_COUNT", "10")

		baselineOut  = "/tmp/baseline.txt"
		candidateOut = "/tmp/candidate.txt"
		benchstatOut = "/tmp/benchstat.txt"
		benchResults = "/tmp/bench-results.json"
	)

	repoRoot, err := os.Getwd()
	if err != nil {
		return err
	}
	bail := func(format string, args ...any) error {
		return harness.BailInstance(env["RESULTS_FILE"], legTitle, env["RUN_ID"], env["TARGET_SHA"],
			fmt.Sprintf(format, args...))
	}

	count, err := strconv.Atoi(countStr)
	if err != nil || count < 1 {
		return bail("invalid BENCH_COUNT %q", countStr)
	}
	if baselineRef == "" {
		return bail("BASELINE_REF unset; the coordinator resolves it from the latest release")
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(env["REGION"]))
	if err != nil {
		return bail("loading AWS config: %v", err)
	}
	s3Client := s3.NewFromConfig(awsCfg)

	baselineDir := filepath.Join(env["WORK_DIR"], "stellar-rpc-baseline")
	logger.Infof("checking out baseline %s into %s", baselineRef, baselineDir)
	baselineSHA, err := checkoutBaseline(ctx, baselineDir, env["REPO"], baselineRef)
	if err != nil {
		return bail("checking out baseline %s: %v", baselineRef, err)
	}

	for _, dir := range []string{baselineDir, repoRoot} {
		logger.Infof("building rpc libs in %s", dir)
		if err := harness.RunStreaming(ctx, dir, nil, 40, "make", "build-libs"); err != nil {
			return bail("make build-libs failed in %s: %v", dir, err)
		}
	}

	logger.Infof("running audited benchmarks (count=%d) on baseline %s", count, baselineRef)
	baselineFails := runSuite(ctx, baselineDir, baselineOut, count)
	logger.Infof("running audited benchmarks (count=%d) on candidate %s", count, env["TARGET_SHA"])
	candidateFails := runSuite(ctx, repoRoot, candidateOut, count)

	logger.Infof("comparing with benchstat")
	if err := runBenchstat(ctx, baselineOut, candidateOut, benchstatOut); err != nil {
		return bail("benchstat failed: %v", err)
	}
	benchstat, err := os.ReadFile(benchstatOut)
	if err != nil {
		return bail("reading benchstat output: %v", err)
	}

	uploaded := uploadRawLogs(ctx, s3Client, env["BUCKET"], env["RESULT_KEY"],
		map[string]string{"baseline.txt": baselineOut, "candidate.txt": candidateOut, "benchstat.txt": benchstatOut})

	var rawLogsPrefix string
	if len(uploaded) > 0 {
		rawLogsPrefix = "s3://" + env["BUCKET"] + "/" + path.Dir(env["RESULT_KEY"]) + "/"
	}
	report := benchReport{
		BaselineRef:    baselineRef,
		BaselineSHA:    baselineSHA,
		TargetSHA:      env["TARGET_SHA"],
		Count:          count,
		Benchstat:      string(benchstat),
		BaselineFails:  baselineFails,
		CandidateFails: candidateFails,
		RawLogsPrefix:  rawLogsPrefix,
		RawLogs:        uploaded,
	}
	if err := os.WriteFile(env["RESULTS_FILE"], []byte(renderMarkdown(report)), 0o644); err != nil {
		return bail("writing results markdown: %v", err)
	}
	// The shell wrapper publishes the fail result from resultsFile on non-zero exit.
	if len(candidateFails) > 0 {
		return fmt.Errorf("candidate benchmarks failed in %s", strings.Join(candidateFails, ", "))
	}

	logger.Infof("results ready; publishing verdict")
	if err := publishOK(ctx, s3Client, report, env, benchResults); err != nil {
		return bail("publishing result: %v", err)
	}
	return nil
}

// publishOK writes the bench metadata and publishes the ok result object.
func publishOK(
	ctx context.Context, client *s3.Client, r benchReport, env map[string]string, benchResults string,
) error {
	meta, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshaling bench metadata: %w", err)
	}
	if err := os.WriteFile(benchResults, meta, 0o644); err != nil {
		return fmt.Errorf("writing bench metadata: %w", err)
	}
	return harness.PublishResult(
		ctx, client, env["BUCKET"], env["RESULT_KEY"], "ok", env["RUN_ID"], r.TargetSHA, env["RESULTS_FILE"], benchResults)
}

// checkoutBaseline shallow-fetches ref (tag, branch, or SHA) from repo into dir
// and returns the checked-out SHA.
func checkoutBaseline(ctx context.Context, dir, repo, ref string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	for _, args := range [][]string{
		{"init", "-q"},
		{"remote", "add", "origin", "https://github.com/" + repo + ".git"},
		{"fetch", "--depth", "1", "origin", ref},
		{"checkout", "--detach", "FETCH_HEAD"},
	} {
		if err := harness.RunStreaming(ctx, dir, nil, 20, "git", args...); err != nil {
			return "", fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
		}
	}
	out, err := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// runSuite runs every benchmark in the module in dir except benchDenylist.
// Only stderr (tool/compile errors, low-volume) streams to the log.
// Returns the packages go test reported as failed (empty on success).
func runSuite(ctx context.Context, dir, outFile string, count int) []string {
	f, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		logger.Warnf("opening %s: %v", outFile, err)
		return []string{"<suite>"}
	}
	defer f.Close()
	cmd := exec.CommandContext(ctx, "go", "test", "-run", "^$", "-bench", ".",
		"-skip", "^("+strings.Join(benchDenylist, "|")+")$",
		"-benchmem", "-count", strconv.Itoa(count), "-timeout", "30m",
		"./...")
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = f, os.Stderr
	if err := cmd.Run(); err != nil {
		failed := parseFailedPkgs(outFile)
		if len(failed) == 0 {
			failed = []string{"<suite>"}
		}
		logger.Warnf("bench run failed in %s: %v (packages: %s)", dir, err, strings.Join(failed, ", "))
		return failed
	}
	return nil
}

// parseFailedPkgs scans a bench output file for go test's FAIL lines.
func parseFailedPkgs(outFile string) []string {
	f, err := os.Open(outFile)
	if err != nil {
		logger.Warnf("reading %s for FAIL lines: %v", outFile, err)
		return nil
	}
	defer f.Close()
	var pkgs []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		if fields := strings.Fields(sc.Text()); len(fields) >= 2 && fields[0] == "FAIL" {
			pkgs = append(pkgs, fields[1])
		}
	}
	return pkgs
}

// runBenchstat compares the two bench outputs into outFile, teeing benchstat's
// stdout there while streaming to the box log.
func runBenchstat(ctx context.Context, baselineOut, candidateOut, outFile string) error {
	f, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	cmd := exec.CommandContext(ctx, "go", "run", "golang.org/x/perf/cmd/benchstat@latest",
		filepath.Base(baselineOut), filepath.Base(candidateOut))
	cmd.Dir = filepath.Dir(baselineOut)
	cmd.Stdout, cmd.Stderr = io.MultiWriter(f, os.Stderr), os.Stderr
	return cmd.Run()
}

// uploadRawLogs best-effort copies the raw bench outputs next to the result
// object, so the comment can stay a summary. Returns the names of the files
// that actually landed sorted for a stable comment.
func uploadRawLogs(
	ctx context.Context, client *s3.Client, bucket, resultKey string, files map[string]string,
) []string {
	if resultKey == "" {
		return nil
	}
	var uploaded []string
	prefix := path.Dir(resultKey)
	for name, p := range files {
		body, err := os.ReadFile(p)
		if err != nil {
			logger.Warnf("skipping raw log upload of %s: %v", p, err)
			continue
		}
		key := prefix + "/" + name
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &bucket,
			Key:         &key,
			Body:        bytes.NewReader(body),
			ContentType: aws.String("text/plain"),
		}); err != nil {
			logger.Warnf("uploading s3://%s/%s: %v", bucket, key, err)
			continue
		}
		uploaded = append(uploaded, name)
	}
	sort.Strings(uploaded)
	return uploaded
}

// benchReport is everything the comparison markdown is rendered from.
type benchReport struct {
	BaselineRef    string   `json:"baselineRef"`
	BaselineSHA    string   `json:"baselineSha"`
	TargetSHA      string   `json:"targetSha"`
	Count          int      `json:"count"`
	BaselineFails  []string `json:"baselineFails,omitempty"`  // packages whose baseline bench run failed
	CandidateFails []string `json:"candidateFails,omitempty"` // packages whose candidate bench run failed
	Benchstat      string   `json:"-"`
	RawLogsPrefix  string   `json:"-"` // s3:// prefix holding the raw logs, "" when none uploaded
	RawLogs        []string `json:"-"` // names of the raw logs that actually uploaded
}

// renderMarkdown renders the leg's comment section: the refs compared + flags
// + any per-package failures + the benchstat output in a drop-down.
func renderMarkdown(r benchReport) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**Baseline** `%s` (`%s`) vs **candidate** `%s` — `-benchmem -count=%d`, "+
		"both refs sequentially on one box.\n",
		r.BaselineRef, r.BaselineSHA[:min(12, len(r.BaselineSHA))], r.TargetSHA[:min(12, len(r.TargetSHA))], r.Count)
	for _, pkg := range r.CandidateFails {
		fmt.Fprintf(&b, "\n❌ Candidate bench run failed in `%s`; see the box log.\n", pkg)
	}
	for _, pkg := range r.BaselineFails {
		fmt.Fprintf(&b, "\n❌ Baseline bench run failed in `%s`; its rows lack a base column.\n", pkg)
	}
	fmt.Fprintf(&b, "\n<details>\n<summary>benchstat: baseline vs candidate</summary>\n\n```\n%s\n```\n\n</details>\n",
		strings.TrimRight(r.Benchstat, "\n"))
	if len(r.RawLogs) > 0 {
		fmt.Fprintf(&b, "\nRaw benchmark logs (`%s`): %s\n", r.RawLogsPrefix, strings.Join(r.RawLogs, ", "))
	}
	return b.String()
}
