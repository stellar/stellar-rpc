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

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/harness"
)

// legDir is this leg's path under the repo root, where its config template is
// checked in (the runner runs with cwd = repo root).
const legDir = "cmd/stellar-rpc/internal/integrationtest/infrastructure/perf-eval/backfill-test"

// backfillDoneRe matches the terminal line emitted on backfill's completion
var backfillDoneRe = regexp.MustCompile(`Backfill process complete, ledgers \[(\d+) -> (\d+)\]`)

// instantiate is the instance's backfill task: it fetches + builds test fixtures,
// runs a timed backfill, then publishes the verdict.
func instantiate(ctx context.Context) error {
	leg, err := harness.LegSetup(ctx, "Backfill ingestion")
	if err != nil {
		return err
	}
	var (
		// ~1/2 day for cheap test runs; the full week is 120960.
		retention = harness.Env("HISTORY_RETENTION_WINDOW", "8640")
		// deliberate perf bound, not lifecycle plumbing: a backfill slower
		// than this is a regression worth failing on
		deadline = harness.Env("BACKFILL_DEADLINE", "4h")

		binaryPath = filepath.Join(leg.WorkDir, "stellar-rpc-bin") // built here (the repo checkout is in WORK_DIR)
	)

	fixtures, err := prepareFixtures(ctx, leg, binaryPath)
	if err != nil {
		return leg.Bail("%v", err)
	}

	// the chained peer drives this box over the VPC, hence the non-loopback bind
	serveAfter := os.Getenv("SERVE_AFTER_BACKFILL") == "true"
	endpoint := "localhost:" + rpcPort
	if serveAfter {
		endpoint = "0.0.0.0:" + rpcPort
	}

	cfgPath, err := renderConfig(leg, fixtures, retention, endpoint)
	if err != nil {
		return leg.Bail("rendering config: %v", err)
	}

	dl, err := time.ParseDuration(deadline)
	if err != nil {
		return leg.Bail("invalid BACKFILL_DEADLINE %q: %v", deadline, err)
	}
	logger.Infof("starting backfill (retention=%s, deadline=%s, serve-after=%t)", retention, deadline, serveAfter)
	elapsed, lo, hi, daemon, err := runBackfill(ctx, dl, binaryPath, cfgPath, serveAfter)
	if err != nil {
		return leg.Bail("%v", err)
	}
	if daemon != nil {
		defer daemon.Stop() // covers the bail paths below; Stop is idempotent
	}
	ingested := hi - lo + 1
	logger.Infof("backfill complete: %d ledgers [%d -> %d] in %s", ingested, lo, hi, elapsed.Round(time.Second))

	md := renderMarkdown(leg.TargetSHA, retention, lo, hi, ingested, elapsed)
	if err := os.WriteFile(leg.ResultsFile, []byte(md), 0o644); err != nil {
		return leg.Bail("writing results: %v", err)
	}
	if err := leg.Publish(ctx, ""); err != nil {
		return leg.Bail("publishing result: %v", err)
	}
	if daemon != nil {
		servePhase(ctx, leg, daemon)
	}
	return nil
}

// fixtures are the on-box artifacts the daemon run needs.
type fixtures struct {
	corePath string // stellar-core binary (fetched from S3)
	coreCfg  string // captive-core pubnet config (from the SDK)
}

// prepareFixtures fetches stellar-core, builds stellar-rpc into binaryPath,
// and writes the SDK's captive-core pubnet config.
func prepareFixtures(ctx context.Context, leg *harness.Leg, binaryPath string) (fixtures, error) {
	corePath, err := leg.Fetch.FetchStellarCore(ctx)
	if err != nil {
		return fixtures{}, err
	}

	logger.Infof("building stellar-rpc")
	if err := harness.RunStreaming(ctx, leg.RepoRoot, nil, 40, "make", "build-libs"); err != nil {
		return fixtures{}, fmt.Errorf("make build-libs failed: %w", err)
	}
	if err := harness.RunStreaming(ctx, leg.RepoRoot, nil, 40,
		"go", "build", "-o", binaryPath, "./cmd/stellar-rpc"); err != nil {
		return fixtures{}, fmt.Errorf("go build failed: %w", err)
	}

	coreCfg := filepath.Join(leg.WorkDir, "captive-core-pubnet.cfg")
	if err := os.WriteFile(coreCfg, ledgerbackend.PubnetDefaultConfig, 0o644); err != nil {
		return fixtures{}, fmt.Errorf("writing captive-core config: %w", err)
	}
	return fixtures{corePath: corePath, coreCfg: coreCfg}, nil
}

// renderConfig fills the config template's ${...} placeholders (box paths, the
// retention window, and the bind endpoint) via os.Expand
func renderConfig(leg *harness.Leg, f fixtures, retention, endpoint string) (string, error) {
	tmpl, err := os.ReadFile(filepath.Join(leg.RepoRoot, legDir, "testdata", "backfill-pubnet.toml.tmpl"))
	if err != nil {
		return "", err
	}
	workDir := leg.WorkDir
	mapping := func(in string) string {
		switch in {
		case "CAPTIVE_CORE_CONFIG_PATH":
			return f.coreCfg
		case "CAPTIVE_CORE_STORAGE_PATH":
			return filepath.Join(workDir, "core-storage")
		case "DB_PATH":
			return filepath.Join(workDir, "backfill.sqlite")
		case "STELLAR_CORE_BINARY_PATH":
			return f.corePath
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

	if elapsed == 0 { // daemon died or the watchdog killed it
		cancel()
		pr.Close()
		_ = cmd.Wait()
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
	shortSHA := sha[:min(12, len(sha))]
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
