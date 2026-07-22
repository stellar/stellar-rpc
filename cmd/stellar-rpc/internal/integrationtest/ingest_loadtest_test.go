package integrationtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	rpcclient "github.com/stellar/go-stellar-sdk/clients/rpcclient"
	"github.com/stellar/go-stellar-sdk/ingest/loadtest"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

const (
	defaultLedgerBundlePath = "./infrastructure/testdata/load-test-ledgers-v27-sac.xdr.zstd"

	// applyLoadNetworkPassphrase is the network apply-load hard-overrides every bundle to.
	applyLoadNetworkPassphrase = "Apply Load"    // needed by RPC for ingest
	ingestStallTimeout         = 3 * time.Minute // rate should = ~1 ledger/sec
)

// TestIngestSyntheticLedgers replays apply-load-generated ledger bundles through RPC
// ingestion and asserts the resulting DB matches the workloads that produced them.
// Bundles are built offline by stellar-core apply-load and fetched from S3.
//
// Requires STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true. Optional: LOADTEST_SQLITE_PATH
// (DB to ingest into; empty = fresh tmp DB), comma-separated LOADTEST_INGEST_LEDGER_PATH
// (bundles, replayed in order), and LOADTEST_MAX_LEDGERS_PER_FILE to cap ledgers per bundle.
func TestIngestSyntheticLedgers(t *testing.T) {
	if os.Getenv("STELLAR_RPC_INTEGRATION_TESTS_ENABLED") != "true" {
		t.Skip("STELLAR_RPC_INTEGRATION_TESTS_ENABLED not set, skipping test")
	}

	ledgerPaths := splitPathList(os.Getenv("LOADTEST_INGEST_LEDGER_PATH"))
	if len(ledgerPaths) == 0 {
		ledgerPaths = []string{defaultLedgerBundlePath}
	}
	for _, p := range ledgerPaths {
		if _, err := os.Stat(p); err != nil {
			t.Skipf("no generated ledger bundle at %q; generate one with stellar-core apply-load "+
				"(or set LOADTEST_INGEST_LEDGER_PATH)", p)
		}
	}
	sqlitePath := os.Getenv("LOADTEST_SQLITE_PATH")

	maxPerFile := maxLedgersPerFile(t)
	segments, total := buildSegments(t, ledgerPaths, maxPerFile)
	prof := ingestProfile{networkPassphrase: applyLoadNetworkPassphrase, totalLedgers: total, segments: segments}

	runIngestPhase(t, sqlitePath, ledgerPaths, prof, maxPerFile)
}

// runIngestPhase boots an RPC daemon ingesting from the bundles, waits for it to
// catch up to the last synthetic ledger, then verifies the range via getTransactions.
func runIngestPhase(t *testing.T, sqlitePath string, ledgerPaths []string, prof ingestProfile, maxPerFile uint32) {
	t.Helper()

	// Empty path -> fresh tmp DB.
	if sqlitePath == "" {
		sqlitePath = filepath.Join(t.TempDir(), "stellar-rpc.sqlite")
	}

	// Read the pre-test bounds and close the handle immediately: holding an
	// idle second SQLite connection through the benchmark invites what-ifs.
	sdb, err := db.OpenSQLiteDB(sqlitePath)
	require.NoError(t, err)
	preTestLast, initialCount, err := getLedgerBounds(t.Context(), sdb)
	require.NoError(t, sdb.Close())
	require.NoError(t, err)
	require.True(t, initialCount == 0 || initialCount >= prof.totalLedgers,
		"seed DB has %d ledgers but the corpus is %d; retention window %d will trim "+
			"the early synthetic ledgers before verification", initialCount, prof.totalLedgers, initialCount)

	// Synthetic ledgers append past the DB's pre-test latest (empty DB -> startSeq 1).
	startSeq := preTestLast + 1
	endSeq := startSeq + prof.totalLedgers - 1

	// The daemon fires OnLedgerIngested per committed ledger and the recorder captures
	// exact per-ledger ingest durations (no polling, no quantization).
	rec := newIngestRecorder(startSeq, endSeq)
	i := infrastructure.NewTest(t, &infrastructure.TestConfig{
		NetworkPassphrase:      prof.networkPassphrase,
		SQLitePath:             sqlitePath,
		HistoryRetentionWindow: initialCount,
		IngestLoadTest: config.IngestLoadTestConfig{
			Files:             ledgerPaths,
			Frequency:         ingestFrequency(t),
			MaxLedgersPerFile: maxPerFile,
			OnLedgerIngested:  rec.onLedger,
		},
	})
	startedAt := time.Now().UTC()
	client := i.GetRPCLient()

	awaitIngest(t, rec)
	finishedAt := time.Now().UTC()
	// elapsed runs from the first to the last synthetic ledger's commit, excluding
	// the backend's one-time corpus preprocessing (not the code under test).
	elapsed := rec.elapsed()
	t.Logf("Ingested %d ledgers in %s", prof.totalLedgers, elapsed)

	verifyLedgerRange(t, sqlitePath, startSeq, endSeq, prof.totalLedgers) // check every ledger present/contiguous
	verifyOpsSample(t, client, startSeq, prof.segments)                   // sample to verify a mixed workload

	versionInfo, err := client.GetVersionInfo(t.Context())
	require.NoError(t, err)

	emitPerfReport(t, perfReportInput{
		startedAt:          startedAt,
		finishedAt:         finishedAt,
		ledgerCount:        prof.totalLedgers,
		initialLedgers:     initialCount,
		durations:          rec.snapshotDurations(),
		elapsed:            elapsed,
		startSeq:           startSeq,
		segments:           prof.segments,
		captiveCoreVersion: versionInfo.CaptiveCoreVersion,
	})
}

// ingestFrequency is the backend's ledger emit rate (LOADTEST_INGEST_FREQUENCY, default 1ms).
func ingestFrequency(t *testing.T) time.Duration {
	t.Helper()
	v := os.Getenv("LOADTEST_INGEST_FREQUENCY")
	if v == "" {
		return time.Millisecond
	}
	d, err := time.ParseDuration(v)
	require.NoError(t, err, "invalid LOADTEST_INGEST_FREQUENCY")
	return d
}

// maxLedgersPerFile is the per-bundle ceiling on replayed ledgers
// (LOADTEST_MAX_LEDGERS_PER_FILE, default 0 = every ledger in every file).
func maxLedgersPerFile(t *testing.T) uint32 {
	t.Helper()
	v := os.Getenv("LOADTEST_MAX_LEDGERS_PER_FILE")
	if v == "" {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 32)
	require.NoError(t, err, "invalid LOADTEST_MAX_LEDGERS_PER_FILE")
	return uint32(n)
}

// ingestRecorder collects exact per-ledger ingest durations from the daemon's
// OnLedgerIngested hook. It records only sequences in [startSeq, endSeq] and
// closes done once endSeq commits.
type ingestRecorder struct {
	startSeq, endSeq uint32
	done             chan struct{}

	mu             sync.Mutex
	durations      map[uint32]time.Duration
	firstAt        time.Time // first in-range commit, for elapsed
	lastAt         time.Time // endSeq commit, for elapsed
	latestSeq      uint32
	lastProgressAt time.Time // for stall detection
}

func newIngestRecorder(startSeq, endSeq uint32) *ingestRecorder {
	return &ingestRecorder{
		startSeq:  startSeq,
		endSeq:    endSeq,
		done:      make(chan struct{}),
		durations: make(map[uint32]time.Duration, endSeq-startSeq+1),
	}
}

// onLedger is the OnLedgerIngested hook. It runs on the ingest loop, so it stays
// to a map insert under a short lock. Duration is measured before the call.
func (r *ingestRecorder) onLedger(seq uint32, d time.Duration) {
	if seq < r.startSeq || seq > r.endSeq {
		return
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.durations[seq] = d
	r.latestSeq = seq
	r.lastProgressAt = now
	if r.firstAt.IsZero() {
		r.firstAt = now
	}
	if seq == r.endSeq {
		r.lastAt = now
		close(r.done)
	}
}

// elapsed is the wall-clock from the first to the last synthetic ledger's commit.
func (r *ingestRecorder) elapsed() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastAt.Sub(r.firstAt)
}

// snapshotDurations returns a copy of the per-ledger durations for offline use.
func (r *ingestRecorder) snapshotDurations() map[uint32]time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[uint32]time.Duration, len(r.durations))
	maps.Copy(out, r.durations)
	return out
}

// progress reports time since the last ledger, the latest sequence, and whether
// any ledger has been seen yet.
func (r *ingestRecorder) progress() (time.Duration, uint32, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastProgressAt.IsZero() {
		return 0, r.startSeq - 1, false
	}
	return time.Since(r.lastProgressAt), r.latestSeq, true
}

// awaitIngest blocks until the recorder sees endSeq committed. It fails fast if
// ingestion stalls (ingestStallTimeout with no progress) and gives up at
// LOADTEST_INGEST_DEADLINE (default 30m).
func awaitIngest(t *testing.T, rec *ingestRecorder) {
	t.Helper()

	ingestDeadline := 30 * time.Minute
	if v := os.Getenv("LOADTEST_INGEST_DEADLINE"); v != "" {
		var err error
		ingestDeadline, err = time.ParseDuration(v)
		require.NoError(t, err, "invalid LOADTEST_INGEST_DEADLINE")
	}
	deadline := time.After(ingestDeadline)
	check := time.NewTicker(10 * time.Second)
	defer check.Stop()

	for {
		select {
		case <-rec.done:
			return
		case <-deadline:
			_, seq, _ := rec.progress()
			t.Fatalf("RPC only ingested through ledger %d; wanted %d within %s", seq, rec.endSeq, ingestDeadline)
		case <-check.C:
			if since, seq, started := rec.progress(); started && since >= ingestStallTimeout {
				t.Fatalf("ingestion stalled at ledger %d for %s; wanted endSeq %d",
					seq, ingestStallTimeout, rec.endSeq)
			}
		}
	}
}

// verifyLedgerRange asserts the DB holds exactly expected synthetic ledgers across
// [startSeq, endSeq]; count==span with matching bounds confirms no gaps.
func verifyLedgerRange(t *testing.T, sqlitePath string, startSeq, endSeq, expected uint32) {
	t.Helper()
	sdb, err := db.OpenSQLiteDB(sqlitePath)
	require.NoError(t, err)
	count, lo, hi, err := db.NewLedgerReader(sdb).GetLedgerCountInRange(t.Context(), startSeq, endSeq)
	require.NoError(t, sdb.Close())
	require.NoError(t, err)
	require.Equal(t, expected, count, "want %d ledgers in [%d,%d], got %d", expected, startSeq, endSeq, count)
	require.Equal(t, startSeq, lo, "first synthetic ledger")
	require.Equal(t, endSeq, hi, "last synthetic ledger")
}

// verifyOpsSample walks every tx of a few ledgers per segment to confirm the corpus is a real mixed workload
// and that every sampled ledger carries ops of mixed activity.
func verifyOpsSample(t *testing.T, client *rpcclient.Client, startSeq uint32, segments []profileSegment) {
	t.Helper()
	lo := startSeq
	for _, seg := range segments {
		totalClassic, totalSoroban := 0, 0
		hi := lo + seg.ledgers - 1
		for _, seq := range []uint32{lo, lo + (seg.ledgers-1)/2, hi} {
			classic, soroban, err := walkTransactionRange(t.Context(), client, seq, seq, 200)
			require.NoError(t, err, "sampling ledger %d (%s)", seq, seg.name)
			require.Positive(t, classic+soroban, "ledger %d (%s) has no ops", seq, seg.name)
			totalClassic += classic
			totalSoroban += soroban
		}
		lo = hi + 1
		require.Positive(t, totalClassic, "no classic ops sampled across the segment %s", seg.name)
		require.Positive(t, totalSoroban, "no Soroban ops sampled across the segment %s", seg.name)
	}
}

// walkTransactionRange pages through getTransactions for ledgers in [lo, hi] and
// counts classic Payment and Soroban ops. Pages are retried so one transient RPC
// error doesn't abort a walk of thousands of pages.
func walkTransactionRange(
	ctx context.Context, client *rpcclient.Client, lo, hi uint32, pageLimit uint,
) (int, int, error) {
	const pageAttempts = 3
	var countClassic, countSoroban int
	cursor := ""
	for {
		// Empty Cursor (omitempty) covers both the first and subsequent pages.
		req := protocol.GetTransactionsRequest{
			Format:     protocol.FormatBase64,
			Pagination: &protocol.LedgerPaginationOptions{Cursor: cursor, Limit: pageLimit},
		}
		if cursor == "" {
			req.StartLedger = lo
		}

		var resp protocol.GetTransactionsResponse
		var err error
		for attempt := 1; ; attempt++ {
			resp, err = client.GetTransactions(ctx, req)
			if err == nil || attempt >= pageAttempts {
				break
			}
			select {
			case <-ctx.Done():
				return countClassic, countSoroban, ctx.Err()
			case <-time.After(time.Second):
			}
		}
		if err != nil {
			return countClassic, countSoroban, err
		}
		if len(resp.Transactions) == 0 {
			return countClassic, countSoroban, nil
		}

		for _, tx := range resp.Transactions {
			if tx.Ledger > hi {
				return countClassic, countSoroban, nil
			}
			var env xdr.TransactionEnvelope
			if err := xdr.SafeUnmarshalBase64(tx.EnvelopeXDR, &env); err != nil {
				return countClassic, countSoroban, fmt.Errorf("unmarshalling tx envelope: %w", err)
			}
			c, s := countOps(env)
			countClassic += c
			countSoroban += s
		}
		if resp.Cursor == "" {
			return countClassic, countSoroban, nil
		}
		cursor = resp.Cursor
	}
}

// countOps counts classic Payment and Soroban ops in one envelope.
func countOps(env xdr.TransactionEnvelope) (int, int) {
	var classic, soroban int
	for _, op := range env.Operations() {
		switch op.Body.Type {
		case xdr.OperationTypePayment:
			classic++
		case xdr.OperationTypeInvokeHostFunction:
			soroban++
		default:
		}
	}
	return classic, soroban
}

// getLedgerBounds returns the DB's latest ledger sequence and its ledger count.
func getLedgerBounds(ctx context.Context, sdb *db.DB) (uint32, uint32, error) {
	r, err := db.NewLedgerReader(sdb).GetLedgerRange(ctx)
	if errors.Is(err, store.ErrEmptyDB) {
		return 0, 0, nil // zero values for empty DB
	}
	return r.LastLedger.Sequence, r.LastLedger.Sequence - r.FirstLedger.Sequence + 1, err
}

// profileSegment is one bundle's slice of the concatenated ledger stream
type profileSegment struct {
	name    string // from bundle filename
	ledgers uint32 // how many ledgers this bundle contributes
}

// ingestProfile is the expectation for an ingest run over one or more bundles:
// the shared network passphrase, total ledger count, and per-bundle segments.
type ingestProfile struct {
	networkPassphrase string
	totalLedgers      uint32
	segments          []profileSegment
}

// buildSegments derives one segment per bundle straight from the corpus: the SDK
// counts each file under maxPerFile (the same cap the backend applies), so the
// per-bundle bounds and total exactly match what gets replayed.
func buildSegments(t *testing.T, ledgerPaths []string, maxPerFile uint32) ([]profileSegment, uint32) {
	t.Helper()
	counts, err := loadtest.CountLedgersPerFile(ledgerPaths, maxPerFile)
	require.NoError(t, err)
	segments := make([]profileSegment, 0, len(counts))
	var total uint32
	for _, c := range counts {
		require.NotZero(t, c.Ledgers, "bundle %s contributed no ledgers", c.Path)
		name := strings.TrimSuffix(filepath.Base(c.Path), ".xdr.zstd")
		segments = append(segments, profileSegment{name: name, ledgers: c.Ledgers})
		total += c.Ledgers
	}
	return segments, total
}

// splitPathList splits a comma-separated list, trimming whitespace and dropping empties.
func splitPathList(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func TestComputeProfilePerf(t *testing.T) {
	// Two segments of 3 ledgers starting at seq 10, with exact per-ledger ingest
	// durations: segment a at 100ms each, segment b at 200ms each.
	durations := make(map[uint32]time.Duration)
	for seq := uint32(10); seq <= 12; seq++ {
		durations[seq] = 100 * time.Millisecond
	}
	for seq := uint32(13); seq <= 15; seq++ {
		durations[seq] = 200 * time.Millisecond
	}

	perf := computeProfilePerf(durations, 10, []profileSegment{
		{name: "a", ledgers: 3},
		{name: "b", ledgers: 3},
	})
	require.Len(t, perf, 2)

	// ms/ledger is the mean of each segment's per-ledger durations; quantiles come
	// straight from the samples (no inter-arrival diffing, no quantization).
	require.Equal(t, "a", perf[0].Profile)
	require.InDelta(t, 100.0, perf[0].MsPerLedger, 1e-9)
	require.InDelta(t, 100.0, perf[0].PerLedgerLatencyMs.P50, 1e-9)
	require.InDelta(t, 100.0, perf[0].PerLedgerLatencyMs.Max, 1e-9)

	require.Equal(t, "b", perf[1].Profile)
	require.InDelta(t, 200.0, perf[1].MsPerLedger, 1e-9)
	require.InDelta(t, 200.0, perf[1].PerLedgerLatencyMs.P99, 1e-9)
}

// --- perf metrics ---------------------------------------------------
//
// When PERF_RESULTS_PATH is set, runIngestPhase writes a JSON report to that
// path summarizing the ingest workload.

type perfReport struct {
	StartedAt   string `json:"startedAt"`
	FinishedAt  string `json:"finishedAt"`
	LedgerCount uint32 `json:"ledgerCount"`
	// InitialLedgerCount is the DB's pre-corpus ledger count: ingestion cost grows
	// with DB size, so runs are only comparable at similar initial sizes.
	InitialLedgerCount uint32 `json:"initialLedgerCount"`

	IngestWallClockSec float64 `json:"ingestWallClockSeconds"` // elapsed time from first to last ledger's commit
	IngestBusySec      float64 `json:"ingestBusySeconds"`      // sum of per-ledger ingest durations
	LedgersPerSecond   float64 `json:"ledgersPerSecond"`
	// PerLedgerLatencyMs is the distribution of exact per-ledger ingest durations
	// (the daemon's ledger_ingestion_duration_seconds{type=total}), not inter-arrival.
	PerLedgerLatencyMs latencyQuantiles `json:"perLedgerLatencyMs"`
	Profiles           []profilePerf    `json:"profiles"`
	CaptiveCoreVersion string           `json:"captiveCoreVersion"`

	// Markdown-only context read from PERF_* env vars by emitPerfReport;
	// excluded from the JSON artifact.
	TargetSha       string `json:"-"`
	RunID           string `json:"-"`
	Repo            string `json:"-"`
	GoldenFetchSecs string `json:"-"`
}

// profilePerf is the per-ledger ingest cost for one bundle's segment. Throughput
// is a system-wide number (backend pacing makes per-segment rate uninformative),
// so it lives only on perfReport, not here.
type profilePerf struct {
	Profile            string           `json:"profile"`
	Ledgers            uint32           `json:"ledgers"`
	MsPerLedger        float64          `json:"msPerLedger"` // mean per-ledger ingest duration
	PerLedgerLatencyMs latencyQuantiles `json:"perLedgerLatencyMs"`
}

type latencyQuantiles struct {
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

type perfReportInput struct {
	startedAt          time.Time
	finishedAt         time.Time
	ledgerCount        uint32
	initialLedgers     uint32
	durations          map[uint32]time.Duration // exact per-ledger ingest time from the hook
	elapsed            time.Duration            // first-to-last commit, for throughput
	startSeq           uint32
	segments           []profileSegment
	captiveCoreVersion string
}

// segmentDurationsMs returns the exact per-ledger ingest durations (ms) for the
// sequences in [lo, hi].
func segmentDurationsMs(durations map[uint32]time.Duration, lo, hi uint32) []float64 {
	out := make([]float64, 0, hi-lo+1)
	for seq := lo; seq <= hi; seq++ {
		if d, ok := durations[seq]; ok {
			out = append(out, float64(d.Microseconds())/1000.0)
		}
	}
	return out
}

// computeProfilePerf slices the per-ledger durations into per-bundle segments (in
// bundle order) and summarizes each segment's ingest cost.
func computeProfilePerf(durations map[uint32]time.Duration, startSeq uint32, segments []profileSegment) []profilePerf {
	out := make([]profilePerf, 0, len(segments))
	lo := startSeq
	for _, seg := range segments {
		hi := lo + seg.ledgers - 1
		out = append(out, summarizeDurations(seg.name, seg.ledgers, segmentDurationsMs(durations, lo, hi)))
		lo = hi + 1
	}
	return out
}

// summarizeDurations builds a profilePerf from per-ledger ingest duration samples (ms).
func summarizeDurations(name string, ledgers uint32, samplesMs []float64) profilePerf {
	q := computeQuantiles(samplesMs)
	return profilePerf{
		Profile:            name,
		Ledgers:            ledgers,
		MsPerLedger:        q.Mean,
		PerLedgerLatencyMs: q,
	}
}

// emitPerfReport writes the perf report as JSON to PERF_RESULTS_PATH and as
// markdown to PERF_RESULTS_MD_PATH; either or both may be unset.
func emitPerfReport(t *testing.T, in perfReportInput) {
	t.Helper()
	jsonPath := os.Getenv("PERF_RESULTS_PATH")
	mdPath := os.Getenv("PERF_RESULTS_MD_PATH")
	if jsonPath == "" && mdPath == "" {
		return
	}

	overallSamples := segmentDurationsMs(in.durations, in.startSeq, in.startSeq+in.ledgerCount-1)
	var busyMs float64
	for _, s := range overallSamples {
		busyMs += s
	}
	var throughput float64
	if in.elapsed > 0 {
		throughput = float64(in.ledgerCount) / in.elapsed.Seconds()
	}
	sha := os.Getenv("PERF_TARGET_SHA")
	sha = sha[:min(len(sha), 7)]
	report := perfReport{
		StartedAt:          in.startedAt.Format(time.RFC3339),
		FinishedAt:         in.finishedAt.Format(time.RFC3339),
		LedgerCount:        in.ledgerCount,
		InitialLedgerCount: in.initialLedgers,
		IngestWallClockSec: in.elapsed.Seconds(),
		IngestBusySec:      busyMs / 1000,
		LedgersPerSecond:   throughput,
		PerLedgerLatencyMs: computeQuantiles(overallSamples),
		Profiles:           computeProfilePerf(in.durations, in.startSeq, in.segments),
		CaptiveCoreVersion: in.captiveCoreVersion,
		TargetSha:          sha,
		RunID:              os.Getenv("PERF_RUN_ID"),
		Repo:               os.Getenv("PERF_REPO"),
		GoldenFetchSecs:    os.Getenv("PERF_GOLDEN_FETCH_SECONDS"),
	}

	if jsonPath != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(jsonPath, data, 0o644))
		t.Logf("perf report written to %s", jsonPath)
	}
	if mdPath != "" {
		require.NoError(t, os.WriteFile(mdPath, []byte(renderPerfMarkdown(report)), 0o644))
		t.Logf("perf markdown written to %s", mdPath)
	}
}

// renderPerfMarkdown returns the PR-comment table for an ingest run; missing
// context fields render as empty cells.
func renderPerfMarkdown(r perfReport) string {
	lines := make([]string, 0, 4+len(r.Profiles)+12)
	lines = append(lines,
		fmt.Sprintf("### 📈 Ingest load test — `%s`", r.TargetSha),
		"",
		"| Profile | Ledgers | ms/ledger | p50 / p95 / p99 ms | max ms |",
		"|---|---|---|---|---|",
	)
	for _, p := range r.Profiles {
		lines = append(lines, fmt.Sprintf("| %s | %d | %.3f | %.3f / %.3f / %.3f | %.3f |",
			p.Profile, p.Ledgers, p.MsPerLedger,
			p.PerLedgerLatencyMs.P50, p.PerLedgerLatencyMs.P95, p.PerLedgerLatencyMs.P99,
			p.PerLedgerLatencyMs.Max))
	}
	util := 0.0
	if r.IngestWallClockSec > 0 {
		util = 100 * r.IngestBusySec / r.IngestWallClockSec
	}
	lines = append(lines,
		"",
		"| Metric | Value |",
		"|---|---|",
		fmt.Sprintf("| Ledgers replayed | %d |", r.LedgerCount),
		fmt.Sprintf("| Initial DB ledger count | %d |", r.InitialLedgerCount),
		fmt.Sprintf("| Throughput | %.2f ledgers/sec |", r.LedgersPerSecond),
		fmt.Sprintf("| Elapsed wall-clock | %.3fs |", r.IngestWallClockSec),
		fmt.Sprintf("| Ingest busy-time | %.3fs (%.1f%% utilization) |", r.IngestBusySec, util),
		fmt.Sprintf("| Per-ledger p50 / p95 / p99 | %.3f / %.3f / %.3f ms |",
			r.PerLedgerLatencyMs.P50, r.PerLedgerLatencyMs.P95, r.PerLedgerLatencyMs.P99),
		fmt.Sprintf("| Golden DB fetch+decompress | %ss |", r.GoldenFetchSecs),
		fmt.Sprintf("| stellar-core | `%s` |", r.CaptiveCoreVersion),
		fmt.Sprintf("| Workflow run | [#%s](https://github.com/%s/actions/runs/%s) |", r.RunID, r.Repo, r.RunID),
		"",
	)
	return strings.Join(lines, "\n")
}

func computeQuantiles(samplesMs []float64) latencyQuantiles {
	if len(samplesMs) == 0 {
		return latencyQuantiles{}
	}
	sorted := make([]float64, len(samplesMs))
	copy(sorted, samplesMs)
	sort.Float64s(sorted)
	at := func(p float64) float64 {
		idx := int(p * float64(len(sorted)-1))
		return sorted[idx]
	}
	var sum float64
	for _, v := range sorted {
		sum += v
	}
	return latencyQuantiles{
		P50:  at(0.50),
		P95:  at(0.95),
		P99:  at(0.99),
		Min:  sorted[0],
		Max:  sorted[len(sorted)-1],
		Mean: sum / float64(len(sorted)),
	}
}
