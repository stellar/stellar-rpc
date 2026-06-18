package integrationtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pelletier/go-toml"
	"github.com/stretchr/testify/require"

	rpcclient "github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

const (
	defaultLedgerBundlePath    = "./infrastructure/testdata/load-test-ledgers-v27-sac.xdr.zstd"
	defaultApplyLoadConfigPath = "./infrastructure/load-test/testdata/apply-load-v27-sac.cfg"
)

// TestIngestSyntheticLedgers replays apply-load-generated ledger bundles through
// RPC ingestion and asserts the resulting DB matches the workloads that produced
// them. Bundles are built offline by stellar-core apply-load and fetched from S3
// by the CI workflow.
//
// Requires STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true. Optional: LOADTEST_SQLITE_PATH
// (DB to ingest into; empty = fresh tmp DB), comma-separated LOADTEST_CONFIG_PATH /
// LOADTEST_INGEST_LEDGER_PATH where config i describes bundle i, and
// LOADTEST_MAX_LEDGERS_PER_FILE to cap ledgers replayed from each bundle. The
// backend replays the bundles in order, rebasing each ledger's sequence.
func TestIngestSyntheticLedgers(t *testing.T) {
	skipUnlessLoadTestSupported(t)

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

	cfgs, err := loadApplyLoadConfigs(t)
	require.NoError(t, err)
	require.Len(t, cfgs, len(ledgerPaths),
		"LOADTEST_CONFIG_PATH and LOADTEST_INGEST_LEDGER_PATH must have the same number of "+
			"comma-separated entries (config i describes bundle i)")

	prof, err := combineConfigs(cfgs)
	require.NoError(t, err)

	runIngestPhase(t, sqlitePath, ledgerPaths, prof, maxLedgersPerFile(t))
}

// runIngestPhase boots an RPC daemon ingesting from the bundles, waits for it to
// catch up to the last synthetic ledger, then verifies the range via getTransactions.
// Shutdown is left to t.Cleanup (a manual Close wouldn't run on assertion failure);
// once the bundles are exhausted ErrLoadTestDone halts ingestion while the daemon serves reads.
func runIngestPhase(t *testing.T, sqlitePath string, ledgerPaths []string, prof ingestProfile, maxPerFile uint32) {
	t.Helper()

	// The op-count expectations must track the backend's per-file cap.
	prof = prof.capPerFile(maxPerFile)

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

	i := infrastructure.NewTest(t, &infrastructure.TestConfig{
		NetworkPassphrase:      prof.networkPassphrase,
		SQLitePath:             sqlitePath,
		HistoryRetentionWindow: initialCount,
		LoadTest: config.LoadTestConfig{
			Files:             ledgerPaths,
			Frequency:         ingestFrequency(t),
			MaxLedgersPerFile: maxPerFile,
		},
	})
	startedAt := time.Now().UTC()
	client := i.GetRPCLient()

	// Synthetic ledgers append past the DB's pre-test latest (empty DB -> startSeq 1).
	startSeq := preTestLast + 1
	endSeq := startSeq + prof.totalLedgers - 1

	arrivals := waitForIngest(t, client, startSeq, endSeq)
	finishedAt := time.Now().UTC()
	// Measure from the first observed synthetic ledger to exclude the backend's
	// one-time corpus preprocessing (not part of the ingestion code under test).
	ingestDuration := arrivals[endSeq].Sub(arrivals[startSeq])
	t.Logf("Ingested %d ledgers in %s", prof.totalLedgers, ingestDuration)

	// Completeness from the DB (every synthetic ledger present, contiguous) plus a
	// sampled per-profile op check — cheaper than walking every transaction.
	verifyLedgerRange(t, sqlitePath, startSeq, endSeq, prof.totalLedgers)
	verifySampledOps(t, client, startSeq, prof.segments)

	versionInfo, err := client.GetVersionInfo(t.Context())
	require.NoError(t, err)

	emitPerfReport(t, perfReportInput{
		startedAt:          startedAt,
		finishedAt:         finishedAt,
		ledgerCount:        prof.totalLedgers,
		initialLedgers:     initialCount,
		arrivals:           arrivals,
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

// waitForIngest polls getHealth at 25ms granularity until endSeq has been
// ingested (or LOADTEST_INGEST_DEADLINE, default 30m, passes), recording the
// first time each sequence is observed for per-ledger latency stats.
func waitForIngest(t *testing.T, client *rpcclient.Client, startSeq, endSeq uint32) map[uint32]time.Time {
	t.Helper()

	ingestDeadline := 30 * time.Minute
	if v := os.Getenv("LOADTEST_INGEST_DEADLINE"); v != "" {
		var err error
		ingestDeadline, err = time.ParseDuration(v)
		require.NoError(t, err, "invalid LOADTEST_INGEST_DEADLINE")
	}
	arrivals := make(map[uint32]time.Time, endSeq-startSeq+1)
	deadline := time.After(ingestDeadline)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	latestSeen := startSeq - 1

	for {
		select {
		case <-deadline:
			t.Fatalf("RPC only ingested through ledger %d; wanted %d within %s", latestSeen, endSeq, ingestDeadline)
		case now := <-tick.C:
			health, err := client.GetHealth(t.Context())
			if err != nil {
				continue
			}
			// Arrivals are contiguous, so sequences past latestSeen are new.
			seen := min(health.LatestLedger, endSeq)
			for seq := latestSeen + 1; seq <= seen; seq++ {
				arrivals[seq] = now
			}
			latestSeen = max(latestSeen, seen)
			if health.LatestLedger >= endSeq {
				return arrivals
			}
		}
	}
}

// verifyLedgerRange asserts the DB holds exactly want synthetic ledgers across
// [startSeq, endSeq]; count==span with matching bounds confirms no gaps.
func verifyLedgerRange(t *testing.T, sqlitePath string, startSeq, endSeq, want uint32) {
	t.Helper()
	sdb, err := db.OpenSQLiteDB(sqlitePath)
	require.NoError(t, err)
	count, lo, hi, err := db.NewLedgerReader(sdb).GetLedgerCountInRange(t.Context(), startSeq, endSeq)
	require.NoError(t, sdb.Close())
	require.NoError(t, err)
	require.Equal(t, want, count, "want %d synthetic ledgers in [%d,%d], got %d", want, startSeq, endSeq, count)
	require.Equal(t, startSeq, lo, "first synthetic ledger")
	require.Equal(t, endSeq, hi, "last synthetic ledger")
}

// verifySampledOps spot-checks the per-ledger op mix at the first, middle, and
// last ledger of each profile segment, instead of walking every transaction.
func verifySampledOps(t *testing.T, client *rpcclient.Client, startSeq uint32, segments []profileSegment) {
	t.Helper()
	lo := startSeq
	for _, seg := range segments {
		hi := lo + seg.ledgers - 1
		seen := map[uint32]bool{}
		for _, seq := range []uint32{lo, lo + (seg.ledgers-1)/2, hi} {
			if seen[seq] {
				continue
			}
			seen[seq] = true
			classic, soroban, err := walkTransactionRange(t.Context(), client, seq, seq, 200)
			require.NoError(t, err, "sampling ledger %d (%s)", seq, seg.name)
			require.EqualValues(t, seg.classicPerLedger, classic, "ledger %d (%s) classic ops", seq, seg.name)
			if seg.sorobanPerLedger > 0 {
				require.EqualValues(t, seg.sorobanPerLedger, soroban, "ledger %d (%s) soroban ops", seq, seg.name)
			} else {
				require.Positive(t, soroban, "ledger %d (%s) soroban ops", seq, seg.name)
			}
		}
		lo = hi + 1
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

// skipUnlessLoadTestSupported skips unless the integration-test gate is on.
func skipUnlessLoadTestSupported(t *testing.T) {
	t.Helper()
	if os.Getenv("STELLAR_RPC_INTEGRATION_TESTS_ENABLED") != "true" {
		t.Skip("STELLAR_RPC_INTEGRATION_TESTS_ENABLED not set")
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

// getLedgerBounds returns the DB's latest ledger sequence and its ledger
// count (zero values for an empty DB).
func getLedgerBounds(ctx context.Context, sdb *db.DB) (uint32, uint32, error) {
	r, err := db.NewLedgerReader(sdb).GetLedgerRange(ctx)
	if errors.Is(err, db.ErrEmptyDB) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	return r.LastLedger.Sequence, r.LastLedger.Sequence - r.FirstLedger.Sequence + 1, nil
}

type applyLoadConfigValues struct {
	NetworkPassphrase      string `toml:"NETWORK_PASSPHRASE"`
	NumSynthetic           uint32 `toml:"APPLY_LOAD_NUM_LEDGERS"`
	NumClassicTxsPerLedger uint32 `toml:"APPLY_LOAD_CLASSIC_TXS_PER_LEDGER"`
	Mode                   string `toml:"APPLY_LOAD_MODE"`
	ModelTx                string `toml:"APPLY_LOAD_MODEL_TX"`
	MaxSorobanTxCount      uint32 `toml:"APPLY_LOAD_MAX_SOROBAN_TX_COUNT"`
	BatchSacCount          uint32 `toml:"APPLY_LOAD_BATCH_SAC_COUNT"`
}

// sorobanTxsPerLedger returns soroban tx envelopes per benchmark-mode ledger, or 0
// when not statically known. For "sac", APPLY_LOAD_BATCH_SAC_COUNT transfers share
// one envelope.
func (cfg applyLoadConfigValues) sorobanTxsPerLedger() uint32 {
	if cfg.Mode != "benchmark" || cfg.MaxSorobanTxCount == 0 {
		return 0
	}
	if cfg.ModelTx == "sac" && cfg.BatchSacCount > 1 {
		return cfg.MaxSorobanTxCount / cfg.BatchSacCount
	}
	return cfg.MaxSorobanTxCount
}

// profileSegment is one bundle's slice of the concatenated ledger stream, in
// bundle order. Per-ledger op counts are uniform within a segment;
// sorobanPerLedger == 0 means the exact soroban count isn't statically known.
type profileSegment struct {
	name             string
	ledgers          uint32
	classicPerLedger uint32
	sorobanPerLedger uint32
}

// ingestProfile is the combined expectation for an ingest run over one or more
// bundles: how many ledgers, and the per-segment op mix used to sample-verify.
type ingestProfile struct {
	networkPassphrase string
	totalLedgers      uint32
	segments          []profileSegment
}

// combineConfigs folds per-bundle configs into one ingest expectation. Configs
// must agree on the network passphrase: the daemon ingests all bundles under one network.
func combineConfigs(cfgs []loadedConfig) (ingestProfile, error) {
	if len(cfgs) == 0 {
		return ingestProfile{}, errors.New("no apply-load configs given")
	}
	prof := ingestProfile{networkPassphrase: cfgs[0].NetworkPassphrase}
	for _, cfg := range cfgs {
		if cfg.NetworkPassphrase != prof.networkPassphrase {
			return ingestProfile{}, fmt.Errorf("config network passphrases differ: %q vs %q",
				prof.networkPassphrase, cfg.NetworkPassphrase)
		}
		prof.segments = append(prof.segments, profileSegment{
			name:             cfg.name,
			ledgers:          cfg.NumSynthetic,
			classicPerLedger: cfg.NumClassicTxsPerLedger,
			sorobanPerLedger: cfg.sorobanTxsPerLedger(),
		})
	}
	return prof.capPerFile(0), nil
}

// capPerFile caps each segment to maxPerFile ledgers (0 = all), as the backend
// does, and re-totals.
func (p ingestProfile) capPerFile(maxPerFile uint32) ingestProfile {
	out := ingestProfile{networkPassphrase: p.networkPassphrase}
	for _, seg := range p.segments {
		if maxPerFile > 0 && maxPerFile < seg.ledgers {
			seg.ledgers = maxPerFile
		}
		out.totalLedgers += seg.ledgers
		out.segments = append(out.segments, seg)
	}
	return out
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

func TestCombineConfigs(t *testing.T) {
	benchmark := applyLoadConfigValues{
		NetworkPassphrase:      "Apply Load",
		NumSynthetic:           2000,
		NumClassicTxsPerLedger: 1000,
		Mode:                   "benchmark",
		ModelTx:                "sac",
		MaxSorobanTxCount:      1000,
		BatchSacCount:          1,
	}
	legacy := applyLoadConfigValues{
		NetworkPassphrase:      "Apply Load",
		NumSynthetic:           1000,
		NumClassicTxsPerLedger: 10,
		MaxSorobanTxCount:      1000, // resource-limited, not an exact per-ledger count
	}

	prof, err := combineConfigs([]loadedConfig{{benchmark, "a"}, {benchmark, "b"}})
	require.NoError(t, err)
	require.EqualValues(t, 4000, prof.totalLedgers)
	require.Equal(t, []profileSegment{
		{name: "a", ledgers: 2000, classicPerLedger: 1000, sorobanPerLedger: 1000},
		{name: "b", ledgers: 2000, classicPerLedger: 1000, sorobanPerLedger: 1000},
	}, prof.segments)

	// A non-benchmark config yields sorobanPerLedger 0 (exact count not statically known).
	prof, err = combineConfigs([]loadedConfig{{legacy, "a"}})
	require.NoError(t, err)
	require.Equal(t, profileSegment{name: "a", ledgers: 1000, classicPerLedger: 10}, prof.segments[0])

	// Batched SAC transfers share one envelope: 1000 / 10 = 100 soroban tx/ledger.
	batched := benchmark
	batched.BatchSacCount = 10
	prof, err = combineConfigs([]loadedConfig{{batched, "a"}})
	require.NoError(t, err)
	require.EqualValues(t, 100, prof.segments[0].sorobanPerLedger)

	mismatched := benchmark
	mismatched.NetworkPassphrase = "Other Network"
	_, err = combineConfigs([]loadedConfig{{benchmark, "a"}, {mismatched, "b"}})
	require.ErrorContains(t, err, "network passphrases differ")

	_, err = combineConfigs(nil)
	require.Error(t, err)
}

func TestIngestProfileCapPerFile(t *testing.T) {
	full := ingestProfile{segments: []profileSegment{
		{name: "a", ledgers: 1000, classicPerLedger: 1000, sorobanPerLedger: 100},
		{name: "b", ledgers: 500, classicPerLedger: 10, sorobanPerLedger: 0},
	}}

	require.EqualValues(t, 1500, full.capPerFile(0).totalLedgers) // 0 = no cap

	// A per-file ceiling caps each bundle independently, leaving the op mix intact.
	capped := full.capPerFile(200)
	require.EqualValues(t, 400, capped.totalLedgers) // 200 + 200
	require.Equal(t, []profileSegment{
		{name: "a", ledgers: 200, classicPerLedger: 1000, sorobanPerLedger: 100},
		{name: "b", ledgers: 200, classicPerLedger: 10, sorobanPerLedger: 0},
	}, capped.segments)

	// A ceiling above a file's size caps only the larger file: min(700,1000)+min(700,500).
	require.EqualValues(t, 1200, full.capPerFile(700).totalLedgers)
}

func TestSplitPathList(t *testing.T) {
	require.Nil(t, splitPathList(""))
	require.Equal(t, []string{"a"}, splitPathList("a"))
	require.Equal(t, []string{"a", "b"}, splitPathList(" a , b ,"))
}

func TestComputeProfilePerf(t *testing.T) {
	// Two segments of 3 ledgers starting at seq 10, one arrival every 100ms.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	arrivals := make(map[uint32]time.Time)
	for seq := uint32(10); seq <= 15; seq++ {
		arrivals[seq] = base.Add(time.Duration(seq-10) * 100 * time.Millisecond)
	}

	perf := computeProfilePerf(arrivals, 10, []profileSegment{
		{name: "a", ledgers: 3},
		{name: "b", ledgers: 3},
	})
	require.Len(t, perf, 2)

	// Segment a has no predecessor ledger: its clock starts at its own first
	// arrival (excluding preprocessing) and covers 2 ledger ingests.
	require.Equal(t, "a", perf[0].Profile)
	require.InDelta(t, 0.2, perf[0].WallClockSec, 1e-9)
	require.InDelta(t, 10.0, perf[0].LedgersPerSecond, 1e-9)
	require.InDelta(t, 100.0, perf[0].MsPerLedger, 1e-9)

	// Segment b's clock starts at segment a's last arrival and covers all 3.
	require.Equal(t, "b", perf[1].Profile)
	require.InDelta(t, 0.3, perf[1].WallClockSec, 1e-9)
	require.InDelta(t, 10.0, perf[1].LedgersPerSecond, 1e-9)
	require.InDelta(t, 100.0, perf[1].MsPerLedger, 1e-9)
	require.InDelta(t, 100.0, perf[1].PerLedgerLatencyMs.P50, 1e-9)
}

// loadedConfig is an apply-load config plus the profile name derived from its
// file name (apply-load-v27-oz.cfg -> apply-load-v27-oz).
type loadedConfig struct {
	applyLoadConfigValues

	name string
}

// loadApplyLoadConfigs loads every config in the comma-separated
// LOADTEST_CONFIG_PATH (default: defaultApplyLoadConfigPath) and
// validates each. Fails if any are missing or invalid.
func loadApplyLoadConfigs(t *testing.T) ([]loadedConfig, error) {
	t.Helper()
	cfgPaths := splitPathList(os.Getenv("LOADTEST_CONFIG_PATH"))
	if len(cfgPaths) == 0 {
		cfgPaths = []string{defaultApplyLoadConfigPath}
	}
	cfgs := make([]loadedConfig, 0, len(cfgPaths))
	for _, cfgPath := range cfgPaths {
		cfgRaw, err := os.ReadFile(cfgPath)
		if err != nil {
			return nil, err
		}
		base := filepath.Base(cfgPath)
		cfg := loadedConfig{name: strings.TrimSuffix(base, filepath.Ext(base))}
		if err := toml.Unmarshal(cfgRaw, &cfg.applyLoadConfigValues); err != nil {
			return nil, fmt.Errorf("%s: %w", cfgPath, err)
		}
		if cfg.NumSynthetic <= 0 || cfg.NumClassicTxsPerLedger <= 0 {
			return nil, fmt.Errorf(
				"invalid config %s: need APPLY_LOAD_NUM_LEDGERS, APPLY_LOAD_CLASSIC_TXS_PER_LEDGER > 0", cfgPath)
		}
		cfgs = append(cfgs, cfg)
	}
	return cfgs, nil
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
	InitialLedgerCount uint32           `json:"initialLedgerCount"`
	IngestWallClockSec float64          `json:"ingestWallClockSeconds"`
	LedgersPerSecond   float64          `json:"ledgersPerSecond"`
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

// profilePerf is the ingest measurement for one bundle's segment.
type profilePerf struct {
	Profile            string           `json:"profile"`
	Ledgers            uint32           `json:"ledgers"`
	WallClockSec       float64          `json:"wallClockSeconds"`
	LedgersPerSecond   float64          `json:"ledgersPerSecond"`
	MsPerLedger        float64          `json:"msPerLedger"`
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
	arrivals           map[uint32]time.Time
	startSeq           uint32
	segments           []profileSegment
	captiveCoreVersion string
}

// arrivalDeltas returns per-ledger latency samples (ms) for [lo, hi]:
// arrivals[seq] - arrivals[seq-1]. Sequences whose predecessor was never recorded
// are skipped, excluding the first synthetic ledger's corpus-preprocessing delta.
func arrivalDeltas(arrivals map[uint32]time.Time, lo, hi uint32) []float64 {
	deltas := make([]float64, 0, hi-lo+1)
	for seq := lo; seq <= hi; seq++ {
		prev, hasPrev := arrivals[seq-1]
		cur, hasCur := arrivals[seq]
		if hasPrev && hasCur {
			deltas = append(deltas, float64(cur.Sub(prev).Microseconds())/1000.0)
		}
	}
	return deltas
}

// computeProfilePerf slices the arrival timeline into per-bundle segments (in
// bundle order) and measures each from its per-ledger latency samples.
func computeProfilePerf(arrivals map[uint32]time.Time, startSeq uint32, segments []profileSegment) []profilePerf {
	out := make([]profilePerf, 0, len(segments))
	lo := startSeq
	for _, seg := range segments {
		hi := lo + seg.ledgers - 1
		out = append(out, perfFromDeltas(seg.name, seg.ledgers, arrivalDeltas(arrivals, lo, hi)))
		lo = hi + 1
	}
	return out
}

// perfFromDeltas summarizes per-ledger latency samples: wall-clock is their sum
// (the deltas telescope) and ms/ledger their mean.
func perfFromDeltas(name string, ledgers uint32, deltasMs []float64) profilePerf {
	var sumMs float64
	for _, d := range deltasMs {
		sumMs += d
	}
	p := profilePerf{
		Profile:            name,
		Ledgers:            ledgers,
		WallClockSec:       sumMs / 1000,
		PerLedgerLatencyMs: computeQuantiles(deltasMs),
	}
	if sumMs > 0 {
		p.MsPerLedger = sumMs / float64(len(deltasMs))
		p.LedgersPerSecond = 1000 * float64(len(deltasMs)) / sumMs
	}
	return p
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

	overallDeltas := arrivalDeltas(in.arrivals, in.startSeq, in.startSeq+in.ledgerCount-1)
	overall := perfFromDeltas("overall", in.ledgerCount, overallDeltas)
	sha := os.Getenv("PERF_TARGET_SHA")
	if len(sha) > 7 {
		sha = sha[:7]
	}
	report := perfReport{
		StartedAt:          in.startedAt.Format(time.RFC3339),
		FinishedAt:         in.finishedAt.Format(time.RFC3339),
		LedgerCount:        in.ledgerCount,
		InitialLedgerCount: in.initialLedgers,
		IngestWallClockSec: overall.WallClockSec,
		LedgersPerSecond:   overall.LedgersPerSecond,
		PerLedgerLatencyMs: overall.PerLedgerLatencyMs,
		Profiles:           computeProfilePerf(in.arrivals, in.startSeq, in.segments),
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
		"| Profile | Ledgers | Wall-clock | Ledgers/sec | ms/ledger | p50 / p95 / p99 ms |",
		"|---|---|---|---|---|---|",
	)
	for _, p := range r.Profiles {
		lines = append(lines, fmt.Sprintf("| %s | %d | %.3fs | %.2f | %.2f | %v / %v / %v |",
			p.Profile, p.Ledgers, p.WallClockSec, p.LedgersPerSecond, p.MsPerLedger,
			p.PerLedgerLatencyMs.P50, p.PerLedgerLatencyMs.P95, p.PerLedgerLatencyMs.P99))
	}
	lines = append(lines,
		"",
		"| Metric | Value |",
		"|---|---|",
		fmt.Sprintf("| Ledgers replayed | %d |", r.LedgerCount),
		fmt.Sprintf("| Initial DB ledger count | %d |", r.InitialLedgerCount),
		fmt.Sprintf("| Overall throughput | %.2f ledgers/sec |", r.LedgersPerSecond),
		fmt.Sprintf("| Overall ingest wall-clock | %.3fs |", r.IngestWallClockSec),
		fmt.Sprintf("| Per-ledger p50 / p95 / p99 | %v / %v / %v ms |",
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
