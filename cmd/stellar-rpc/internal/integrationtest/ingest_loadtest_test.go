package integrationtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pelletier/go-toml"
	"github.com/stretchr/testify/require"

	rpcclient "github.com/stellar/go-stellar-sdk/clients/rpcclient"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

var (
	DEFAULT_OUTPUT_LEDGER_PATH     = "./infrastructure/testdata/load-test-ledgers-v25.xdr.zstd"
	DEFAULT_APPLY_LOAD_CONFIG_PATH = "./infrastructure/load-test/testdata/apply-load.cfg" // fallback if LOADTEST_CONFIG_PATH not set
)

// TestIngestSyntheticLedgers replays previously-generated synthetic ledger
// bundles through the RPC ingestion path, and asserts that the resulting DB state
// matches the workloads that produced the bundles. Bundles are produced offline
// by stellar-core's apply-load (see infrastructure/load-test/) and fetched from
// S3 by the CI workflow.
//
// Required env vars:
//   - STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true
//
// Optional env vars:
//   - LOADTEST_SQLITE_PATH: Path to RPC SQLite DB to ingest synthetic ledgers into.
//     If empty, uses a fresh tmp DB (the "no DB" case).
//   - LOADTEST_CONFIG_PATH: Comma-separated apply-load config file paths
//     (default: infrastructure/load-test/testdata/apply-load.cfg).
//   - LOADTEST_INGEST_LEDGER_PATH: Comma-separated .xdr.zstd ledger bundle paths
//     to ingest (default: OUTPUT_LEDGER_PATH). Config i must describe bundle i.
//
// Multiple bundles are byte-concatenated and ingested as one continuous ledger
// stream: loadtest.LedgerBackend rewrites every ledger's sequence to its
// position in the requested range (the rebase is computed per ledger), so the
// per-bundle sequence resets at the concatenation seams are harmless.
//
// Requires generated ledger file(s). By default it looks for
// the checked-in DEFAULT_OUTPUT_LEDGER_PATH; override with LOADTEST_INGEST_LEDGER_PATH
// to point at new bundles.
func TestIngestSyntheticLedgers(t *testing.T) {
	skipUnlessLoadTestSupported(t)

	ledgerPaths := splitPathList(os.Getenv("LOADTEST_INGEST_LEDGER_PATH"))
	if len(ledgerPaths) == 0 {
		ledgerPaths = []string{DEFAULT_OUTPUT_LEDGER_PATH}
	}
	for _, p := range ledgerPaths {
		if _, err := os.Stat(p); err != nil {
			t.Skipf("no generated ledger bundle at %q; generate one with stellar-core apply-load (or set LOADTEST_INGEST_LEDGER_PATH)", p)
		}
	}
	sqlitePath := os.Getenv("LOADTEST_SQLITE_PATH")

	cfgs, err := loadApplyLoadConfigs(t) // uses env var LOADTEST_CONFIG_PATH or default path
	require.NoError(t, err)
	require.Len(t, cfgs, len(ledgerPaths),
		"LOADTEST_CONFIG_PATH and LOADTEST_INGEST_LEDGER_PATH must have the same number of comma-separated entries (config i describes bundle i)")

	prof, err := combineConfigs(cfgs)
	require.NoError(t, err)

	runIngestPhase(t, sqlitePath, combineBundles(t, ledgerPaths), prof)
}

// runIngestPhase boots an RPC daemon that ingests from a pre-generated
// synthetic ledger bundle, waits for ingestion to catch up to the last synthetic
// ledger, then uses getTransactions to verify the ingested range.
//
// Daemon shutdown is delegated to the harness's t.Cleanup registration.
// We don't call Close() manually because (a) it wouldn't run on assertion
// failure, and (b) after the last synthetic ledger loadtest.LedgerBackend
// returns ErrLoadTestDone and rpc's ingest service retries forever (see daemon.go:292-294).
func runIngestPhase(t *testing.T, sqlitePath, ledgerPath string, prof ingestProfile) {
	t.Helper()

	// Empty path -> fresh tmp DB (covers the "no DB" case).
	if sqlitePath == "" {
		sqlitePath = filepath.Join(t.TempDir(), "stellar-rpc.sqlite")
	}

	sdb, err := db.OpenSQLiteDB(sqlitePath)
	require.NoError(t, err)
	defer sdb.Close()

	preTestBounds, initialCount, err := getLedgerBounds(t.Context(), sdb)
	require.NoError(t, err)

	i := infrastructure.NewTest(t, &infrastructure.TestConfig{
		NetworkPassphrase:      prof.networkPassphrase,
		SQLitePath:             sqlitePath,
		HistoryRetentionWindow: initialCount,
		LoadTest: config.LoadTestConfig{
			File:      ledgerPath,
			Frequency: 1 * time.Millisecond, // frequency with which we emit ledgers for ingestion
		},
	})
	startedAt := time.Now().UTC()
	client := i.GetRPCLient()

	// Synthetic ledgers append past the DB's pre-test latest. For an empty
	// DB preTestBounds.Last == 0 -> startSeq == 1.
	startSeq := preTestBounds.Last + 1
	endSeq := startSeq + prof.totalLedgers - 1

	arrivals := waitForIngest(t, client, startSeq, endSeq)
	finishedAt := time.Now().UTC()
	// Measure from the first observed synthetic ledger: the loadtest backend
	// preprocesses the whole corpus before serving ledger 1, and that cost
	// (a function of corpus size, not of the ingestion code under test) must
	// not dilute the throughput numbers.
	ingestDuration := arrivals[endSeq].Sub(arrivals[startSeq])
	t.Logf("Ingested %d ledgers in %s", prof.totalLedgers, ingestDuration)

	countClassic, countSoroban := countIngestedOps(t, client, startSeq, endSeq)
	require.EqualValues(t, prof.expectedClassic, countClassic,
		"Expected %d classic Payment ops, got %d", prof.expectedClassic, countClassic)
	if prof.expectedSoroban > 0 {
		require.EqualValues(t, prof.expectedSoroban, countSoroban,
			"Expected %d Soroban InvokeHostFunction ops, got %d", prof.expectedSoroban, countSoroban)
	} else {
		require.Greater(t, countSoroban, 0,
			"Expected at least one Soroban InvokeHostFunction op in ingested range")
	}

	versionInfo, err := client.GetVersionInfo(t.Context())
	require.NoError(t, err)

	emitPerfReport(t, perfReportInput{
		startedAt:          startedAt,
		finishedAt:         finishedAt,
		ingestDuration:     ingestDuration,
		ledgerCount:        prof.totalLedgers,
		initialLedgers:     initialCount,
		arrivals:           arrivals,
		startSeq:           startSeq,
		endSeq:             endSeq,
		segments:           prof.segments,
		captiveCoreVersion: versionInfo.CaptiveCoreVersion,
	})
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
			// Arrivals are recorded contiguously, so exactly the sequences
			// past latestSeen are new.
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

// countIngestedOps verifies the ingested range by paginating every transaction
// through getTransactions, returning the total classic Payment and Soroban
// InvokeHostFunction op counts. At millions of txs and 200 txs/page a single
// cursor chain takes hours, so the range is split across parallel walkers.
func countIngestedOps(t *testing.T, client *rpcclient.Client, startSeq, endSeq uint32) (int, int) {
	t.Helper()

	const pageLimit uint = 200
	const walkers = 8
	var (
		mu           sync.Mutex
		countClassic int
		countSoroban int
		walkErrs     []error
	)
	span := (endSeq - startSeq + walkers) / walkers
	var wg sync.WaitGroup
	for w := uint32(0); w < walkers; w++ {
		lo := startSeq + w*span
		if lo > endSeq {
			break
		}
		hi := min(lo+span-1, endSeq)
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, s, err := walkTransactionRange(t.Context(), client, lo, hi, pageLimit)
			mu.Lock()
			defer mu.Unlock()
			countClassic += c
			countSoroban += s
			if err != nil {
				walkErrs = append(walkErrs, fmt.Errorf("walker [%d..%d]: %w", lo, hi, err))
			}
		}()
	}
	wg.Wait()
	require.Empty(t, walkErrs)
	return countClassic, countSoroban
}

// walkTransactionRange pages through getTransactions for ledgers in [lo, hi]
// and counts classic Payment and Soroban InvokeHostFunction ops. Each page is
// retried a few times so one transient RPC error doesn't abort a walk of
// thousands of pages.
func walkTransactionRange(
	ctx context.Context, client *rpcclient.Client, lo, hi uint32, pageLimit uint,
) (int, int, error) {
	const pageAttempts = 3
	var countClassic, countSoroban int
	cursor := ""
	for {
		// An empty Cursor marshals identically to an absent one (omitempty),
		// so one request shape covers both the first and subsequent pages.
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

// skipUnlessLoadTestSupported skips the test unless the integration-test
// gate is on and the local stellar-core advertises protocol 25 or higher.
func skipUnlessLoadTestSupported(t *testing.T) {
	t.Helper()
	if os.Getenv("STELLAR_RPC_INTEGRATION_TESTS_ENABLED") != "true" {
		t.Skip("STELLAR_RPC_INTEGRATION_TESTS_ENABLED not set")
	}
}

// countOps counts classic Payment and Soroban InvokeHostFunction ops in one
// envelope. Single source of truth for the file walker (getCountTxs) and the
// RPC walker (walkTransactionRange).
func countOps(env xdr.TransactionEnvelope) (classic, soroban int) {
	for _, op := range env.Operations() {
		switch op.Body.Type {
		case xdr.OperationTypePayment:
			classic++
		case xdr.OperationTypeInvokeHostFunction:
			soroban++
		}
	}
	return
}

// getLedgerBounds returns the DB's ledger range and its ledger count
// (zero values for an empty DB).
func getLedgerBounds(ctx context.Context, sdb *db.DB) (db.LedgerSeqRange, uint32, error) {
	r, err := db.NewLedgerReader(sdb).GetLedgerRange(ctx)
	if errors.Is(err, db.ErrEmptyDB) {
		return db.LedgerSeqRange{}, 0, nil
	}
	if err != nil {
		return db.LedgerSeqRange{}, 0, err
	}
	ledgerRange := db.LedgerSeqRange{First: r.FirstLedger.Sequence, Last: r.LastLedger.Sequence}
	return ledgerRange, ledgerRange.Last - ledgerRange.First + 1, nil
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

// sorobanTxsPerLedger returns the exact number of Soroban tx envelopes
// apply-load packs into each benchmark-mode ledger, or 0 if the count is not
// statically known from the config. Outside benchmark mode the generated
// soroban load is shaped by resource limits rather than an exact count, so
// only benchmark mode yields a usable expectation. For the "sac" model tx,
// APPLY_LOAD_BATCH_SAC_COUNT transfers are batched into a single envelope.
func (cfg applyLoadConfigValues) sorobanTxsPerLedger() uint32 {
	if cfg.Mode != "benchmark" || cfg.MaxSorobanTxCount == 0 {
		return 0
	}
	if cfg.ModelTx == "sac" && cfg.BatchSacCount > 1 {
		return cfg.MaxSorobanTxCount / cfg.BatchSacCount
	}
	return cfg.MaxSorobanTxCount
}

// profileSegment is one bundle's slice of the concatenated ledger stream:
// segment i covers the i-th block of ledgers, in bundle order.
type profileSegment struct {
	name    string
	ledgers uint32
}

// ingestProfile is the combined expectation for an ingest run over one or
// more concatenated bundles.
type ingestProfile struct {
	networkPassphrase string
	totalLedgers      uint32
	expectedClassic   uint64
	// expectedSoroban == 0 means "exact count unknown, assert > 0 only"
	// (legacy non-benchmark configs don't pin their soroban tx count).
	expectedSoroban uint64
	segments        []profileSegment
}

// combineConfigs folds per-bundle configs into one ingest expectation. All
// configs must agree on the network passphrase since the daemon ingests the
// concatenated bundles under a single network.
func combineConfigs(cfgs []loadedConfig) (ingestProfile, error) {
	if len(cfgs) == 0 {
		return ingestProfile{}, errors.New("no apply-load configs given")
	}
	prof := ingestProfile{networkPassphrase: cfgs[0].NetworkPassphrase}
	sorobanKnown := true
	for _, cfg := range cfgs {
		if cfg.NetworkPassphrase != prof.networkPassphrase {
			return ingestProfile{}, fmt.Errorf("config network passphrases differ: %q vs %q",
				prof.networkPassphrase, cfg.NetworkPassphrase)
		}
		prof.totalLedgers += cfg.NumSynthetic
		prof.expectedClassic += uint64(cfg.NumClassicTxsPerLedger) * uint64(cfg.NumSynthetic)
		if perLedger := cfg.sorobanTxsPerLedger(); perLedger > 0 {
			prof.expectedSoroban += uint64(perLedger) * uint64(cfg.NumSynthetic)
		} else {
			sorobanKnown = false
		}
		prof.segments = append(prof.segments, profileSegment{name: cfg.name, ledgers: cfg.NumSynthetic})
	}
	if !sorobanKnown {
		prof.expectedSoroban = 0
	}
	return prof, nil
}

// splitPathList splits a comma-separated path list, trimming whitespace and
// dropping empty entries.
func splitPathList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// combineBundles returns a single .xdr.zstd path holding all the given
// bundles in order. Zstd streams are valid when byte-concatenated (the
// decoder reads frames back-to-back), so this is a plain byte copy.
func combineBundles(t *testing.T, paths []string) string {
	t.Helper()
	if len(paths) == 1 {
		return paths[0]
	}
	combinedPath := filepath.Join(t.TempDir(), "combined.xdr.zstd")
	combined, err := os.Create(combinedPath)
	require.NoError(t, err)
	defer combined.Close()
	for _, p := range paths {
		f, err := os.Open(p)
		require.NoError(t, err)
		_, err = io.Copy(combined, f)
		f.Close()
		require.NoError(t, err)
	}
	require.NoError(t, combined.Sync())
	return combinedPath
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
	require.EqualValues(t, 4_000_000, prof.expectedClassic)
	require.EqualValues(t, 4_000_000, prof.expectedSoroban)
	require.Equal(t, []profileSegment{{name: "a", ledgers: 2000}, {name: "b", ledgers: 2000}}, prof.segments)

	// A non-benchmark config in the mix means the exact soroban total is unknown.
	prof, err = combineConfigs([]loadedConfig{{benchmark, "a"}, {legacy, "b"}})
	require.NoError(t, err)
	require.EqualValues(t, 3000, prof.totalLedgers)
	require.EqualValues(t, 2_010_000, prof.expectedClassic)
	require.Zero(t, prof.expectedSoroban)

	// Batched SAC transfers share one envelope.
	batched := benchmark
	batched.BatchSacCount = 10
	prof, err = combineConfigs([]loadedConfig{{batched, "a"}})
	require.NoError(t, err)
	require.EqualValues(t, 200_000, prof.expectedSoroban)

	mismatched := benchmark
	mismatched.NetworkPassphrase = "Other Network"
	_, err = combineConfigs([]loadedConfig{{benchmark, "a"}, {mismatched, "b"}})
	require.ErrorContains(t, err, "network passphrases differ")

	_, err = combineConfigs(nil)
	require.Error(t, err)
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
// LOADTEST_CONFIG_PATH (default: DEFAULT_APPLY_LOAD_CONFIG_PATH) and
// validates each. Fails if any are missing or invalid.
func loadApplyLoadConfigs(t *testing.T) ([]loadedConfig, error) {
	t.Helper()
	cfgPaths := splitPathList(os.Getenv("LOADTEST_CONFIG_PATH"))
	if len(cfgPaths) == 0 {
		cfgPaths = []string{DEFAULT_APPLY_LOAD_CONFIG_PATH}
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
			return nil, fmt.Errorf("invalid config %s: need APPLY_LOAD_NUM_LEDGERS, APPLY_LOAD_CLASSIC_TXS_PER_LEDGER > 0", cfgPath)
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
	StartedAt   string `json:"started_at"`
	FinishedAt  string `json:"finished_at"`
	LedgerCount uint32 `json:"ledger_count"`
	// InitialLedgerCount is how many ledgers the DB already held before the
	// synthetic corpus: ingestion cost grows with DB size (bigger indexes,
	// bigger retention trims), so runs are only comparable at similar sizes.
	InitialLedgerCount uint32           `json:"initial_ledger_count"`
	IngestWallClockSec float64          `json:"ingest_wall_clock_seconds"`
	LedgersPerSecond   float64          `json:"ledgers_per_second"`
	PerLedgerLatencyMs latencyQuantiles `json:"per_ledger_latency_ms"`
	Profiles           []profilePerf    `json:"profiles"`
	CaptiveCoreVersion string           `json:"captive_core_version"`

	// Markdown-only context read from PERF_* env vars by emitPerfReport;
	// excluded from the JSON artifact.
	TargetSha       string `json:"-"`
	RunID           string `json:"-"`
	Repo            string `json:"-"`
	GoldenFetchSecs string `json:"-"`
}

// profilePerf is the ingest measurement for one bundle's segment of the
// concatenated stream: "RPC ingests profile N in X ms".
type profilePerf struct {
	Profile            string           `json:"profile"`
	Ledgers            uint32           `json:"ledgers"`
	WallClockSec       float64          `json:"wall_clock_seconds"`
	LedgersPerSecond   float64          `json:"ledgers_per_second"`
	MsPerLedger        float64          `json:"ms_per_ledger"`
	PerLedgerLatencyMs latencyQuantiles `json:"per_ledger_latency_ms"`
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
	ingestDuration     time.Duration
	ledgerCount        uint32
	initialLedgers     uint32
	arrivals           map[uint32]time.Time
	startSeq           uint32
	endSeq             uint32
	segments           []profileSegment
	captiveCoreVersion string
}

// arrivalDeltas returns per-ledger latency samples (ms) for ledgers in
// [lo, hi]: arrivals[seq] - arrivals[seq-1], skipping seq's whose predecessor
// was never recorded (i.e. the very first synthetic ledger, whose delta would
// otherwise measure the backend's corpus preprocessing).
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

// computeProfilePerf slices the arrival timeline into per-bundle segments
// (segment i covers the i-th block of ledgers, in bundle order) and measures
// each segment's ingest wall-clock and rate. A segment's clock starts at the
// arrival of the ledger just before it; the first segment starts at its own
// first arrival, excluding the backend's corpus preprocessing, and so counts
// one fewer ledger in its rate.
func computeProfilePerf(arrivals map[uint32]time.Time, startSeq uint32, segments []profileSegment) []profilePerf {
	out := make([]profilePerf, 0, len(segments))
	lo := startSeq
	for _, seg := range segments {
		hi := lo + seg.ledgers - 1
		base, ok := arrivals[lo-1]
		counted := seg.ledgers
		if !ok {
			base, counted = arrivals[lo], seg.ledgers-1
		}
		dur := arrivals[hi].Sub(base)
		p := profilePerf{
			Profile:            seg.name,
			Ledgers:            seg.ledgers,
			WallClockSec:       dur.Seconds(),
			PerLedgerLatencyMs: computeQuantiles(arrivalDeltas(arrivals, lo, hi)),
		}
		if dur > 0 && counted > 0 {
			p.LedgersPerSecond = float64(counted) / dur.Seconds()
			p.MsPerLedger = dur.Seconds() * 1000 / float64(counted)
		}
		out = append(out, p)
		lo = hi + 1
	}
	return out
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

	// Overall rate spans first arrival -> last arrival, so it covers
	// ledgerCount-1 ledger ingests and excludes corpus preprocessing.
	ledgersPerSecond := 0.0
	if in.ingestDuration > 0 && in.ledgerCount > 1 {
		ledgersPerSecond = float64(in.ledgerCount-1) / in.ingestDuration.Seconds()
	}
	sha := os.Getenv("PERF_TARGET_SHA")
	if len(sha) > 7 {
		sha = sha[:7]
	}
	report := perfReport{
		StartedAt:          in.startedAt.Format(time.RFC3339),
		FinishedAt:         in.finishedAt.Format(time.RFC3339),
		LedgerCount:        in.ledgerCount,
		InitialLedgerCount: in.initialLedgers,
		IngestWallClockSec: in.ingestDuration.Seconds(),
		LedgersPerSecond:   ledgersPerSecond,
		PerLedgerLatencyMs: computeQuantiles(arrivalDeltas(in.arrivals, in.startSeq, in.endSeq)),
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
	lines := []string{
		fmt.Sprintf("### 📈 Ingest load test — `%s`", r.TargetSha),
		"",
		"| Profile | Ledgers | Wall-clock | Ledgers/sec | ms/ledger | p50 / p95 / p99 ms |",
		"|---|---|---|---|---|---|",
	}
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
