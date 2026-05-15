package integrationtest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/pelletier/go-toml"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/loadtest"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

var (
	DEFAULT_OUTPUT_LEDGER_PATH     = "./infrastructure/testdata/load-test-ledgers-v25.xdr.zstd"
	DEFAULT_OUTPUT_FIXTURES_PATH   = "./infrastructure/testdata/load-test-fixtures-v25.xdr.zstd"
	DEFAULT_APPLY_LOAD_CONFIG_PATH = "./infrastructure/testdata/apply-load.cfg" // fallback if LOADTEST_CONFIG_PATH not set
)

// TestGenerateLedgers (phase 1) generates ledgers using stellar-core's apply-load
// command and writes them to OUTPUT_LEDGER_PATH.
//
// Required env vars:
//   - STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true
//
// Optional env vars:
//   - STELLAR_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN: Path to stellar-core 25.x
//     with BUILD_TESTS enabled (default: looks for "stellar-core" in PATH)
//   - LOADTEST_CONFIG_PATH: Path to an apply-load config file
//     (default: infrastructure/testdata/apply-load.cfg)
func TestGenerateLedgers(t *testing.T) {
	skipUnlessLoadTestSupported(t)

	cfg, err := loadApplyLoadConfig(t)
	require.NoError(t, err)
	runApplyLoad(t, DEFAULT_OUTPUT_LEDGER_PATH, DEFAULT_OUTPUT_FIXTURES_PATH, cfg)
}

// TestIngestSyntheticLedgers (phase 2) replays a previously-generated synthetic ledger
// bundle through the RPC ingestion path, and asserts that the resulting DB state
// matches the workload that produced the bundle.
//
// Required env vars:
//   - STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true
//
// Optional env vars:
//   - LOADTEST_SQLITE_PATH: Path to RPC SQLite DB to ingest synthetic ledgers into.
//     If empty, uses a fresh tmp DB (the "no DB" case).
//   - LOADTEST_CONFIG_PATH: Path to an apply-load config file (default: infrastructure/testdata/apply-load.cfg).
//   - LOADTEST_INGEST_LEDGER_PATH: Path to the .xdr.zstd ledger bundle to ingest
//     (default: OUTPUT_LEDGER_PATH)
//
// Requires a generated ledger file. By default it looks for
// the checked-in DEFAULT_OUTPUT_LEDGER_PATH; override with LOADTEST_INGEST_LEDGER_PATH
// to point at a new bundle.
func TestIngestSyntheticLedgers(t *testing.T) {
	skipUnlessLoadTestSupported(t)

	ledgerPath := os.Getenv("LOADTEST_INGEST_LEDGER_PATH")
	if ledgerPath == "" {
		ledgerPath = DEFAULT_OUTPUT_LEDGER_PATH
	}
	if _, err := os.Stat(ledgerPath); err != nil {
		t.Skipf("no generated ledger file at %q; run TestGenerateLedgers or TestApplyLoadFlow first (or set LOADTEST_INGEST_LEDGER_PATH)", ledgerPath)
	}
	sqlitePath := os.Getenv("LOADTEST_SQLITE_PATH")

	cfg, err := loadApplyLoadConfig(t) // uses env var LOADTEST_CONFIG_PATH or default path
	require.NoError(t, err)

	runIngestPhase(t, sqlitePath, ledgerPath, cfg)
}

// TestApplyLoadThenIngest runs both the ledger generation phase and the ingestion
// phase using the output of the former as the input of the latter.
// Use this when you want one command to validate the whole pipeline.
func TestApplyLoadThenIngest(t *testing.T) {
	skipUnlessLoadTestSupported(t)

	dir := t.TempDir()
	ledgerPath := filepath.Join(dir, "load-test-ledgers.xdr.zstd")
	fixturesPath := filepath.Join(dir, "load-test-fixtures.xdr.zstd")
	sqlitePath := os.Getenv("LOADTEST_SQLITE_PATH")

	cfg, err := loadApplyLoadConfig(t)
	require.NoError(t, err)

	runApplyLoad(t, ledgerPath, fixturesPath, cfg)
	runIngestPhase(t, sqlitePath, ledgerPath, cfg)
}

// runApplyLoad runs stellar-core apply-load, writes the benchmark ledgers to
// ledgerPath and pre-benchmark fixtures to fixturesPath, and asserts the
// generated workload matches the apply-load.cfg profile (mixed classic +
// Soroban activity, expected ledger count).
func runApplyLoad(t *testing.T, ledgerPath, fixturesPath string, cfg applyLoadConfigValues) {
	t.Helper()

	// If "", falls back to looking for "stellar-core" in PATH
	coreBinaryPath := os.Getenv("STELLAR_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")

	configPath := os.Getenv("LOADTEST_CONFIG_PATH")
	if configPath == "" {
		configPath = "infrastructure/testdata/apply-load.cfg"
	}

	res, err := loadtest.ApplyLoad(t.Context(), loadtest.Options{
		CoreBinaryPath: coreBinaryPath,
		ConfigPath:     configPath,
		OutputPath:     ledgerPath,
		FixturesPath:   fixturesPath,
		WorkDirPath:    t.TempDir(),
		Logger:         newTestLogger(t),
	})
	require.NoError(t, err)
	require.EqualValues(t, cfg.NumSynthetic, res.CountLedgers,
		"Expected %d ledgers, got %d", cfg.NumSynthetic, res.CountLedgers)
	require.Greater(t, res.CountFixtures, 0,
		"Expected at least 1 fixture, got %d", res.CountFixtures)

	expectedClassicTxs := cfg.NumClassicTxsPerLedger * cfg.NumSynthetic
	countClassic, countSoroban := getCountTxs(t, ledgerPath)
	require.EqualValues(t, expectedClassicTxs, countClassic,
		"Expected %d classic Payment ops, got %d", expectedClassicTxs, countClassic)
	require.Greater(t, countSoroban, 0,
		"Expected at least one Soroban InvokeHostFunction op in generated ledgers")
}

// runIngestPhase boots an RPC daemon that ingests from a pre-generated
// synthetic ledger bundle, waits for ingestion to catch up to the last synthetic
// ledger, then uses getTransactions to verify the ingested range.
//
// Daemon shutdown is delegated to the harness's t.Cleanup registration.
// We don't call Close() manually because (a) it wouldn't run on assertion
// failure, and (b) after the last synthetic ledger loadtest.LedgerBackend
// returns ErrLoadTestDone and rpc's ingest service retries forever (see daemon.go:292-294).
func runIngestPhase(t *testing.T, sqlitePath, ledgerPath string, cfg applyLoadConfigValues) {
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
		NetworkPassphrase:      cfg.NetworkPassphrase,
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
	endSeq := startSeq + uint32(cfg.NumSynthetic) - 1

	// Poll getHealth at 25ms granularity, recording the first time each
	// sequence is observed for per-ledger latency stats.
	arrivals := make(map[uint32]time.Time, cfg.NumSynthetic+1)
	arrivals[startSeq-1] = startedAt // synthetic ingestion "began" at startedAt
	deadline := time.After(10 * time.Minute)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()

waitForIngest:
	for {
		select {
		case <-deadline:
			t.Fatalf("RPC never ingested through ledger %d", endSeq)
		case now := <-tick.C:
			health, err := client.GetHealth(t.Context())
			if err != nil {
				continue
			}
			seen := min(health.LatestLedger, endSeq)
			for seq := startSeq; seq <= seen; seq++ {
				if _, ok := arrivals[seq]; !ok {
					arrivals[seq] = now
				}
			}
			if health.LatestLedger >= endSeq {
				break waitForIngest
			}
		}
	}
	ingestDuration := time.Since(startedAt)
	finishedAt := time.Now().UTC()
	t.Logf("Ingested %d ledgers in %s", cfg.NumSynthetic, ingestDuration)

	// Paginate through every transaction in [startSeq, endSeq] w/ getTransactions.
	// Page size is bounded by max Tx limit of 200.
	const pageLimit uint = 200
	var (
		countClassic int
		countSoroban int
		cursor       string
	)
walk:
	for {
		req := protocol.GetTransactionsRequest{
			Format: protocol.FormatBase64,
		}
		if cursor == "" {
			req.StartLedger = startSeq
			req.Pagination = &protocol.LedgerPaginationOptions{Limit: pageLimit}
		} else {
			req.Pagination = &protocol.LedgerPaginationOptions{Cursor: cursor, Limit: pageLimit}
		}

		resp, err := client.GetTransactions(t.Context(), req)
		require.NoError(t, err)
		if len(resp.Transactions) == 0 {
			break
		}

		envs := make([]xdr.TransactionEnvelope, 0, len(resp.Transactions))
		past := false
		for _, tx := range resp.Transactions {
			if tx.Ledger > endSeq {
				past = true
				break
			}
			var env xdr.TransactionEnvelope
			require.NoError(t, xdr.SafeUnmarshalBase64(tx.EnvelopeXDR, &env))
			envs = append(envs, env)
		}
		c, s := countOpTypes(envs)
		countClassic += c
		countSoroban += s
		if past {
			break walk
		}
		if resp.Cursor == "" {
			break
		}
		cursor = resp.Cursor
	}

	expectedClassic := cfg.NumClassicTxsPerLedger * cfg.NumSynthetic
	require.EqualValues(t, expectedClassic, countClassic,
		"Expected %d classic Payment ops, got %d", expectedClassic, countClassic)
	require.Greater(t, countSoroban, 0,
		"Expected at least one Soroban InvokeHostFunction op in ingested range")

	emitPerfReport(t, perfReportInput{
		startedAt:      startedAt,
		finishedAt:     finishedAt,
		ingestDuration: ingestDuration,
		ledgerCount:    cfg.NumSynthetic,
		arrivals:       arrivals,
		startSeq:       startSeq,
		endSeq:         endSeq,
	})
}

// skipUnlessLoadTestSupported skips the test unless the integration-test
// gate is on and the local stellar-core advertises protocol 25 or higher.
func skipUnlessLoadTestSupported(t *testing.T) {
	t.Helper()
	if os.Getenv("STELLAR_RPC_INTEGRATION_TESTS_ENABLED") != "true" {
		t.Skip("STELLAR_RPC_INTEGRATION_TESTS_ENABLED not set")
	}
}

// getCountTxs walks every benchmark ledger in the .xdr.zstd file and counts
// classic Payment ops and Soroban InvokeHostFunction ops.
func getCountTxs(t *testing.T, ledgersPath string) (int, int) {
	t.Helper()

	f, err := os.Open(ledgersPath)
	require.NoError(t, err)
	defer f.Close()

	stream, err := xdr.NewZstdStream(f)
	require.NoError(t, err)
	defer stream.Close()

	var countClassic, countSoroban int
	for {
		var ledger xdr.LedgerCloseMeta
		err := stream.ReadOne(&ledger)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		c, s := countOpTypes(ledger.TransactionEnvelopes())
		countClassic += c
		countSoroban += s
	}
	return countClassic, countSoroban
}

// countOpTypes counts classic Payment and Soroban InvokeHostFunction ops
// across the given envelopes. Single source of truth for the file walker
// (getCountTxs) and the RPC walker (runIngestPhase).
func countOpTypes(envs []xdr.TransactionEnvelope) (classic, soroban int) {
	for _, env := range envs {
		for _, op := range env.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypePayment:
				classic++
			case xdr.OperationTypeInvokeHostFunction:
				soroban++
			}
		}
	}
	return
}

// Gets
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
}

// Fetches values from the config file and validates them. Fails if any are missing or invalid.
// Uses LOADTEST_CONFIG_PATH env var or defaults to DEFAULT_APPLY_LOAD_CONFIG_PATH.
func loadApplyLoadConfig(t *testing.T) (applyLoadConfigValues, error) {
	t.Helper()
	cfgPath := os.Getenv("LOADTEST_CONFIG_PATH")
	if cfgPath == "" {
		cfgPath = DEFAULT_APPLY_LOAD_CONFIG_PATH
	}
	cfgRaw, err := os.ReadFile(cfgPath)
	if err != nil {
		return applyLoadConfigValues{}, err
	}
	var cfg applyLoadConfigValues
	if err := toml.Unmarshal(cfgRaw, &cfg); err != nil {
		return cfg, err
	}
	if cfg.NumSynthetic <= 0 || cfg.NumClassicTxsPerLedger <= 0 {
		return cfg, errors.New("invalid config: need APPLY_LOAD_NUM_LEDGERS, APPLY_LOAD_CLASSIC_TXS_PER_LEDGER > 0")
	}
	return cfg, nil
}

type testWriter struct {
	test *testing.T
}

// newTestLogger pipes go-stellar-sdk log output through t.Log
func newTestLogger(t *testing.T) *log.Entry {
	t.Helper()
	logger := log.New()
	logger.SetOutput(&testWriter{test: t})
	logger.SetLevel(log.InfoLevel)
	return logger
}

func (w *testWriter) Write(p []byte) (n int, err error) {
	w.test.Log(string(p))
	return len(p), nil
}

// --- perf metrics ---------------------------------------------------
//
// When PERF_RESULTS_PATH is set, runIngestPhase writes a JSON report to that
// path summarizing the ingest workload.

type perfReport struct {
	StartedAt          string           `json:"started_at"`
	FinishedAt         string           `json:"finished_at"`
	LedgerCount        uint32           `json:"ledger_count"`
	IngestWallClockSec float64          `json:"ingest_wall_clock_seconds"`
	LedgersPerSecond   float64          `json:"ledgers_per_second"`
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
	startedAt      time.Time
	finishedAt     time.Time
	ingestDuration time.Duration
	ledgerCount    uint32
	arrivals       map[uint32]time.Time
	startSeq       uint32
	endSeq         uint32
}

// emitPerfReport writes the perf report to PERF_RESULTS_PATH if set.
func emitPerfReport(t *testing.T, in perfReportInput) {
	t.Helper()
	path := os.Getenv("PERF_RESULTS_PATH")
	if path == "" {
		return
	}

	// Compute per-ledger latency deltas: arrivals[seq] - arrivals[seq-1].
	// arrivals[startSeq-1] is the ingest-start timestamp, so the first delta
	// is "time-to-first-synthetic-ledger".
	deltas := make([]float64, 0, in.ledgerCount)
	for seq := in.startSeq; seq <= in.endSeq; seq++ {
		prev, hasPrev := in.arrivals[seq-1]
		cur, hasCur := in.arrivals[seq]
		if hasPrev && hasCur {
			deltas = append(deltas, float64(cur.Sub(prev).Microseconds())/1000.0)
		}
	}

	report := perfReport{
		StartedAt:          in.startedAt.Format(time.RFC3339),
		FinishedAt:         in.finishedAt.Format(time.RFC3339),
		LedgerCount:        in.ledgerCount,
		IngestWallClockSec: in.ingestDuration.Seconds(),
		LedgersPerSecond:   float64(in.ledgerCount) / in.ingestDuration.Seconds(),
		PerLedgerLatencyMs: computeQuantiles(deltas),
	}
	data, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	t.Logf("perf report written to %s", path)
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
