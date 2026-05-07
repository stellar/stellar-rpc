package integrationtest

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/loadtest"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

var (
	LOADTEST_NETWORK_PASSPHRASE              = "Standalone Network ; February 2017"
	OUTPUT_LEDGER_PATH                       = "./infrastructure/testdata/load-test-ledgers-v25.xdr.zstd"
	OUTPUT_FIXTURES_PATH                     = "./infrastructure/testdata/load-test-fixtures-v25.xdr.zstd"
	LOADTEST_EXPECTED_NUM_LEDGERS            = 30
	LOADTEST_EXPECTED_CLASSIC_TXS_PER_LEDGER = 10
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
//   - LOADTEST_CORE_CONFIG_PATH: Path to a custom apply-load config file
//     (default: infrastructure/testdata/apply-load.cfg)
func TestGenerateLedgers(t *testing.T) {
	skipUnlessLoadTestSupported(t)
	runApplyLoad(t, OUTPUT_LEDGER_PATH, OUTPUT_FIXTURES_PATH)
}

// TestIngestSyntheticLedgers (phase 2) replays a previously-generated synthetic ledger
// bundle through the RPC ingestion path via loadtest.LedgerBackend, and asserts
// the resulting DB state matches the workload that produced the bundle.
//
// Requires a generated ledger file. By default it looks for
// the checked-in OUTPUT_LEDGER_PATH; override with LOADTEST_INGEST_LEDGER_PATH
// (and optionally LOADTEST_INGEST_FIXTURES_PATH) to point at a new bundle.
func TestIngestSyntheticLedgers(t *testing.T) {
	skipUnlessLoadTestSupported(t)

	ledgerPath := os.Getenv("LOADTEST_INGEST_LEDGER_PATH")
	if ledgerPath == "" {
		ledgerPath = OUTPUT_LEDGER_PATH
	}
	if _, err := os.Stat(ledgerPath); err != nil {
		t.Skipf("no generated ledger file at %q; run TestGenerateLedgers or TestApplyLoadFlow first (or set LOADTEST_INGEST_LEDGER_PATH)", ledgerPath)
	}

	fixturesPath := os.Getenv("LOADTEST_INGEST_FIXTURES_PATH")
	if fixturesPath == "" {
		fixturesPath = OUTPUT_FIXTURES_PATH
	}

	runIngestPhase(t, ledgerPath, fixturesPath, LOADTEST_NETWORK_PASSPHRASE)
}

// TestApplyLoadThenIngest runs both the ledger generation phase and the ingestion
// phase using the output of the former as the input of the latter.
// Use this when you want one command to validate the whole pipeline.
func TestApplyLoadThenIngest(t *testing.T) {
	skipUnlessLoadTestSupported(t)

	dir := t.TempDir()
	ledgerPath := filepath.Join(dir, "load-test-ledgers.xdr.zstd")
	fixturesPath := filepath.Join(dir, "load-test-fixtures.xdr.zstd")

	t.Run("apply-load phase", func(t *testing.T) {
		runApplyLoad(t, ledgerPath, fixturesPath)
	})
	require.False(t, t.Failed(), "apply-load phase failed")
	t.Run("ingest phase", func(t *testing.T) {
		runIngestPhase(t, ledgerPath, fixturesPath, LOADTEST_NETWORK_PASSPHRASE)
	})
	require.False(t, t.Failed(), "ingest phase failed")
}

// skipUnlessLoadTestSupported skips the test unless the integration-test
// gate is on and the local stellar-core advertises protocol 25 or higher.
func skipUnlessLoadTestSupported(t *testing.T) {
	t.Helper()
	if os.Getenv("STELLAR_RPC_INTEGRATION_TESTS_ENABLED") != "true" {
		t.Skip("STELLAR_RPC_INTEGRATION_TESTS_ENABLED not set")
	}
	if infrastructure.GetCoreMaxSupportedProtocol() < 25 {
		t.Skip("apply-load requires Protocol 25 or higher")
	}
}

// newTestLogger pipes go-stellar-sdk log output through t.Log
func newTestLogger(t *testing.T) *log.Entry {
	t.Helper()
	logger := log.New()
	logger.SetOutput(&testWriter{test: t})
	logger.SetLevel(log.InfoLevel)
	return logger
}

// runApplyLoad runs stellar-core apply-load, writes the benchmark ledgers to
// ledgerPath and pre-benchmark fixtures to fixturesPath, and asserts the
// generated workload matches the apply-load.cfg profile (mixed classic +
// Soroban activity, expected ledger count).
func runApplyLoad(t *testing.T, ledgerPath, fixturesPath string) {
	t.Helper()

	// If "", falls back to looking for "stellar-core" in PATH
	coreBinaryPath := os.Getenv("STELLAR_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")

	configPath := os.Getenv("LOADTEST_CORE_CONFIG_PATH")
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
	require.Equal(t, LOADTEST_EXPECTED_NUM_LEDGERS, res.CountLedgers,
		"Expected %d ledgers, got %d", LOADTEST_EXPECTED_NUM_LEDGERS, res.CountLedgers)
	require.Greater(t, res.CountFixtures, 0,
		"Expected at least 1 fixture, got %d", res.CountFixtures)

	expectedClassicTxs := LOADTEST_EXPECTED_CLASSIC_TXS_PER_LEDGER * LOADTEST_EXPECTED_NUM_LEDGERS
	countClassic, countSoroban := getCountTxs(t, ledgerPath)
	require.Equal(t, expectedClassicTxs, countClassic,
		"Expected %d classic Payment ops, got %d", expectedClassicTxs, countClassic)
	require.Greater(t, countSoroban, 0,
		"Expected at least one Soroban InvokeHostFunction op in generated ledgers")
}

// runIngestPhase boots an RPC daemon that ingests from a pre-generated
// synthetic ledger bundle via loadtest.LedgerBackend, waits for ingestion to
// catch up to the last synthetic ledger, then uses getTransactions across the
// ingested range to assert the workload's classic + Soroban op counts match
// the apply-load.cfg profile.
//
// Daemon shutdown is delegated to the harness's t.Cleanup registration.
// We don't call Close() manually because (a) it wouldn't run on assertion
// failure, and (b) after the last synthetic ledger loadtest.LedgerBackend
// returns ErrLoadTestDone and rpc's ingest service retries forever (see daemon.go:292-294).
func runIngestPhase(t *testing.T, ledgerPath, fixturesPath, networkPassphrase string) {
	t.Helper()
	require.FileExists(t, ledgerPath)
	require.FileExists(t, fixturesPath)

	i := infrastructure.NewTest(t, &infrastructure.TestConfig{
		NetworkPassphrase: networkPassphrase,
		LoadTest: config.LoadTestConfig{
			File:      ledgerPath,
			Frequency: 100 * time.Millisecond,
		},
	})
	client := i.GetRPCLient()
	ctx := t.Context()

	// loadtest.LedgerBackend rebases the synthetic ledger sequences to start
	// at the value reported by the history archive's CurrentLedger.
	// test.go's startFakeHistoryArchive returns 1, so the synthetic stream
	// runs [1, LOADTEST_EXPECTED_NUM_LEDGERS] in our daemon.
	const startSeq uint32 = 1
	endSeq := startSeq + uint32(LOADTEST_EXPECTED_NUM_LEDGERS) - 1

	// Wait for ingestion to catch up. With LoadTestFrequency=100ms × 30
	// ledgers we expect ~3s; 60s is generous slack for the harness boot.
	require.Eventually(t, func() bool {
		latest, err := client.GetLatestLedger(ctx)
		return err == nil && latest.Sequence >= endSeq
	}, 60*time.Second, 250*time.Millisecond,
		"RPC never ingested through ledger %d", endSeq)

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

		resp, err := client.GetTransactions(ctx, req)
		require.NoError(t, err)
		if len(resp.Transactions) == 0 {
			break
		}

		for _, tx := range resp.Transactions {
			if tx.Ledger > endSeq {
				break walk
			}
			var env xdr.TransactionEnvelope
			require.NoError(t, xdr.SafeUnmarshalBase64(tx.EnvelopeXDR, &env))
			for _, op := range env.Operations() {
				switch op.Body.Type {
				case xdr.OperationTypePayment:
					countClassic++
				case xdr.OperationTypeInvokeHostFunction:
					countSoroban++
				}
			}
		}

		if resp.Cursor == "" {
			break
		}
		cursor = resp.Cursor
	}

	expectedClassic := LOADTEST_EXPECTED_CLASSIC_TXS_PER_LEDGER * LOADTEST_EXPECTED_NUM_LEDGERS
	require.Equal(t, expectedClassic, countClassic,
		"Expected %d classic Payment ops, got %d", expectedClassic, countClassic)
	require.Greater(t, countSoroban, 0,
		"Expected at least one Soroban InvokeHostFunction op in ingested range")
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

		for _, env := range ledger.TransactionEnvelopes() {
			for _, op := range env.Operations() {
				switch op.Body.Type {
				case xdr.OperationTypePayment:
					countClassic++
				case xdr.OperationTypeInvokeHostFunction:
					countSoroban++
				}
			}
		}
	}
	return countClassic, countSoroban
}

type testWriter struct {
	test *testing.T
}

func (w *testWriter) Write(p []byte) (n int, err error) {
	w.test.Log(string(p))
	return len(p), nil
}
