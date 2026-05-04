package integrationtest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/loadtest"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

// TestGenerateLedgers generates ledgers using stellar-core's apply-load command.
// The generated ledgers can be written to a compressed XDR file for use in load testing.
// It also extracts ledger entry fixtures from the pre-benchmark checkpoint.
//
// Required env vars:
//   - STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true
//
// Optional env vars:
//   - STELLAR_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN: Path to stellar-core 25.x with BUILD_TESTS enabled
//     (default: looks for "stellar-core" in PATH)
//   - LOADTEST_CORE_CONFIG_PATH: Path to custom apply-load config file
//     (default: testdata/apply-load.cfg)
//   - LOADTEST_OUTPUT_PATH: Destination path for compressed ledger XDR output
//     (default: empty, no file written)
//   - LOADTEST_FIXTURES_PATH: Destination path for compressed fixtures XDR output
//     (default: empty, no file written)
func TestGenerateLedgers(t *testing.T) {
	if os.Getenv("STELLAR_RPC_INTEGRATION_TESTS_ENABLED") != "true" {
		t.Skip("STELLAR_RPC_INTEGRATION_TESTS_ENABLED not set")
	}
	if infrastructure.GetCoreMaxSupportedProtocol() < 25 {
		t.Skip("This test run does not support less than Protocol 25")
	}

	// If "", falls back to looking for "stellar-core" in PATH
	coreBinaryPath := os.Getenv("STELLAR_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN")

	// Use custom config if provided, otherwise use default
	configPath := os.Getenv("LOADTEST_CORE_CONFIG_PATH")
	if configPath == "" {
		configPath = "infrastructure/testdata/apply-load.cfg"
	}

	outputPath := os.Getenv("LOADTEST_OUTPUT_PATH")
	fixturesPath := os.Getenv("LOADTEST_FIXTURES_PATH")
	workDir := t.TempDir()

	logger := log.New()
	logger.SetOutput(&testWriter{test: t})
	logger.SetLevel(log.InfoLevel)

	// Create and run the apply-load generator
	a, err := loadtest.NewApplyLoad(logger, coreBinaryPath, configPath, outputPath, fixturesPath, workDir)
	require.NoError(t, err)
	t.Log("Successfully initialized apply-load generator")
	require.NoError(t, a.RunApplyLoadAndWrite(t.Context()))
}

type testWriter struct {
	test *testing.T
}

func (w *testWriter) Write(p []byte) (n int, err error) {
	w.test.Log(string(p))
	return len(p), nil
}
