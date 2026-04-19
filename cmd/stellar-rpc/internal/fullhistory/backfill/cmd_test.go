package backfill

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// minimalValidTOML returns a TOML payload the full validation pipeline
// accepts. Kept local to cmd_test.go so the subcommand test remains
// self-contained and does not reach into the config package's internal
// test helpers.
const minimalValidTOML = `
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"

[BACKFILL.BSB]
BUCKET_PATH = "bucket"
`

// writeTOMLFile writes body to a fresh temp file and returns the path.
// t.TempDir handles cleanup, so each test gets an isolated directory.
func writeTOMLFile(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "backfill.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp TOML: %v", err)
	}
	return path
}

// TestNewCmd_Flags verifies that every flag #684's design doc calls for is
// registered. Drives the command from construction to the flag inventory
// — if a flag gets accidentally dropped, this test catches it before any
// operator runs hit "unknown flag" at the CLI.
func TestNewCmd_Flags(t *testing.T) {
	cmd := NewCmd()

	require.Equal(t, "full-history-backfill", cmd.Use)

	wanted := []string{
		"config",
		"start-ledger",
		"end-ledger",
		"workers",
		"max-retries",
		"verify-recsplit",
		"log-level",
		"log-format",
	}
	for _, name := range wanted {
		require.NotNilf(t, cmd.Flags().Lookup(name), "missing flag: %s", name)
	}
}

// TestNewCmd_ValidConfig_PrintsSummary exercises the golden path: a TOML
// file that validates successfully → subcommand exits 0 and prints the
// expected summary fields to stdout. Assertion is substring-match, so a
// cosmetic layout change in printSummary does not break the test.
func TestNewCmd_ValidConfig_PrintsSummary(t *testing.T) {
	configPath := writeTOMLFile(t, minimalValidTOML)

	cmd := NewCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--config", configPath,
		"--start-ledger", "2",
		"--end-ledger", "10001", // chunk 0's last ledger
		"--workers", "4",
	})

	require.NoError(t, cmd.Execute())

	// Range fields populated and surfaced.
	for _, want := range []string{
		"effective start ledger : 2",
		"effective end ledger   : 10001",
		"chunks-per-txhash-index: 1000",
		"workers        : 4",
		"max-retries    : 3", // default from flag registration
		"verify-recsplit: true",
		"bucket-path: bucket",
		"meta store  : /data/stellar-rpc/meta/rocksdb", // path default resolved
	} {
		require.Containsf(t, out.String(), want, "summary missing %q; full output:\n%s", want, out.String())
	}
}

// TestNewCmd_InvalidConfig_ReturnsError covers the failure branches at
// the subcommand layer — each table case exercises a distinct rule.
// We rely on cobra surfacing RunE errors as non-zero Execute returns,
// mirroring what the operator sees from the shell.
func TestNewCmd_InvalidConfig_ReturnsError(t *testing.T) {
	cases := []struct {
		name       string
		toml       string
		startFlag  string
		endFlag    string
		wantErrSub string
	}{
		{
			name:       "start-ledger below minimum",
			toml:       minimalValidTOML,
			startFlag:  "1",
			endFlag:    "10001",
			wantErrSub: "start-ledger",
		},
		{
			name:       "end-ledger not greater than start-ledger",
			toml:       minimalValidTOML,
			startFlag:  "100",
			endFlag:    "100",
			wantErrSub: "end-ledger",
		},
		{
			name:       "TOML missing DEFAULT_DATA_DIR",
			toml:       "[BACKFILL.BSB]\nBUCKET_PATH = \"bucket\"\n",
			startFlag:  "2",
			endFlag:    "10001",
			wantErrSub: "DEFAULT_DATA_DIR",
		},
		{
			name:       "TOML missing [BACKFILL.BSB]",
			toml:       "[SERVICE]\nDEFAULT_DATA_DIR = \"/data\"\n",
			startFlag:  "2",
			endFlag:    "10001",
			wantErrSub: "BACKFILL.BSB",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			configPath := writeTOMLFile(t, tc.toml)

			cmd := NewCmd()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{
				"--config", configPath,
				"--start-ledger", tc.startFlag,
				"--end-ledger", tc.endFlag,
				"--workers", "1",
			})

			err := cmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErrSub)
		})
	}
}

// TestNewCmd_MissingConfigFile verifies that a non-existent --config path
// surfaces a file-not-found error rather than silently proceeding with
// an empty Config. Realistic operator-error case; catching it fast is
// the whole point.
func TestNewCmd_MissingConfigFile(t *testing.T) {
	cmd := NewCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--config", "/tmp/does-not-exist-" + t.Name() + ".toml",
		"--start-ledger", "2",
		"--end-ledger", "10001",
		"--workers", "1",
	})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "read config file")
}
