package backfill

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const minimalValidTOML = `
[SERVICE]
DEFAULT_DATA_DIR = "/data/stellar-rpc"

[BACKFILL.BSB]
BUCKET_PATH = "bucket"
`

func writeTOMLFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "backfill.toml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// TestNewCmd_Flags guards against a flag getting accidentally dropped —
// operators would otherwise see "unknown flag" at the CLI.
func TestNewCmd_Flags(t *testing.T) {
	cmd := NewCmd()
	assert.Equal(t, "full-history-backfill", cmd.Use)

	for _, name := range []string{
		"config", "start-ledger", "end-ledger", "workers",
		"max-retries", "verify-recsplit", "log-level", "log-format",
	} {
		assert.NotNilf(t, cmd.Flags().Lookup(name), "missing flag: %s", name)
	}
}

// TestNewCmd_ValidConfig_PrintsSummary is the golden path: a valid TOML
// produces exit 0 and a summary containing the resolved fields.
// Substring assertions tolerate layout changes in printSummary.
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

	// require — if Execute returns an error, out.String() is meaningless.
	require.NoError(t, cmd.Execute())

	for _, want := range []string{
		"effective start ledger : 2",
		"effective end ledger   : 10001",
		"chunks-per-txhash-index: 1000",
		"workers        : 4",
		"max-retries    : 3",
		"verify-recsplit: true",
		"bucket-path: bucket",
		"meta store  : /data/stellar-rpc/meta/rocksdb",
	} {
		assert.Containsf(t, out.String(), want, "summary missing %q; full output:\n%s", want, out.String())
	}
}

// TestNewCmd_InvalidConfig_ReturnsError — each case maps to one
// validation rule. Cobra surfaces RunE errors as a non-zero Execute
// return, which is what the operator sees.
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
			cmd := NewCmd()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{
				"--config", writeTOMLFile(t, tc.toml),
				"--start-ledger", tc.startFlag,
				"--end-ledger", tc.endFlag,
				"--workers", "1",
			})

			err := cmd.Execute()
			// require — must be non-nil before we can read .Error().
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrSub)
		})
	}
}

// TestNewCmd_MissingConfigFile — a non-existent --config path must
// surface a file-read error, not silently proceed. Uses a guaranteed-
// missing path under t.TempDir() (the dir exists, the file does not).
func TestNewCmd_MissingConfigFile(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "does-not-exist.toml")

	cmd := NewCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--config", missingPath,
		"--start-ledger", "2",
		"--end-ledger", "10001",
		"--workers", "1",
	})

	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read config file")
}
