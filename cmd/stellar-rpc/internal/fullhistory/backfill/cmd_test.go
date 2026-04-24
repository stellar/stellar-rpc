package backfill

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCmd_Placeholder(t *testing.T) {
	cmd := NewCmd()

	require.Equal(t, "full-history-backfill", cmd.Use)

	for _, name := range []string{
		"config",
		"start-ledger",
		"end-ledger",
		"workers",
		"max-retries",
		"verify-recsplit",
	} {
		require.NotNilf(t, cmd.Flags().Lookup(name), "missing flag: %s", name)
	}

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--config", "/dev/null",
		"--start-ledger", "2",
		"--end-ledger", "10002",
	})
	require.NoError(t, cmd.Execute())
	require.Contains(t, out.String(), "not yet implemented")
}
