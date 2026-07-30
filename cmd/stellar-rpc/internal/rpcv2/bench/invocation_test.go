package bench

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWriteInvocationJSON verifies that writeInvocationJSON produces a valid
// invocation.json file with the expected schema and content.
func TestWriteInvocationJSON(t *testing.T) {
	outDir := t.TempDir()

	// Create a minimal cobra command for testing with proper hierarchy
	parent := &cobra.Command{Use: "bench-ingest"}
	cmd := &cobra.Command{Use: "cold"}
	parent.AddCommand(cmd)

	flags := map[string]string{
		"start-chunk": "1000",
		"num-chunks":  "10",
		"workers":     "4",
	}

	startedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	finishedAt := time.Date(2026, 7, 21, 12, 5, 30, 0, time.UTC)

	err := writeInvocationJSON(outDir, cmd, flags, startedAt, finishedAt)
	require.NoError(t, err)

	// Verify the file exists and is readable
	filePath := filepath.Join(outDir, "invocation.json")
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// Unmarshal and verify the content
	var record invocationRecord
	err = json.Unmarshal(data, &record)
	require.NoError(t, err)

	// Verify schema version and command
	assert.Equal(t, 1, record.SchemaVersion)
	assert.Equal(t, "bench-ingest cold", record.Command) // CommandPath returns "parent child"

	// Verify flags are captured
	assert.Contains(t, record.Flags, "start-chunk")
	assert.Equal(t, "1000", record.Flags["start-chunk"])
	assert.Contains(t, record.Flags, "num-chunks")
	assert.Equal(t, "10", record.Flags["num-chunks"])

	// Verify timestamps
	assert.Equal(t, "2026-07-21T12:00:00Z", record.StartedAt)
	assert.Equal(t, "2026-07-21T12:05:30Z", record.FinishedAt)

	// Verify binary info fields are present (even if empty during test)
	assert.NotNil(t, record.Binary)

	// Verify trailing newline
	assert.Equal(t, byte('\n'), data[len(data)-1])
}

// TestCaptureFlags verifies that captureFlags extracts all flag values from
// a cobra command's flag set.
func TestCaptureFlags(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("string-flag", "default-val", "a string")
	cmd.Flags().Int("int-flag", 42, "an int")
	cmd.Flags().Bool("bool-flag", false, "a bool")

	// Set some flags
	cmd.Flags().Set("string-flag", "custom-val")
	cmd.Flags().Set("int-flag", "100")
	cmd.Flags().Set("bool-flag", "true")

	flags := captureFlags(cmd)

	assert.Equal(t, "custom-val", flags["string-flag"])
	assert.Equal(t, "100", flags["int-flag"])
	assert.Equal(t, "true", flags["bool-flag"])
}
