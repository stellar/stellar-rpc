package bench

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCommandRecordsFailedRun drives the real bench-ingest hot command through
// cobra against an empty pack tree: the run fails at the first ledger read, and
// the newBenchCommand wrapper still writes invocation.json into --out with the
// command path, the parsed flag values, and the run's error.
func TestCommandRecordsFailedRun(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "csv")
	packDir := t.TempDir() // no pack file for chunk 0

	cmd := NewCommand()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"hot",
		"--pack-dir", packDir,
		"--start-chunk", "0",
		"--hot-dir", t.TempDir(),
		"--out", outDir,
	})
	err := cmd.Execute()
	require.Error(t, err)

	data, readErr := os.ReadFile(filepath.Join(outDir, "invocation.json"))
	require.NoError(t, readErr)
	var record invocationRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Equal(t, "bench-ingest hot", record.Command)
	assert.Equal(t, err.Error(), record.Error)
	assert.Equal(t, packDir, record.Flags["pack-dir"])
	assert.Equal(t, outDir, record.Flags["out"])
	assert.NotEmpty(t, record.StartedAt)
	assert.NotEmpty(t, record.FinishedAt)
}

// TestCommandWritesInvocationBeforeTheRun drives bench-ingest hot with a source
// its validation rejects, so the run returns before it would have created --out.
// The record is there anyway, which only the write at the start of the run can
// have produced.
func TestCommandWritesInvocationBeforeTheRun(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "csv")

	cmd := NewCommand()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"hot",
		"--source", "bogus",
		"--start-chunk", "0",
		"--hot-dir", t.TempDir(),
		"--out", outDir,
	})
	err := cmd.Execute()
	require.ErrorContains(t, err, "--source=bogus")

	data, readErr := os.ReadFile(filepath.Join(outDir, "invocation.json"))
	require.NoError(t, readErr)
	var record invocationRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Equal(t, "bench-ingest hot", record.Command)
	assert.Equal(t, "bogus", record.Flags["source"])
	assert.NotEmpty(t, record.StartedAt)
	// The run finished, so the second write replaced the start record with the
	// outcome.
	assert.Equal(t, err.Error(), record.Error)
	assert.NotEmpty(t, record.FinishedAt)
}

// TestWriteStartInvocationJSON pins the record a run leaves the moment it
// starts: the command, its flags and a start time, with no finishedAt and no
// error key, so a reader can tell a run that never reached its end from one that
// did.
func TestWriteStartInvocationJSON(t *testing.T) {
	outDir := t.TempDir()
	parent := &cobra.Command{Use: "bench-query"}
	cmd := &cobra.Command{Use: "cold"}
	parent.AddCommand(cmd)

	flags := map[string]string{"cold-dir": "/bench/ds", "types": "ledgers,txhash"}
	startedAt := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	require.NoError(t, writeStartInvocationJSON(outDir, cmd, flags, nil, startedAt))

	data, err := os.ReadFile(filepath.Join(outDir, "invocation.json"))
	require.NoError(t, err)

	var record invocationRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Equal(t, 1, record.SchemaVersion)
	assert.Equal(t, "bench-query cold", record.Command)
	assert.Equal(t, "2026-08-28T09:00:00Z", record.StartedAt)
	assert.Equal(t, "/bench/ds", record.Flags["cold-dir"])

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.NotContains(t, raw, "finishedAt")
	assert.NotContains(t, raw, "error")
}

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

	extra := map[string]string{"pageCacheEviction": "on"}

	err := writeInvocationJSON(outDir, cmd, flags, extra, startedAt, finishedAt, nil)
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

	// The run's own facts land beside the flags, not among them.
	assert.Equal(t, "on", record.Extra["pageCacheEviction"])

	// Verify timestamps
	assert.Equal(t, "2026-07-21T12:00:00Z", record.StartedAt)
	assert.Equal(t, "2026-07-21T12:05:30Z", record.FinishedAt)

	// Verify binary info fields are present (even if empty during test)
	assert.NotNil(t, record.Binary)

	// Verify trailing newline
	assert.Equal(t, byte('\n'), data[len(data)-1])

	// A successful run's record carries no error key at all, so consumers can
	// tell success from failure by the key's presence.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.NotContains(t, raw, "error")
}

// TestWriteInvocationJSONWithError verifies that a failed run's record carries
// the run error's message in the error field.
func TestWriteInvocationJSONWithError(t *testing.T) {
	outDir := t.TempDir()
	cmd := &cobra.Command{Use: "cold"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

	runErr := errors.New("backfill [chunk 3, chunk 3]: boom")
	require.NoError(t, writeInvocationJSON(outDir, cmd, nil, nil, now, now, runErr))

	data, err := os.ReadFile(filepath.Join(outDir, "invocation.json"))
	require.NoError(t, err)

	var record invocationRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Equal(t, runErr.Error(), record.Error)
	assert.Equal(t, 1, record.SchemaVersion)

	// A run that recorded no facts of its own omits the key entirely.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.NotContains(t, raw, "extra")
}

// TestCaptureFlags verifies that captureFlags extracts all flag values from
// a cobra command's flag set.
func TestCaptureFlags(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("string-flag", "default-val", "a string")
	cmd.Flags().Int("int-flag", 42, "an int")
	cmd.Flags().Bool("bool-flag", false, "a bool")

	// Set some flags
	require.NoError(t, cmd.Flags().Set("string-flag", "custom-val"))
	require.NoError(t, cmd.Flags().Set("int-flag", "100"))
	require.NoError(t, cmd.Flags().Set("bool-flag", "true"))

	flags := captureFlags(cmd)

	assert.Equal(t, "custom-val", flags["string-flag"])
	assert.Equal(t, "100", flags["int-flag"])
	assert.Equal(t, "true", flags["bool-flag"])
}
