package bench

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// TestCommandRecordsFailedRun drives the real bench-ingest hot command through
// cobra against an empty pack tree: the run fails at the first ledger read, and
// the newBenchCommand wrapper still writes run.json into --out with the
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

	data, readErr := os.ReadFile(filepath.Join(outDir, runRecordFile))
	require.NoError(t, readErr)
	var record runRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Equal(t, "bench-ingest hot", record.Command)
	assert.Equal(t, err.Error(), record.Error)
	assert.Equal(t, packDir, record.Flags["pack-dir"])
	assert.Equal(t, outDir, record.Flags["out"])
	assert.NotEmpty(t, record.StartedAt)
	assert.NotEmpty(t, record.FinishedAt)
	assert.Equal(t, runStatusFailed, record.Status)
}

// TestCommandWritesRunRecordBeforeTheRun: a run that fails validation before
// it would create --out still leaves run.json there.
func TestCommandWritesRunRecordBeforeTheRun(t *testing.T) {
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

	data, readErr := os.ReadFile(filepath.Join(outDir, runRecordFile))
	require.NoError(t, readErr)
	var record runRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Equal(t, "bench-ingest hot", record.Command)
	assert.Equal(t, "bogus", record.Flags["source"])
	assert.NotEmpty(t, record.StartedAt)
	assert.Equal(t, err.Error(), record.Error)
	assert.NotEmpty(t, record.FinishedAt)
	assert.Equal(t, runStatusFailed, record.Status)
}

// TestCommandWritesTheInProgressRecord: the run body finds run.json
// already in --out, carrying startedAt and neither finishedAt nor error.
func TestCommandWritesTheInProgressRecord(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "csv")
	var ran bool

	cmd := newBenchCommand("probe", "", &profileFlags{},
		func(_ context.Context, _ *supportlog.Entry, env runEnv) error {
			ran = true
			data, err := os.ReadFile(filepath.Join(env.OutDir, runRecordFile))
			require.NoError(t, err)

			var record runRecord
			require.NoError(t, json.Unmarshal(data, &record))
			assert.Equal(t, "probe", record.Command)
			assert.Equal(t, outDir, record.Flags["out"])
			assert.NotEmpty(t, record.StartedAt)
			assert.Equal(t, runStatusRunning, record.Status)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &raw))
			assert.NotContains(t, raw, "finishedAt")
			assert.NotContains(t, raw, "error")
			return nil
		})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--out", outDir})

	require.NoError(t, cmd.Execute())
	require.True(t, ran, "the run body never ran")
}

// TestRefuseStaleCSVs: refuseStaleCSVs names any CSV in the dir and passes a
// dir that holds only run.json and passes a missing dir.
func TestRefuseStaleCSVs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files []string
		stale string
	}{
		{"csv", []string{runRecordFile, "cold.csv"}, "cold.csv"},
		{"run record only", []string{runRecordFile}, ""},
		{"missing dir", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := t.TempDir()
			if tc.files == nil {
				out = filepath.Join(out, "missing")
			}
			for _, name := range tc.files {
				require.NoError(t, os.WriteFile(filepath.Join(out, name), nil, 0o600))
			}
			err := refuseStaleCSVs(out)
			if tc.stale == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, out)
			require.ErrorContains(t, err, tc.stale)
		})
	}
}

// TestIngestCommandsRefuseOutWithCSVs: an ingest command whose --out holds a
// CSV fails before it rewrites run.json or reads the source: the pack
// dir does not exist, so reaching the run body would fail with a different
// error.
func TestIngestCommandsRefuseOutWithCSVs(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-pack-dir")
	for _, args := range [][]string{
		{"cold", "--start-chunk", "0", "--cold-out-dir", t.TempDir(), "--pack-dir", missing},
		{"hot", "--start-chunk", "0", "--hot-dir", t.TempDir(), "--pack-dir", missing},
	} {
		t.Run(args[0], func(t *testing.T) {
			requireRefusesStaleOut(t, NewCommand(), args, "cold.csv", missing)
		})
	}
}

// requireRefusesStaleOut runs cmd with args and an --out that holds
// run.json and the CSV stale. The command must fail with an error that
// names stale and not missing, which only the run body would report, and must
// leave --out unchanged.
func requireRefusesStaleOut(t *testing.T, cmd *cobra.Command, args []string, stale, missing string) {
	t.Helper()
	out := t.TempDir()
	earlier := []byte(`{"command":"earlier run"}`)
	require.NoError(t, os.WriteFile(filepath.Join(out, runRecordFile), earlier, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(out, stale), nil, 0o600))

	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append(args, "--out", out))
	err := cmd.Execute()
	require.ErrorContains(t, err, stale)
	require.ErrorContains(t, err, "pass an empty --out")
	assert.NotContains(t, err.Error(), missing, "the run body must not run")

	got, readErr := os.ReadFile(filepath.Join(out, runRecordFile))
	require.NoError(t, readErr)
	assert.Equal(t, string(earlier), string(got), "a refused run must leave run.json unchanged")
	entries, readErr := os.ReadDir(out)
	require.NoError(t, readErr)
	assert.Len(t, entries, 2, "a refused run must add no files to --out")
}

// TestWriteRunRecordInProgress: a new record is in the running state, sizes the
// process, and has no finishedAt, peakRssBytes or error key.
func TestWriteRunRecordInProgress(t *testing.T) {
	outDir := t.TempDir()
	parent := &cobra.Command{Use: "bench-query"}
	cmd := &cobra.Command{Use: "cold"}
	parent.AddCommand(cmd)

	flags := map[string]string{"cold-dir": "/bench/ds", "types": "ledgers,txhash"}
	startedAt := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	require.NoError(t, writeRunRecord(outDir, newRunRecord(cmd, flags, startedAt)))

	data, err := os.ReadFile(filepath.Join(outDir, runRecordFile))
	require.NoError(t, err)

	var record runRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Equal(t, 2, record.SchemaVersion)
	assert.Equal(t, "bench-query cold", record.Command)
	assert.Equal(t, "2026-08-28T09:00:00Z", record.StartedAt)
	assert.Equal(t, "/bench/ds", record.Flags["cold-dir"])
	assert.Equal(t, runStatusRunning, record.Status)
	assert.Equal(t, runtime.GOMAXPROCS(0), record.GOMAXPROCS)
	assert.Equal(t, runtime.NumCPU(), record.NumCPU)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	for _, key := range []string{"finishedAt", "peakRssBytes", "settings", "setupNs", "error"} {
		assert.NotContains(t, raw, key)
	}
}

// TestWriteRunRecord: a finished record carries the flags, the run's settings
// and setup times, the peak RSS, and no error key.
func TestWriteRunRecord(t *testing.T) {
	outDir := t.TempDir()
	parent := &cobra.Command{Use: "bench-ingest"}
	cmd := &cobra.Command{Use: "cold"}
	parent.AddCommand(cmd)

	flags := map[string]string{"start-chunk": "1000", "num-chunks": "10", "workers": "4"}
	startedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	finishedAt := time.Date(2026, 7, 21, 12, 5, 30, 0, time.UTC)
	env := runEnv{
		Settings:   map[string]string{"pageCacheEviction": "on"},
		SetupTimes: map[string]time.Duration{"storeOpen": 1500 * time.Millisecond},
	}

	record := newRunRecord(cmd, flags, startedAt)
	record.finish(finishedAt, env, 4096, nil)
	require.NoError(t, writeRunRecord(outDir, record))

	data, err := os.ReadFile(filepath.Join(outDir, runRecordFile))
	require.NoError(t, err)
	var got runRecord
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, 2, got.SchemaVersion)
	assert.Equal(t, "bench-ingest cold", got.Command) // CommandPath returns "parent child"
	assert.Equal(t, "1000", got.Flags["start-chunk"])
	assert.Equal(t, "10", got.Flags["num-chunks"])
	assert.Equal(t, "on", got.Settings["pageCacheEviction"])
	assert.Equal(t, int64(1_500_000_000), got.SetupNs["storeOpen"])
	assert.Equal(t, uint64(4096), got.PeakRSSBytes)
	assert.Equal(t, "2026-07-21T12:00:00Z", got.StartedAt)
	assert.Equal(t, "2026-07-21T12:05:30Z", got.FinishedAt)
	assert.Equal(t, runStatusOK, got.Status)
	assert.Equal(t, byte('\n'), data[len(data)-1])

	// A successful run's record has no error key, so a reader can tell success
	// from failure by the key's presence.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.NotContains(t, raw, "error")
}

// TestWriteRunRecordWithError: a failed run's record carries the run error's
// message, and a run that set nothing has no settings, setupNs or
// peakRssBytes key.
func TestWriteRunRecordWithError(t *testing.T) {
	outDir := t.TempDir()
	cmd := &cobra.Command{Use: "cold"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

	runErr := errors.New("backfill [chunk 3, chunk 3]: boom")
	record := newRunRecord(cmd, nil, now)
	record.finish(now, runEnv{}, 0, runErr)
	require.NoError(t, writeRunRecord(outDir, record))

	data, err := os.ReadFile(filepath.Join(outDir, runRecordFile))
	require.NoError(t, err)

	var got runRecord
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, runErr.Error(), got.Error)
	assert.Equal(t, runStatusFailed, got.Status)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	for _, key := range []string{"settings", "setupNs", "peakRssBytes"} {
		assert.NotContains(t, raw, key)
	}
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
