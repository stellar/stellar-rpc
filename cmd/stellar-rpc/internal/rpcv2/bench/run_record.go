package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

// runRecordFile is the run record's basename in --out.
const runRecordFile = "run.json"

// runRecord describes one bench run: what ran, where, with which settings, and
// how it ended. The JSON keys are a versioned schema (schemaVersion) that
// downstream tooling reads; treat key renames as breaking changes. See
// README.md for every key.
type runRecord struct {
	SchemaVersion int               `json:"schemaVersion"`
	Command       string            `json:"command"`
	Flags         map[string]string `json:"flags"`
	Binary        binaryInfo        `json:"binary"`
	Hostname      string            `json:"hostname"`
	// GOMAXPROCS and NumCPU size the process that ran the bench.
	GOMAXPROCS int    `json:"gomaxprocs"`
	NumCPU     int    `json:"numCpu"`
	StartedAt  string `json:"startedAt"`
	// FinishedAt is absent while the run is in progress.
	FinishedAt string `json:"finishedAt,omitempty"`
	// PeakRSSBytes is the process's peak resident set size (VmHWM) at the end
	// of the run. Absent while the run is in progress and where /proc is not
	// available.
	PeakRSSBytes uint64 `json:"peakRssBytes,omitempty"`
	// Settings holds what the run put in runEnv.Settings. Absent when empty.
	Settings map[string]string `json:"settings,omitempty"`
	// SetupNs holds what the run put in runEnv.SetupTimes, in nanoseconds.
	// Absent when empty.
	SetupNs map[string]int64 `json:"setupNs,omitempty"`
	// Status is the run's state. The start record says running; the end
	// record says ok or failed. A record still at running after the process
	// exits means the run died before its final write.
	Status string `json:"status"`
	// Error carries a failed run's error message; absent on a successful run.
	Error string `json:"error,omitempty"`
}

// runRecord.Status values.
const (
	runStatusRunning = "running"
	runStatusOK      = "ok"
	runStatusFailed  = "failed"
)

// binaryInfo holds build-time information about the binary.
type binaryInfo struct {
	Version        string `json:"version"`
	CommitHash     string `json:"commitHash"`
	BuildTimestamp string `json:"buildTimestamp"`
	Branch         string `json:"branch"`
}

// newRunRecord returns the record of a run of cmd that started at startedAt
// with flags, in the running state.
func newRunRecord(cmd *cobra.Command, flags map[string]string, startedAt time.Time) runRecord {
	hostname, _ := os.Hostname() // empty string on error
	return runRecord{
		SchemaVersion: 2,
		Command:       cmd.CommandPath(),
		Flags:         flags,
		Binary: binaryInfo{
			Version:        version.Version,
			CommitHash:     version.CommitHash,
			BuildTimestamp: version.BuildTimestamp,
			Branch:         version.Branch,
		},
		Hostname:   hostname,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		NumCPU:     runtime.NumCPU(),
		StartedAt:  startedAt.UTC().Format(time.RFC3339),
		Status:     runStatusRunning,
	}
}

// finish moves the record to its end state: ok when runErr is nil, failed
// with runErr's message otherwise. A zero peakRSS leaves the field out.
func (r *runRecord) finish(finishedAt time.Time, env runEnv, peakRSS uint64, runErr error) {
	r.FinishedAt = finishedAt.UTC().Format(time.RFC3339)
	r.PeakRSSBytes = peakRSS
	r.Settings = env.Settings
	if len(env.SetupTimes) > 0 {
		r.SetupNs = make(map[string]int64, len(env.SetupTimes))
		for name, d := range env.SetupTimes {
			r.SetupNs[name] = d.Nanoseconds()
		}
	}
	r.Status = runStatusOK
	r.Error = ""
	if runErr != nil {
		r.Status = runStatusFailed
		r.Error = runErr.Error()
	}
}

// writeRunRecord writes r to outDir/run.json through a temp file and a rename,
// so a kill during the write leaves the previous record whole.
func writeRunRecord(outDir string, r runRecord) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal run record: %w", err)
	}

	path := filepath.Join(outDir, runRecordFile)
	tmp, err := os.CreateTemp(outDir, "."+runRecordFile+".*.tmp")
	if err != nil {
		return fmt.Errorf("write %s: %w", runRecordFile, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // a no-op once the rename lands
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", runRecordFile, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write %s: %w", runRecordFile, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace %s: %w", runRecordFile, err)
	}
	return nil
}

// captureFlags extracts all flag values from a cobra command's flag set,
// returning them as a map of flag name to string value. Uses VisitAll to
// capture all flags (default and explicitly-set).
func captureFlags(cmd *cobra.Command) map[string]string {
	flags := make(map[string]string)
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		flags[f.Name] = f.Value.String()
	})
	return flags
}
