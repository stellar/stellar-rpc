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

const runRecordFile = "run.json"

// runRecord is the run.json schema. Treat key renames as breaking changes.
type runRecord struct {
	SchemaVersion int               `json:"schemaVersion"`
	Command       string            `json:"command"`
	Flags         map[string]string `json:"flags"`
	Binary        binaryInfo        `json:"binary"`
	Hostname      string            `json:"hostname"`
	GOMAXPROCS    int               `json:"gomaxprocs"`
	NumCPU        int               `json:"numCpu"`
	StartedAt     string            `json:"startedAt"`
	FinishedAt    string            `json:"finishedAt,omitempty"`
	// PeakRSSBytes is VmHWM at the end of the run.
	PeakRSSBytes uint64 `json:"peakRssBytes,omitempty"`
	// Settings is runEnv.Settings.
	Settings map[string]string `json:"settings,omitempty"`
	// SetupNs is runEnv.SetupTimes in nanoseconds.
	SetupNs map[string]int64 `json:"setupNs,omitempty"`
	// Status stays running if the process dies before the run ends.
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
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

// newRunRecord returns a record with status running.
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

// finish records how the run ended.
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

// writeRunRecord atomically replaces outDir/run.json with r.
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

// captureFlags returns every flag value of cmd, keyed by flag name.
func captureFlags(cmd *cobra.Command) map[string]string {
	flags := make(map[string]string)
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		flags[f.Name] = f.Value.String()
	})
	return flags
}
