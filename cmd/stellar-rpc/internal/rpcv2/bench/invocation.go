package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

// invocationRecord holds metadata about a benchmark invocation. The JSON
// keys are a versioned schema (schemaVersion) that downstream tooling
// consumes; treat key renames as breaking changes.
type invocationRecord struct {
	SchemaVersion int               `json:"schemaVersion"`
	Command       string            `json:"command"`
	Flags         map[string]string `json:"flags"`
	Binary        binaryInfo        `json:"binary"`
	Hostname      string            `json:"hostname"`
	StartedAt     string            `json:"startedAt"`
	FinishedAt    string            `json:"finishedAt"`
	// Error carries a failed run's error message; absent on a successful run.
	Error string `json:"error,omitempty"`
}

// binaryInfo holds build-time information about the binary.
type binaryInfo struct {
	Version        string `json:"version"`
	CommitHash     string `json:"commitHash"`
	BuildTimestamp string `json:"buildTimestamp"`
	Branch         string `json:"branch"`
}

// writeInvocationJSON writes an invocation record as JSON to outDir/invocation.json.
// startedAt and finishedAt should be UTC times. runErr is the run's outcome: nil
// for a successful run, otherwise its message lands in the record's error field.
// The JSON is formatted with indentation and a trailing newline.
func writeInvocationJSON(
	outDir string,
	cmd *cobra.Command,
	flags map[string]string,
	startedAt, finishedAt time.Time,
	runErr error,
) error {
	hostname, _ := os.Hostname() // empty string on error

	var errMsg string
	if runErr != nil {
		errMsg = runErr.Error()
	}

	record := invocationRecord{
		SchemaVersion: 1,
		Command:       cmd.CommandPath(),
		Flags:         flags,
		Binary: binaryInfo{
			Version:        version.Version,
			CommitHash:     version.CommitHash,
			BuildTimestamp: version.BuildTimestamp,
			Branch:         version.Branch,
		},
		Hostname:   hostname,
		StartedAt:  startedAt.UTC().Format(time.RFC3339),
		FinishedAt: finishedAt.UTC().Format(time.RFC3339),
		Error:      errMsg,
	}

	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal invocation record: %w", err)
	}

	path := filepath.Join(outDir, "invocation.json")
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("write invocation.json: %w", err)
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
