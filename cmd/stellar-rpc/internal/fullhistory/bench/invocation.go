package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
)

// invocationRecord holds metadata about a benchmark invocation.
type invocationRecord struct {
	SchemaVersion int               `json:"schema_version"`
	Command       string            `json:"command"`
	Flags         map[string]string `json:"flags"`
	Binary        binaryInfo        `json:"binary"`
	Hostname      string            `json:"hostname"`
	StartedAt     string            `json:"started_at"`
	FinishedAt    string            `json:"finished_at"`
}

// binaryInfo holds build-time information about the binary.
type binaryInfo struct {
	Version        string `json:"version"`
	CommitHash     string `json:"commit_hash"`
	BuildTimestamp string `json:"build_timestamp"`
	Branch         string `json:"branch"`
}

// writeInvocationJSON writes an invocation record as JSON to outDir/invocation.json.
// startedAt and finishedAt should be UTC times. The JSON is formatted with
// indentation and a trailing newline.
func writeInvocationJSON(
	outDir string,
	cmd *cobra.Command,
	flags map[string]string,
	startedAt, finishedAt time.Time,
) error {
	hostname, _ := os.Hostname() // empty string on error

	record := invocationRecord{
		SchemaVersion: 1,
		Command:       cmd.CommandPath(),
		Flags:         flags,
		Binary: binaryInfo{
			Version:        config.Version,
			CommitHash:     config.CommitHash,
			BuildTimestamp: config.BuildTimestamp,
			Branch:         config.Branch,
		},
		Hostname:   hostname,
		StartedAt:  startedAt.UTC().Format(time.RFC3339),
		FinishedAt: finishedAt.UTC().Format(time.RFC3339),
	}

	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal invocation record: %w", err)
	}

	path := filepath.Join(outDir, "invocation.json")
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
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
