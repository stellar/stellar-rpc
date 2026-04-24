// Package backfill implements the offline full-history backfill pipeline
// and owns its CLI wiring.
package backfill

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewCmd builds the `full-history-backfill` subcommand, an offline ingest
// entry point whose body is wired up in subsequent slices.
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "full-history-backfill",
		Short: "Offline backfill of historical Stellar ledger data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), "full-history-backfill: not yet implemented")
			return err
		},
	}
	cmd.Flags().String("config", "", "Path to TOML configuration file (required)")
	cmd.Flags().Uint32("start-ledger", 0, "First ledger to ingest (inclusive, >= 2)")
	cmd.Flags().Uint32("end-ledger", 0, "Last ledger to ingest (inclusive, > start-ledger)")
	cmd.Flags().Int("workers", 0, "Concurrent DAG task slots (0 = GOMAXPROCS)")
	cmd.Flags().Int("max-retries", 3, "Max retries per task before marking failed")
	cmd.Flags().Bool("verify-recsplit", true, "Run RecSplit verify phase after build")
	if err := cmd.MarkFlagRequired("config"); err != nil {
		panic(err)
	}
	return cmd
}
