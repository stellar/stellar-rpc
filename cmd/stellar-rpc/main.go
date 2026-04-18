package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon"
)

// newFullHistoryBackfillCmd builds the `full-history-backfill` subcommand,
// an offline ingest entry point whose body is wired up in subsequent slices.
func newFullHistoryBackfillCmd() *cobra.Command {
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
	cmd.Flags().Int("workers", 0, "Concurrent DAG task slots (default GOMAXPROCS)")
	cmd.Flags().Int("max-retries", 3, "Max retries per task before marking failed")
	cmd.Flags().Bool("verify-recsplit", true, "Run RecSplit verify phase after build")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}

func main() {
	var cfg config.Config

	rootCmd := &cobra.Command{
		Use:   "stellar-rpc",
		Short: "Start the remote stellar-rpc server",
		Run: func(_ *cobra.Command, _ []string) {
			if err := cfg.SetValues(os.LookupEnv); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := cfg.Validate(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			daemon.MustNew(&cfg, supportlog.New()).Run()
		},
	}

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information and exit",
		Run: func(_ *cobra.Command, _ []string) {
			if config.CommitHash == "" {
				//nolint:forbidigo
				fmt.Printf("stellar-rpc dev\n")
			} else {
				// avoid printing the branch for the main branch
				// ( since that's what the end-user would typically have )
				// but keep it for internal build ( so that we'll know from which branch it
				// was built )
				branch := config.Branch
				if branch == "main" {
					branch = ""
				}
				//nolint:forbidigo
				fmt.Printf("stellar-rpc %s (%s) %s\n", config.Version, config.CommitHash, branch)
			}
			//nolint:forbidigo
			fmt.Printf("stellar-xdr %s\n", goxdr.CommitHash)
			//nolint:forbidigo
			fmt.Printf("soroban-env-host-prev %s\n", config.RSSorobanEnvVersionPrev)
			//nolint:forbidigo
			fmt.Printf("soroban-env-host-curr %s\n", config.RSSorobanEnvVersionCurr)
		},
	}

	genConfigFileCmd := &cobra.Command{
		Use:   "gen-config-file",
		Short: "Generate a config file with default settings",
		Run: func(_ *cobra.Command, _ []string) {
			// We can't call 'Validate' here because the config file we are
			// generating might not be complete. e.g. It might not include a network passphrase.
			if err := cfg.SetValues(os.LookupEnv); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			out, err := cfg.MarshalTOML()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			fmt.Println(string(out))
		},
	}

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(genConfigFileCmd)
	rootCmd.AddCommand(newFullHistoryBackfillCmd())

	if err := cfg.AddFlags(rootCmd); err != nil {
		fmt.Fprintf(os.Stderr, "could not parse config options: %v\n", err)
		os.Exit(1)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "could not run: %v\n", err)

		os.Exit(1)
	}
}
