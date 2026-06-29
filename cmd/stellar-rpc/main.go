package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory"
)

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

	// TODO(#772): standalone subcommand only for the Phase-1 cold-only daemon. Once
	// Phase-2 ingestion lands, full-history backfill folds into the general RPC start
	// command (no separate entrypoint), and this subcommand goes away.
	var fullHistoryConfigPath string
	fullHistoryCmd := &cobra.Command{
		Use:   "full-history",
		Short: "Run the full-history streaming ingestion daemon",
		Run: func(_ *cobra.Command, _ []string) {
			// Cancel the supervised run loop on SIGINT/SIGTERM for a clean shutdown.
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			if err := fullhistory.RunDaemon(ctx, fullHistoryConfigPath); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		},
	}
	fullHistoryCmd.Flags().StringVar(&fullHistoryConfigPath, "config", "",
		"path to the full-history streaming daemon TOML config")
	if err := fullHistoryCmd.MarkFlagRequired("config"); err != nil {
		fmt.Fprintf(os.Stderr, "could not configure full-history command: %v\n", err)
		os.Exit(1)
	}

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(genConfigFileCmd)
	rootCmd.AddCommand(fullHistoryCmd)

	if err := cfg.AddFlags(rootCmd); err != nil {
		fmt.Fprintf(os.Stderr, "could not parse config options: %v\n", err)
		os.Exit(1)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "could not run: %v\n", err)

		os.Exit(1)
	}
}
