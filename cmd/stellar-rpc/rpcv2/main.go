package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/bench"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

func main() {
	var configPath string
	rootCmd := &cobra.Command{
		Use:   "stellar-rpc-v2",
		Short: "Run the full-history streaming ingestion daemon",
		Run: func(_ *cobra.Command, _ []string) {
			// Cancel the supervised run loop on SIGINT/SIGTERM for a clean shutdown.
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			if err := rpcv2.RunDaemon(ctx, configPath); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		},
	}
	rootCmd.Flags().StringVar(&configPath, "config", "",
		"path to the full-history streaming daemon TOML config")
	if err := rootCmd.MarkFlagRequired("config"); err != nil {
		fmt.Fprintf(os.Stderr, "could not configure root command: %v\n", err)
		os.Exit(1)
	}

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information and exit",
		Run: func(_ *cobra.Command, _ []string) {
			if version.CommitHash == "" {
				//nolint:forbidigo
				fmt.Printf("stellar-rpc dev\n")
			} else {
				// avoid printing the branch for the main branch
				// ( since that's what the end-user would typically have )
				// but keep it for internal build ( so that we'll know from which branch it
				// was built )
				branch := version.Branch
				if branch == "main" {
					branch = ""
				}
				//nolint:forbidigo
				fmt.Printf("stellar-rpc %s (%s) %s\n", version.Version, version.CommitHash, branch)
			}
			//nolint:forbidigo
			fmt.Printf("stellar-xdr %s\n", goxdr.CommitHash)
			//nolint:forbidigo
			fmt.Printf("soroban-env-host-prev %s\n", version.RSSorobanEnvVersionPrev)
			//nolint:forbidigo
			fmt.Printf("soroban-env-host-curr %s\n", version.RSSorobanEnvVersionCurr)
		},
	}

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(bench.NewCommand())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "could not run: %v\n", err)

		os.Exit(1)
	}
}
