package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

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

	rootCmd.AddCommand(version.NewCommand())
	rootCmd.AddCommand(bench.NewCommand())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "could not run: %v\n", err)

		os.Exit(1)
	}
}
