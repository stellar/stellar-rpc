package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pelletier/go-toml"
	"github.com/spf13/cobra"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/bench"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

func main() {
	var configPath string
	var printRuntimeConfig bool
	rootCmd := &cobra.Command{
		Use:   "stellar-rpc-v2",
		Short: "Run the full-history streaming ingestion daemon",
		Run: func(cmd *cobra.Command, _ []string) {
			if printRuntimeConfig {
				cfg, err := config.LoadConfigWithFlags(configPath, cmd.Flags())
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				out, err := toml.Marshal(cfg)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				fmt.Fprintln(os.Stdout, string(out))
				return
			}
			// Cancel the daemon on SIGINT/SIGTERM for a clean shutdown.
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			if err := rpcv2.RunDaemon(ctx, configPath, cmd.Flags()); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		},
	}
	rootCmd.Flags().StringVar(&configPath, "config", "",
		"path to the full-history streaming daemon TOML config")
	rootCmd.Flags().BoolVar(&printRuntimeConfig, "print-runtime-config", false,
		"resolve the config (file, then any override flags, then compiled defaults) "+
			"and print it as TOML instead of running the daemon")
	// Every TOML key is also a flag named by its dotted path
	// (--storage.default_data_dir, --service.methods.getLedgers.queue_limit);
	// set flags override the file.
	config.BindFlags(rootCmd.Flags())
	if err := rootCmd.MarkFlagRequired("config"); err != nil {
		fmt.Fprintf(os.Stderr, "could not configure root command: %v\n", err)
		os.Exit(1)
	}

	rootCmd.AddCommand(version.NewCommand())
	rootCmd.AddCommand(bench.NewCommand())
	rootCmd.AddCommand(bench.NewQueryCommand())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "could not run: %v\n", err)

		os.Exit(1)
	}
}
