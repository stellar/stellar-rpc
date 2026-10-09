package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/daemon"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
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
			fmt.Fprintln(os.Stdout, string(out))
		},
	}

	rootCmd.AddCommand(version.NewCommand())
	rootCmd.AddCommand(genConfigFileCmd)

	if err := cfg.AddFlags(rootCmd); err != nil {
		fmt.Fprintf(os.Stderr, "could not parse config options: %v\n", err)
		os.Exit(1)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "could not run: %v\n", err)

		os.Exit(1)
	}
}
