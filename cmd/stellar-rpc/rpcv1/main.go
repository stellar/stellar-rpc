package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
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
			logger := supportlog.New()
			if err := runDaemon(&cfg, logger); err != nil {
				reportFailure(&cfg, logger, err)
				os.Exit(1)
			}
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

// runDaemon builds the daemon and serves until SIGINT or SIGTERM. The signal
// context covers startup too, so a signal during a long backfill stops it,
// closes what is open, and counts as a shutdown request, not a failure.
func runDaemon(cfg *config.Config, logger *supportlog.Entry) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	d, err := daemon.New(ctx, cfg, logger)
	if err != nil {
		if ctx.Err() != nil {
			logger.WithError(err).Info("shutdown requested during startup")
			return nil
		}
		return err
	}
	return d.Run(ctx)
}

// reportFailure logs err at level error. The daemon applies the configured
// log level before anything can fail, and a level above error would hide the
// only line that says why the process exits, so stderr gets it instead then.
func reportFailure(cfg *config.Config, logger *supportlog.Entry, err error) {
	if cfg.LogLevel >= logrus.ErrorLevel {
		logger.WithError(err).Error("stellar-rpc failed")
		return
	}
	fmt.Fprintln(os.Stderr, "stellar-rpc failed:", err)
}
