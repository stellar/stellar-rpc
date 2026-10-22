package cli

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/pelletier/go-toml"
	"github.com/spf13/cobra"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/bench"
	v2config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

func (e *Entry) rpcv2Command(runErr *error) *cobra.Command {
	var configPath string
	var printRuntimeConfig bool

	root := &cobra.Command{
		Use:   "stellar-rpc",
		Short: "Run the full-history streaming ingestion daemon (rpcv2)",
		Run: func(cmd *cobra.Command, _ []string) {
			*runErr = e.runRPCv2(cmd, configPath, printRuntimeConfig)
		},
	}
	// The environment variable is the flag's default, so the flag wins when
	// both are set and --help shows the value in effect.
	envPath, _ := e.LookupEnv(ConfigPathEnv)
	root.Flags().StringVar(&configPath, ConfigPathFlag, envPath,
		"path to the full-history streaming daemon TOML config ("+ConfigPathEnv+")")
	root.Flags().BoolVar(&printRuntimeConfig, "print-runtime-config", false,
		"resolve the config (file, then any override flags, then compiled defaults) "+
			"and print it as TOML instead of running the daemon")
	// Every TOML key is also a flag named by its dotted path
	// (--storage.default_data_dir, --service.methods.getLedgers.queue_limit);
	// set flags override the file.
	v2config.BindFlags(root.Flags())

	genConfigFileCmd := &cobra.Command{
		Use:   genConfigFileUse,
		Short: "Generate a config file with default settings",
		Run: func(cmd *cobra.Command, _ []string) {
			*runErr = printTOML(cmd, v2config.Config{}.WithDefaults())
		},
	}
	root.AddCommand(genConfigFileCmd)
	root.AddCommand(bench.NewCommand())
	return root
}

func (e *Entry) runRPCv2(cmd *cobra.Command, configPath string, printRuntimeConfig bool) error {
	if configPath == "" {
		return errors.New("rpcv2 mode needs a config file: pass --" + ConfigPathFlag + " or set " + ConfigPathEnv)
	}
	if printRuntimeConfig {
		cfg, err := v2config.LoadConfigWithFlags(configPath, cmd.Flags())
		if err != nil {
			return err
		}
		return printTOML(cmd, cfg)
	}
	return e.startRPCv2(configPath, cmd.Flags())
}

func printTOML(cmd *cobra.Command, cfg v2config.Config) error {
	out, err := toml.Marshal(cfg)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(out))
	return nil
}

func startRPCv2Daemon(configPath string, flags v2config.FlagOverrides) error {
	// Cancel the daemon on SIGINT/SIGTERM for a clean shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return rpcv2.RunDaemon(ctx, configPath, flags)
}
