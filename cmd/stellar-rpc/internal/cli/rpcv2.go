package cli

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pelletier/go-toml"
	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	v1config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/bench"
	v2config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
)

func (e *Entry) rpcv2Command(runErr *error) *cobra.Command {
	var printRuntimeConfig bool

	root := &cobra.Command{
		Use:   "stellar-rpc",
		Short: "Run the full-history streaming ingestion daemon (rpcv2)",
		Run: func(cmd *cobra.Command, _ []string) {
			*runErr = e.runRPCv2(cmd, printRuntimeConfig)
		},
	}
	root.Flags().String(ConfigPathFlag, "",
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
			*runErr = genRPCv2ConfigFile(cmd)
		},
	}
	root.AddCommand(genConfigFileCmd)
	root.AddCommand(bench.NewCommand())
	return root
}

func (e *Entry) runRPCv2(cmd *cobra.Command, printRuntimeConfig bool) error {
	path, err := e.configPath(cmd)
	if err != nil {
		return err
	}
	if path == "" {
		return errors.New("rpcv2 mode needs a config file: pass --" + ConfigPathFlag + " or set " + ConfigPathEnv)
	}
	// The file is read once, here. The daemon gets the loaded config.
	cfg, err := v2config.LoadConfigWithFlags(path, cmd.Flags())
	if err != nil {
		return err
	}
	if printRuntimeConfig {
		return printTOML(cmd, cfg)
	}
	// One logger for the whole process: the daemon's, built from [logging],
	// so these startup lines come out in the operator's level and format.
	logger, err := rpcv2.NewLogger(cfg.Logging)
	if err != nil {
		return err
	}
	logger.SetOutput(e.Stderr)
	e.logIgnoredRPCv1Env(logger)
	logger.WithField("mode", ModeRPCv2.String()).Info("starting in rpcv2 mode" + e.switchHint(ModeRPCv2, "; %s"))
	// Flags are already applied to cfg, so the daemon gets only the config.
	return e.startRPCv2(path, rpcv2.Options{Config: &cfg, Logger: logger})
}

// configPath resolves the rpcv2 config file: the flag, then
// STELLAR_RPC_CONFIG_PATH. An empty result means no file. rpcv1 resolves its
// own path through its config package and rpcv1LookupEnv.
func (e *Entry) configPath(cmd *cobra.Command) (string, error) {
	if cmd.Flags().Changed(ConfigPathFlag) {
		return cmd.Flags().GetString(ConfigPathFlag)
	}
	if path, ok := e.LookupEnv(ConfigPathEnv); ok {
		return path, nil
	}
	return "", nil
}

// logIgnoredRPCv1Env lists the rpcv1 environment variables that are set.
// rpcv2 reads none of them. The line is info, not warn: the stock
// /etc/default/stellar-rpc in the Debian package sets three of them, so every
// .deb operator in rpcv2 mode sees it.
func (e *Entry) logIgnoredRPCv1Env(logger *supportlog.Entry) {
	var ignored []string
	for _, name := range (&v1config.Config{}).EnvVarNames() {
		if _, ok := e.LookupEnv(name); ok {
			ignored = append(ignored, name)
		}
	}
	if len(ignored) == 0 {
		return
	}
	logger.WithField("ignored", strings.Join(ignored, " ")).
		Info("rpcv1 environment variables are ignored in rpcv2 mode")
}

func genRPCv2ConfigFile(cmd *cobra.Command) error {
	return printTOML(cmd, v2config.Config{}.WithDefaults())
}

func printTOML(cmd *cobra.Command, cfg v2config.Config) error {
	out, err := toml.Marshal(cfg)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(out))
	return nil
}

func startRPCv2Daemon(configPath string, opts rpcv2.Options) error {
	// Cancel the daemon on SIGINT/SIGTERM for a clean shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return rpcv2.RunDaemonWithOptions(ctx, configPath, opts)
}
