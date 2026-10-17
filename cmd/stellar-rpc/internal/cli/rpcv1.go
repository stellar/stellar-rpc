package cli

import (
	"fmt"

	"github.com/pelletier/go-toml"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	v1config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/daemon"
)

func (e *Entry) rpcv1Command(runErr *error) (*cobra.Command, error) {
	var cfg v1config.Config

	// rpcv1's logger, before the config is read: the shape refusal below logs
	// through it. rpcv2 builds its own from [logging] once its file is loaded.
	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetOutput(e.Stderr)

	root := &cobra.Command{
		Use:   "stellar-rpc",
		Short: "Start the remote stellar-rpc server (rpcv1)",
		Run: func(cmd *cobra.Command, _ []string) {
			*runErr = e.runRPCv1(cmd, &cfg, logger)
		},
	}

	genConfigFileCmd := &cobra.Command{
		Use:   genConfigFileUse,
		Short: "Generate a config file with default settings",
		Run: func(cmd *cobra.Command, _ []string) {
			// We can't call 'Validate' here because the config file we are
			// generating might not be complete. e.g. It might not include a network passphrase.
			*runErr = e.genRPCv1ConfigFile(cmd, &cfg)
		},
	}
	root.AddCommand(genConfigFileCmd)

	if err := cfg.AddFlags(root); err != nil {
		return nil, err
	}
	return root, nil
}

func (e *Entry) runRPCv1(_ *cobra.Command, cfg *v1config.Config, logger *supportlog.Entry) error {
	// The file is read once. The shape check looks at the tree before the
	// config takes values from it.
	loadTOML := func(path string) (*toml.Tree, error) {
		tree, err := toml.LoadFile(path)
		if err != nil {
			return nil, err
		}
		if table := rpcv2TableIn(tree); table != "" {
			logger.WithFields(supportlog.F{"config_path": path, "table": table}).
				Error("the config file is a rpcv2 config" + e.switchHint(ModeRPCv1, "; %s"))
			return nil, errLogged
		}
		return tree, nil
	}
	if err := cfg.SetValuesWithLoader(e.rpcv1LookupEnv(), loadTOML); err != nil {
		return err
	}
	// The daemon sets the same level and format again in setupLogger. Doing
	// it here puts the mode line in the operator's configured format.
	logger.SetLevel(cfg.LogLevel)
	if cfg.LogFormat == v1config.LogFormatJSON {
		logger.UseJSONFormatter()
	}
	logger.WithField("mode", ModeRPCv1.String()).Info("starting in rpcv1 mode" + e.switchHint(ModeRPCv1, "; %s"))
	return e.startRPCv1(cfg, logger)
}

func (e *Entry) genRPCv1ConfigFile(cmd *cobra.Command, cfg *v1config.Config) error {
	if err := cfg.SetValues(e.rpcv1LookupEnv()); err != nil {
		return err
	}
	out, err := cfg.MarshalTOML()
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(out))
	return nil
}

// rpcv1LookupEnv makes STELLAR_RPC_CONFIG_PATH an alias for the rpcv1 env var
// SOROBAN_RPC_CONFIG_PATH. The new name wins when both are set.
func (e *Entry) rpcv1LookupEnv() func(string) (string, bool) {
	return func(key string) (string, bool) {
		if key == legacyConfigPathEnv {
			if value, ok := e.LookupEnv(ConfigPathEnv); ok {
				return value, true
			}
		}
		return e.LookupEnv(key)
	}
}

func startRPCv1Daemon(cfg *v1config.Config, logger *supportlog.Entry) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	daemon.MustNew(cfg, logger).Run()
	return nil
}
