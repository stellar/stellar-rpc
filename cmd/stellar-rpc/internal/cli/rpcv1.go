package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	v1config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/daemon"
)

func (e *Entry) rpcv1Command(runErr *error) (*cobra.Command, error) {
	var cfg v1config.Config

	root := &cobra.Command{
		Use:   "stellar-rpc",
		Short: "Start the remote stellar-rpc server (rpcv1)",
		Run: func(_ *cobra.Command, _ []string) {
			*runErr = e.runRPCv1(&cfg)
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

func (e *Entry) runRPCv1(cfg *v1config.Config) error {
	if err := cfg.SetValues(e.rpcv1LookupEnv()); err != nil {
		return err
	}
	// rpcv1 parses its file non-strictly, so SetValues accepts a rpcv2 file
	// without an error and leaves every value at its default. Nothing has
	// been opened yet, so this is the place to refuse it.
	if err := refuseRPCv2File(cfg.ConfigPath, e.switchHint(ModeRPCv1, "; %s")); err != nil {
		return err
	}
	return e.startRPCv1(cfg)
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

func startRPCv1Daemon(cfg *v1config.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	daemon.MustNew(cfg, supportlog.New()).Run()
	return nil
}
