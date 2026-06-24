package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming"
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

	// full-history-streaming launches the full-history streaming daemon (Issue 13
	// entrypoint). It is a SEPARATE subcommand from the default v1 run: the full
	// SQLite→full-history cutover that flips the default `run` path is issue #772.
	// TODO(#772): when #772 lands, fold this into the daemon's primary flow (or
	// flip `run` to it) and retire the v1 SQLite ingestion/preflight path.
	//
	// TODO(windows): this import wires the full-history daemon into the
	// cross-platform binary, but the daemon is Unix-only by construction —
	// streaming/config_lock.go takes a flock via golang.org/x/sys/unix (no
	// Windows build) and the hot tier is cgo RocksDB/grocksdb (needs RocksDB
	// libs). So `go build ./cmd/stellar-rpc` on windows-latest fails to compile;
	// #805–#807 pass only because their main.go does not yet import streaming.
	// Before the Windows build matrix can be green with the daemon wired in,
	// build-constrain the daemon path off Windows (a //go:build unix tag on the
	// streaming/daemon packages + a Windows stub for this subcommand, per the
	// packfile/writeback_* and txhash/odirect_* precedent), or drop windows-latest
	// from the daemon build.
	var fullHistoryConfigPath string
	fullHistoryCmd := &cobra.Command{
		Use:   "full-history-streaming",
		Short: "Run the full-history streaming daemon (experimental; see #772 for the v1 cutover)",
		Run: func(cmd *cobra.Command, _ []string) {
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			if err := streaming.RunDaemon(ctx, fullHistoryConfigPath); err != nil {
				fmt.Fprintf(os.Stderr, "full-history streaming daemon: %v\n", err)
				os.Exit(1)
			}
		},
	}
	fullHistoryCmd.Flags().StringVar(&fullHistoryConfigPath, "config", "",
		"path to the full-history streaming daemon TOML config (required)")
	_ = fullHistoryCmd.MarkFlagRequired("config")

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
