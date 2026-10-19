// Package cli is the entry point of the stellar-rpc binary. It picks the mode,
// builds that mode's command tree, and hands off to the daemon the way each
// mode's own main did before #1108.
package cli

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	v1config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
	v2config "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

// Entry is one run of the stellar-rpc command line. A zero field takes the
// process default: os.Args[1:], os.LookupEnv, os.Stdout, or os.Stderr.
type Entry struct {
	Args      []string
	LookupEnv func(string) (string, bool)
	Stdout    io.Writer
	Stderr    io.Writer

	// Tests replace the two daemon launches with recorders.
	startRPCv1 func(cfg *v1config.Config) error
	startRPCv2 func(configPath string, flags v2config.FlagOverrides) error
}

// genConfigFileUse is the subcommand that prints the mode's compiled defaults.
const genConfigFileUse = "gen-config-file"

// Run runs the command line and returns the process exit code: 0 after a
// clean run or a signal, 1 on any failure.
func (e *Entry) Run() int {
	e.fillDefaults()

	mode, err := SelectMode(e.Args, e.LookupEnv)
	if err != nil {
		fmt.Fprintln(e.Stderr, err)
		return 1
	}

	var runErr error
	root, err := e.rootCommand(mode, &runErr)
	if err != nil {
		fmt.Fprintf(e.Stderr, "could not parse config options: %v\n", err)
		return 1
	}
	root.SetArgs(e.Args)
	root.SetOut(e.Stdout)
	root.SetErr(e.Stderr)
	if err := root.Execute(); err != nil {
		fmt.Fprintf(e.Stderr, "could not run: %v\n", err)
		return 1
	}
	if runErr != nil {
		fmt.Fprintln(e.Stderr, runErr)
		return 1
	}
	return 0
}

func (e *Entry) fillDefaults() {
	if e.Args == nil {
		e.Args = os.Args[1:]
	}
	if e.LookupEnv == nil {
		e.LookupEnv = os.LookupEnv
	}
	if e.Stdout == nil {
		e.Stdout = os.Stdout
	}
	if e.Stderr == nil {
		e.Stderr = os.Stderr
	}
	if e.startRPCv1 == nil {
		e.startRPCv1 = startRPCv1Daemon
	}
	if e.startRPCv2 == nil {
		e.startRPCv2 = startRPCv2Daemon
	}
}

// rootCommand builds the command tree of one mode only, so --help shows one
// mode and a flag of the other mode is an unknown flag.
func (e *Entry) rootCommand(mode Mode, runErr *error) (*cobra.Command, error) {
	var root *cobra.Command
	var err error
	if mode == ModeRPCv2 {
		root = e.rpcv2Command(runErr)
	} else {
		root, err = e.rpcv1Command(runErr)
		if err != nil {
			return nil, err
		}
	}
	// SelectMode already read this flag from the raw arguments. It is
	// registered here so cobra accepts it and --help lists it.
	root.PersistentFlags().Bool(EnableRPCv2Flag, mode == ModeRPCv2,
		"run the rpcv2 full-history daemon instead of rpcv1 ("+EnableRPCv2Env+")")
	root.AddCommand(versionCommand(mode))
	return root, nil
}

func versionCommand(mode Mode) *cobra.Command {
	cmd := version.NewCommand()
	cmd.PostRun = func(cmd *cobra.Command, _ []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "mode: %s (%s)\n", mode, mode.howToSwitch())
	}
	return cmd
}
