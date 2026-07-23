package version

import (
	"fmt"

	"github.com/spf13/cobra"

	goxdr "github.com/stellar/go-stellar-sdk/xdr"
)

// NewCommand returns the `version` subcommand shared by both the stellar-rpc
// and stellar-rpc-v2 binaries. Both intentionally print the same literal
// "stellar-rpc" name string, so their outputs stay byte-identical.
func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information and exit",
		Run: func(_ *cobra.Command, _ []string) {
			if CommitHash == "" {
				//nolint:forbidigo
				fmt.Printf("stellar-rpc dev\n")
			} else {
				// avoid printing the branch for the main branch
				// ( since that's what the end-user would typically have )
				// but keep it for internal build ( so that we'll know from which branch it
				// was built )
				branch := Branch
				if branch == "main" {
					branch = ""
				}
				//nolint:forbidigo
				fmt.Printf("stellar-rpc %s (%s) %s\n", Version, CommitHash, branch)
			}
			//nolint:forbidigo
			fmt.Printf("stellar-xdr %s\n", goxdr.CommitHash)
			//nolint:forbidigo
			fmt.Printf("soroban-env-host-prev %s\n", RSSorobanEnvVersionPrev)
			//nolint:forbidigo
			fmt.Printf("soroban-env-host-curr %s\n", RSSorobanEnvVersionCurr)
		},
	}
}
