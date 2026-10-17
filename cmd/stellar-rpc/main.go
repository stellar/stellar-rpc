// stellar-rpc runs rpcv1 by default and rpcv2 behind --enable-rpcv2.
package main

import (
	"os"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/cli"
)

func main() {
	entry := cli.Entry{}
	os.Exit(entry.Run())
}
