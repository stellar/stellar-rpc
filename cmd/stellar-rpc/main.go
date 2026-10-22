// stellar-rpc runs rpcv1 by default and rpcv2 behind --enable-rpcv2.
package main

import (
	"os"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/cli"
)

//nolint:gochecknoglobals // make build-stellar-rpc sets it from RPCV2 with -ldflags -X
var rpcv2 = "true"

func main() {
	entry := cli.Entry{DisableRPCv2: rpcv2 != "true"}
	os.Exit(entry.Run())
}
