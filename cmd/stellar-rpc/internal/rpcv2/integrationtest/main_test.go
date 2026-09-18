package integrationtest

import (
	"fmt"
	"os"
	"testing"
)

// Every test in this package needs the rpcv2 daemon. Default the harness
// selector to it, and refuse to run against anything else.
func TestMain(m *testing.M) {
	const envVar = "STELLAR_RPC_INTEGRATION_TESTS_DAEMON"
	switch os.Getenv(envVar) {
	case "":
		if err := os.Setenv(envVar, "rpcv2"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "rpcv2":
	default:
		fmt.Fprintf(os.Stderr, "%s=%q: this package only runs against rpcv2\n", envVar, os.Getenv(envVar))
		os.Exit(1)
	}
	os.Exit(m.Run())
}
